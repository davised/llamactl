package instance

import (
	"context"
	"fmt"
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"llamactl/pkg/slurm"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// fakeScheduler stands in for Slurm. Jobs move only when the test says so.
type fakeScheduler struct {
	mu        sync.Mutex
	nextID    int
	queue     []*slurm.Job      // what squeue lists, in submission order
	finished  map[string]string // job ID -> final state, what sacct reports
	submitted []string          // jobs llamactl submitted
	cancelled []string          // jobs llamactl cancelled
	acctDown  bool
	lastArgs  []string
	// cancelLags: a cancelled job keeps its state until the scheduler gets
	// to it, as in a squeue listing taken just before scancel
	cancelLags bool
}

func newFakeScheduler() *fakeScheduler {
	return &fakeScheduler{nextID: 1000, finished: map[string]string{}}
}

func (f *fakeScheduler) add(name, state, node string) *slurm.Job {
	f.nextID++
	j := &slurm.Job{ID: strconv.Itoa(f.nextID), Name: name, State: state, Nodes: node}
	f.queue = append(f.queue, j)
	return j
}

func (f *fakeScheduler) Submit(_ context.Context, args []string, _ map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := ""
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, "--job-name="); ok {
			name = v
		}
	}
	j := f.add(name, slurm.StatePending, "")
	f.submitted = append(f.submitted, j.ID)
	f.lastArgs = args
	return j.ID, nil
}

func (f *fakeScheduler) Cancel(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, id)
	for _, j := range f.queue {
		if j.ID == id {
			f.finished[id] = slurm.StateCancelled
			if !f.cancelLags {
				// A cancelled job lingers while its processes are cleaned up
				j.State = slurm.StateCompleting
			}
		}
	}
	return nil
}

func (f *fakeScheduler) Accounting(_ context.Context, id string) (*slurm.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.finished[id]
	if f.acctDown || !ok {
		return nil, fmt.Errorf("sacct unavailable")
	}
	return &slurm.Job{ID: id, State: state}, nil
}

func (f *fakeScheduler) Run(_ context.Context, argv []string) (string, error) {
	return strings.Join(argv, " "), nil
}

func (f *fakeScheduler) listing() []slurm.Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]slurm.Job, len(f.queue))
	for i, j := range f.queue {
		out[i] = *j
	}
	return out
}

// newSlurmTestInstance returns an instance whose Slurm driver talks to
// sched, with every compute node resolving to a test server whose health
// endpoint answers 200 while healthy is set. Call cleanup when done.
func newSlurmTestInstance(so *SlurmOptions, sched *fakeScheduler, healthy *atomic.Bool) (inst *Instance, d *slurmJob, cleanup func()) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	dir, _ := os.MkdirTemp("", "llamactl-slurm-test")
	cleanup = func() { srv.Close(); os.RemoveAll(dir) }
	u, _ := url.Parse(srv.URL)
	port, _ := strconv.Atoi(u.Port())

	cfg := &config.AppConfig{
		Instances: config.InstancesConfig{LogsDir: dir, InstancesDir: dir},
		LocalNode: "main",
		Slurm:     config.SlurmConfig{NodeHostTemplate: "127.0.0.1"},
	}
	inst = New("qwen", cfg, &Options{
		Slurm: so,
		BackendOptions: backends.Options{
			BackendType: backends.BackendTypeVllm,
			// The host in the options is never used: requests must go to the
			// job's node (127.0.0.1 via the template), not here
			VllmServerOptions: &backends.VllmServerOptions{Model: "m", Host: "options-host.invalid", Port: port},
		},
	}, nil)
	d = inst.driver.(*slurmJob)
	d.client = sched
	return inst, d, cleanup
}

// The Slurm driver, driven through random sequences of user actions,
// scheduler decisions, hand-submitted jobs, health changes and accounting
// outages, keeps these promises after every squeue poll:
//
//   - it only ever follows a job with the instance's job name, and never one
//     it cancelled itself
//   - the instance's status says where its job is: queued -> pending,
//     running but not answering -> starting, answering -> running, no job ->
//     stopped or failed
//   - if an eligible job is in the queue, it is followed
//   - a job that ends on its own leaves the instance failed exactly when
//     sacct says it ended badly
//   - requests reach a running job's node, and logs are read from inside
//     the followed job (and refused without one)
//
// and these on every action:
//
//   - start never submits a second job while one is queued or running
//   - a squeue listing taken just before a submission can't end the new job
//   - stop cancels the followed job and leaves the instance stopped
func TestSlurmDriver_StateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		jobName := rapid.SampledFrom([]string{"", "vllm-qwen"}).Draw(t, "jobName")
		sched := newFakeScheduler()
		sched.cancelLags = rapid.Bool().Draw(t, "cancelLags")
		var healthy atomic.Bool
		inst, d, cleanup := newSlurmTestInstance(&SlurmOptions{Command: "serve", JobName: jobName, LogPath: "/logs/{name}-{jobid}.log"}, sched, &healthy)
		defer cleanup()
		name := d.jobName()

		tracked := func() *slurm.Job {
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.job == nil {
				return nil
			}
			j := *d.job
			return &j
		}
		active := func() bool {
			j := tracked()
			return j != nil && j.Active()
		}

		t.Repeat(map[string]func(*rapid.T){
			"start": func(t *rapid.T) {
				wasActive := active()
				before := len(sched.submitted)
				stale, staleAt := sched.listing(), time.Now()
				if err := inst.Start(); err != nil {
					t.Fatalf("start: %v", err)
				}
				if !wasActive && rapid.Bool().Draw(t, "staleListing") {
					// A poll that ran squeue before the submission lands now
					d.update(context.Background(), stale, staleAt)
					if j := tracked(); j == nil || j.ID != sched.submitted[before] {
						t.Fatalf("stale listing ended the new job: tracked %+v", j)
					}
				}
				switch {
				case wasActive && len(sched.submitted) != before:
					t.Fatal("submitted a second job while one was active")
				case !wasActive && len(sched.submitted) != before+1:
					t.Fatal("start did not submit a job")
				case !wasActive && inst.GetStatus() != Pending:
					t.Fatalf("status after submit = %v, want pending", inst.GetStatus())
				}
			},
			"stop": func(t *rapid.T) {
				j := tracked()
				if err := inst.Stop(); err != nil {
					t.Fatalf("stop: %v", err)
				}
				if j != nil && !slices.Contains(sched.cancelled, j.ID) {
					t.Fatalf("stop did not cancel job %s", j.ID)
				}
				if inst.GetStatus() != Stopped || tracked() != nil {
					t.Fatalf("after stop: status %v, tracked %+v", inst.GetStatus(), tracked())
				}
			},
			"submitByHand": func(t *rapid.T) {
				n := rapid.SampledFrom([]string{name, "someone-else"}).Draw(t, "name")
				state := rapid.SampledFrom([]string{slurm.StatePending, slurm.StateRunning}).Draw(t, "state")
				node := ""
				if state == slurm.StateRunning {
					node = "gpu01"
				}
				sched.mu.Lock()
				sched.add(n, state, node)
				sched.mu.Unlock()
			},
			"schedule": func(t *rapid.T) {
				sched.mu.Lock()
				defer sched.mu.Unlock()
				if len(sched.queue) == 0 {
					t.Skip("empty queue")
				}
				i := rapid.IntRange(0, len(sched.queue)-1).Draw(t, "job")
				j := sched.queue[i]
				if slices.Contains(sched.cancelled, j.ID) && j.State != slurm.StateCompleting {
					j.State = slurm.StateCompleting // the lagging cancel lands
					return
				}
				switch j.State {
				case slurm.StatePending:
					j.State = rapid.SampledFrom([]string{slurm.StateConfiguring, slurm.StateRunning}).Draw(t, "next")
					j.Nodes = rapid.SampledFrom([]string{"gpu01", "gpu[02-03]"}).Draw(t, "nodes")
				case slurm.StateConfiguring:
					j.State = slurm.StateRunning
				case slurm.StateRunning:
					sched.finished[j.ID] = rapid.SampledFrom([]string{
						slurm.StateCompleted, slurm.StateCancelled, "FAILED", "TIMEOUT", "OUT_OF_MEMORY",
					}).Draw(t, "end")
					sched.queue = slices.Delete(sched.queue, i, i+1)
				case slurm.StateCompleting:
					sched.queue = slices.Delete(sched.queue, i, i+1)
				}
			},
			"toggleHealth": func(t *rapid.T) {
				healthy.Store(!healthy.Load())
			},
			"toggleAccounting": func(t *rapid.T) {
				sched.mu.Lock()
				sched.acctDown = !sched.acctDown
				sched.mu.Unlock()
			},
			"poll": func(t *rapid.T) {
				before := tracked()
				listing := sched.listing()
				d.update(context.Background(), listing, time.Now())
				j := tracked()
				st := inst.GetStatus()

				if j != nil {
					if j.Name != name {
						t.Fatalf("following job %s named %q, want %q", j.ID, j.Name, name)
					}
					if slices.Contains(sched.cancelled, j.ID) && before == nil {
						t.Fatalf("re-adopted job %s that llamactl cancelled", j.ID)
					}
				}

				logs, logErr := inst.GetLogs(7)
				if j == nil || j.FirstNode() == "" {
					if logErr == nil {
						t.Fatal("logs must be refused without a job on a node")
					}
				} else if want := fmt.Sprintf("srun --jobid=%s --overlap -N1 -n1 tail -n 7 /logs/qwen-%s.log", j.ID, j.ID); logs != want {
					t.Fatalf("logs = %q, %v; want %q", logs, logErr, want)
				}

				switch {
				case j == nil:
					if st != Stopped && st != Failed {
						t.Fatalf("no job but status %v", st)
					}
					for _, l := range listing {
						if l.Name == name && l.Active() && !slices.Contains(sched.cancelled, l.ID) {
							t.Fatalf("eligible job %s (%s) not followed", l.ID, l.State)
						}
					}
				case j.State == slurm.StatePending:
					if st != Pending {
						t.Fatalf("queued job but status %v", st)
					}
				case j.State == slurm.StateConfiguring:
					if st != Starting {
						t.Fatalf("configuring job but status %v", st)
					}
				case j.State == slurm.StateRunning:
					if healthy.Load() && st != Running {
						t.Fatalf("running, healthy job but status %v", st)
					}
					if !healthy.Load() && st != Starting && st != Running {
						t.Fatalf("running job but status %v", st)
					}
					if inst.GetHost() != "127.0.0.1" {
						t.Fatalf("running job but host %q", inst.GetHost())
					}
					rec := httptest.NewRecorder()
					if err := inst.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil)); err != nil || rec.Code == http.StatusBadGateway {
						t.Fatalf("request didn't reach the node: %v, %d", err, rec.Code)
					}
				}

				// A followed job that left the queue: failed iff it ended badly
				if before != nil && !slices.ContainsFunc(listing, func(l slurm.Job) bool { return l.ID == before.ID }) && j == nil {
					sched.mu.Lock()
					end, known := sched.finished[before.ID]
					down := sched.acctDown
					sched.mu.Unlock()
					bad := known && !down && end != slurm.StateCompleted && end != slurm.StateCancelled
					if (st == Failed) != bad {
						t.Fatalf("job %s ended %q (sacct down=%v) but status %v", before.ID, end, down, st)
					}
				}
			},
		})
	})
}

// sbatch receives exactly the options that were set, each once, then the
// extra arguments in order, then the command; output goes nowhere unless a
// file is chosen.
func TestSlurmDriver_SbatchArgsMirrorOptions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		str := func(label string) string { return rapid.StringMatching(`([A-Za-z0-9:/._-]{1,12})?`).Draw(t, label) }
		num := func(label string) int { return rapid.IntRange(0, 8).Draw(t, label) }
		so := &SlurmOptions{
			Command: rapid.StringMatching(`[^\x00]{1,30}`).Draw(t, "command"),
			JobName: str("jobName"), Partition: str("partition"), Nodelist: str("nodelist"), Account: str("account"),
			GPUs: num("gpus"), CPUs: num("cpus"), Memory: str("memory"), TimeLimit: str("time"), Output: str("output"),
			ExtraArgs: rapid.SliceOfN(rapid.StringMatching(`--[a-z]{1,8}=[a-z0-9]{1,4}`), 0, 3).Draw(t, "extra"),
		}
		sched := newFakeScheduler()
		var healthy atomic.Bool
		inst, _, cleanup := newSlurmTestInstance(so, sched, &healthy)
		defer cleanup()
		if err := inst.Start(); err != nil {
			t.Fatal(err)
		}

		or := func(v, def string) string {
			if v == "" {
				return def
			}
			return v
		}
		count := func(n int) string {
			if n == 0 {
				return ""
			}
			return strconv.Itoa(n)
		}
		want := []string{"--job-name=" + or(so.JobName, "llamactl-qwen"), "--output=" + or(so.Output, "/dev/null")}
		for flag, v := range map[string]string{
			"--partition=": so.Partition, "--nodelist=": so.Nodelist, "--account=": so.Account,
			"--gres=gpu:": count(so.GPUs), "--cpus-per-task=": count(so.CPUs), "--mem=": so.Memory, "--time=": so.TimeLimit,
		} {
			if v != "" {
				want = append(want, flag+v)
			}
		}

		args := sched.lastArgs
		options := args[:len(args)-len(so.ExtraArgs)-1]
		if !slices.Equal(sorted(options), sorted(want)) ||
			!slices.Equal(args[len(options):len(args)-1], so.ExtraArgs) ||
			args[len(args)-1] != "--wrap="+so.Command {
			t.Fatalf("sbatch args %q, want options %q, extras %q, command %q", args, want, so.ExtraArgs, so.Command)
		}
	})
}

func sorted(s []string) []string {
	c := slices.Clone(s)
	slices.Sort(c)
	return c
}
