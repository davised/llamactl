package instance

import (
	"encoding/json"
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func newDriverTestInstance(dir string, opts *Options, nodes ...string) *Instance {
	cfg := &config.AppConfig{
		Instances: config.InstancesConfig{LogsDir: dir, InstancesDir: dir},
		LocalNode: "main",
	}
	if len(nodes) > 0 {
		opts.Nodes = map[string]struct{}{}
		for _, n := range nodes {
			opts.Nodes[n] = struct{}{}
		}
	}
	if opts.BackendOptions.BackendType == "" {
		opts.BackendOptions = backends.Options{
			BackendType:       backends.BackendTypeVllm,
			VllmServerOptions: &backends.VllmServerOptions{Model: "m", Host: "gpu01", Port: 8000},
		}
	}
	return New("svc", cfg, opts, nil)
}

// The driver follows the options: a Slurm job if slurm is set (it wins over
// external), an external service if external is set, and nothing (a local
// process, or another llamactl node's instance) otherwise.
func TestNewDriver_FollowsOptions(t *testing.T) {
	dir := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		ext := rapid.Bool().Draw(t, "external")
		slm := rapid.Bool().Draw(t, "slurm")
		remote := rapid.Bool().Draw(t, "remote")

		opts := &Options{}
		if ext {
			opts.External = &ExternalOptions{}
		}
		if slm {
			opts.Slurm = &SlurmOptions{Command: "x"}
		}
		var nodes []string
		if remote {
			nodes = []string{"worker1"}
		}
		inst := newDriverTestInstance(dir, opts, nodes...)

		switch d := inst.driver.(type) {
		case *slurmJob:
			if !slm || remote {
				t.Fatalf("slurm driver for slurm=%v remote=%v", slm, remote)
			}
		case *external:
			if !ext || slm || remote {
				t.Fatalf("external driver for external=%v slurm=%v remote=%v", ext, slm, remote)
			}
		case nil:
			if (ext || slm) && !remote {
				t.Fatal("missing driver")
			}
		default:
			t.Fatalf("unexpected driver %T", d)
		}
		if inst.IsExternal() != (inst.driver != nil) {
			t.Fatal("IsExternal must match having a driver")
		}
		if inst.driver != nil && inst.process != nil {
			t.Fatal("an instance run elsewhere must not also get a local process")
		}
	})
}

// Slurm instances don't start on demand or stop when idle unless asked to;
// explicit settings are kept, and other instances keep the global defaults.
func TestOptions_SlurmDefaultsOff(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		global := &config.InstancesConfig{
			DefaultOnDemandStart: rapid.Bool().Draw(t, "globalOnDemand"),
			DefaultIdleTimeout:   rapid.IntRange(0, 120).Draw(t, "globalIdle"),
		}
		slm := rapid.Bool().Draw(t, "slurm")
		var onDemand *bool
		if rapid.Bool().Draw(t, "setOnDemand") {
			v := rapid.Bool().Draw(t, "onDemand")
			onDemand = &v
		}
		var idle *int
		if rapid.Bool().Draw(t, "setIdle") {
			v := rapid.IntRange(0, 120).Draw(t, "idle")
			idle = &v
		}

		opts := &Options{OnDemandStart: onDemand, IdleTimeout: idle}
		if slm {
			opts.Slurm = &SlurmOptions{Command: "x"}
		}
		opts.validateAndApplyDefaults("x", global)

		wantOnDemand, wantIdle := global.DefaultOnDemandStart, global.DefaultIdleTimeout
		if slm {
			wantOnDemand, wantIdle = false, 0
		}
		if onDemand != nil {
			wantOnDemand = *onDemand
		}
		if idle != nil {
			wantIdle = *idle
		}
		if *opts.OnDemandStart != wantOnDemand || *opts.IdleTimeout != wantIdle {
			t.Fatalf("got on_demand=%v idle=%d, want %v %d", *opts.OnDemandStart, *opts.IdleTimeout, wantOnDemand, wantIdle)
		}
	})
}

// Every status, including the driver-only ones, survives JSON (the API
// and the database both store statuses by name).
func TestStatus_EveryStatusRoundTrips(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.SampledFrom([]Status{Stopped, Running, Failed, Restarting, ShuttingDown, Pending, Starting}).Draw(t, "status")
		data, err := json.Marshal(s)
		var back Status
		if err != nil || json.Unmarshal(data, &back) != nil || back != s {
			t.Fatalf("%v round-tripped through %s to %v (%v)", s, data, back, err)
		}
	})
}

// fakeService is a service whose state is a file; the commands it hands
// out start, stop and check it the way systemctl would.
type fakeService struct{ dir string }

func (f fakeService) up() bool {
	_, err := os.Stat(filepath.Join(f.dir, "up"))
	return err == nil
}

func (f fakeService) set(up bool) {
	if up {
		os.WriteFile(filepath.Join(f.dir, "up"), nil, 0644)
	} else {
		os.Remove(filepath.Join(f.dir, "up"))
	}
}

func (f fakeService) cmd(script string) []string {
	return []string{"sh", "-c", strings.ReplaceAll(script, "UP", filepath.Join(f.dir, "up"))}
}

// An external service, driven through random starts, stops, outside
// crashes and restarts and status checks, always ends up where its
// commands put it: start leaves it up and running, stop leaves it down and
// stopped, and a status check reports what the status command says. With
// no start/stop command the service is never touched.
func TestExternalDriver_StateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		dir, _ := os.MkdirTemp("", "llamactl-ext-test")
		defer os.RemoveAll(dir)
		svc := fakeService{dir}

		managed := rapid.Bool().Draw(t, "managed")
		ext := &ExternalOptions{StatusCommand: svc.cmd(`test -f UP`)}
		if managed {
			ext.StartCommand = svc.cmd(`touch UP`)
			ext.StopCommand = svc.cmd(`rm -f UP`)
		}
		inst := newDriverTestInstance(dir, &Options{External: ext})
		defer inst.Stop()

		t.Repeat(map[string]func(*rapid.T){
			"start": func(t *rapid.T) {
				if inst.IsRunning() {
					t.Skip("already running")
				}
				wasUp := svc.up()
				if err := inst.Start(); err != nil {
					t.Fatal(err)
				}
				if !inst.IsRunning() || (managed && !svc.up()) || (!managed && svc.up() != wasUp) {
					t.Fatalf("after start: running=%v up=%v managed=%v", inst.IsRunning(), svc.up(), managed)
				}
			},
			"stop": func(t *rapid.T) {
				if !inst.IsRunning() {
					t.Skip("not running")
				}
				wasUp := svc.up()
				if err := inst.Stop(); err != nil {
					t.Fatal(err)
				}
				if inst.GetStatus() != Stopped || (managed && svc.up()) || (!managed && svc.up() != wasUp) {
					t.Fatalf("after stop: status=%v up=%v managed=%v", inst.GetStatus(), svc.up(), managed)
				}
			},
			"crashOrRevive": func(t *rapid.T) {
				svc.set(!svc.up())
			},
			"checkStatus": func(t *rapid.T) {
				inst.SyncExternalStatus()
				if inst.IsRunning() != svc.up() {
					t.Fatalf("status check: running=%v but service up=%v", inst.IsRunning(), svc.up())
				}
			},
		})
	})
}

// External commands get "{lines}" replaced in every argument (and nothing
// else changed), and the instance's environment variables, as a local
// process would.
func TestExternalDriver_CommandArgumentsAndEnvironment(t *testing.T) {
	dir := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		lines := rapid.IntRange(1, 100000).Draw(t, "lines")
		args := rapid.SliceOfN(rapid.SampledFrom([]string{"-n", "{lines}", "x{lines}y", "plain", "{line}"}), 1, 5).Draw(t, "args")
		mark := rapid.StringMatching(`[^\x00\n]{0,30}`).Draw(t, "mark")

		// Prints $MARK, then each argument on its own line
		inst := newDriverTestInstance(dir, &Options{
			Environment: map[string]string{"MARK": mark},
			External: &ExternalOptions{
				LogsCommand: append([]string{"sh", "-c", `printf '%s\n' "$MARK" "$@"`, "sh"}, args...),
			},
		})
		out, err := inst.GetLogs(lines)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{mark}
		for _, a := range args {
			want = append(want, strings.ReplaceAll(a, "{lines}", strconv.Itoa(lines)))
		}
		if got := strings.Split(strings.TrimSuffix(out, "\n"), "\n"); !slices.Equal(got, want) {
			t.Fatalf("command saw %q, want %q", got, want)
		}
	})
}
