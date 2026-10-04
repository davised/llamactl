package manager

import (
	"fmt"
	"io"
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"llamactl/pkg/database"
	"llamactl/pkg/instance"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// fakeSlurm is a stand-in for the Slurm tools, injected through the
// command prefix. squeue and sacct print queue.json and sacct.json from its
// state directory; sbatch, scancel and srun record their arguments.
type fakeSlurm struct {
	t   *testing.T
	dir string
}

func newFakeSlurm(t *testing.T) *fakeSlurm {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
STATE="` + dir + `"
cmd="$1"; shift
case "$cmd" in
  sbatch)  echo "$@" > "$STATE/sbatch.args"; echo "101" ;;
  squeue)  if [ -f "$STATE/queue.json" ]; then cat "$STATE/queue.json"; else echo '{"jobs":[]}'; fi ;;
  sacct)   if [ -f "$STATE/sacct.json" ]; then cat "$STATE/sacct.json"; else echo "sacct: accounting disabled" >&2; exit 1; fi ;;
  scancel) echo "$@" >> "$STATE/scancel.args" ;;
  srun)    echo "srun $@" ;;
  *)       echo "unexpected command $cmd" >&2; exit 2 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "slurm"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return &fakeSlurm{t: t, dir: dir}
}

func (f *fakeSlurm) prefix() []string { return []string{filepath.Join(f.dir, "slurm")} }

// queue sets what squeue reports: entries of "id name STATE node"
func (f *fakeSlurm) queue(jobs ...string) {
	f.t.Helper()
	var parts []string
	for _, j := range jobs {
		fields := strings.Fields(j)
		node := ""
		if len(fields) > 3 {
			node = fields[3]
		}
		parts = append(parts, fmt.Sprintf(
			`{"job_id":%s,"name":%q,"job_state":[%q],"state_reason":"None","nodes":%q,"partition":"gpu","time_limit":{"set":true,"number":60}}`,
			fields[0], fields[1], fields[2], node))
	}
	f.write("queue.json", `{"jobs":[`+strings.Join(parts, ",")+`]}`)
}

func (f *fakeSlurm) write(name, content string) {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, name), []byte(content), 0644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fakeSlurm) read(name string) string {
	data, _ := os.ReadFile(filepath.Join(f.dir, name))
	return strings.TrimSpace(string(data))
}

// newSlurmTestManager returns a manager whose Slurm poller only runs when
// the test calls poll(), and a backend server standing in for the job.
func newSlurmTestManager(t *testing.T, fake *fakeSlurm) (*instanceManager, *httptest.Server) {
	t.Helper()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "backend:"+r.URL.Path)
	}))
	t.Cleanup(backend.Close)

	dir := t.TempDir()
	cfg := &config.AppConfig{
		Instances: config.InstancesConfig{
			PortRange:            [2]int{8000, 9000},
			MaxInstances:         10,
			MaxRunningInstances:  10,
			LogsDir:              dir,
			InstancesDir:         dir,
			TimeoutCheckInterval: 5,
			DefaultOnDemandStart: true,
			DefaultIdleTimeout:   30,
		},
		Slurm: config.SlurmConfig{
			PollInterval:  time.Hour,
			CommandPrefix: fake.prefix(),
			// Every compute node resolves to the test backend
			NodeHostTemplate: "127.0.0.1",
		},
	}
	db, err := database.Open(&database.Config{Path: ":memory:", MaxOpenConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RunMigrations(db); err != nil {
		t.Fatal(err)
	}
	im := New(cfg, db).(*instanceManager)
	t.Cleanup(im.Shutdown)
	return im, backend
}

func slurmOptions(port int, so *instance.SlurmOptions) *instance.Options {
	return &instance.Options{
		Slurm: so,
		BackendOptions: backends.Options{
			BackendType: backends.BackendTypeVllm,
			VllmServerOptions: &backends.VllmServerOptions{
				Model: "test-model",
				Port:  port,
			},
		},
	}
}

func backendPort(t *testing.T, srv *httptest.Server) int {
	u, _ := url.Parse(srv.URL)
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestSlurm_Lifecycle(t *testing.T) {
	fake := newFakeSlurm(t)
	im, backend := newSlurmTestManager(t, fake)

	inst, err := im.CreateInstance("qwen", slurmOptions(backendPort(t, backend), &instance.SlurmOptions{
		Command:   "/opt/vllm/bin/slurm-launch.sh qwen",
		Partition: "gpu",
		GPUs:      2,
		CPUs:      16,
		Memory:    "300G",
		TimeLimit: "1-00:00:00",
		ExtraArgs: []string{"--qos=long"},
		LogPath:   "/scratch/logs/{name}-latest.log",
	}))
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	if opts := inst.GetOptions(); opts.OnDemandStart == nil || *opts.OnDemandStart {
		t.Error("on-demand start should default to off for Slurm instances")
	}
	if opts := inst.GetOptions(); opts.IdleTimeout == nil || *opts.IdleTimeout != 0 {
		t.Error("idle timeout should default to off for Slurm instances")
	}

	// Submit
	if _, err := im.StartInstance("qwen"); err != nil {
		t.Fatalf("StartInstance: %v", err)
	}
	wantArgs := "--parsable --job-name=llamactl-qwen --output=/dev/null --partition=gpu --gres=gpu:2 " +
		"--cpus-per-task=16 --mem=300G --time=1-00:00:00 --qos=long --wrap=/opt/vllm/bin/slurm-launch.sh qwen"
	if got := fake.read("sbatch.args"); got != wantArgs {
		t.Errorf("sbatch args:\n got  %s\n want %s", got, wantArgs)
	}
	if inst.GetStatus() != instance.Pending {
		t.Errorf("expected pending after submit, got %v", inst.GetStatus())
	}
	if j := inst.SlurmJob(); j == nil || j.ID != "101" {
		t.Errorf("expected job 101 to be tracked, got %+v", j)
	}

	// Starting again while queued is a no-op, not a second submission
	fake.write("sbatch.args", "")
	if _, err := im.StartInstance("qwen"); err != nil {
		t.Fatalf("second StartInstance: %v", err)
	}
	if fake.read("sbatch.args") != "" {
		t.Error("a queued job must not be submitted again")
	}

	// Queued
	fake.queue("101 llamactl-qwen PENDING")
	im.slurm.poll()
	if inst.GetStatus() != instance.Pending {
		t.Errorf("expected pending, got %v", inst.GetStatus())
	}

	// Running and healthy: requests reach the node
	fake.queue("101 llamactl-qwen RUNNING gpu07")
	im.slurm.poll()
	if inst.GetStatus() != instance.Running {
		t.Fatalf("expected running, got %v", inst.GetStatus())
	}
	if inst.GetHost() != "127.0.0.1" {
		t.Errorf("expected host from node template, got %q", inst.GetHost())
	}
	rec := httptest.NewRecorder()
	if err := inst.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/models", nil)); err != nil {
		t.Fatalf("ServeHTTP: %v", err)
	}
	if rec.Body.String() != "backend:/v1/models" {
		t.Errorf("unexpected proxied response %q", rec.Body.String())
	}

	// Logs come from a step inside the job
	logs, err := im.GetInstanceLogs("qwen", 50)
	if err != nil {
		t.Fatalf("GetInstanceLogs: %v", err)
	}
	if logs != "srun --jobid=101 --overlap -N1 -n1 tail -n 50 /scratch/logs/qwen-latest.log\n" {
		t.Errorf("unexpected logs command: %q", logs)
	}

	// Deleting a running job is refused
	if err := im.DeleteInstance("qwen"); err == nil {
		t.Error("expected delete of a running Slurm instance to fail")
	}

	// Stop cancels the job; while it lingers as COMPLETING it is not
	// picked up again by name
	if _, err := im.StopInstance("qwen"); err != nil {
		t.Fatalf("StopInstance: %v", err)
	}
	if fake.read("scancel.args") != "101" {
		t.Errorf("expected scancel 101, got %q", fake.read("scancel.args"))
	}
	if inst.GetStatus() != instance.Stopped {
		t.Errorf("expected stopped, got %v", inst.GetStatus())
	}
	fake.queue("101 llamactl-qwen COMPLETING gpu07")
	im.slurm.poll()
	if inst.GetStatus() != instance.Stopped {
		t.Errorf("cancelled job must not be re-adopted, got %v", inst.GetStatus())
	}
	if _, err := im.GetInstanceLogs("qwen", 10); err == nil {
		t.Error("expected logs to be unavailable without a running job")
	}
}

func TestSlurm_AdoptsJobByName(t *testing.T) {
	fake := newFakeSlurm(t)
	im, backend := newSlurmTestManager(t, fake)

	inst, err := im.CreateInstance("qwen", slurmOptions(backendPort(t, backend), &instance.SlurmOptions{
		Command: "run",
		JobName: "vllm-qwen",
	}))
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}

	// A job submitted by hand, plus unrelated jobs
	fake.queue("200 vllm-other RUNNING gpu01", "201 vllm-qwen PENDING", "202 vllm-qwen RUNNING gpu02")
	im.slurm.poll()

	if j := inst.SlurmJob(); j == nil || j.ID != "202" {
		t.Fatalf("expected the running job 202 to be adopted, got %+v", j)
	}
	if inst.GetStatus() != instance.Running {
		t.Errorf("expected running, got %v", inst.GetStatus())
	}
}

func TestSlurm_JobEndsOnItsOwn(t *testing.T) {
	fake := newFakeSlurm(t)
	im, backend := newSlurmTestManager(t, fake)

	inst, err := im.CreateInstance("qwen", slurmOptions(backendPort(t, backend), &instance.SlurmOptions{Command: "run"}))
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	fake.queue("300 llamactl-qwen RUNNING gpu01")
	im.slurm.poll()
	if inst.GetStatus() != instance.Running {
		t.Fatalf("expected running, got %v", inst.GetStatus())
	}

	// The job hits its time limit and leaves the queue
	fake.queue()
	fake.write("sacct.json", `{"jobs":[{"job_id":300,"name":"llamactl-qwen","state":{"current":["TIMEOUT"],"reason":"None"}}]}`)
	im.slurm.poll()
	if inst.GetStatus() != instance.Failed {
		t.Errorf("expected failed after TIMEOUT, got %v", inst.GetStatus())
	}
	if j := inst.SlurmJob(); j == nil || j.State != "TIMEOUT" {
		t.Errorf("expected last job to report TIMEOUT, got %+v", j)
	}

	// Without accounting, a vanished job is just stopped
	fake.queue("301 llamactl-qwen RUNNING gpu01")
	im.slurm.poll()
	fake.queue()
	os.Remove(filepath.Join(fake.dir, "sacct.json"))
	im.slurm.poll()
	if inst.GetStatus() != instance.Stopped {
		t.Errorf("expected stopped when sacct is unavailable, got %v", inst.GetStatus())
	}
}

func TestSlurm_StartingUntilHealthy(t *testing.T) {
	fake := newFakeSlurm(t)
	im, backend := newSlurmTestManager(t, fake)

	// Nothing listens on this port, so the health check fails
	closed := httptest.NewServer(http.NotFoundHandler())
	port := backendPort(t, closed)
	closed.Close()
	_ = backend

	inst, err := im.CreateInstance("qwen", slurmOptions(port, &instance.SlurmOptions{Command: "run"}))
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	fake.queue("400 llamactl-qwen RUNNING gpu01")
	im.slurm.poll()
	if inst.GetStatus() != instance.Starting {
		t.Errorf("expected starting while the server is down, got %v", inst.GetStatus())
	}
	if err := inst.WaitForHealthy(1); err == nil {
		t.Error("expected WaitForHealthy to time out")
	}
}

func TestSlurm_Validation(t *testing.T) {
	fake := newFakeSlurm(t)
	im, _ := newSlurmTestManager(t, fake)

	tests := []struct {
		name string
		opts *instance.Options
	}{
		{"missing command", slurmOptions(8000, &instance.SlurmOptions{})},
		{"missing port", slurmOptions(0, &instance.SlurmOptions{Command: "run"})},
		{"job name with spaces", slurmOptions(8000, &instance.SlurmOptions{Command: "run", JobName: "a b"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := im.CreateInstance("s", tt.opts); err == nil {
				t.Error("expected validation error")
			}
		})
	}

	both := slurmOptions(8000, &instance.SlurmOptions{Command: "run"})
	both.External = &instance.ExternalOptions{}
	if _, err := im.CreateInstance("s", both); err == nil {
		t.Error("expected error for an instance that is both external and Slurm")
	}

	// Slurm instances don't need a host and don't reserve local ports
	if _, err := im.CreateInstance("s1", slurmOptions(8000, &instance.SlurmOptions{Command: "run"})); err != nil {
		t.Fatalf("CreateInstance s1: %v", err)
	}
	if _, err := im.CreateInstance("s2", slurmOptions(8000, &instance.SlurmOptions{Command: "run"})); err != nil {
		t.Fatalf("two Slurm instances may share a port: %v", err)
	}

	// The executor can't change after creation
	if _, err := im.UpdateInstance("s1", slurmOptions(8000, nil)); err == nil {
		t.Error("expected error switching a Slurm instance to local")
	}
	if _, err := im.UpdateInstance("s1", slurmOptions(8001, &instance.SlurmOptions{Command: "run2"})); err != nil {
		t.Errorf("expected Slurm update to succeed: %v", err)
	}
}
