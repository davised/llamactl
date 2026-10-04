package instance

import (
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func newExternalTestInstance(t *testing.T, host string, port int, ext *ExternalOptions) *Instance {
	t.Helper()
	cfg := &config.AppConfig{
		Instances: config.InstancesConfig{
			LogsDir:      t.TempDir(),
			InstancesDir: t.TempDir(),
		},
		LocalNode: "main",
	}
	opts := &Options{
		External: ext,
		BackendOptions: backends.Options{
			BackendType: backends.BackendTypeVllm,
			VllmServerOptions: &backends.VllmServerOptions{
				Model: "test-model",
				Host:  host,
				Port:  port,
			},
		},
	}
	return New("ext", cfg, opts, nil)
}

func TestExternal_NewUsesExternalExecutor(t *testing.T) {
	inst := newExternalTestInstance(t, "gpu01", 8000, &ExternalOptions{})

	if !inst.IsExternal() {
		t.Fatal("expected instance to be external")
	}
	if inst.external == nil {
		t.Error("expected external executor to be set")
	}
	if inst.process != nil || inst.logger != nil {
		t.Error("external instances must not have a local process or logger")
	}
	if inst.proxy == nil || inst.proxy.targetURL.String() != "http://gpu01:8000" {
		t.Errorf("expected proxy to target http://gpu01:8000, got %v", inst.proxy)
	}
}

func TestExternal_StartStopRunCommands(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "running")
	inst := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{
		StartCommand: []string{"touch", marker},
		StopCommand:  []string{"rm", marker},
	})

	if err := inst.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if inst.GetStatus() != Running {
		t.Errorf("expected Running after start, got %v", inst.GetStatus())
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("start command did not run: %v", err)
	}
	if err := inst.Start(); err == nil {
		t.Error("expected error starting an already running instance")
	}

	if err := inst.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if inst.GetStatus() != Stopped {
		t.Errorf("expected Stopped after stop, got %v", inst.GetStatus())
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("stop command did not run: %v", err)
	}
}

func TestExternal_NoCommandsOnlyTracksStatus(t *testing.T) {
	inst := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{})

	if err := inst.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !inst.IsRunning() {
		t.Error("expected instance to be running")
	}
	if err := inst.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if inst.IsRunning() {
		t.Error("expected instance to be stopped")
	}
}

func TestExternal_FailedStartLeavesInstanceStopped(t *testing.T) {
	inst := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{
		StartCommand: []string{"sh", "-c", "echo unit not found >&2; exit 5"},
	})

	err := inst.Start()
	if err == nil {
		t.Fatal("expected start to fail")
	}
	if !strings.Contains(err.Error(), "unit not found") {
		t.Errorf("expected command output in error, got: %v", err)
	}
	if inst.GetStatus() != Stopped {
		t.Errorf("expected Stopped after failed start, got %v", inst.GetStatus())
	}
}

func TestExternal_FailedStopKeepsInstanceRunning(t *testing.T) {
	inst := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{
		StopCommand: []string{"false"},
	})

	if err := inst.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := inst.Stop(); err == nil {
		t.Fatal("expected stop to fail")
	}
	if inst.GetStatus() != Running {
		t.Errorf("expected Running after failed stop, got %v", inst.GetStatus())
	}
}

func TestExternal_SyncStatus(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "running")
	inst := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{
		StatusCommand: []string{"test", "-e", marker},
	})

	if !inst.SyncExternalStatus() {
		t.Fatal("expected SyncExternalStatus to report a status command")
	}
	if inst.GetStatus() != Stopped {
		t.Errorf("expected Stopped while status command fails, got %v", inst.GetStatus())
	}

	if err := os.WriteFile(marker, nil, 0644); err != nil {
		t.Fatal(err)
	}
	inst.SyncExternalStatus()
	if inst.GetStatus() != Running {
		t.Errorf("expected Running while status command succeeds, got %v", inst.GetStatus())
	}
	if err := inst.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	noStatus := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{})
	if noStatus.SyncExternalStatus() {
		t.Error("expected false without a status command")
	}
}

func TestExternal_GetLogsSubstitutesLines(t *testing.T) {
	inst := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{
		LogsCommand: []string{"echo", "-n", "last {lines} lines"},
	})

	logs, err := inst.GetLogs(25)
	if err != nil {
		t.Fatalf("GetLogs failed: %v", err)
	}
	if logs != "last 25 lines" {
		t.Errorf("expected substituted output, got %q", logs)
	}

	noLogs := newExternalTestInstance(t, "localhost", 8000, &ExternalOptions{})
	if _, err := noLogs.GetLogs(10); err == nil {
		t.Error("expected error without a logs command")
	}
}

func TestExternal_WaitForHealthyProbesRemoteAddress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	host, portStr, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portStr)

	inst := newExternalTestInstance(t, host, port, &ExternalOptions{})
	if err := inst.WaitForHealthy(1); err == nil {
		t.Error("expected error health checking a stopped instance")
	}
	if err := inst.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := inst.WaitForHealthy(2); err != nil {
		t.Errorf("expected healthy, got: %v", err)
	}
}

func TestExternalOptions_Validate(t *testing.T) {
	if err := (&ExternalOptions{StartCommand: []string{"ssh", "gpu01"}}).Validate(); err != nil {
		t.Errorf("expected valid options, got: %v", err)
	}
	if err := (&ExternalOptions{StopCommand: []string{" ", "x"}}).Validate(); err == nil {
		t.Error("expected error for command without a program")
	}
}
