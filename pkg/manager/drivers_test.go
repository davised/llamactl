package manager_test

import (
	"llamactl/pkg/backends"
	"llamactl/pkg/database"
	"llamactl/pkg/instance"
	"llamactl/pkg/manager"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// driverOptions builds vLLM options run by the given driver kind: "local",
// "external", "slurm" or "both" (an invalid mix).
func driverOptions(kind, host string, port int, command string, program string) *instance.Options {
	opts := &instance.Options{
		BackendOptions: backends.Options{
			BackendType:       backends.BackendTypeVllm,
			VllmServerOptions: &backends.VllmServerOptions{Model: "m", Host: host, Port: port},
		},
	}
	if kind == "external" || kind == "both" {
		opts.External = &instance.ExternalOptions{StartCommand: []string{program, "start"}}
	}
	if kind == "slurm" || kind == "both" {
		opts.Slurm = &instance.SlurmOptions{Command: command}
	}
	return opts
}

var genKind = rapid.SampledFrom([]string{"local", "external", "slurm", "both"})

func newDriverTestManager(t *rapid.T, dir string) manager.InstanceManager {
	cfg := createTestAppConfig(dir)
	cfg.Slurm.PollInterval = time.Hour
	cfg.Slurm.CommandPrefix = []string{"false"} // never reaches a real scheduler
	db, err := database.Open(&database.Config{Path: ":memory:", MaxOpenConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RunMigrations(db); err != nil {
		t.Fatal(err)
	}
	return manager.New(cfg, db)
}

// An instance is accepted exactly when its driver has what it needs:
// an external service needs a host, a port and named programs; a Slurm job
// needs a command and a port (its host is wherever the job lands); and an
// instance can't be both.
func TestCreateInstance_DriverRequirements(t *testing.T) {
	dir := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		kind := genKind.Draw(t, "kind")
		host := rapid.SampledFrom([]string{"", "gpu01"}).Draw(t, "host")
		port := rapid.SampledFrom([]int{0, 8100}).Draw(t, "port")
		command := rapid.SampledFrom([]string{"", "  ", "serve.sh m"}).Draw(t, "command")
		program := rapid.SampledFrom([]string{"", "ssh"}).Draw(t, "program")

		valid := true
		switch kind {
		case "both":
			valid = false
		case "external":
			valid = host != "" && port > 0 && program != ""
		case "slurm":
			valid = strings.TrimSpace(command) != "" && port > 0
		}

		mgr := newDriverTestManager(t, dir)
		defer mgr.Shutdown()
		_, err := mgr.CreateInstance("svc", driverOptions(kind, host, port, command, program))
		if (err == nil) != valid {
			t.Fatalf("kind=%s host=%q port=%d command=%q program=%q: err=%v, want valid=%v",
				kind, host, port, command, program, err, valid)
		}
	})
}

// How an instance runs is fixed when it's created: an update keeps the
// same kind or is refused.
func TestUpdateInstance_DriverKindIsFixed(t *testing.T) {
	dir := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		from := rapid.SampledFrom([]string{"local", "external", "slurm"}).Draw(t, "from")
		to := rapid.SampledFrom([]string{"local", "external", "slurm"}).Draw(t, "to")

		mgr := newDriverTestManager(t, dir)
		defer mgr.Shutdown()
		if _, err := mgr.CreateInstance("svc", driverOptions(from, "gpu01", 8100, "run", "ssh")); err != nil {
			t.Fatal(err)
		}
		_, err := mgr.UpdateInstance("svc", driverOptions(to, "gpu02", 8101, "run2", "ssh"))
		if (err == nil) != (from == to) {
			t.Fatalf("update %s -> %s: err=%v", from, to, err)
		}
	})
}

// Servers that run elsewhere use none of the local resources: they don't
// reserve ports from the local range, count toward max_instances, or
// count toward max_running_instances while running.
func TestDriverInstances_UseNoLocalResources(t *testing.T) {
	dir := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		mgr := newDriverTestManager(t, dir) // max_instances 10, max_running 10
		defer mgr.Shutdown()

		ports := rapid.SliceOfN(rapid.IntRange(8000, 8020), 11, 15).Draw(t, "ports")
		for i, port := range ports {
			kind := rapid.SampledFrom([]string{"external", "slurm"}).Draw(t, "kind")
			name := "svc" + string(rune('a'+i))
			// An external service with no commands is always on: start just marks it running
			opts := driverOptions(kind, "gpu01", port, "run", "ssh")
			if opts.External != nil {
				opts.External.StartCommand = nil
			}
			if _, err := mgr.CreateInstance(name, opts); err != nil {
				t.Fatalf("instance %d of %d (more than max_instances): %v", i+1, len(ports), err)
			}
			if kind == "external" {
				if _, err := mgr.StartInstance(name); err != nil {
					t.Fatal(err)
				}
			}
		}
		if mgr.AtMaxRunning() {
			t.Fatal("instances running elsewhere must not count toward the local running limit")
		}

		// Every port they listen on is still free locally
		for i, port := range ports {
			local := &instance.Options{BackendOptions: backends.Options{
				BackendType:        backends.BackendTypeLlamaCpp,
				LlamaServerOptions: &backends.LlamaServerOptions{Model: "/m.gguf", Port: port},
			}}
			name := "local" + string(rune('a'+i))
			if _, err := mgr.CreateInstance(name, local); err != nil {
				if strings.Contains(err.Error(), "already in use") && containsBefore(ports, i, port) {
					continue // a local instance already took it
				}
				if strings.Contains(err.Error(), "maximum number of instances") {
					return // the local limit applies to local instances only
				}
				t.Fatalf("local port %d: %v", port, err)
			}
		}
	})
}

func containsBefore(ports []int, i, port int) bool {
	for _, p := range ports[:i] {
		if p == port {
			return true
		}
	}
	return false
}
