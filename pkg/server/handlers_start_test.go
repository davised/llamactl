package server

import (
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"llamactl/pkg/database"
	"llamactl/pkg/instance"
	"llamactl/pkg/manager"
	"testing"
)

func newStartTestHandler(t *testing.T, instances config.InstancesConfig) *Handler {
	t.Helper()
	cfg := config.AppConfig{
		Instances: instances,
		LocalNode: "main",
	}
	cfg.Instances.LogsDir = t.TempDir()
	cfg.Instances.InstancesDir = t.TempDir()
	cfg.Instances.PortRange = [2]int{8000, 9000}

	db, err := database.Open(&database.Config{Path: ":memory:", MaxOpenConnections: 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	if err := database.RunMigrations(db); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}
	mgr := manager.New(&cfg, db)
	t.Cleanup(mgr.Shutdown)

	return &Handler{InstanceManager: mgr, cfg: cfg}
}

func createOnDemandExternal(t *testing.T, h *Handler) *instance.Instance {
	t.Helper()
	onDemand := true
	inst, err := h.InstanceManager.CreateInstance("ext", &instance.Options{
		OnDemandStart: &onDemand,
		External:      &instance.ExternalOptions{},
		BackendOptions: backends.Options{
			BackendType: backends.BackendTypeVllm,
			VllmServerOptions: &backends.VllmServerOptions{
				Model: "test-model",
				Host:  "gpu01",
				Port:  8000,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateInstance failed: %v", err)
	}
	return inst
}

// With LRU eviction disabled, on-demand start must still start the instance
// (it previously returned after the capacity check without starting it).
func TestMaybeStartLocked_StartsWithLRUEvictionDisabled(t *testing.T) {
	h := newStartTestHandler(t, config.InstancesConfig{
		EnableLRUEviction:   false,
		MaxRunningInstances: -1,
		MaxInstances:        -1,
	})
	inst := createOnDemandExternal(t, h)

	if err := h.maybeStartLocked(inst, true); err != nil {
		t.Fatalf("maybeStartLocked failed: %v", err)
	}
	if !inst.IsRunning() {
		t.Error("expected instance to be started")
	}
}

// External instances run on other hosts, so a full local capacity must not
// block them or trigger eviction.
func TestMaybeStartLocked_ExternalIgnoresLocalCapacity(t *testing.T) {
	for _, lru := range []bool{true, false} {
		h := newStartTestHandler(t, config.InstancesConfig{
			EnableLRUEviction:   lru,
			MaxRunningInstances: 0, // local capacity is always full
			MaxInstances:        -1,
		})
		inst := createOnDemandExternal(t, h)

		for _, canEvict := range []bool{true, false} {
			if inst.IsRunning() {
				if _, err := h.InstanceManager.StopInstance(inst.Name); err != nil {
					t.Fatalf("StopInstance failed: %v", err)
				}
			}
			if err := h.maybeStartLocked(inst, canEvict); err != nil {
				t.Errorf("lru=%v canEvict=%v: maybeStartLocked failed: %v", lru, canEvict, err)
			}
		}
	}
}
