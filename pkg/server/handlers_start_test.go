package server

import (
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"llamactl/pkg/database"
	"llamactl/pkg/instance"
	"llamactl/pkg/manager"
	"testing"

	"pgregory.net/rapid"
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

// An on-demand start starts the instance whatever the eviction settings,
// as long as there is room. (With LRU eviction disabled it used to return
// after the capacity check without starting anything.) Servers that run
// elsewhere use no local capacity, so a full local limit never blocks them.
func TestMaybeStartLocked_StartsWheneverThereIsRoom(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		lru := rapid.Bool().Draw(rt, "lru")
		canEvict := rapid.Bool().Draw(rt, "canEvict")
		maxRunning := rapid.SampledFrom([]int{-1, 0, 1, 5}).Draw(rt, "maxRunning")

		h := newStartTestHandler(t, config.InstancesConfig{
			EnableLRUEviction:   lru,
			MaxRunningInstances: maxRunning,
			MaxInstances:        -1,
		})
		inst := createOnDemandExternal(t, h)

		if err := h.maybeStartLocked(inst, canEvict); err != nil {
			rt.Fatalf("lru=%v canEvict=%v maxRunning=%d: %v", lru, canEvict, maxRunning, err)
		}
		if !inst.IsRunning() {
			rt.Fatalf("lru=%v canEvict=%v maxRunning=%d: not started", lru, canEvict, maxRunning)
		}
	})
}
