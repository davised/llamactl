package manager_test

import (
	"llamactl/pkg/backends"
	"llamactl/pkg/database"
	"llamactl/pkg/instance"
	"llamactl/pkg/manager"
	"testing"
	"time"
)

func externalOptions(host string, port int, ext *instance.ExternalOptions) *instance.Options {
	return &instance.Options{
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
}

func TestCreateInstance_ExternalDoesNotAllocateLocalPorts(t *testing.T) {
	mgr := createTestManager(t)
	defer mgr.Shutdown()

	// Two services on different hosts can share a port, and neither
	// reserves it in the local port range
	if _, err := mgr.CreateInstance("ext-a", externalOptions("gpu01", 8000, &instance.ExternalOptions{})); err != nil {
		t.Fatalf("CreateInstance ext-a failed: %v", err)
	}
	if _, err := mgr.CreateInstance("ext-b", externalOptions("gpu02", 8000, &instance.ExternalOptions{})); err != nil {
		t.Fatalf("CreateInstance ext-b failed: %v", err)
	}

	local := &instance.Options{
		BackendOptions: backends.Options{
			BackendType: backends.BackendTypeLlamaCpp,
			LlamaServerOptions: &backends.LlamaServerOptions{
				Model: "/path/to/model.gguf",
				Port:  8000,
			},
		},
	}
	if _, err := mgr.CreateInstance("local", local); err != nil {
		t.Fatalf("local instance should get port 8000: %v", err)
	}

	if err := mgr.DeleteInstance("ext-a"); err != nil {
		t.Fatalf("DeleteInstance failed: %v", err)
	}
}

func TestCreateInstance_ExternalValidation(t *testing.T) {
	mgr := createTestManager(t)
	defer mgr.Shutdown()

	tests := []struct {
		name string
		opts *instance.Options
	}{
		{"missing port", externalOptions("gpu01", 0, &instance.ExternalOptions{})},
		{"missing host", externalOptions("", 8000, &instance.ExternalOptions{})},
		{"empty program", externalOptions("gpu01", 8000, &instance.ExternalOptions{StartCommand: []string{""}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := mgr.CreateInstance("ext", tt.opts); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestUpdateInstance_CannotSwitchExecutor(t *testing.T) {
	mgr := createTestManager(t)
	defer mgr.Shutdown()

	if _, err := mgr.CreateInstance("ext", externalOptions("gpu01", 8000, &instance.ExternalOptions{})); err != nil {
		t.Fatalf("CreateInstance failed: %v", err)
	}
	if _, err := mgr.UpdateInstance("ext", externalOptions("gpu01", 8000, nil)); err == nil {
		t.Error("expected error switching external instance to local")
	}
	if _, err := mgr.UpdateInstance("ext", externalOptions("gpu01", 8001, &instance.ExternalOptions{})); err != nil {
		t.Errorf("expected external update to succeed: %v", err)
	}
}

func TestAtMaxRunning_IgnoresExternalInstances(t *testing.T) {
	tempDir := t.TempDir()
	appConfig := createTestAppConfig(tempDir)
	appConfig.Instances.MaxRunningInstances = 1
	db, err := database.Open(&database.Config{Path: ":memory:", MaxOpenConnections: 1})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	if err := database.RunMigrations(db); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}
	mgr := manager.New(appConfig, db)
	defer mgr.Shutdown()

	if _, err := mgr.CreateInstance("ext", externalOptions("gpu01", 8000, &instance.ExternalOptions{})); err != nil {
		t.Fatalf("CreateInstance failed: %v", err)
	}
	if _, err := mgr.StartInstance("ext"); err != nil {
		t.Fatalf("StartInstance failed: %v", err)
	}
	if mgr.AtMaxRunning() {
		t.Error("external instances must not count against the local running limit")
	}
	if _, err := mgr.StopInstance("ext"); err != nil {
		t.Fatalf("StopInstance failed: %v", err)
	}
}

func TestManager_SyncsExternalStatusOnLoad(t *testing.T) {
	tempDir := t.TempDir()
	appConfig := createTestAppConfig(tempDir)
	appConfig.Database.Path = tempDir + "/test.db"

	open := func() manager.InstanceManager {
		db, err := database.Open(&database.Config{Path: appConfig.Database.Path, MaxOpenConnections: 1})
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		if err := database.RunMigrations(db); err != nil {
			t.Fatalf("Failed to run migrations: %v", err)
		}
		return manager.New(appConfig, db)
	}

	// Persisted as stopped, but the status command says the service is up
	mgr1 := open()
	if _, err := mgr1.CreateInstance("ext", externalOptions("gpu01", 8000, &instance.ExternalOptions{
		StatusCommand: []string{"true"},
	})); err != nil {
		t.Fatalf("CreateInstance failed: %v", err)
	}
	mgr1.Shutdown()

	mgr2 := open()
	defer mgr2.Shutdown()

	deadline := time.Now().Add(5 * time.Second)
	for {
		inst, err := mgr2.GetInstance("ext")
		if err != nil {
			t.Fatalf("GetInstance failed: %v", err)
		}
		if inst.IsRunning() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected external instance to be marked running from its status command")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
