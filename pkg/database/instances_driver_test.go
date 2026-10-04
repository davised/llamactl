package database

import (
	"llamactl/pkg/backends"
	"llamactl/pkg/config"
	"llamactl/pkg/instance"
	"testing"

	"pgregory.net/rapid"
)

// Every status an instance can have, including the Slurm-only ones the
// table's CHECK constraint doesn't list, can be saved and loaded back.
func TestSave_EveryStatusIsStorable(t *testing.T) {
	db := newTestDB(t)
	cfg := &config.AppConfig{Instances: config.InstancesConfig{LogsDir: t.TempDir(), InstancesDir: t.TempDir()}}

	rapid.Check(t, func(rt *rapid.T) {
		status := rapid.SampledFrom([]instance.Status{
			instance.Stopped, instance.Running, instance.Failed, instance.Restarting,
			instance.ShuttingDown, instance.Pending, instance.Starting,
		}).Draw(rt, "status")

		inst := instance.New("svc", cfg, &instance.Options{
			Slurm: &instance.SlurmOptions{Command: "run"},
			BackendOptions: backends.Options{
				BackendType:       backends.BackendTypeVllm,
				VllmServerOptions: &backends.VllmServerOptions{Model: "m", Port: 8000},
			},
		}, nil)
		inst.SetStatus(status)

		if err := db.Save(inst); err != nil {
			rt.Fatalf("saving status %v: %v", status, err)
		}
		all, err := db.LoadAll()
		if err != nil || len(all) != 1 {
			rt.Fatalf("LoadAll: %d instances, %v", len(all), err)
		}
		if all[0].GetOptions().Slurm == nil {
			rt.Fatal("slurm options lost")
		}
		if err := db.Delete("svc"); err != nil {
			rt.Fatal(err)
		}
	})
}
