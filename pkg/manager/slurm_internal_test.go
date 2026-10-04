package manager

import (
	"fmt"
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
	"testing"
	"time"

	"pgregory.net/rapid"
)

// A Slurm job outlives llamactl: after a restart, an instance loaded from
// the database follows its job again, in the state squeue reports, whatever
// transitional status was stored. (How the driver follows jobs is covered
// by the state machine in pkg/instance; this is the manager's wiring.)
func TestSlurm_JobsSurviveRestart(t *testing.T) {
	// squeue on PATH prints whatever the test wrote to queue.json
	bin := t.TempDir()
	queue := filepath.Join(bin, "queue.json")
	script := "#!/bin/sh\ncat " + queue + "\n"
	if err := os.WriteFile(filepath.Join(bin, "squeue"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	port, _ := strconv.Atoi(u.Port())

	rapid.Check(t, func(rt *rapid.T) {
		state := rapid.SampledFrom([]string{"PENDING", "RUNNING"}).Draw(rt, "state")
		jobName := rapid.SampledFrom([]string{"", "vllm-qwen"}).Draw(rt, "jobName")
		stored := rapid.SampledFrom([]instance.Status{instance.Stopped, instance.Pending, instance.Starting, instance.Running}).Draw(rt, "stored")
		dbPath := filepath.Join(t.TempDir(), "llamactl.db")

		open := func() *instanceManager {
			db, err := database.Open(&database.Config{Path: dbPath, MaxOpenConnections: 1})
			if err != nil {
				rt.Fatal(err)
			}
			if err := database.RunMigrations(db); err != nil {
				rt.Fatal(err)
			}
			return New(&config.AppConfig{
				Instances: config.InstancesConfig{PortRange: [2]int{8000, 9000}, MaxInstances: -1, LogsDir: t.TempDir(), InstancesDir: t.TempDir(), TimeoutCheckInterval: 5},
				Slurm:     config.SlurmConfig{PollInterval: time.Hour, NodeHostTemplate: "127.0.0.1"},
			}, db).(*instanceManager)
		}
		setQueue := func(jobs string) {
			if err := os.WriteFile(queue, []byte(`{"jobs":[`+jobs+`]}`), 0644); err != nil {
				rt.Fatal(err)
			}
		}

		setQueue("")
		im := open()
		inst, err := im.CreateInstance("qwen", &instance.Options{
			Slurm: &instance.SlurmOptions{Command: "run", JobName: jobName},
			BackendOptions: backends.Options{
				BackendType:       backends.BackendTypeVllm,
				VllmServerOptions: &backends.VllmServerOptions{Model: "m", Port: port},
			},
		})
		if err != nil {
			rt.Fatal(err)
		}
		inst.SetStatus(stored)
		if err := im.persistInstance(inst); err != nil {
			rt.Fatal(err)
		}
		im.Shutdown()

		wantName := jobName
		if wantName == "" {
			wantName = "llamactl-qwen"
		}
		setQueue(fmt.Sprintf(`{"job_id":101,"name":%q,"job_state":[%q],"nodes":"gpu01"}`, wantName, state))
		im = open()
		defer im.Shutdown()
		im.slurm.poll()

		inst, _ = im.GetInstance("qwen")
		want := map[string]instance.Status{"PENDING": instance.Pending, "RUNNING": instance.Running}[state]
		if j := inst.SlurmJob(); j == nil || j.ID != "101" || inst.GetStatus() != want {
			rt.Fatalf("after restart (stored %v): job %+v, status %v; want job 101 %v", stored, j, inst.GetStatus(), want)
		}
	})
}
