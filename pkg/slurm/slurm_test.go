package slurm

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseJobs_SqueueModern(t *testing.T) {
	// Slurm 24+/25.x shape: job_state is a list, numbers are objects
	data := []byte(`{"jobs":[{
		"job_id": 5457511,
		"name": "vllm-qwen",
		"job_state": ["RUNNING"],
		"state_reason": "None",
		"nodes": "haughton",
		"partition": "lpi",
		"time_limit": {"set": true, "infinite": false, "number": 1440},
		"start_time": {"set": true, "infinite": false, "number": 1759500000},
		"submit_time": {"set": true, "infinite": false, "number": 1759499000}
	},{
		"job_id": 5457512,
		"name": "vllm-other",
		"job_state": ["PENDING"],
		"state_reason": "Resources",
		"nodes": "",
		"time_limit": {"set": false, "infinite": true, "number": 0}
	}],"errors":[]}`)

	jobs, err := ParseJobs(data)
	if err != nil {
		t.Fatalf("ParseJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	j := jobs[0]
	if j.ID != "5457511" || j.Name != "vllm-qwen" || j.State != StateRunning || j.Reason != "" ||
		j.Nodes != "haughton" || j.Partition != "lpi" || j.TimeLimit != 1440 ||
		j.StartTime != 1759500000 || j.SubmitTime != 1759499000 {
		t.Errorf("unexpected job: %+v", j)
	}
	if !j.Active() {
		t.Error("running job should be active")
	}
	p := jobs[1]
	if p.State != StatePending || p.Reason != "Resources" || p.TimeLimit != 0 || p.FirstNode() != "" {
		t.Errorf("unexpected pending job: %+v", p)
	}
}

func TestParseJobs_SqueueLegacy(t *testing.T) {
	// Slurm 22/23 shape: plain strings and numbers
	data := []byte(`{"jobs":[{"job_id":12,"name":"x","job_state":"RUNNING","nodes":"gpu[03-04]","time_limit":60,"start_time":100}]}`)
	jobs, err := ParseJobs(data)
	if err != nil {
		t.Fatalf("ParseJobs: %v", err)
	}
	if jobs[0].State != StateRunning || jobs[0].TimeLimit != 60 || jobs[0].FirstNode() != "gpu03" {
		t.Errorf("unexpected job: %+v", jobs[0])
	}
}

func TestParseJobs_Sacct(t *testing.T) {
	data := []byte(`{"jobs":[{
		"job_id": 77,
		"name": "vllm-qwen",
		"state": {"current": ["TIMEOUT"], "reason": "None"},
		"nodes": "haughton",
		"time": {"limit": {"set": true, "number": 30}, "start": 5, "submission": 4}
	},{
		"job_id": 78,
		"name": "y",
		"state": {"current": "CANCELLED by 1234", "reason": "None"}
	}]}`)
	jobs, err := ParseJobs(data)
	if err != nil {
		t.Fatalf("ParseJobs: %v", err)
	}
	if jobs[0].State != "TIMEOUT" || jobs[0].TimeLimit != 30 || jobs[0].StartTime != 5 || jobs[0].SubmitTime != 4 {
		t.Errorf("unexpected sacct job: %+v", jobs[0])
	}
	if jobs[1].State != StateCancelled {
		t.Errorf("expected CANCELLED, got %q", jobs[1].State)
	}
	if jobs[0].Active() {
		t.Error("timed-out job must not be active")
	}
}

func TestParseJobs_Errors(t *testing.T) {
	if _, err := ParseJobs([]byte(`not json`)); err == nil {
		t.Error("expected parse error")
	}
	_, err := ParseJobs([]byte(`{"jobs":[],"errors":[{"error":"x","description":"Invalid user"}]}`))
	if err == nil || !strings.Contains(err.Error(), "Invalid user") {
		t.Errorf("expected Slurm error to surface, got %v", err)
	}
}

func TestFirstNode(t *testing.T) {
	tests := map[string]string{
		"":                    "",
		"(null)":              "",
		"None assigned":       "",
		"haughton":            "haughton",
		"gpu01,gpu02":         "gpu01",
		"gpu[01-04]":          "gpu01",
		"gpu[03,05-06],cpu01": "gpu03",
		"gpu[07]-ib":          "gpu07-ib",
	}
	for in, want := range tests {
		if got := FirstNode(in); got != want {
			t.Errorf("FirstNode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseSubmitOutput(t *testing.T) {
	tests := map[string]string{
		"123\n":                       "123",
		"456;cluster\n":               "456",
		"sbatch: warning: foo\n789\n": "789",
	}
	for in, want := range tests {
		got, err := parseSubmitOutput(in)
		if err != nil || got != want {
			t.Errorf("parseSubmitOutput(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := parseSubmitOutput("Submitted batch job 1"); err == nil {
		t.Error("expected error for non-parsable output")
	}
}

// fakeTool writes a script that records its arguments and prints output.
func fakeTool(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestClient_PrefixEnvAndErrors(t *testing.T) {
	argsFile := filepath.Join(t.TempDir(), "args")
	tool := fakeTool(t, `echo "$@ MYVAR=$MYVAR" > `+argsFile+`
case "$1" in
  sbatch) echo "991;c1" ;;
  scancel) echo "scancel: error: Invalid job id" >&2; exit 1 ;;
esac`)
	c := &Client{Prefix: []string{tool}}

	id, err := c.Submit(context.Background(), []string{"--job-name=x", "--wrap=run"}, map[string]string{"MYVAR": "v1"})
	if err != nil || id != "991" {
		t.Fatalf("Submit = %q, %v", id, err)
	}
	got, _ := os.ReadFile(argsFile)
	if strings.TrimSpace(string(got)) != "sbatch --parsable --job-name=x --wrap=run MYVAR=v1" {
		t.Errorf("unexpected argv/env: %q", got)
	}

	err = c.Cancel(context.Background(), "991")
	if err == nil || !strings.Contains(err.Error(), "Invalid job id") {
		t.Errorf("expected scancel stderr in error, got %v", err)
	}
}
