package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

var (
	genStateName = rapid.SampledFrom([]string{
		StatePending, StateConfiguring, StateRunning, StateCompleting,
		StateCompleted, StateCancelled, "FAILED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL",
	})
	genHostPart = rapid.StringMatching(`[a-z][a-z0-9-]{0,8}`)
)

func genJob() *rapid.Generator[Job] {
	return rapid.Custom(func(t *rapid.T) Job {
		return Job{
			ID:         strconv.FormatUint(rapid.Uint64Range(1, 1<<40).Draw(t, "id"), 10),
			Name:       rapid.StringMatching(`[A-Za-z0-9._-]{1,20}`).Draw(t, "name"),
			State:      genStateName.Draw(t, "state"),
			Reason:     rapid.SampledFrom([]string{"", "Resources", "Priority", "QOSMaxGRESPerUser"}).Draw(t, "reason"),
			Nodes:      rapid.SampledFrom([]string{"", "gpu01", "gpu[01-02]"}).Draw(t, "nodes"),
			Partition:  rapid.SampledFrom([]string{"", "gpu", "debug"}).Draw(t, "partition"),
			TimeLimit:  rapid.Int64Range(0, 100000).Draw(t, "timeLimit"),
			StartTime:  rapid.Int64Range(0, 2_000_000_000).Draw(t, "start"),
			SubmitTime: rapid.Int64Range(0, 2_000_000_000).Draw(t, "submit"),
		}
	})
}

// numberJSON encodes n the way a given Slurm release does: a plain number
// (22/23) or a {"set","infinite","number"} object (24+), where 0 means unset.
func numberJSON(n int64, modern bool) any {
	if !modern {
		return n
	}
	if n == 0 {
		return map[string]any{"set": false, "infinite": false, "number": 0}
	}
	return map[string]any{"set": true, "infinite": false, "number": n}
}

func reasonJSON(r string) string {
	if r == "" {
		return "None"
	}
	return r
}

// squeueJSON renders jobs as `squeue --json` output of the given vintage.
func squeueJSON(jobs []Job, modern bool) []byte {
	var out []map[string]any
	for _, j := range jobs {
		id, _ := strconv.ParseInt(j.ID, 10, 64)
		var state any = j.State
		if modern {
			state = []string{j.State}
		}
		out = append(out, map[string]any{
			"job_id": id, "name": j.Name, "job_state": state, "state_reason": reasonJSON(j.Reason),
			"nodes": j.Nodes, "partition": j.Partition,
			"time_limit":  numberJSON(j.TimeLimit, modern),
			"start_time":  numberJSON(j.StartTime, modern),
			"submit_time": numberJSON(j.SubmitTime, modern),
		})
	}
	data, _ := json.Marshal(map[string]any{"jobs": out, "errors": []any{}})
	return data
}

// sacctJSON renders jobs as `sacct --json` output, which nests state and
// times differently from squeue.
func sacctJSON(jobs []Job, modern bool) []byte {
	var out []map[string]any
	for _, j := range jobs {
		id, _ := strconv.ParseInt(j.ID, 10, 64)
		var current any = j.State
		if modern {
			current = []string{j.State}
		}
		out = append(out, map[string]any{
			"job_id": id, "name": j.Name, "nodes": j.Nodes, "partition": j.Partition,
			"state": map[string]any{"current": current, "reason": reasonJSON(j.Reason)},
			"time": map[string]any{
				"limit":      numberJSON(j.TimeLimit, modern),
				"start":      numberJSON(j.StartTime, modern),
				"submission": numberJSON(j.SubmitTime, modern),
			},
		})
	}
	data, _ := json.Marshal(map[string]any{"jobs": out})
	return data
}

// Every JSON shape Slurm has used for squeue and sacct reads back as the
// same jobs, so llamactl works across Slurm releases.
func TestParseJobs_AllShapesReadTheSameJobs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		jobs := rapid.SliceOfN(genJob(), 0, 5).Draw(t, "jobs")
		modern := rapid.Bool().Draw(t, "modern")
		sacct := rapid.Bool().Draw(t, "sacct")

		data := squeueJSON(jobs, modern)
		if sacct {
			data = sacctJSON(jobs, modern)
		}
		got, err := ParseJobs(data)
		if err != nil {
			t.Fatalf("ParseJobs: %v\n%s", err, data)
		}
		if len(got) != len(jobs) {
			t.Fatalf("got %d jobs, want %d", len(got), len(jobs))
		}
		for i := range jobs {
			if got[i] != jobs[i] {
				t.Fatalf("job %d:\n got  %+v\n want %+v\n%s", i, got[i], jobs[i], data)
			}
		}
	})
}

// A state can carry a suffix ("CANCELLED by 1234"); only the state counts.
func TestParseJobs_StateSuffixIsDropped(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		state := genStateName.Draw(t, "state")
		suffix := rapid.StringMatching(`( [a-z0-9 ]{1,10})?`).Draw(t, "suffix")
		data := fmt.Sprintf(`{"jobs":[{"job_id":1,"name":"x","state":{"current":%q}}]}`, state+suffix)
		got, err := ParseJobs([]byte(data))
		if err != nil || got[0].State != state {
			t.Fatalf("got %+v, %v; want state %s", got, err, state)
		}
	})
}

// Errors reported by Slurm surface instead of reading as an empty queue.
func TestParseJobs_ReportedErrorsSurface(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		msg := rapid.StringMatching(`[A-Za-z][A-Za-z ]{0,30}`).Draw(t, "msg")
		data, _ := json.Marshal(map[string]any{"jobs": []any{}, "errors": []any{map[string]string{"description": msg}}})
		if _, err := ParseJobs(data); err == nil || !strings.Contains(err.Error(), msg) {
			t.Fatalf("expected error containing %q, got %v", msg, err)
		}
	})
}

// A job is active exactly while it is queued or holds its allocation.
func TestJob_ActiveWhileQueuedOrRunning(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		state := genStateName.Draw(t, "state")
		want := state == StatePending || state == StateConfiguring || state == StateRunning
		if (Job{State: state}).Active() != want {
			t.Fatalf("Active(%s) != %v", state, want)
		}
	})
}

// The first host of a node list is what llamactl connects to, whatever
// range syntax Slurm uses to compress the list.
func TestFirstNode_IsTheFirstHostOfTheList(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		prefix := genHostPart.Draw(t, "prefix")
		suffix := rapid.StringMatching(`(-[a-z0-9]{1,4})?`).Draw(t, "suffix")
		width := rapid.IntRange(1, 4).Draw(t, "width")
		pad := func(n int) string { return fmt.Sprintf("%0*d", width, n) }

		// A bracketed list of numbers and ranges, e.g. [03,05-07]
		first := rapid.IntRange(0, 999).Draw(t, "first")
		items := []string{pad(first)}
		if rapid.Bool().Draw(t, "firstIsRange") {
			items[0] += "-" + pad(first+rapid.IntRange(1, 9).Draw(t, "span"))
		}
		for _, n := range rapid.SliceOfN(rapid.IntRange(0, 999), 0, 3).Draw(t, "more") {
			items = append(items, pad(n))
		}
		list := prefix + "[" + strings.Join(items, ",") + "]" + suffix
		for _, other := range rapid.SliceOfN(genHostPart, 0, 2).Draw(t, "others") {
			list += "," + other
		}

		if got, want := FirstNode(list), prefix+pad(first)+suffix; got != want {
			t.Fatalf("FirstNode(%q) = %q, want %q", list, got, want)
		}
	})
}

func TestFirstNode_PlainHostsAndPlaceholders(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		hosts := rapid.SliceOfN(genHostPart, 1, 4).Draw(t, "hosts")
		if got := FirstNode(strings.Join(hosts, ",")); got != hosts[0] {
			t.Fatalf("FirstNode(%v) = %q", hosts, got)
		}
		// Placeholders squeue prints for jobs without nodes mean "no node"
		empty := rapid.SampledFrom([]string{"", "(null)", "None assigned", "(Resources)"}).Draw(t, "empty")
		if got := FirstNode(empty); got != "" {
			t.Fatalf("FirstNode(%q) = %q, want empty", empty, got)
		}
	})
}

// sbatch --parsable prints "<id>" or "<id>;<cluster>", possibly after
// warnings; the job ID is always recovered, and anything else is an error.
func TestParseSubmitOutput_RecoversTheJobID(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		id := strconv.FormatUint(rapid.Uint64().Draw(t, "id"), 10)
		cluster := rapid.StringMatching(`(;[a-z0-9]{1,8})?`).Draw(t, "cluster")
		warnings := rapid.SliceOfN(rapid.StringMatching(`sbatch: warning: [a-z ]{0,20}`), 0, 2).Draw(t, "warnings")
		out := strings.Join(append(warnings, id+cluster), "\n") + "\n"

		got, err := parseSubmitOutput(out)
		if err != nil || got != id {
			t.Fatalf("parseSubmitOutput(%q) = %q, %v; want %s", out, got, err, id)
		}

		bad := rapid.StringMatching(`[A-Za-z][A-Za-z ]{0,20}`).Draw(t, "bad")
		if _, err := parseSubmitOutput(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	})
}

// recorder is a fake Slurm tool that writes its argv (NUL-separated) and
// the MARK environment variable to files, and fails when told to.
func recorder(t *testing.T) (prefix []string, dir string) {
	t.Helper()
	dir = t.TempDir()
	script := `#!/bin/sh
printf '%s\0' "$@" > "` + dir + `/argv"
printf '%s' "$MARK" > "` + dir + `/env"
if [ -f "` + dir + `/fail" ]; then cat "` + dir + `/fail" >&2; exit 3; fi
case "$1" in sbatch) echo 42 ;; esac
`
	path := filepath.Join(dir, "tool")
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return []string{path}, dir
}

// Commands run without a shell: every argument and environment value
// reaches the tool exactly as given, with the prefix in front.
func TestClient_PassesArgumentsVerbatim(t *testing.T) {
	prefix, dir := recorder(t)
	c := &Client{Prefix: prefix}

	rapid.Check(t, func(t *rapid.T) {
		arg := rapid.StringMatching(`[^\x00]{0,20}`)
		args := rapid.SliceOfN(arg, 0, 5).Draw(t, "args")
		mark := rapid.StringMatching(`[^\x00]{0,20}`).Draw(t, "mark")

		id, err := c.Submit(context.Background(), args, map[string]string{"MARK": mark})
		if err != nil || id != "42" {
			t.Fatalf("Submit = %q, %v", id, err)
		}

		raw, _ := os.ReadFile(filepath.Join(dir, "argv"))
		got := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
		want := append([]string{"sbatch", "--parsable"}, args...)
		if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
			t.Fatalf("argv:\n got  %q\n want %q", got, want)
		}
		if env, _ := os.ReadFile(filepath.Join(dir, "env")); string(env) != mark {
			t.Fatalf("env MARK = %q, want %q", env, mark)
		}
	})
}

// A failing command's stderr is part of the error, so the UI can show why
// Slurm refused.
func TestClient_FailuresCarryStderr(t *testing.T) {
	prefix, dir := recorder(t)
	c := &Client{Prefix: prefix}

	rapid.Check(t, func(t *rapid.T) {
		msg := rapid.StringMatching(`[A-Za-z][A-Za-z0-9 :]{0,40}`).Draw(t, "msg")
		if err := os.WriteFile(filepath.Join(dir, "fail"), []byte(msg), 0644); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(filepath.Join(dir, "fail"))

		err := c.Cancel(context.Background(), "1")
		if err == nil || !strings.Contains(err.Error(), strings.TrimSpace(msg)) || !bytes.Contains([]byte(err.Error()), []byte("scancel")) {
			t.Fatalf("expected scancel error with %q, got %v", msg, err)
		}
	})
}
