package slurm

import (
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

var genHostPart = rapid.StringMatching(`[a-z][a-z0-9-]{0,8}`)

var genJob = rapid.Custom(func(t *rapid.T) Job {
	return Job{
		ID:         strconv.FormatUint(rapid.Uint64Range(1, 1<<40).Draw(t, "id"), 10),
		Name:       rapid.StringMatching(`[A-Za-z0-9._-]{1,20}`).Draw(t, "name"),
		State:      rapid.SampledFrom([]string{StatePending, StateRunning, StateCompleted, StateCancelled, "TIMEOUT"}).Draw(t, "state"),
		Reason:     rapid.SampledFrom([]string{"", "Resources"}).Draw(t, "reason"),
		Nodes:      rapid.SampledFrom([]string{"", "gpu01", "gpu[01-02]"}).Draw(t, "nodes"),
		Partition:  rapid.SampledFrom([]string{"", "gpu"}).Draw(t, "partition"),
		TimeLimit:  rapid.Int64Range(0, 100000).Draw(t, "timeLimit"),
		StartTime:  rapid.Int64Range(0, 2_000_000_000).Draw(t, "start"),
		SubmitTime: rapid.Int64Range(0, 2_000_000_000).Draw(t, "submit"),
	}
})

// encodeJobs renders jobs the way a given Slurm release prints them:
// squeue or sacct layout, plain values (22/23) or lists and
// {"set","infinite","number"} objects (24+, where 0 means unset), and
// states with a suffix such as "CANCELLED by 1234".
func encodeJobs(jobs []Job, sacct, modern bool, suffix string) []byte {
	num := func(n int64) any {
		if !modern {
			return n
		}
		return map[string]any{"set": n != 0, "infinite": false, "number": n}
	}
	state := func(s string) any {
		if modern {
			return []string{s + suffix}
		}
		return s + suffix
	}
	reason := func(r string) string {
		if r == "" {
			return "None"
		}
		return r
	}
	out := []map[string]any{}
	for _, j := range jobs {
		id, _ := strconv.ParseInt(j.ID, 10, 64)
		m := map[string]any{"job_id": id, "name": j.Name, "nodes": j.Nodes, "partition": j.Partition}
		if sacct {
			m["state"] = map[string]any{"current": state(j.State), "reason": reason(j.Reason)}
			m["time"] = map[string]any{"limit": num(j.TimeLimit), "start": num(j.StartTime), "submission": num(j.SubmitTime)}
		} else {
			m["job_state"], m["state_reason"] = state(j.State), reason(j.Reason)
			m["time_limit"], m["start_time"], m["submit_time"] = num(j.TimeLimit), num(j.StartTime), num(j.SubmitTime)
		}
		out = append(out, m)
	}
	data, _ := json.Marshal(map[string]any{"jobs": out})
	return data
}

// Every layout Slurm has used for squeue and sacct reads back as the same
// jobs, so llamactl works across Slurm releases.
func TestParseJobs_EveryLayoutReadsTheSameJobs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		jobs := rapid.SliceOfN(genJob, 0, 5).Draw(t, "jobs")
		suffix := rapid.SampledFrom([]string{"", " by 1234"}).Draw(t, "suffix")
		data := encodeJobs(jobs, rapid.Bool().Draw(t, "sacct"), rapid.Bool().Draw(t, "modern"), suffix)

		got, err := ParseJobs(data)
		if err != nil || len(got) != len(jobs) {
			t.Fatalf("ParseJobs: %d jobs, %v\n%s", len(got), err, data)
		}
		for i := range jobs {
			if got[i] != jobs[i] {
				t.Fatalf("job %d:\n got  %+v\n want %+v\n%s", i, got[i], jobs[i], data)
			}
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

// The first host of a node list is what llamactl connects to, whatever
// range syntax compresses the list; placeholders for "no node" read as "".
func TestFirstNode_IsTheFirstHostOfTheList(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		prefix := genHostPart.Draw(t, "prefix")
		suffix := rapid.StringMatching(`(-[a-z0-9]{1,4})?`).Draw(t, "suffix")
		width := rapid.IntRange(1, 4).Draw(t, "width")
		first := rapid.IntRange(0, 999).Draw(t, "first")
		pad := func(n int) string { return fmt.Sprintf("%0*d", width, n) }

		var entry, want string
		if rapid.Bool().Draw(t, "bracketed") {
			// prefix[03-05,07]suffix
			items := []string{pad(first)}
			if rapid.Bool().Draw(t, "range") {
				items[0] += "-" + pad(first+1+rapid.IntRange(0, 9).Draw(t, "span"))
			}
			for _, n := range rapid.SliceOfN(rapid.IntRange(0, 999), 0, 3).Draw(t, "more") {
				items = append(items, pad(n))
			}
			entry, want = prefix+"["+strings.Join(items, ",")+"]"+suffix, prefix+pad(first)+suffix
		} else {
			entry, want = prefix+suffix, prefix+suffix
		}
		list := strings.Join(append([]string{entry}, rapid.SliceOfN(genHostPart, 0, 2).Draw(t, "others")...), ",")
		if got := FirstNode(list); got != want {
			t.Fatalf("FirstNode(%q) = %q, want %q", list, got, want)
		}

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
		if got, err := parseSubmitOutput(out); err != nil || got != id {
			t.Fatalf("parseSubmitOutput(%q) = %q, %v; want %s", out, got, err, id)
		}
		if bad := rapid.StringMatching(`[A-Za-z][A-Za-z ]{0,20}`).Draw(t, "bad"); func() bool {
			_, err := parseSubmitOutput(bad)
			return err == nil
		}() {
			t.Fatalf("expected error for %q", bad)
		}
	})
}

// Commands run without a shell: arguments and environment values reach the
// tool exactly as given, after the prefix, and a failure carries the tool's
// stderr so the UI can show why Slurm refused.
func TestClient_RunsCommandsVerbatim(t *testing.T) {
	dir := t.TempDir()
	tool := filepath.Join(dir, "tool")
	script := `#!/bin/sh
printf '%s\0' "$@" > "` + dir + `/argv"
printf '%s' "$MARK" > "` + dir + `/env"
if [ -n "$FAIL" ]; then printf '%s' "$FAIL" >&2; exit 3; fi
echo 42
`
	if err := os.WriteFile(tool, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	c := &Client{Prefix: []string{tool}}

	rapid.Check(t, func(t *rapid.T) {
		args := rapid.SliceOfN(rapid.StringMatching(`[^\x00]{0,20}`), 0, 5).Draw(t, "args")
		mark := rapid.StringMatching(`[^\x00]{0,20}`).Draw(t, "mark")
		fail := rapid.StringMatching(`([A-Za-z][A-Za-z0-9 :]{0,30})?`).Draw(t, "fail")

		id, err := c.Submit(context.Background(), args, map[string]string{"MARK": mark, "FAIL": fail})
		if fail != "" {
			if err == nil || !strings.Contains(err.Error(), strings.TrimSpace(fail)) {
				t.Fatalf("expected error carrying %q, got %v", fail, err)
			}
			return
		}
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
