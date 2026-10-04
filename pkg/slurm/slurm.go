// Package slurm runs the Slurm command-line tools (sbatch, squeue, sacct,
// scancel, srun) and parses their output. It talks to Slurm only through
// those commands, so it works wherever they are on PATH (or reachable
// through a command prefix such as ssh to a submit host) and needs no
// slurmrestd.
package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// DefaultTimeout bounds every Slurm command so a hung controller can't wedge
// a caller.
const DefaultTimeout = 60 * time.Second

// Job states reported by squeue and sacct (the subset llamactl acts on).
const (
	StatePending     = "PENDING"
	StateConfiguring = "CONFIGURING"
	StateRunning     = "RUNNING"
	StateCompleting  = "COMPLETING"
	StateCompleted   = "COMPLETED"
	StateCancelled   = "CANCELLED"
)

// Job is the part of a squeue/sacct job record llamactl uses.
type Job struct {
	ID        string `json:"job_id"`
	Name      string `json:"name"`
	State     string `json:"state"`
	Reason    string `json:"reason,omitempty"`
	Nodes     string `json:"nodes,omitempty"`
	Partition string `json:"partition,omitempty"`
	// TimeLimit is the job's wall time limit in minutes (0 if unlimited or unknown)
	TimeLimit int64 `json:"time_limit,omitempty"`
	// StartTime and SubmitTime are Unix timestamps (0 if unknown)
	StartTime  int64 `json:"start_time,omitempty"`
	SubmitTime int64 `json:"submit_time,omitempty"`
}

// Active reports whether the job is queued or running, i.e. it still holds
// or is waiting for an allocation.
func (j Job) Active() bool {
	switch j.State {
	case StatePending, StateConfiguring, StateRunning:
		return true
	}
	return false
}

// FirstNode returns the first host in the job's node list, expanding the
// common "prefix[01-04,07]" range form.
func (j Job) FirstNode() string {
	return FirstNode(j.Nodes)
}

// Client runs Slurm commands.
type Client struct {
	// Prefix is prepended to every command, e.g. ["ssh", "-o", "BatchMode=yes", "submit01"]
	// when llamactl doesn't run on a submit host.
	Prefix []string
	// Timeout bounds each command (DefaultTimeout if zero).
	Timeout time.Duration
}

// Submit runs `sbatch --parsable <args>` with extra environment variables and
// returns the job ID.
func (c *Client) Submit(ctx context.Context, args []string, env map[string]string) (string, error) {
	out, err := c.run(ctx, append([]string{"sbatch", "--parsable"}, args...), env)
	if err != nil {
		return "", err
	}
	return parseSubmitOutput(out)
}

// Cancel runs `scancel <jobID>`.
func (c *Client) Cancel(ctx context.Context, jobID string) error {
	_, err := c.run(ctx, []string{"scancel", jobID}, nil)
	return err
}

// Queue returns the current user's jobs from `squeue --me --json`.
func (c *Client) Queue(ctx context.Context) ([]Job, error) {
	out, err := c.run(ctx, []string{"squeue", "--me", "--json"}, nil)
	if err != nil {
		return nil, err
	}
	return ParseJobs([]byte(out))
}

// Accounting returns a finished job's record from `sacct -j <id> --json`.
func (c *Client) Accounting(ctx context.Context, jobID string) (*Job, error) {
	out, err := c.run(ctx, []string{"sacct", "-X", "-j", jobID, "--json"}, nil)
	if err != nil {
		return nil, err
	}
	jobs, err := ParseJobs([]byte(out))
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		if j.ID == jobID {
			return &j, nil
		}
	}
	return nil, fmt.Errorf("job %s not found in accounting", jobID)
}

// Run runs an arbitrary command (with the client's prefix) and returns its
// combined output. Used for log commands such as `srun --overlap ... tail`.
func (c *Client) Run(ctx context.Context, argv []string) (string, error) {
	return c.run(ctx, argv, nil)
}

func (c *Client) run(ctx context.Context, argv []string, env map[string]string) (string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	full := append(append([]string{}, c.Prefix...), argv...)
	cmd := exec.CommandContext(ctx, full[0], full[1:]...)
	if len(env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(stdout.String())
		}
		if len(msg) > 500 {
			msg = msg[len(msg)-500:]
		}
		if msg != "" {
			return stdout.String(), fmt.Errorf("%s: %w: %s", argv[0], err, msg)
		}
		return stdout.String(), fmt.Errorf("%s: %w", argv[0], err)
	}
	return stdout.String(), nil
}

// parseSubmitOutput extracts the job ID from `sbatch --parsable` output,
// which is "<jobid>" or "<jobid>;<cluster>".
func parseSubmitOutput(out string) (string, error) {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	last := strings.TrimSpace(lines[len(lines)-1])
	id, _, _ := strings.Cut(last, ";")
	if _, err := strconv.ParseUint(id, 10, 64); err != nil {
		return "", fmt.Errorf("unexpected sbatch output %q", strings.TrimSpace(out))
	}
	return id, nil
}

// ParseJobs parses `squeue --json` or `sacct --json` output. Field shapes
// changed across Slurm releases (job_state became a list, numbers became
// {"set","infinite","number"} objects, sacct moved state under "state"), so
// every variant is accepted.
func ParseJobs(data []byte) ([]Job, error) {
	var doc struct {
		Jobs []struct {
			JobID      json.RawMessage `json:"job_id"`
			Name       string          `json:"name"`
			JobState   json.RawMessage `json:"job_state"`
			State      json.RawMessage `json:"state"`
			Reason     string          `json:"state_reason"`
			Nodes      string          `json:"nodes"`
			Partition  string          `json:"partition"`
			TimeLimit  json.RawMessage `json:"time_limit"`
			StartTime  json.RawMessage `json:"start_time"`
			SubmitTime json.RawMessage `json:"submit_time"`
			Time       *struct {
				Limit      json.RawMessage `json:"limit"`
				Start      json.RawMessage `json:"start"`
				Submission json.RawMessage `json:"submission"`
			} `json:"time"`
		} `json:"jobs"`
		Errors []struct {
			Error       string `json:"error"`
			Description string `json:"description"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("failed to parse Slurm JSON: %w", err)
	}
	if len(doc.Errors) > 0 {
		e := doc.Errors[0]
		msg := e.Description
		if msg == "" {
			msg = e.Error
		}
		return nil, fmt.Errorf("slurm reported an error: %s", msg)
	}

	jobs := make([]Job, 0, len(doc.Jobs))
	for _, raw := range doc.Jobs {
		j := Job{
			ID:        strconv.FormatInt(number(raw.JobID), 10),
			Name:      raw.Name,
			Nodes:     raw.Nodes,
			Partition: raw.Partition,
			Reason:    raw.Reason,
		}

		// squeue: "job_state"; sacct: "state": {"current": ..., "reason": ...}
		if len(raw.JobState) > 0 {
			j.State = firstState(raw.JobState)
		} else if len(raw.State) > 0 {
			var st struct {
				Current json.RawMessage `json:"current"`
				Reason  string          `json:"reason"`
			}
			if err := json.Unmarshal(raw.State, &st); err == nil && len(st.Current) > 0 {
				j.State = firstState(st.Current)
				if j.Reason == "" {
					j.Reason = st.Reason
				}
			} else {
				j.State = firstState(raw.State)
			}
		}
		if j.Reason == "None" {
			j.Reason = ""
		}

		j.TimeLimit = number(raw.TimeLimit)
		j.StartTime = number(raw.StartTime)
		j.SubmitTime = number(raw.SubmitTime)
		if raw.Time != nil {
			if j.TimeLimit == 0 {
				j.TimeLimit = number(raw.Time.Limit)
			}
			if j.StartTime == 0 {
				j.StartTime = number(raw.Time.Start)
			}
			if j.SubmitTime == 0 {
				j.SubmitTime = number(raw.Time.Submission)
			}
		}

		jobs = append(jobs, j)
	}
	return jobs, nil
}

// firstState reads a job state given as "RUNNING" or ["RUNNING", ...] and
// strips suffixes such as "CANCELLED by 1234".
func firstState(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		var list []string
		if err := json.Unmarshal(raw, &list); err != nil || len(list) == 0 {
			return ""
		}
		s = list[0]
	}
	s, _, _ = strings.Cut(strings.TrimSpace(s), " ")
	return s
}

// number reads 42, "42" or {"set": true, "infinite": false, "number": 42}.
// Unset or infinite values read as 0.
func number(raw json.RawMessage) int64 {
	if len(raw) == 0 {
		return 0
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		n, _ = strconv.ParseInt(s, 10, 64)
		return n
	}
	var obj struct {
		Set      *bool `json:"set"`
		Infinite bool  `json:"infinite"`
		Number   int64 `json:"number"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		if (obj.Set != nil && !*obj.Set) || obj.Infinite {
			return 0
		}
		return obj.Number
	}
	return 0
}

// FirstNode returns the first host in a Slurm node list such as
// "gpu01", "gpu[01-04]", "gpu[03,05-06],cpu01" or "(null)".
func FirstNode(list string) string {
	list = strings.TrimSpace(list)
	if list == "" || list == "None assigned" || strings.HasPrefix(list, "(") {
		return ""
	}

	// Take the first comma-separated entry, treating commas inside
	// brackets as part of the entry
	depth := 0
	end := len(list)
	for i, r := range list {
		if r == '[' {
			depth++
		} else if r == ']' {
			depth--
		} else if r == ',' && depth == 0 {
			end = i
			break
		}
	}
	entry := list[:end]

	open := strings.IndexByte(entry, '[')
	if open < 0 {
		return entry
	}
	inner, suffix, _ := strings.Cut(entry[open+1:], "]")
	first, _, _ := strings.Cut(inner, ",")
	first, _, _ = strings.Cut(first, "-")
	return entry[:open] + first + suffix
}
