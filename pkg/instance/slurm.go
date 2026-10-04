package instance

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"llamactl/pkg/config"
	"llamactl/pkg/slurm"
)

// defaultSlurmLogsCommand reads the job's log from inside its own allocation,
// so it works on any compute node without ssh access.
var defaultSlurmLogsCommand = []string{
	"srun", "--jobid={jobid}", "--overlap", "-N1", "-n1",
	"tail", "-n", "{lines}", "{log_path}",
}

// SlurmOptions describes a backend server that runs as a Slurm batch job.
// llamactl submits the job, follows it through the queue, proxies to the
// node it lands on and cancels it on stop. The job runs Command; llamactl
// does not build the server's command line, so any launcher script works.
type SlurmOptions struct {
	// Command is the shell command the job runs (sbatch --wrap). It must
	// start the server on the port set in backend options.
	Command string `json:"command"`
	// JobName defaults to "llamactl-<instance name>". Running jobs with this
	// name are picked up automatically, including ones submitted by hand.
	JobName string `json:"job_name,omitempty"`

	Partition string `json:"partition,omitempty"`
	Nodelist  string `json:"nodelist,omitempty"`
	Account   string `json:"account,omitempty"`
	// GPUs requests --gres=gpu:<n>; use ExtraArgs for typed GPUs or --gpus
	GPUs int `json:"gpus,omitempty"`
	// CPUs requests --cpus-per-task
	CPUs int `json:"cpus,omitempty"`
	// Memory requests --mem, e.g. "300G"
	Memory string `json:"memory,omitempty"`
	// TimeLimit requests --time, e.g. "1-00:00:00"
	TimeLimit string `json:"time_limit,omitempty"`
	// Output is the job's stdout/stderr file (--output). Defaults to
	// /dev/null so nothing lands in the submit directory; the server should
	// write its own log to node-local storage (see LogPath).
	Output string `json:"output,omitempty"`
	// ExtraArgs are passed to sbatch unchanged, e.g. ["--qos=long"]
	ExtraArgs []string `json:"extra_args,omitempty"`

	// LogPath is where the server writes its log on the compute node.
	// "{name}", "{jobid}" and "{node}" are substituted.
	LogPath string `json:"log_path,omitempty"`
	// LogsCommand prints recent log lines. "{jobid}", "{node}", "{lines}",
	// "{log_path}" and "{name}" are substituted. Defaults to
	// srun --jobid={jobid} --overlap -N1 -n1 tail -n {lines} {log_path}.
	LogsCommand []string `json:"logs_command,omitempty"`
}

// Validate checks the options that can be checked without Slurm.
func (o *SlurmOptions) Validate() error {
	if strings.TrimSpace(o.Command) == "" {
		return fmt.Errorf("slurm command is required")
	}
	if o.GPUs < 0 || o.CPUs < 0 {
		return fmt.Errorf("slurm gpus and cpus cannot be negative")
	}
	if len(o.LogsCommand) > 0 && strings.TrimSpace(o.LogsCommand[0]) == "" {
		return fmt.Errorf("slurm logs_command must start with a program name")
	}
	if o.JobName != "" && strings.ContainsAny(o.JobName, " \t\n") {
		return fmt.Errorf("slurm job_name cannot contain whitespace")
	}
	return nil
}

// slurmClient is the part of *slurm.Client the job uses (an interface so
// tests can stand in for the scheduler).
type slurmClient interface {
	Submit(ctx context.Context, args []string, env map[string]string) (string, error)
	Cancel(ctx context.Context, jobID string) error
	Accounting(ctx context.Context, jobID string) (*slurm.Job, error)
	Run(ctx context.Context, argv []string) (string, error)
}

// slurmJob runs an instance as a Slurm batch job.
type slurmJob struct {
	instance *Instance
	cfg      config.SlurmConfig
	client   slurmClient

	mu sync.Mutex
	// job is the tracked job while it is queued or running
	job *slurm.Job
	// last is the most recent job record, kept after the job ends so the
	// UI can show how it finished
	last *slurm.Job
	// submittedAt is when llamactl submitted the tracked job
	submittedAt time.Time
	// node is the compute node the server runs on ("" until the job starts)
	node string
	// ignored holds jobs llamactl cancelled. They can linger in squeue as
	// COMPLETING and must not be picked up again by job name.
	ignored map[string]struct{}
}

func newSlurmJob(instance *Instance, cfg config.SlurmConfig) *slurmJob {
	return &slurmJob{
		instance: instance,
		cfg:      cfg,
		client:   &slurm.Client{Prefix: cfg.CommandPrefix},
		ignored:  map[string]struct{}{},
	}
}

func (s *slurmJob) opts() *SlurmOptions {
	opts := s.instance.GetOptions()
	if opts == nil || opts.Slurm == nil {
		return &SlurmOptions{}
	}
	return opts.Slurm
}

func (s *slurmJob) jobName() string {
	if name := s.opts().JobName; name != "" {
		return name
	}
	return "llamactl-" + s.instance.Name
}

// sbatchArgs builds the sbatch arguments (after --parsable) for the job.
func (s *slurmJob) sbatchArgs() []string {
	o := s.opts()
	output := o.Output
	if output == "" {
		output = "/dev/null"
	}

	args := []string{"--job-name=" + s.jobName(), "--output=" + output}
	if o.Partition != "" {
		args = append(args, "--partition="+o.Partition)
	}
	if o.Nodelist != "" {
		args = append(args, "--nodelist="+o.Nodelist)
	}
	if o.Account != "" {
		args = append(args, "--account="+o.Account)
	}
	if o.GPUs > 0 {
		args = append(args, "--gres=gpu:"+strconv.Itoa(o.GPUs))
	}
	if o.CPUs > 0 {
		args = append(args, "--cpus-per-task="+strconv.Itoa(o.CPUs))
	}
	if o.Memory != "" {
		args = append(args, "--mem="+o.Memory)
	}
	if o.TimeLimit != "" {
		args = append(args, "--time="+o.TimeLimit)
	}
	args = append(args, o.ExtraArgs...)
	return append(args, "--wrap="+o.Command)
}

// host returns the address of the node running the server, or "" while
// the job has no node.
func (s *slurmJob) host() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hostLocked()
}

func (s *slurmJob) hostLocked() string {
	if s.node == "" {
		return ""
	}
	tmpl := s.cfg.NodeHostTemplate
	if tmpl == "" {
		tmpl = "{node}"
	}
	return strings.ReplaceAll(tmpl, "{node}", s.node)
}

// currentJob returns a copy of the tracked job, or the last one if none is
// tracked.
func (s *slurmJob) currentJob() *slurm.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.job
	if j == nil {
		j = s.last
	}
	if j == nil {
		return nil
	}
	c := *j
	return &c
}

func (s *slurmJob) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Already queued or running: nothing to do (on-demand start calls this
	// for a job that is still waiting in the queue)
	if s.job != nil && s.job.Active() {
		return nil
	}

	opts := s.instance.GetOptions()
	var env map[string]string
	if opts != nil {
		env = opts.Environment
	}

	id, err := s.client.Submit(context.Background(), s.sbatchArgs(), env)
	if err != nil {
		return fmt.Errorf("failed to submit job for instance %s: %w", s.instance.Name, err)
	}
	log.Printf("Instance %s: submitted Slurm job %s", s.instance.Name, id)

	s.submittedAt = time.Now()
	s.job = &slurm.Job{ID: id, Name: s.jobName(), State: slurm.StatePending, SubmitTime: s.submittedAt.Unix()}
	s.last = nil
	s.setNodeLocked("")
	if s.instance.proxy != nil {
		s.instance.proxy.updateLastRequestTime()
	}
	s.instance.SetStatus(Pending)
	return nil
}

func (s *slurmJob) stop() error {
	// Give in-flight requests up to 30 seconds before cancelling, without
	// holding the lock so status updates continue meanwhile
	if s.instance.IsRunning() {
		s.instance.SetStatus(ShuttingDown)
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && s.instance.GetInflightRequests() > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.job == nil {
		s.instance.SetStatus(Stopped)
		return nil
	}

	if err := s.client.Cancel(context.Background(), s.job.ID); err != nil {
		// The job may still be alive; the next poll restores its status
		return fmt.Errorf("failed to cancel job %s for instance %s: %w", s.job.ID, s.instance.Name, err)
	}
	log.Printf("Instance %s: cancelled Slurm job %s", s.instance.Name, s.job.ID)

	s.ignored[s.job.ID] = struct{}{}
	cancelled := *s.job
	cancelled.State = slurm.StateCancelled
	s.last = &cancelled
	s.job = nil
	s.setNodeLocked("")
	s.instance.SetStatus(Stopped)
	return nil
}

func (s *slurmJob) restart() error {
	if err := s.stop(); err != nil {
		return fmt.Errorf("failed to stop instance during restart: %w", err)
	}
	return s.start()
}

// waitForHealthy waits for the job to start and its server to answer. A job
// can sit in the queue for a long time, so a timeout while still pending
// says so instead of reporting an unhealthy server.
func (s *slurmJob) waitForHealthy(timeout int) error {
	if timeout <= 0 {
		timeout = 30
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)

	for {
		switch st := s.instance.GetStatus(); st {
		case Running:
			remaining := int(time.Until(deadline).Seconds())
			if remaining < 1 {
				remaining = 1
			}
			return pollHealthy(s.instance, remaining)
		case Pending, Starting:
			if time.Now().After(deadline) {
				if j := s.currentJob(); j != nil && st == Pending {
					reason := ""
					if j.Reason != "" {
						reason = " (" + j.Reason + ")"
					}
					return fmt.Errorf("slurm job %s for instance %s is still queued%s", j.ID, s.instance.Name, reason)
				}
				return fmt.Errorf("instance %s did not become healthy within %d seconds", s.instance.Name, timeout)
			}
			time.Sleep(time.Second)
		default:
			if j := s.currentJob(); j != nil && j.State != "" {
				return fmt.Errorf("instance %s is not running (slurm job %s %s)", s.instance.Name, j.ID, strings.ToLower(j.State))
			}
			return fmt.Errorf("instance %s is not running", s.instance.Name)
		}
	}
}

// update sets the instance status from a squeue listing of the user's jobs
// taken at queriedAt. It follows the tracked job, picks up an active job
// with the instance's job name when none is tracked, and asks sacct how a
// job ended once it leaves the queue.
func (s *slurmJob) update(ctx context.Context, jobs []slurm.Job, queriedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byID := make(map[string]slurm.Job, len(jobs))
	for _, j := range jobs {
		byID[j.ID] = j
	}

	// Forget cancelled jobs once they have left the queue
	for id := range s.ignored {
		if _, ok := byID[id]; !ok {
			delete(s.ignored, id)
		}
	}

	if s.job != nil {
		if j, ok := byID[s.job.ID]; ok {
			s.job = &j
		} else if s.submittedAt.After(queriedAt) {
			// Submitted after this listing was taken; wait for the next one
			return
		} else {
			s.finishLocked(ctx)
		}
	}

	if s.job == nil {
		if j := s.findByNameLocked(jobs); j != nil {
			log.Printf("Instance %s: tracking Slurm job %s (%s)", s.instance.Name, j.ID, strings.ToLower(j.State))
			s.job = j
			s.submittedAt = time.Time{}
			s.last = nil
		}
	}

	if s.job == nil {
		s.setNodeLocked("")
		if st := s.instance.GetStatus(); st != Stopped && st != Failed {
			s.instance.SetStatus(Stopped)
		}
		return
	}

	switch s.job.State {
	case slurm.StatePending:
		s.setNodeLocked("")
		s.instance.SetStatus(Pending)
	case slurm.StateConfiguring:
		s.setNodeLocked(s.job.FirstNode())
		s.instance.SetStatus(Starting)
	case slurm.StateRunning:
		s.setNodeLocked(s.job.FirstNode())
		if s.instance.GetStatus() == Running {
			return
		}
		if s.checkHealthLocked(ctx) {
			log.Printf("Instance %s: server is up on %s", s.instance.Name, s.hostLocked())
			if s.instance.proxy != nil {
				s.instance.proxy.updateLastRequestTime()
			}
			s.instance.SetStatus(Running)
		} else {
			s.instance.SetStatus(Starting)
		}
	case slurm.StateCompleting:
		s.instance.SetStatus(ShuttingDown)
	default:
		// SUSPENDED, REQUEUED, RESIZING, ... : held, not serving
		s.instance.SetStatus(Pending)
	}
}

// finishLocked records how the tracked job ended. Caller must hold s.mu.
func (s *slurmJob) finishLocked(ctx context.Context) {
	ended := *s.job
	if acct, err := s.client.Accounting(ctx, ended.ID); err == nil {
		ended = *acct
	} else {
		log.Printf("Instance %s: job %s left the queue; sacct failed: %v", s.instance.Name, ended.ID, err)
		ended.State = ""
	}

	switch ended.State {
	case slurm.StateCompleted, slurm.StateCancelled, "":
		s.instance.SetStatus(Stopped)
	default:
		// FAILED, TIMEOUT, OUT_OF_MEMORY, NODE_FAIL, PREEMPTED, BOOT_FAIL, DEADLINE
		log.Printf("Instance %s: Slurm job %s ended in state %s", s.instance.Name, ended.ID, ended.State)
		s.instance.SetStatus(Failed)
	}

	s.last = &ended
	s.job = nil
	s.setNodeLocked("")
}

// findByNameLocked returns the active job with the instance's job name,
// preferring a running job and then the newest. Caller must hold s.mu.
func (s *slurmJob) findByNameLocked(jobs []slurm.Job) *slurm.Job {
	name := s.jobName()
	var best *slurm.Job
	for i := range jobs {
		j := jobs[i]
		if j.Name != name || !j.Active() {
			continue
		}
		if _, skip := s.ignored[j.ID]; skip {
			continue
		}
		if best == nil || jobRank(j) > jobRank(*best) {
			best = &j
		}
	}
	return best
}

func jobRank(j slurm.Job) int64 {
	id, _ := strconv.ParseInt(j.ID, 10, 64)
	if j.State != slurm.StatePending {
		id += 1 << 62
	}
	return id
}

// setNodeLocked records the job's node and points the proxy at it when it
// changes. Caller must hold s.mu.
func (s *slurmJob) setNodeLocked(node string) {
	if node == s.node {
		return
	}
	s.node = node
	if s.instance.proxy != nil {
		s.instance.proxy.retarget(s.hostLocked(), s.instance.GetPort())
	}
}

// checkHealthLocked makes one health request to the job's server. Caller
// must hold s.mu (so it can't go through GetHost, which takes it).
func (s *slurmJob) checkHealthLocked(ctx context.Context) bool {
	return checkHealthAt(ctx, s.hostLocked(), s.instance.GetPort(), s.instance.GetHealthPath(), 3*time.Second)
}

// getLogs runs the logs command for the running job. Logs live on the
// compute node, so they can only be read while the job holds it.
func (s *slurmJob) getLogs(numLines int) (string, error) {
	s.mu.Lock()
	var jobID, node string
	if s.job != nil {
		jobID, node = s.job.ID, s.node
	}
	s.mu.Unlock()

	if jobID == "" || node == "" {
		return "", fmt.Errorf("instance %s has no running Slurm job; its logs are on the compute node and can only be read while the job runs", s.instance.Name)
	}

	o := s.opts()
	cmd := o.LogsCommand
	if len(cmd) == 0 {
		cmd = defaultSlurmLogsCommand
	}

	lines := strconv.Itoa(numLines)
	if numLines <= 0 {
		lines = "100"
	}
	vars := strings.NewReplacer("{name}", s.instance.Name, "{jobid}", jobID, "{node}", node)
	logPath := vars.Replace(o.LogPath)

	args := make([]string, len(cmd))
	for i, arg := range cmd {
		if strings.Contains(arg, "{log_path}") && logPath == "" {
			return "", fmt.Errorf("instance %s has no slurm log_path configured", s.instance.Name)
		}
		args[i] = strings.NewReplacer(
			"{name}", s.instance.Name, "{jobid}", jobID, "{node}", node,
			"{lines}", lines, "{log_path}", logPath,
		).Replace(arg)
	}

	return s.client.Run(context.Background(), args)
}
