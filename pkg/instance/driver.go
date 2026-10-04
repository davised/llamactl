package instance

// Drivers run an instance's backend server somewhere other than a local
// llamactl process: an external service managed through commands
// (external.go) or a Slurm batch job (slurm.go). Everything they need from
// the rest of the package is kept in this file so the upstream files only
// carry one-line hooks that defer to i.driver.

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"llamactl/pkg/config"
	"llamactl/pkg/slurm"
)

// driver starts, stops and reads logs for a server llamactl doesn't run as
// a local process.
type driver interface {
	start() error
	stop() error
	restart() error
	waitForHealthy(timeout int) error
	getLogs(numLines int) (string, error)
}

// Optional driver capabilities.
type (
	// hostResolver: the server's host isn't fixed in the options
	hostResolver interface{ host() string }
	// statusSyncer: the driver can find out whether the server is up
	statusSyncer interface{ syncStatus() bool }
	// jobReporter: the server runs as a batch job worth showing in the UI
	jobReporter interface{ currentJob() *slurm.Job }
	// queueFollower: the driver is updated from a shared squeue listing
	queueFollower interface {
		update(ctx context.Context, jobs []slurm.Job, queriedAt time.Time)
	}
)

// newDriver returns the driver for an instance's options, or nil for a
// local process or a remote (other llamactl node) instance.
func newDriver(inst *Instance, cfg *config.AppConfig) driver {
	opts := inst.GetOptions()
	if opts == nil || inst.IsRemote() {
		return nil
	}
	switch {
	case opts.Slurm != nil:
		return newSlurmJob(inst, cfg.Slurm)
	case opts.External != nil:
		return newExternal(inst)
	}
	return nil
}

// RunsElsewhere reports whether the backend server runs outside llamactl
// (an external service or a Slurm job) rather than as a local process.
func (c *Options) RunsElsewhere() bool {
	return c.External != nil || c.Slurm != nil
}

// applyDriverDefaults fills defaults that differ from the global ones.
// A Slurm job can queue for hours, so starting one on demand or cancelling
// one when idle must be asked for explicitly.
func (c *Options) applyDriverDefaults() {
	if c.Slurm == nil {
		return
	}
	if c.OnDemandStart == nil {
		off := false
		c.OnDemandStart = &off
	}
	if c.IdleTimeout == nil {
		off := 0
		c.IdleTimeout = &off
	}
}

// IsExternal returns true if the backend server runs outside llamactl
// (an external service or a Slurm job), so it uses no local capacity or ports.
func (i *Instance) IsExternal() bool {
	opts := i.GetOptions()
	return opts != nil && opts.RunsElsewhere() && !i.IsRemote()
}

// IsSlurm returns true if the backend server runs as a Slurm job.
func (i *Instance) IsSlurm() bool {
	opts := i.GetOptions()
	return opts != nil && opts.Slurm != nil && !i.IsRemote()
}

// IsActive returns true if the instance is running or on its way up
// (a queued or starting Slurm job).
func (i *Instance) IsActive() bool {
	return i.GetStatus().IsActive()
}

// SyncExternalStatus sets the instance's status from its driver (an
// external service's status command). It returns false if there is
// nothing to sync from. Slurm instances report true: their status comes
// from the manager's squeue poller.
func (i *Instance) SyncExternalStatus() bool {
	if _, ok := i.driver.(queueFollower); ok {
		return true
	}
	if s, ok := i.driver.(statusSyncer); ok {
		return s.syncStatus()
	}
	return false
}

// UpdateSlurm sets a Slurm instance's status from a squeue listing of the
// user's jobs, taken at queriedAt (just before squeue ran). It is a no-op
// for other instances.
func (i *Instance) UpdateSlurm(ctx context.Context, jobs []slurm.Job, queriedAt time.Time) {
	if f, ok := i.driver.(queueFollower); ok {
		f.update(ctx, jobs, queriedAt)
	}
}

// SlurmJob returns the instance's current (or most recent) Slurm job, or nil.
func (i *Instance) SlurmJob() *slurm.Job {
	if r, ok := i.driver.(jobReporter); ok {
		return r.currentJob()
	}
	return nil
}

// jobInfo is SlurmJob as an untyped value for JSON, nil when there is no
// job so that omitempty drops it.
func (i *Instance) jobInfo() any {
	if j := i.SlurmJob(); j != nil {
		return j
	}
	return nil
}

// retarget points the proxy at a new backend address, for servers whose
// host is only known at run time. An empty host keeps the old target.
func (p *proxy) retarget(host string, port int) {
	if host == "" {
		return
	}
	target, err := url.Parse(fmt.Sprintf("http://%s:%d", host, port))
	if err != nil {
		return
	}
	p.mu.Lock()
	p.targetURL = target
	p.mu.Unlock()
	p.clear()
}

// pollHealthy polls the instance's health endpoint until it returns 2xx or
// the timeout (in seconds) elapses. Same contract as the local process's
// health wait, but for a host that may not be fixed in the options.
func pollHealthy(inst *Instance, timeout int) error {
	if timeout <= 0 {
		timeout = 30
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if checkHealth(ctx, inst, 5*time.Second) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for instance %s to become healthy after %d seconds", inst.Name, timeout)
		case <-ticker.C:
		}
	}
}

// checkHealth makes one request to the instance's health endpoint.
func checkHealth(ctx context.Context, inst *Instance, timeout time.Duration) bool {
	return checkHealthAt(ctx, inst.GetHost(), inst.GetPort(), inst.GetHealthPath(), timeout)
}

// checkHealthAt makes one request to http://host:port/path; any 2xx counts
// as healthy (some servers return 204).
func checkHealthAt(ctx context.Context, host string, port int, path string, timeout time.Duration) bool {
	if host == "" || port == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d%s", host, port, path), nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
