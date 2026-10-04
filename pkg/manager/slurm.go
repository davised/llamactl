package manager

import (
	"context"
	"llamactl/pkg/instance"
	"llamactl/pkg/slurm"
	"log"
	"sync"
	"time"
)

// slurmPoller updates every Slurm instance from one `squeue --me --json`
// call per interval, rather than querying the scheduler once per instance.
type slurmPoller struct {
	manager  *instanceManager
	client   *slurm.Client
	interval time.Duration

	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	// pollMu serializes polls (the loop and on-demand refreshes)
	pollMu sync.Mutex
}

func newSlurmPoller(im *instanceManager) *slurmPoller {
	cfg := im.globalConfig.Slurm
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &slurmPoller{
		manager:  im,
		client:   &slurm.Client{Prefix: cfg.CommandPrefix},
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

func (p *slurmPoller) start() {
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		// Poll right away so jobs that survived a llamactl restart show
		// their real state immediately
		p.poll()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				p.poll()
			}
		}
	}()
}

func (p *slurmPoller) stop() {
	p.stopOnce.Do(func() {
		p.cancel()
		<-p.done
	})
}

// poll runs squeue once and updates every Slurm instance from the result.
func (p *slurmPoller) poll() {
	p.pollMu.Lock()
	defer p.pollMu.Unlock()

	var instances []*instance.Instance
	for _, inst := range p.manager.registry.list() {
		if inst.IsSlurm() {
			instances = append(instances, inst)
		}
	}
	if len(instances) == 0 {
		return
	}

	queriedAt := time.Now()
	jobs, err := p.client.Queue(p.ctx)
	if err != nil {
		if p.ctx.Err() == nil {
			log.Printf("Slurm: squeue failed, keeping previous instance states: %v", err)
		}
		return
	}

	for _, inst := range instances {
		before := inst.GetStatus()
		inst.UpdateSlurm(p.ctx, jobs, queriedAt)
		if inst.GetStatus() != before {
			if err := p.manager.persistInstance(inst); err != nil {
				log.Printf("Warning: failed to persist instance %s: %v", inst.Name, err)
			}
		}
	}
}
