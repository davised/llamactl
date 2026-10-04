package instance

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// externalCommandTimeout bounds every start/stop/status/logs command so a
	// hung ssh session can't wedge the instance.
	externalCommandTimeout = 60 * time.Second
	// externalStatusInterval is how often the status command is polled while
	// the instance is running.
	externalStatusInterval = 30 * time.Second
)

// ExternalOptions describes a backend server that runs outside llamactl, such
// as a systemd unit on another host. llamactl proxies to the backend's
// host:port and runs these commands to manage it. Commands are argv arrays
// executed directly (no shell), e.g.
// ["ssh", "-o", "BatchMode=yes", "gpu01", "systemctl", "--user", "start", "vllm-qwen"].
type ExternalOptions struct {
	// StartCommand starts the service. If empty, the service is assumed to be
	// always on and starting the instance only marks it running.
	StartCommand []string `json:"start_command,omitempty"`
	// StopCommand stops the service. If empty, stopping only marks the
	// instance stopped.
	StopCommand []string `json:"stop_command,omitempty"`
	// StatusCommand exits 0 while the service is running. When set, it is
	// polled to detect services that die, and used at startup to discover
	// whether the service is already running.
	StatusCommand []string `json:"status_command,omitempty"`
	// LogsCommand prints recent log output. "{lines}" in any argument is
	// replaced with the requested number of lines.
	LogsCommand []string `json:"logs_command,omitempty"`
}

// external manages an instance whose server runs outside llamactl.
type external struct {
	instance *Instance

	mu            sync.Mutex
	monitorCancel context.CancelFunc
}

func newExternal(instance *Instance) *external {
	return &external{instance: instance}
}

func (e *external) opts() *ExternalOptions {
	opts := e.instance.GetOptions()
	if opts == nil || opts.External == nil {
		return &ExternalOptions{}
	}
	return opts.External
}

func (e *external) start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.instance.IsRunning() {
		return fmt.Errorf("instance %s is already running", e.instance.Name)
	}

	if cmd := e.opts().StartCommand; len(cmd) > 0 {
		if _, err := runExternalCommand(cmd); err != nil {
			return fmt.Errorf("failed to start instance %s: %w", e.instance.Name, err)
		}
	}

	if e.instance.proxy != nil {
		e.instance.proxy.updateLastRequestTime()
	}
	e.instance.SetStatus(Running)
	e.startMonitorLocked()

	return nil
}

func (e *external) stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.instance.IsRunning() {
		e.stopMonitorLocked()
		return fmt.Errorf("instance %s is not running", e.instance.Name)
	}

	e.stopMonitorLocked()
	e.instance.SetStatus(ShuttingDown)

	// Wait for inflight requests to complete (max 30 seconds)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && e.instance.GetInflightRequests() > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	if cmd := e.opts().StopCommand; len(cmd) > 0 {
		if _, err := runExternalCommand(cmd); err != nil {
			// The service may still be up; keep reporting it as running.
			e.instance.SetStatus(Running)
			e.startMonitorLocked()
			return fmt.Errorf("failed to stop instance %s: %w", e.instance.Name, err)
		}
	}

	e.instance.SetStatus(Stopped)
	return nil
}

func (e *external) restart() error {
	if err := e.stop(); err != nil && e.instance.IsRunning() {
		return fmt.Errorf("failed to stop instance during restart: %w", err)
	}
	return e.start()
}

func (e *external) waitForHealthy(timeout int) error {
	if !e.instance.IsRunning() {
		return fmt.Errorf("instance %s is not running", e.instance.Name)
	}
	return pollHealthy(e.instance, timeout)
}

// syncStatus runs the status command and sets the instance status to match.
// It is a no-op returning false when no status command is configured.
func (e *external) syncStatus() bool {
	cmd := e.opts().StatusCommand
	if len(cmd) == 0 {
		return false
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, err := runExternalCommand(cmd); err != nil {
		e.stopMonitorLocked()
		e.instance.SetStatus(Stopped)
	} else {
		e.instance.SetStatus(Running)
		e.startMonitorLocked()
	}
	return true
}

func (e *external) getLogs(numLines int) (string, error) {
	cmd := e.opts().LogsCommand
	if len(cmd) == 0 {
		return "", fmt.Errorf("instance %s has no logs command configured", e.instance.Name)
	}

	lines := strconv.Itoa(numLines)
	if numLines <= 0 {
		lines = "100"
	}
	args := make([]string, len(cmd))
	for i, arg := range cmd {
		args[i] = strings.ReplaceAll(arg, "{lines}", lines)
	}

	return runExternalCommand(args)
}

// startMonitorLocked polls the status command while the instance is running
// and marks it failed if the service goes away. Caller must hold e.mu.
func (e *external) startMonitorLocked() {
	e.stopMonitorLocked()

	cmd := e.opts().StatusCommand
	if len(cmd) == 0 {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.monitorCancel = cancel

	go func() {
		ticker := time.NewTicker(externalStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			_, err := runExternalCommand(cmd)

			e.mu.Lock()
			// Ignore results from a monitor that was cancelled while the
			// command ran (the instance was stopped or restarted meanwhile).
			if ctx.Err() == nil && err != nil && e.instance.GetStatus() == Running {
				log.Printf("Instance %s: status command reports service is down: %v", e.instance.Name, err)
				e.instance.SetStatus(Failed)
				e.monitorCancel = nil
				cancel()
			}
			e.mu.Unlock()
		}
	}()
}

// stopMonitorLocked cancels the status monitor. Caller must hold e.mu.
func (e *external) stopMonitorLocked() {
	if e.monitorCancel != nil {
		e.monitorCancel()
		e.monitorCancel = nil
	}
}

// runExternalCommand runs argv with a timeout and returns its combined output.
func runExternalCommand(argv []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), externalCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(out.String())
		if len(msg) > 500 {
			msg = msg[len(msg)-500:]
		}
		if msg != "" {
			return out.String(), fmt.Errorf("%s: %w: %s", argv[0], err, msg)
		}
		return out.String(), fmt.Errorf("%s: %w", argv[0], err)
	}
	return out.String(), nil
}

// Validate checks that every configured command has a program to run.
func (o *ExternalOptions) Validate() error {
	commands := map[string][]string{
		"start_command":  o.StartCommand,
		"stop_command":   o.StopCommand,
		"status_command": o.StatusCommand,
		"logs_command":   o.LogsCommand,
	}
	for name, cmd := range commands {
		if len(cmd) > 0 && strings.TrimSpace(cmd[0]) == "" {
			return fmt.Errorf("external %s must start with a program name", name)
		}
	}
	return nil
}
