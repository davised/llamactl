// Package command runs external programs for drivers that manage servers
// llamactl doesn't run itself (external services, Slurm jobs).
package command

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Run runs argv directly (no shell) with extra environment variables and a
// timeout, and returns its stdout. On failure the error names the program
// and carries the end of its stderr (or stdout), so callers can show why.
func Run(ctx context.Context, timeout time.Duration, argv []string, env map[string]string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	if len(env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

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
