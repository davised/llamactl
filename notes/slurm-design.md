# Slurm and external service support: design notes

Working notes for extending this fork of llamactl to manage vLLM servers on an
HPC cluster: Slurm jobs and long-running services on compute nodes. This
records the decisions made so far and what's left.

Last updated: 2026-10-04

## Goal

Host llamactl on a web server and use its UI to:

- submit vLLM jobs to Slurm, watch them move through the queue and start up,
  and stop them
- manage vLLM servers that run as `systemctl --user` services on some nodes
- route OpenAI-compatible requests to any of them by instance name

Single user (the owner) for now.

## Environment facts

| Fact | Consequence |
|---|---|
| The web server can run `sbatch` and other Slurm commands directly | One llamactl process on the web server, running as the owner. No agent inside the cluster, so the remote-node feature isn't needed. Jobs run under the owner's account. |
| Slurm 25.11 (or similar recent release) | `squeue --json` and `sacct --json` are available. |
| `slurmrestd` isn't running | Drive the Slurm commands directly. |
| The web server can reach compute nodes on any port | llamactl can proxy straight to `node:port`. |
| A shared filesystem (NFS) exists, but **logs must stay node-local** | NFS isn't considered secure enough for log contents (prompts and outputs can appear in them). See [Logs and data handling](#logs-and-data-handling). |
| The owner can't ssh to every compute node | ssh can't be the only way to read logs. |
| Some vLLM servers run as `systemctl --user` services | Covered by the external service executor (done). |
| vLLM environments and per-model settings live in a separate repo (not on GitHub yet) | Job templates will live in that repo. Its layout decides the template interface. |

## Architecture

Each instance has an executor that starts and stops its backend server
(`executor` interface in `pkg/instance/instance.go`):

| Executor | Start | Stop | Status | Address | State |
|---|---|---|---|---|---|
| local (upstream) | starts a local process | stop signal, then kill | waits on the process | fixed host:port | existing |
| external | optional `start_command` | optional `stop_command` | optional `status_command`, polled every 30 s | fixed host:port | **done** (commit `03c8a14`) |
| slurm | `sbatch --parsable` on a script built from a template | `scancel <jobid>` | shared `squeue --json` poll, then `sacct` once the job leaves the queue | node from `squeue`, port chosen by llamactl | planned |

### External services (done)

- Options: `external.{start,stop,status,logs}_command`. Each is a list of
  arguments run directly (no shell) with a 60 s timeout. `{lines}` in
  `logs_command` is replaced with the number of lines requested.
- Requires `host` and `port` in `backend_options`. Doesn't take ports from the
  local range.
- Excluded from `max_instances` / `max_running_instances`, never evicted for
  local capacity, left running on llamactl shutdown. Group quotas still apply.
- `status_command` runs when llamactl starts, so the instance shows the
  service's real state rather than whatever was saved.
- Can't be switched between external and local after creation.
- UI: Backend tab, Execution Context, External Service.
- User docs: `docs/managing-instances.md`, "External Services".

Example for a user service on `gpu01` (works only where ssh is allowed):

```json
"external": {
  "start_command":  ["ssh", "-o", "BatchMode=yes", "gpu01", "systemctl", "--user", "start", "vllm-qwen"],
  "stop_command":   ["ssh", "-o", "BatchMode=yes", "gpu01", "systemctl", "--user", "stop", "vllm-qwen"],
  "status_command": ["ssh", "-o", "BatchMode=yes", "gpu01", "systemctl", "--user", "is-active", "--quiet", "vllm-qwen"],
  "logs_command":   ["ssh", "-o", "BatchMode=yes", "gpu01", "journalctl", "--user", "-u", "vllm-qwen", "-n", "{lines}", "--no-pager"]
}
```

### Slurm executor (planned)

**No Go library.** The available ones need `slurmrestd` (SlinkyProject,
ubccr/slurmrest, jontk, pasqal-io) or are small unmaintained wrappers.
Calling the commands directly is about 200 lines.

- **Submit:** render the job script with Go's `text/template`, then run
  `sbatch --parsable`, which prints only the job ID. Store the job ID in the
  database (new migration).
- **Status:** one background loop runs `squeue --me --json` every 10–15 s and
  updates every Slurm instance from that single call, instead of running a
  command per instance and loading the scheduler. When a job leaves the queue,
  `sacct -j <id> --json` gives the final state (completed, failed, timeout,
  out of memory, cancelled).
- **Stop:** `scancel <jobid>`.
- **Address:** node comes from `squeue`. llamactl picks the port and passes it
  to the template. Possible collision if two jobs share a node; handle by
  retrying with a new port.
- **New statuses:** `pending` (queued), `starting` (job running, health check
  not passing yet), `timeout` (hit the time limit). The frontend types and
  badges need updating too.
- **Restarts:** on startup, match stored job IDs against `squeue` to pick up
  running jobs. Never `scancel` on llamactl shutdown.
- **On-demand start:** off for Slurm instances by default (a queue wait can be
  hours), or return a 503 saying the job is queued.
- **Idle timeout:** maps to `scancel`, which frees GPUs.
- **UI:** job ID, partition, queue state, node, time remaining, and a form for
  Slurm options (partition, GPUs, time limit, account, extra `#SBATCH` lines,
  template).

Template sketch (the real one belongs in the vLLM env repo):

```bash
#!/bin/bash
#SBATCH --job-name=llamactl-{{.Name}}
#SBATCH --partition={{.Slurm.Partition}}
#SBATCH --gres=gpu:{{.Slurm.GPUs}}
#SBATCH --time={{.Slurm.Time}}
#SBATCH --output=/dev/null
#SBATCH --error=/dev/null
{{range .Slurm.Extra}}#SBATCH {{.}}
{{end}}
# Redirect everything to node-local scratch first, so errors from setup
# steps never reach Slurm's default output file on NFS
exec > "{{.LogPath}}" 2>&1

{{.Setup}}      # e.g. module load cuda; source ~/envs/vllm-x/bin/activate
{{range $k, $v := .Env}}export {{$k}}={{$v}}
{{end}}
exec {{.Command}} {{.Args}} --host 0.0.0.0 --port {{.Port}}
```

Planned split: **the vLLM env repo owns environment setup** (module loads,
which virtual environment, per-model `#SBATCH` defaults), found through a
config setting such as `slurm.templates_dir`. **llamactl owns the
`vllm serve` arguments** through its existing vLLM options form. Revisit once
the repo is available.

## Logs and data handling

**Decision:** logs stay on node-local scratch. llamactl reads them only when
the Logs dialog is opened, and never writes them to disk, NFS or its own log.

- The job script redirects output on its first line (`exec > "$LOG" 2>&1`)
  and sets `#SBATCH --output=/dev/null` and `--error=/dev/null`, so Slurm
  never creates an output file in the submit directory (usually on NFS).
- **Default way to read logs, no ssh needed:** a job step inside the job's
  own allocation:

  ```
  srun --jobid={jobid} --overlap -N1 -n1 tail -n {lines} {log_path}
  ```

  Works on any node the job lands on. Each read shows up as a short step in
  `sacct`. Runs only on demand.
- **Optional alternative:** `ssh -o BatchMode=yes {node} tail -n {lines} {log_path}`,
  for nodes where ssh is allowed.
- Both are just settings on a logs command with `{jobid}`, `{node}`,
  `{log_path}` and `{lines}` filled in, the same mechanism the external
  executor uses.
- Logs can't be read after the job ends and scratch is cleaned. If they need
  keeping, the job script can copy them somewhere trusted on exit (out of
  scope for llamactl).

### Where data travels

| Path | Encrypted? | Notes |
|---|---|---|
| Browser to llamactl | Only behind a reverse proxy | llamactl has no built-in HTTPS. Put it behind nginx or Caddy with TLS, and keep management login on. |
| llamactl to vLLM | No (plain HTTP) | Prompts and responses cross the cluster network. Same exposure as calling vLLM directly today. vLLM can serve HTTPS (`--ssl-keyfile` / `--ssl-certfile`) if needed. |
| `srun` log output to llamactl | Only if Slurm TLS is on | Slurm's step I/O is authenticated but not encrypted without its TLS support. Check with `scontrol show config \| grep -i tls`. Same exposure class as the row above, so fix both together if it matters. |
| ssh log reads | Yes | Only where ssh to the node is allowed. |
| llamactl's own log | n/a | Records method, URL, status and size only, never prompts or responses. Failed external commands include up to 500 characters of their output in the error (shown in the UI for logs commands, logged for status commands). |
| vLLM's own logs | n/a | Contain request contents only if request logging is on in the vLLM flags. Check the env repo, since those are the logs llamactl will display. |

## Related fixes

- Upstream bug fixed in `03c8a14`: on-demand start did nothing when
  `enable_lru_eviction: false`, because `maybeStartLocked` returned before
  calling `StartInstance`. Regression test in
  `pkg/server/handlers_start_test.go`. Worth sending upstream on its own.

## Code quality observations

- Generally solid: clear package structure, careful concurrency in
  `pkg/instance/process.go`, good test coverage. Backend tests pass with the
  race detector.
- Rough edges: some error checks compare message strings
  (`process.restart()`), remote detection lives in two places
  (`Instance.IsRemote()` and `remoteManager`), the proxy only uses the first
  assigned node, and `ExecutionTab.tsx` / `BackendConfigurationCard.tsx`
  appear to be unused (the live form is `BackendTab.tsx`).
- `go test ./...` needs `webui/dist` to exist; run `npm run build` in `webui/`
  first, or create a placeholder `webui/dist/index.html`.

## Open questions

1. Is Slurm TLS on? (`scontrol show config | grep -i tls`)
2. Does the site allow extra steps in a running job? Test:
   `srun --jobid=<id> --overlap -N1 -n1 tail -n 5 <log path>`
3. Layout of the vLLM env repo: where environment setup lives, how per-model
   settings are expressed, and whether to use the split above or let
   templates own everything.
4. Node-local scratch path convention, and whether scratch is cleaned at job
   end.
5. Port selection on shared nodes: a fixed range per user, or pick and retry.

## Next steps

1. Owner: make the vLLM env repo reachable from a session (e.g. a private
   GitHub repo) and answer the open questions.
2. Slurm executor backend: job ID storage (migration), new statuses, `sbatch`
   / `squeue` / `sacct` / `scancel` wrapper with a shared status loop,
   template rendering, runtime address lookup, matching jobs on startup.
3. Logs command with `{jobid}`, `{node}`, `{log_path}`, `{lines}`; default
   `srun --overlap`.
4. UI: Slurm form fields, queue state and time remaining on instance cards.
5. Deploy behind a TLS reverse proxy on the web server.
