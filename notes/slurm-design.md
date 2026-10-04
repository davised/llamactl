# Slurm and external service support: design notes

Working notes for extending this fork of llamactl to manage vLLM servers on an
HPC cluster: Slurm jobs and long-running services on compute nodes. This
records the decisions made so far and what's left.

Last updated: 2026-10-04 (Slurm executor built)

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
| slurm | `sbatch --parsable --wrap=<command>` | `scancel <jobid>` | shared `squeue --json` poll, then `sacct` once the job leaves the queue | node from `squeue`, fixed port | **done** |

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

### Slurm executor (done)

Code: `pkg/slurm` (command wrapper and JSON parsing), `pkg/instance/slurm.go`
(executor), `pkg/manager/slurm.go` (shared poller). User docs:
`docs/managing-instances.md`, "Slurm Jobs"; config: `docs/configuration.md`,
"Slurm Configuration".

**Decisions, as built:**

- **HPC-agnostic.** llamactl calls plain `sbatch --parsable ... --wrap=<command>`
  with standard flags (partition, nodelist, account, `--gres=gpu:N`,
  `--cpus-per-task`, `--mem`, `--time`, plus raw `extra_args`). It knows
  nothing about the vLLM repo, hpcman or hqsub. **No job template:** the
  launcher script owns environment setup and the `vllm serve` arguments,
  which matches how the vLLM repo already works (`bin/slurm-launch.sh <name>`
  reads `services/models/<name>.env`).
- **Port is fixed per instance** (`backend_options.port`), not picked by
  llamactl: the vLLM repo's `.env` files set `PORT=` and override anything
  passed in. The host is the job's first node from `squeue`, mapped through
  `slurm.node_host_template`.
- **No job ID migration.** Jobs are found by **job name** (default
  `llamactl-<name>`, settable, e.g. `vllm-<name>`): when an instance has no
  tracked job, an active job with its name is adopted. This also picks up
  jobs submitted by hand and jobs that survived a llamactl restart. Jobs
  llamactl cancelled are ignored until they leave the queue, so a
  `COMPLETING` job isn't re-adopted.
- **Statuses:** `pending` (queued) and `starting` (job running, health check
  not passing). The status column's CHECK constraint only allows the original
  five values, and rebuilding the table would cascade-delete
  `key_permissions`, so both are stored as `restarting`; the poller re-derives
  the real state on startup. No `timeout` status: `sacct` gives the final
  state (shown on the card), and FAILED / TIMEOUT / OUT_OF_MEMORY / NODE_FAIL
  etc. map to `failed`, COMPLETED / CANCELLED to `stopped`.
- **One `squeue --me --json` per poll** (`slurm.poll_interval`, default
  15 s) updates every Slurm instance. A listing taken before a submission
  can't mark that new job as finished (submit time is compared with the
  listing time).
- **On-demand start and idle timeout** default to off for Slurm instances
  (an idle timeout would cancel a job that may have queued for hours). Starting an
  already queued job is a no-op, so on-demand requests wait for the same
  job. Stop works on queued and starting jobs (it cancels them).
- `slurm.command_prefix` (e.g. `ssh submit01`) runs every Slurm command
  through another host if llamactl can't submit directly.
- Parser accepts both the Slurm 22/23 and 24+/25.x JSON shapes.

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

## The vLLM repo (davised/vllm-service)

What it settled:

- **Slurm track:** `bin/slurm-launch.sh <name>` is the job command. It
  sources `services/models/<name>.env`, sets node-local caches under
  `$TMPDIR`, runs `bin/serve.sh` and health-gates. Submitted by hand today
  with `hpcman queue submit -r vllm-<name> -q <partition> -w <node> -P <cpus>
  -G <gpus> -f <mem> ...`, so `job_name: vllm-<name>` adopts those jobs.
- **Logs:** `serve.sh` writes `${VLLM_LOG_DIR:-/scratch/davised/vllm-logs}/<name>-latest.log`
  on the node, so `log_path` is `/scratch/davised/vllm-logs/{name}-latest.log`.
- **systemd track:** `services/vllm@.service`, a template unit on barringer
  and remi (`ConditionHost`). llamactl will run on barringer (local
  `systemctl --user`) and can ssh to remi. The unit stays `activating` until
  `health-wait.sh` passes (up to 3 h), hence the UI's systemd fill uses
  `start --no-block` and counts `activating` as up. On remi, logs are under
  `/data` (`VLLM_LOG_DIR` in the host env file).
- **Ports** come from `.env` files (8002 upward); two collisions exist (8010:
  `deepseek-v4-flash-nvfp4` and the GLM-5.2 quadlet; 8013: two mutually
  exclusive DeepSeek instances). They don't matter to llamactl unless both run
  on one node.
- **Bifrost** is the client gateway on barringer (`127.0.0.1:8099`); backends
  are provider keys named `<served>@<shorthost>`, added by hand
  (`bifrost/ADDING-BACKENDS.md`) or by `bin/bifrost-reconcile.sh`, which only
  adds.

## Open questions

1. Is Slurm TLS on? (`scontrol show config | grep -i tls`)
2. Does the site allow extra steps in a running job? Test:
   `srun --jobid=<id> --overlap -N1 -n1 tail -n 5 <log path>`. If not, use
   the ssh logs command on nodes that allow it.
3. Is barringer a Slurm submit host? If not, set `slurm.command_prefix`.
4. `squeue` node names: do they resolve from barringer, or is
   `node_host_template: "{node}.hpc.oregonstate.edu"` needed?

## Next steps

1. **Optional Bifrost registration** (owner wants it, off unless
   configured): when an instance becomes healthy, add a `<served>@<host>`
   provider key; on stop, remove it. Needs a `bifrost:` config section
   (URL, admin credentials, served-name source) and must be a no-op when
   unset.
2. Try it on the cluster: one Slurm instance for
   `qwen38-flash-next-nvfp4` (port 8019, `job_name: vllm-qwen38-flash-next-nvfp4`,
   partition `lpi`, nodelist `haughton`, 2 GPUs, 32 CPUs, 300G) and one
   external instance for a `vllm@<name>` unit on barringer and on remi.
3. Deploy behind a TLS reverse proxy on barringer.
4. Possible: return 503 "job queued" instead of 500 when on-demand start
   times out on a queued job.
