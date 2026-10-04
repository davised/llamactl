import { z } from 'zod'

// External service: the backend runs outside llamactl (e.g. a systemd unit
// on another host) and is managed through these commands
export const ExternalOptionsSchema = z.object({
  start_command: z.array(z.string()).optional(),
  stop_command: z.array(z.string()).optional(),
  status_command: z.array(z.string()).optional(),
  logs_command: z.array(z.string()).optional(),
})

// Slurm job: the backend runs as a batch job on a compute node
export const SlurmOptionsSchema = z.object({
  command: z.string(),
  job_name: z.string().optional(),
  partition: z.string().optional(),
  nodelist: z.string().optional(),
  account: z.string().optional(),
  gpus: z.number().optional(),
  cpus: z.number().optional(),
  memory: z.string().optional(),
  time_limit: z.string().optional(),
  output: z.string().optional(),
  extra_args: z.array(z.string()).optional(),
  log_path: z.string().optional(),
  logs_command: z.array(z.string()).optional(),
})

// The Slurm job behind a Slurm instance (the current one, or the last one
// after it ends), as reported by the API
export interface SlurmJob {
  job_id: string
  name: string
  state: string
  reason?: string
  nodes?: string
  partition?: string
  time_limit?: number // minutes
  start_time?: number // Unix seconds
  submit_time?: number // Unix seconds
}
