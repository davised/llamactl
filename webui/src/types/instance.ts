import type { CreateInstanceOptions } from '@/schemas/instanceOptions'

export type { CreateInstanceOptions } from '@/schemas/instanceOptions'

export const BackendType = {
  LLAMA_CPP: 'llama_cpp',
  MLX_LM: 'mlx_lm',
  VLLM: 'vllm',
  CUSTOM: 'custom',
  // MLX_VLM: 'mlx_vlm',  // Future expansion
} as const

export type BackendTypeValue = typeof BackendType[keyof typeof BackendType]

// 'pending' (queued) and 'starting' (running, not healthy yet) are used by
// Slurm job instances
export type InstanceStatus = 'running' | 'stopped' | 'failed' | 'restarting' | 'shutting_down' | 'pending' | 'starting'

export type HealthState = 'stopped' | 'pending' | 'starting' | 'ready' | 'failed' | 'restarting' | 'shutting_down'

export interface HealthStatus {
  state: HealthState
  instanceStatus: InstanceStatus
  lastChecked: Date
  error?: string
  source: 'backend' | 'http'
}

// The Slurm job behind a Slurm instance (the current one, or the last one
// after it ends)
export interface SlurmJob {
  job_id: string;
  name: string;
  state: string;
  reason?: string;
  nodes?: string;
  partition?: string;
  time_limit?: number; // minutes
  start_time?: number; // Unix seconds
  submit_time?: number; // Unix seconds
}

export interface Instance {
  id: number;
  name: string;
  status: InstanceStatus;
  options?: CreateInstanceOptions;
  slurm_job?: SlurmJob;
}