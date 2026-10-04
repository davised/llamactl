import type { CreateInstanceOptions } from '@/schemas/instanceOptions'
import type { SlurmJob } from '@/schemas/runElsewhere'

export type { CreateInstanceOptions } from '@/schemas/instanceOptions'
export type { SlurmJob } from '@/schemas/runElsewhere'

export const BackendType = {
  LLAMA_CPP: 'llama_cpp',
  MLX_LM: 'mlx_lm',
  VLLM: 'vllm',
  CUSTOM: 'custom',
  // MLX_VLM: 'mlx_vlm',  // Future expansion
} as const

export type BackendTypeValue = typeof BackendType[keyof typeof BackendType]

export type InstanceStatus = 'running' | 'stopped' | 'failed' | 'restarting' | 'shutting_down' | 'pending' | 'starting' // last two: Slurm jobs

export type HealthState = 'stopped' | 'pending' | 'starting' | 'ready' | 'failed' | 'restarting' | 'shutting_down'

export interface HealthStatus {
  state: HealthState
  instanceStatus: InstanceStatus
  lastChecked: Date
  error?: string
  source: 'backend' | 'http'
}

export interface Instance {
  id: number;
  name: string;
  status: InstanceStatus;
  options?: CreateInstanceOptions;
  slurm_job?: SlurmJob;
}