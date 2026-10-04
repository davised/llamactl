import type { CreateInstanceOptions } from '@/types/instance'

// How an instance's server runs when llamactl doesn't run it as a local
// process: an external service it manages through commands, or a Slurm job
export type RunElsewhere = 'external' | 'slurm'

export function runsElsewhere(options: CreateInstanceOptions): RunElsewhere | undefined {
  if (options.slurm !== undefined) return 'slurm'
  if (options.external !== undefined) return 'external'
  return undefined
}

export const runElsewhereLabels: Record<RunElsewhere, string> = {
  external: 'External service',
  slurm: 'Slurm job',
}
