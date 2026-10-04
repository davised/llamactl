import type { SlurmJob } from '@/types/instance'

// Formats a duration in minutes as "1d 2h", "3h 5m" or "45m"
export function formatMinutes(minutes: number): string {
  const total = Math.max(0, Math.floor(minutes))
  const d = Math.floor(total / 1440)
  const h = Math.floor((total % 1440) / 60)
  const m = total % 60
  if (d > 0) return `${d}d ${h}h`
  if (h > 0) return `${h}h ${m}m`
  return `${m}m`
}

// One line describing a Slurm job: id, state, partition, then the node and
// time left while it runs, or the reason it is still queued
export function describeSlurmJob(job: SlurmJob, nowSeconds: number = Date.now() / 1000): string {
  const parts = [`Job ${job.job_id}`, job.state.toLowerCase()]
  if (job.partition) parts.push(job.partition)
  const running = job.state === 'RUNNING'
  if (running && job.nodes) parts.push(`on ${job.nodes}`)
  if (job.reason) parts.push(`(${job.reason})`)
  if (running && job.start_time && job.time_limit) {
    const left = job.time_limit - (nowSeconds - job.start_time) / 60
    if (left > 0) parts.push(`${formatMinutes(left)} left`)
  }
  return parts.join(' · ')
}
