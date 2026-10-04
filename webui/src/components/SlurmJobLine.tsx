import type { SlurmJob } from '@/types/instance'
import { describeSlurmJob } from '@/lib/slurmJob'

// The Slurm job behind an instance, shown on its card
function SlurmJobLine({ job }: { job: SlurmJob }) {
  return (
    <p className="text-xs text-muted-foreground font-mono" data-testid="slurm-job">
      {describeSlurmJob(job)}
    </p>
  )
}

export default SlurmJobLine
