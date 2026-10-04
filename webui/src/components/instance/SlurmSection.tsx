import type React from 'react'
import type { CreateInstanceOptions } from '@/types/instance'
import TextInput from '@/components/form/TextInput'
import NumberInput from '@/components/form/NumberInput'
import CommandInput from '@/components/form/CommandInput'

type SlurmOptions = NonNullable<CreateInstanceOptions['slurm']>

interface SlurmSectionProps {
  instanceName?: string
  slurm: SlurmOptions
  port: number | undefined
  onSlurmChange: (slurm: SlurmOptions) => void
  onPortChange: (port: number | undefined) => void
}

// Settings for an instance that runs as a Slurm batch job
const SlurmSection: React.FC<SlurmSectionProps> = ({
  instanceName,
  slurm,
  port,
  onSlurmChange,
  onPortChange
}) => {
  const set = <K extends keyof SlurmOptions>(key: K, value: SlurmOptions[K] | undefined) => {
    onSlurmChange({ ...slurm, [key]: value })
  }

  return (
    <div className="space-y-4">
      <TextInput
        id="slurm_command"
        label="Job Command *"
        value={slurm.command}
        onChange={(value) => set('command', value ?? '')}
        placeholder="/path/to/launch-server.sh my-model"
        description="Shell command the job runs (sbatch --wrap). It must start the server on the port below."
        className="font-mono text-sm"
      />
      <NumberInput
        id="slurm_port"
        label="Port *"
        value={port}
        onChange={onPortChange}
        placeholder="8000"
        description="Port the server listens on. The host is the node the job lands on."
      />
      <TextInput
        id="slurm_job_name"
        label="Job Name"
        value={slurm.job_name}
        onChange={(value) => set('job_name', value)}
        placeholder={`llamactl-${instanceName || '<name>'}`}
        description="Queued or running jobs with this name are tracked automatically, including ones submitted by hand"
      />
      <div className="grid grid-cols-2 gap-4">
        <TextInput
          id="slurm_partition"
          label="Partition"
          value={slurm.partition}
          onChange={(value) => set('partition', value)}
          placeholder="gpu"
        />
        <TextInput
          id="slurm_nodelist"
          label="Node List"
          value={slurm.nodelist}
          onChange={(value) => set('nodelist', value)}
          placeholder="gpu01"
        />
        <NumberInput
          id="slurm_gpus"
          label="GPUs"
          value={slurm.gpus}
          onChange={(value) => set('gpus', value)}
          placeholder="1"
          description="--gres=gpu:N"
        />
        <NumberInput
          id="slurm_cpus"
          label="CPUs"
          value={slurm.cpus}
          onChange={(value) => set('cpus', value)}
          placeholder="8"
          description="--cpus-per-task"
        />
        <TextInput
          id="slurm_memory"
          label="Memory"
          value={slurm.memory}
          onChange={(value) => set('memory', value)}
          placeholder="64G"
        />
        <TextInput
          id="slurm_time_limit"
          label="Time Limit"
          value={slurm.time_limit}
          onChange={(value) => set('time_limit', value)}
          placeholder="1-00:00:00"
        />
        <TextInput
          id="slurm_account"
          label="Account"
          value={slurm.account}
          onChange={(value) => set('account', value)}
        />
        <TextInput
          id="slurm_output"
          label="Job Output"
          value={slurm.output}
          onChange={(value) => set('output', value)}
          placeholder="/dev/null"
          description="--output; keep logs off shared storage"
        />
      </div>
      <CommandInput
        id="slurm_extra_args"
        label="Extra sbatch Arguments"
        value={slurm.extra_args}
        onChange={(value) => set('extra_args', value)}
        placeholder="--qos=long --gres=gpu:a100:2"
      />
      <TextInput
        id="slurm_log_path"
        label="Server Log Path"
        value={slurm.log_path}
        onChange={(value) => set('log_path', value)}
        placeholder="/scratch/alice/logs/{name}-latest.log"
        description="Where the server writes its log on the compute node; {name}, {jobid} and {node} are substituted"
        className="font-mono text-sm"
      />
      <CommandInput
        id="slurm_logs_command"
        label="Logs Command"
        value={slurm.logs_command}
        onChange={(value) => set('logs_command', value)}
        placeholder="srun --jobid={jobid} --overlap -N1 -n1 tail -n {lines} {log_path}"
        description="Reads logs while the job runs. Leave empty for the default above; use ssh {node} tail ... where ssh to nodes is allowed."
      />
    </div>
  )
}

export default SlurmSection
