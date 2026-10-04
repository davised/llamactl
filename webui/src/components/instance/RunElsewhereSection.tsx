import type React from 'react'
import type { CreateInstanceOptions } from '@/types/instance'
import CheckboxInput from '@/components/form/CheckboxInput'
import { runsElsewhere } from '@/lib/runElsewhere'
import ExternalSection from './ExternalSection'
import SlurmSection from './SlurmSection'

interface RunElsewhereSectionProps {
  formData: CreateInstanceOptions
  onChange: (key: keyof CreateInstanceOptions, value: unknown) => void
  onBackendFieldChange: (key: string, value: unknown) => void
}

// Choosing to run the backend outside llamactl (an external service or a
// Slurm job) and the settings for each. The two are mutually exclusive and
// can't be changed after creation.
const RunElsewhereSection: React.FC<RunElsewhereSectionProps> = ({
  formData,
  onChange,
  onBackendFieldChange
}) => {
  const mode = runsElsewhere(formData)
  const backendOptions = formData.backend_options as Record<string, unknown> | undefined
  const port = backendOptions?.port as number | undefined

  return (
    <>
      {mode !== 'external' && (
        <CheckboxInput
          id="slurm"
          label="Slurm Job"
          value={mode === 'slurm'}
          onChange={(value) => onChange('slurm', value ? { command: '' } : undefined)}
          description="Run the backend as a Slurm batch job; llamactl submits it, follows it through the queue and proxies to the node it lands on. Can't be changed after creation."
        />
      )}
      {formData.slurm && (
        <SlurmSection
          slurm={formData.slurm}
          port={port}
          onSlurmChange={(value) => onChange('slurm', value)}
          onPortChange={(value) => onBackendFieldChange('port', value)}
        />
      )}

      {mode !== 'slurm' && (
        <CheckboxInput
          id="external"
          label="External Service"
          value={mode === 'external'}
          onChange={(value) => onChange('external', value ? {} : undefined)}
          description="The backend runs outside llamactl (e.g. a systemd unit on another host); llamactl proxies to its host and port and runs the commands below to manage it. Can't be changed after creation."
        />
      )}
      {formData.external && (
        <ExternalSection
          external={formData.external}
          host={backendOptions?.host as string | undefined}
          port={port}
          onExternalChange={(value) => onChange('external', value)}
          onBackendFieldChange={onBackendFieldChange}
        />
      )}
    </>
  )
}

export default RunElsewhereSection
