import type React from 'react'
import { useState } from 'react'
import type { CreateInstanceOptions } from '@/types/instance'
import TextInput from '@/components/form/TextInput'
import NumberInput from '@/components/form/NumberInput'
import CommandInput from '@/components/form/CommandInput'
import SystemdFill from './SystemdFill'

type ExternalOptions = NonNullable<CreateInstanceOptions['external']>

interface ExternalSectionProps {
  external: ExternalOptions
  host: string | undefined
  port: number | undefined
  onExternalChange: (external: ExternalOptions) => void
  onBackendFieldChange: (key: string, value: unknown) => void
}

// Settings for a backend server that runs outside llamactl and is managed
// through commands
const ExternalSection: React.FC<ExternalSectionProps> = ({
  external,
  host,
  port,
  onExternalChange,
  onBackendFieldChange
}) => {
  // Remounts the command inputs after "Fill commands" so they show the new values
  const [fillCount, setFillCount] = useState(0)

  const set = (key: keyof ExternalOptions, value: string[] | undefined) => {
    onExternalChange({ ...external, [key]: value })
  }

  return (
    <div className="space-y-4">
      <SystemdFill
        onFill={(commands) => {
          onExternalChange({ ...external, ...commands })
          setFillCount((n) => n + 1)
        }}
      />
      <TextInput
        id="external_host"
        label="Host"
        value={host}
        onChange={(value) => onBackendFieldChange('host', value)}
        placeholder="gpu01.cluster.local"
        description="Host the backend server listens on"
      />
      <NumberInput
        id="external_port"
        label="Port"
        value={port}
        onChange={(value) => onBackendFieldChange('port', value)}
        placeholder="8000"
        description="Port the backend server listens on"
      />
      <div className="space-y-4" key={fillCount}>
        <CommandInput
          id="external_start_command"
          label="Start Command"
          value={external.start_command}
          onChange={(value) => set('start_command', value)}
          placeholder="ssh -o BatchMode=yes gpu01 systemctl --user start vllm-qwen"
          description="Leave empty if the service is always on"
        />
        <CommandInput
          id="external_stop_command"
          label="Stop Command"
          value={external.stop_command}
          onChange={(value) => set('stop_command', value)}
          placeholder="ssh -o BatchMode=yes gpu01 systemctl --user stop vllm-qwen"
          description="Leave empty to only mark the instance stopped"
        />
        <CommandInput
          id="external_status_command"
          label="Status Command"
          value={external.status_command}
          onChange={(value) => set('status_command', value)}
          placeholder="ssh -o BatchMode=yes gpu01 systemctl --user is-active --quiet vllm-qwen"
          description="Exits 0 while the service is running; polled to detect a dead service and checked when llamactl starts"
        />
        <CommandInput
          id="external_logs_command"
          label="Logs Command"
          value={external.logs_command}
          onChange={(value) => set('logs_command', value)}
          placeholder="ssh -o BatchMode=yes gpu01 journalctl --user -u vllm-qwen -n {lines} --no-pager"
          description="Prints recent logs; {lines} is replaced with the number of lines requested"
        />
      </div>
    </div>
  )
}

export default ExternalSection
