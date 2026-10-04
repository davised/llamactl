import type React from 'react'
import { useState } from 'react'
import { BackendType, type CreateInstanceOptions } from '@/types/instance'
import { Button } from '@/components/ui/button'
import { Terminal, ChevronDown, ChevronRight } from 'lucide-react'
import { getBasicBackendFields } from '@/lib/zodFormUtils'
import BackendFormField from '@/components/BackendFormField'
import SelectInput from '@/components/form/SelectInput'
import CheckboxInput from '@/components/form/CheckboxInput'
import TextInput from '@/components/form/TextInput'
import EnvVarsInput from '@/components/form/EnvVarsInput'
import NumberInput from '@/components/form/NumberInput'
import CommandInput from '@/components/form/CommandInput'
import SlurmSection from './SlurmSection'
import SystemdFill from './SystemdFill'
import { useBackendSettings, useCustomBackends, useCustomBackendSettings } from '@/hooks/useConfig'
import PresetDialog from './PresetDialog'

const CUSTOM_OPTION_PREFIX = 'custom:'

interface BackendTabProps {
  formData: CreateInstanceOptions
  onBackendFieldChange: (key: string, value: unknown) => void
  onChange: (key: keyof CreateInstanceOptions, value: unknown) => void
  onParseCommand: () => void
}

const BackendTab: React.FC<BackendTabProps> = ({
  formData,
  onBackendFieldChange,
  onChange,
  onParseCommand
}) => {
  const [showExecutionContext, setShowExecutionContext] = useState(false)
  const [showPresetDialog, setShowPresetDialog] = useState(false)
  const backendSettings = useBackendSettings(formData.backend_type)
  const basicBackendFields = getBasicBackendFields(formData.backend_type)

  const isCustom = formData.backend_type === BackendType.CUSTOM
  const customName = (formData.backend_options as Record<string, unknown> | undefined)?.name as string | undefined
  const customBackends = useCustomBackends()
  const customSettings = useCustomBackendSettings(customName)

  const backendOptions = formData.backend_options as Record<string, unknown> | undefined
  const external = formData.external
  const isExternal = external !== undefined
  const slurm = formData.slurm
  const isSlurm = slurm !== undefined
  // Remounts the command inputs after "Fill commands" so they show the new values
  const [fillCount, setFillCount] = useState(0)

  const handleExternalChange = (key: keyof NonNullable<CreateInstanceOptions['external']>, value: string[] | undefined) => {
    onChange('external', { ...external, [key]: value })
  }

  const handleBackendTypeChange = (value: string | undefined) => {
    if (value?.startsWith(CUSTOM_OPTION_PREFIX)) {
      const name = value.slice(CUSTOM_OPTION_PREFIX.length)
      onChange('backend_type', BackendType.CUSTOM)
      onBackendFieldChange('name', name)
      onChange('docker_enabled', customBackends[name]?.docker?.enabled ?? false)
      return
    }
    onChange('backend_type', value)
  }

  const getCommandPlaceholder = () => {
    if (isCustom) {
      return customSettings?.command || 'configured in backends.custom.<name>'
    }

    if (backendSettings?.command) {
      return backendSettings.command
    }

    switch (formData.backend_type) {
      case BackendType.LLAMA_CPP:
        return "llama-server"
      case BackendType.VLLM:
        return "vllm"
      case BackendType.MLX_LM:
        return "mlx_lm.server"
      default:
        return ""
    }
  }

  const configuredNames = Object.keys(customBackends)
  // Keep an existing selection resolvable even if its config entry was
  // removed after creation; the server rejects the save with a clear error
  if (isCustom && customName && !configuredNames.includes(customName)) {
    configuredNames.push(customName)
  }

  const backendTypeOptions = [
    { value: BackendType.LLAMA_CPP, label: 'Llama Server' },
    { value: BackendType.MLX_LM, label: 'MLX LM' },
    { value: BackendType.VLLM, label: 'vLLM' },
    ...configuredNames.map((name) => ({
      value: `${CUSTOM_OPTION_PREFIX}${name}`,
      label: name,
    })),
  ]

  const selectedBackendType = isCustom
    ? `${CUSTOM_OPTION_PREFIX}${customName ?? ''}`
    : formData.backend_type || BackendType.LLAMA_CPP

  return (
    <div className="space-y-6 py-4">
      <SelectInput
        id="backend_type"
        label="Backend Type"
        value={selectedBackendType}
        onChange={handleBackendTypeChange}
        options={backendTypeOptions}
        description="Select the backend server type"
      />

      <div className="space-y-4">
        {/* biome-ignore lint/a11y/useSemanticElements: toggle contains an <h3>, so a <button> would be invalid HTML; keyboard support provided via role/tabIndex/onKeyDown */}
        <div
          role="button"
          tabIndex={0}
          className="flex items-center justify-between cursor-pointer"
          onClick={() => setShowExecutionContext(!showExecutionContext)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' || e.key === ' ') {
              e.preventDefault()
              setShowExecutionContext(!showExecutionContext)
            }
          }}
        >
          <div className="flex items-center gap-2">
            {showExecutionContext ? (
              <ChevronDown className="h-4 w-4" />
            ) : (
              <ChevronRight className="h-4 w-4" />
            )}
            <h3 className="text-md font-medium">Execution Context</h3>
          </div>
          {!showExecutionContext && (
            <div className="text-sm text-muted-foreground">
              {isExternal && <span className="mr-3">External service</span>}
              {isSlurm && <span className="mr-3">Slurm job</span>}
              {formData.docker_enabled && <span className="mr-3">Docker enabled</span>}
              {formData.command_override && <span className="mr-3">Custom command</span>}
              {formData.environment && Object.keys(formData.environment).length > 0 && (
                <span>{Object.keys(formData.environment).length} env var{Object.keys(formData.environment).length > 1 ? 's' : ''}</span>
              )}
              {!isExternal && !isSlurm && !formData.docker_enabled && !formData.command_override && (!formData.environment || Object.keys(formData.environment).length === 0) && (
                <span>Default</span>
              )}
            </div>
          )}
        </div>

        {showExecutionContext && (
          <div className="space-y-4 pl-6 border-l-2 border-muted">
            {!isExternal && (
              <CheckboxInput
                id="slurm"
                label="Slurm Job"
                value={isSlurm}
                onChange={(value) => onChange('slurm', value ? { command: '' } : undefined)}
                description="Run the backend as a Slurm batch job; llamactl submits it, follows it through the queue and proxies to the node it lands on. Can't be changed after creation."
              />
            )}

            {isSlurm && slurm && (
              <SlurmSection
                slurm={slurm}
                port={backendOptions?.port as number | undefined}
                onSlurmChange={(value) => onChange('slurm', value)}
                onPortChange={(value) => onBackendFieldChange('port', value)}
              />
            )}

            {!isSlurm && (
              <CheckboxInput
                id="external"
                label="External Service"
                value={isExternal}
                onChange={(value) => onChange('external', value ? {} : undefined)}
                description="The backend runs outside llamactl (e.g. a systemd unit on another host); llamactl proxies to its host and port and runs the commands below to manage it. Can't be changed after creation."
              />
            )}

            {isExternal && (
              <div className="space-y-4" key={fillCount}>
                <SystemdFill
                  onFill={(commands) => {
                    onChange('external', { ...external, ...commands })
                    setFillCount((n) => n + 1)
                  }}
                />
                <TextInput
                  id="external_host"
                  label="Host"
                  value={backendOptions?.host as string | undefined}
                  onChange={(value) => onBackendFieldChange('host', value)}
                  placeholder="gpu01.cluster.local"
                  description="Host the backend server listens on"
                />
                <NumberInput
                  id="external_port"
                  label="Port"
                  value={backendOptions?.port as number | undefined}
                  onChange={(value) => onBackendFieldChange('port', value)}
                  placeholder="8000"
                  description="Port the backend server listens on"
                />
                <CommandInput
                  id="external_start_command"
                  label="Start Command"
                  value={external?.start_command}
                  onChange={(value) => handleExternalChange('start_command', value)}
                  placeholder="ssh -o BatchMode=yes gpu01 systemctl --user start vllm-qwen"
                  description="Leave empty if the service is always on"
                />
                <CommandInput
                  id="external_stop_command"
                  label="Stop Command"
                  value={external?.stop_command}
                  onChange={(value) => handleExternalChange('stop_command', value)}
                  placeholder="ssh -o BatchMode=yes gpu01 systemctl --user stop vllm-qwen"
                  description="Leave empty to only mark the instance stopped"
                />
                <CommandInput
                  id="external_status_command"
                  label="Status Command"
                  value={external?.status_command}
                  onChange={(value) => handleExternalChange('status_command', value)}
                  placeholder="ssh -o BatchMode=yes gpu01 systemctl --user is-active --quiet vllm-qwen"
                  description="Exits 0 while the service is running; polled to detect a dead service and checked when llamactl starts"
                />
                <CommandInput
                  id="external_logs_command"
                  label="Logs Command"
                  value={external?.logs_command}
                  onChange={(value) => handleExternalChange('logs_command', value)}
                  placeholder="ssh -o BatchMode=yes gpu01 journalctl --user -u vllm-qwen -n {lines} --no-pager"
                  description="Prints recent logs; {lines} is replaced with the number of lines requested"
                />
              </div>
            )}

            {!isExternal && !isSlurm && formData.backend_type !== BackendType.MLX_LM && (
              <CheckboxInput
                id="docker_enabled"
                label="Enable Docker"
                value={formData.docker_enabled}
                onChange={(value) => onChange('docker_enabled', value)}
                description="Run backend in Docker container"
              />
            )}

            {!isExternal && !isSlurm && (formData.backend_type === BackendType.MLX_LM || formData.docker_enabled !== true) && (
              <TextInput
                id="command_override"
                label="Command Override"
                value={formData.command_override || ''}
                onChange={(value) => onChange('command_override', value)}
                placeholder={getCommandPlaceholder()}
                description="Custom path to backend executable (leave empty to use config default)"
              />
            )}

            {!isExternal && (
              <EnvVarsInput
                id="environment"
                label="Environment Variables"
                value={formData.environment}
                onChange={(value) => onChange('environment', value)}
                description={isSlurm ? "Exported to the job through sbatch" : "Custom environment variables for the instance"}
              />
            )}
          </div>
        )}
      </div>

      {!isCustom && (
        <div className="flex flex-col gap-2">
          <Button
            variant="outline"
            onClick={onParseCommand}
            className="flex items-center gap-2 w-fit"
          >
            <Terminal className="h-4 w-4" />
            Parse Command
          </Button>
          <p className="text-sm text-muted-foreground">
            Import settings from your backend command
          </p>
        </div>
      )}

      {basicBackendFields.length > 0 && (
        <div className="space-y-4">
          <h3 className="text-md font-medium">Basic Backend Options</h3>
          {basicBackendFields.map((fieldKey) => (
            <BackendFormField
              key={fieldKey}
              fieldKey={fieldKey}
              value={(formData.backend_options as Record<string, unknown>)?.[fieldKey] as string | number | boolean | string[] | undefined}
              onChange={onBackendFieldChange}
              formData={formData}
              onOpenPresetDialog={fieldKey === 'models_preset' ? () => setShowPresetDialog(true) : undefined}
            />
          ))}
        </div>
      )}

      {formData.backend_type === BackendType.LLAMA_CPP && (
        <PresetDialog
          open={showPresetDialog}
          onOpenChange={setShowPresetDialog}
          formData={formData}
          onChange={onChange}
        />
      )}
    </div>
  )
}

export default BackendTab
