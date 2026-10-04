import type React from 'react'
import { useState } from 'react'
import { Button } from '@/components/ui/button'
import TextInput from '@/components/form/TextInput'
import { systemdCommands, type ExternalOptions } from '@/lib/systemd'

const UNIT_PATTERN = /^[A-Za-z0-9@._:-]+$/
const HOST_PATTERN = /^[A-Za-z0-9._@-]+$/
const PATH_PATTERN = /^[A-Za-z0-9/._@{}~-]+$/

interface SystemdFillProps {
  onFill: (external: ExternalOptions) => void
}

const SystemdFill: React.FC<SystemdFillProps> = ({ onFill }) => {
  const [unit, setUnit] = useState<string | undefined>()
  const [host, setHost] = useState<string | undefined>()
  const [logFile, setLogFile] = useState<string | undefined>()

  const unitOk = !!unit && UNIT_PATTERN.test(unit)
  const hostOk = !host || HOST_PATTERN.test(host)
  const logOk = !logFile || PATH_PATTERN.test(logFile)

  return (
    <div className="space-y-3 rounded-md border border-dashed p-3">
      <p className="text-sm font-medium">Fill from a systemd user unit</p>
      <div className="grid grid-cols-3 gap-3">
        <TextInput
          id="systemd_unit"
          label="Unit"
          value={unit}
          onChange={setUnit}
          placeholder="vllm@my-model"
        />
        <TextInput
          id="systemd_host"
          label="SSH Host"
          value={host}
          onChange={setHost}
          placeholder="blank = this host"
        />
        <TextInput
          id="systemd_log_file"
          label="Log File"
          value={logFile}
          onChange={setLogFile}
          placeholder="blank = journalctl"
        />
      </div>
      <Button
        type="button"
        size="sm"
        variant="outline"
        disabled={!unitOk || !hostOk || !logOk}
        onClick={() => unit && onFill(systemdCommands(unit, host, logFile))}
      >
        Fill commands
      </Button>
    </div>
  )
}

export default SystemdFill
