import type { CreateInstanceOptions } from '@/types/instance'

export type ExternalOptions = NonNullable<CreateInstanceOptions['external']>

// Builds external-service commands for a `systemctl --user` unit, run
// locally or over ssh. Start uses --no-block because units that wait for
// their server's health check stay "activating" for as long as a model
// takes to load, and "activating" counts as up for the status check.
export function systemdCommands(unit: string, sshHost?: string, logFile?: string): ExternalOptions {
  const remote = sshHost ? ['ssh', '-o', 'BatchMode=yes', sshHost] : []
  const isUp = `systemctl --user is-active ${unit} | grep -qxE 'active|activating|reloading'`
  return {
    start_command: [...remote, 'systemctl', '--user', 'start', '--no-block', unit],
    stop_command: [...remote, 'systemctl', '--user', 'stop', unit],
    // ssh hands its arguments to the remote shell; locally run it with sh
    status_command: sshHost ? [...remote, isUp] : ['sh', '-c', isUp],
    logs_command: logFile
      ? [...remote, 'tail', '-n', '{lines}', logFile]
      : [...remote, 'journalctl', '--user', '-u', unit, '-n', '{lines}', '--no-pager'],
  }
}
