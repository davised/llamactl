import { describe, it, expect } from 'vitest'
import { systemdCommands } from '@/lib/systemd'

describe('systemdCommands', () => {
  it('builds local commands', () => {
    expect(systemdCommands('vllm@qwen')).toEqual({
      start_command: ['systemctl', '--user', 'start', '--no-block', 'vllm@qwen'],
      stop_command: ['systemctl', '--user', 'stop', 'vllm@qwen'],
      status_command: ['sh', '-c', "systemctl --user is-active vllm@qwen | grep -qxE 'active|activating|reloading'"],
      logs_command: ['journalctl', '--user', '-u', 'vllm@qwen', '-n', '{lines}', '--no-pager'],
    })
  })

  it('wraps commands in ssh and tails a log file', () => {
    const cmds = systemdCommands('vllm@glm', 'remi', '/data/logs/glm-latest.log')
    const ssh = ['ssh', '-o', 'BatchMode=yes', 'remi']
    expect(cmds.start_command).toEqual([...ssh, 'systemctl', '--user', 'start', '--no-block', 'vllm@glm'])
    // ssh passes the pipeline to the remote shell as one argument
    expect(cmds.status_command).toEqual([...ssh, "systemctl --user is-active vllm@glm | grep -qxE 'active|activating|reloading'"])
    expect(cmds.logs_command).toEqual([...ssh, 'tail', '-n', '{lines}', '/data/logs/glm-latest.log'])
  })
})
