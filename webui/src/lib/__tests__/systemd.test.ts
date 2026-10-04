import { describe, expect } from 'vitest'
import { test, fc } from '@fast-check/vitest'
import { systemdCommands } from '@/lib/systemd'

const unit = fc.stringMatching(/^[A-Za-z0-9@._:-]{1,30}$/)
const host = fc.option(fc.stringMatching(/^[a-z][a-z0-9.-]{0,20}$/), { nil: undefined })
const logFile = fc.option(fc.stringMatching(/^\/[a-z0-9/._{}-]{1,30}$/), { nil: undefined })

describe('systemdCommands', () => {
  test.prop([unit, host, logFile])('every command acts on the unit, locally or over ssh', (u, h, log) => {
    const cmds = systemdCommands(u, h, log)
    for (const cmd of Object.values(cmds) as string[][]) {
      // Remote commands go through non-interactive ssh to that host; local ones never use ssh
      if (h) {
        expect(cmd.slice(0, 4)).toEqual(['ssh', '-o', 'BatchMode=yes', h])
      } else {
        expect(cmd[0]).not.toBe('ssh')
      }
      if (cmd !== cmds.logs_command || !log) {
        expect(cmd.join(' ')).toContain(u)
      }
    }
  })

  // Units that wait for a health check stay "activating" while a model
  // loads: start must not wait for that, and the status check counts it as up
  test.prop([unit, host])('does not wait for a slow unit and counts activating as up', (u, h) => {
    const cmds = systemdCommands(u, h)
    expect(cmds.start_command).toContain('--no-block')
    const status = cmds.status_command ?? []
    expect(status[status.length - 1]).toMatch(/activating/)
  })

  test.prop([unit, host, logFile])('reads logs from the file when given, else the journal', (u, h, log) => {
    const logs = systemdCommands(u, h, log).logs_command ?? []
    if (log) {
      expect(logs.slice(-4)).toEqual(['tail', '-n', '{lines}', log])
    } else {
      expect(logs).toContain('journalctl')
      expect(logs).toContain('{lines}')
    }
  })
})
