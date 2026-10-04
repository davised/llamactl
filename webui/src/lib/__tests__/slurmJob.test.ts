import { describe, expect } from 'vitest'
import { test, fc } from '@fast-check/vitest'
import { describeSlurmJob, formatMinutes } from '@/lib/slurmJob'
import type { SlurmJob } from '@/types/instance'

const states = ['PENDING', 'CONFIGURING', 'RUNNING', 'COMPLETING', 'COMPLETED', 'FAILED', 'TIMEOUT', 'CANCELLED']

const job: fc.Arbitrary<SlurmJob> = fc.record({
  job_id: fc.integer({ min: 1, max: 1e9 }).map(String),
  name: fc.stringMatching(/^[a-z0-9-]{1,12}$/),
  state: fc.constantFrom(...states),
  reason: fc.option(fc.constantFrom('Resources', 'Priority'), { nil: undefined }),
  nodes: fc.option(fc.constantFrom('gpu01', 'gpu[01-02]'), { nil: undefined }),
  partition: fc.option(fc.constantFrom('gpu', 'debug'), { nil: undefined }),
  time_limit: fc.option(fc.integer({ min: 1, max: 20000 }), { nil: undefined }),
  start_time: fc.option(fc.integer({ min: 1, max: 2e9 }), { nil: undefined }),
})

describe('formatMinutes', () => {
  // The largest two units are exact: days and hours above a day, hours and
  // minutes below it
  test.prop([fc.double({ min: 0, max: 1e6, noNaN: true })])('reads back as the duration it shows', (minutes) => {
    const total = Math.floor(minutes)
    const m = formatMinutes(minutes).match(/^(?:(\d+)d )?(?:(\d+)h ?)?(?:(\d+)m)?$/)
    expect(m).not.toBeNull()
    const [d, h, mm] = [m?.[1], m?.[2], m?.[3]].map((x) => Number(x ?? 0))
    if (total >= 1440) {
      expect(d * 1440 + h * 60).toBe(total - (total % 60))
    } else {
      expect(h * 60 + mm).toBe(total)
    }
  })
})

describe('describeSlurmJob', () => {
  test.prop([job, fc.integer({ min: 0, max: 2e9 })])('says which job, its state and why it waits', (j, now) => {
    const line = describeSlurmJob(j, now)
    expect(line.startsWith(`Job ${j.job_id} · ${j.state.toLowerCase()}`)).toBe(true)
    expect(line.includes(`(${j.reason})`)).toBe(!!j.reason)
    if (j.partition) expect(line).toContain(j.partition)
    // The node only matters once the job runs on it
    expect(line.includes(' on ')).toBe(j.state === 'RUNNING' && !!j.nodes)
  })

  test.prop([job, fc.integer({ min: 0, max: 20000 })])('shows the time left while running', (j, elapsedMinutes) => {
    const running = { ...j, state: 'RUNNING', start_time: 1_000_000, time_limit: j.time_limit ?? 60 }
    const line = describeSlurmJob(running, 1_000_000 + elapsedMinutes * 60)
    const left = running.time_limit - elapsedMinutes
    expect(line.endsWith(`${formatMinutes(left)} left`)).toBe(left > 0)
  })
})
