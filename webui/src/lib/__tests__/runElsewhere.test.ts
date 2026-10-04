import { describe, expect } from 'vitest'
import { test, fc } from '@fast-check/vitest'
import { runsElsewhere } from '@/lib/runElsewhere'

describe('runsElsewhere', () => {
  // A Slurm job wins over an external service (the backend refuses both
  // anyway); without either the instance is a local process
  test.prop([fc.boolean(), fc.boolean()])('names how the server runs', (external, slurm) => {
    const mode = runsElsewhere({
      ...(external ? { external: {} } : {}),
      ...(slurm ? { slurm: { command: 'x' } } : {}),
    })
    expect(mode).toBe(slurm ? 'slurm' : external ? 'external' : undefined)
  })
})
