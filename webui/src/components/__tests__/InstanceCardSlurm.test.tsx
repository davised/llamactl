import { describe, expect, vi } from 'vitest'
import { test, fc } from '@fast-check/vitest'
import { render, screen } from '@testing-library/react'
import InstanceCard from '@/components/InstanceCard'
import { BackendType, type Instance, type InstanceStatus } from '@/types/instance'

vi.mock('@/hooks/useInstanceHealth', () => ({
  useInstanceHealth: vi.fn(() => undefined),
}))

const statuses: InstanceStatus[] = ['running', 'stopped', 'failed', 'restarting', 'shutting_down', 'pending', 'starting']

describe('InstanceCard for servers running elsewhere', () => {
  // A server that is running, or a Slurm job on its way up (queued or
  // starting), can be stopped; anything else can be started
  test.prop([fc.constantFrom(...statuses), fc.boolean()])('offers stop exactly while the server is up or coming up', (status, withJob) => {
    const instance: Instance = {
      id: 1,
      name: 'qwen',
      status,
      options: { backend_type: BackendType.VLLM, slurm: { command: 'run' } },
      ...(withJob ? { slurm_job: { job_id: '7', name: 'vllm-qwen', state: 'PENDING' } } : {}),
    }
    const { unmount } = render(
      <InstanceCard instance={instance} startInstance={vi.fn()} stopInstance={vi.fn()} deleteInstance={vi.fn()} editInstance={vi.fn()} />
    )
    try {
      const active = status === 'running' || status === 'pending' || status === 'starting'
      expect(screen.getByTitle(active ? 'Stop instance' : 'Start instance')).toBeInTheDocument()
      // The job line is shown whenever the API reports a job
      expect(screen.queryByTestId('slurm-job') !== null).toBe(withJob)
    } finally {
      unmount()
    }
  })
})
