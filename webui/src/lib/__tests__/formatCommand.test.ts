import { describe, expect } from 'vitest'
import { test, fc } from '@fast-check/vitest'
import { parseArgs } from '@/lib/parseArgs'
import { formatCommand } from '@/lib/formatCommand'

describe('formatCommand', () => {
  // A command shown for editing reads back as exactly the same arguments,
  // whatever they contain: spaces, tabs, either kind of quote, or nothing
  test.prop([fc.array(fc.string({ unit: fc.constantFrom('a', 'b', ' ', '\t', '"', "'", '-', '{', '}', '|') }))])(
    'round-trips through parseArgs',
    (argv) => {
      expect(parseArgs(formatCommand(argv))).toEqual(argv)
    }
  )

  // Arguments that need no quoting are shown as typed
  test.prop([fc.array(fc.stringMatching(/^[A-Za-z0-9@._:{}=/-]+$/), { minLength: 1 })])(
    'leaves plain arguments alone',
    (argv) => {
      expect(formatCommand(argv)).toBe(argv.join(' '))
    }
  )
})
