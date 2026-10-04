import { describe, expect, it } from 'vitest'
import { formatCommand, parseArgs } from '@/lib/parseArgs'

describe('parseArgs', () => {
  it('splits on whitespace', () => {
    expect(parseArgs('--delay=30s --port 8053')).toEqual(['--delay=30s', '--port', '8053'])
  })

  it('collapses repeated whitespace', () => {
    expect(parseArgs('  serve   --port   {port}  ')).toEqual(['serve', '--port', '{port}'])
  })

  it('keeps quoted values as single arguments', () => {
    expect(parseArgs('--tensor-split "3,1"')).toEqual(['--tensor-split', '3,1'])
    expect(parseArgs("--msg 'hello world'")).toEqual(['--msg', 'hello world'])
  })

  it('handles quotes glued to the value', () => {
    expect(parseArgs('--split="1, 2"')).toEqual(['--split=1, 2'])
  })

  it('returns an empty array for empty or blank input', () => {
    expect(parseArgs('')).toEqual([])
    expect(parseArgs('   ')).toEqual([])
  })
})

describe('formatCommand', () => {
  it('joins plain arguments with spaces', () => {
    expect(formatCommand(['ssh', 'gpu01', 'systemctl', '--user', 'start', 'vllm'])).toBe('ssh gpu01 systemctl --user start vllm')
  })

  it('round-trips arguments with spaces and quotes through parseArgs', () => {
    const argv = ['sh', '-c', 'echo "hi there"', "it's", '']
    expect(parseArgs(formatCommand(argv))).toEqual(argv)
  })

  it('returns an empty string for undefined', () => {
    expect(formatCommand(undefined)).toBe('')
  })
})
