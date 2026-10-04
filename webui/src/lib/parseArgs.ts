// Splits a string into arguments the way a shell would: whitespace
// separates arguments, single and double quotes group values containing
// spaces or commas (e.g. --tensor-split "3,1").
export function parseArgs(input: string): string[] {
  const args: string[] = []
  let current = ''
  let quote: string | null = null
  let hasToken = false

  for (const ch of input) {
    if (quote) {
      if (ch === quote) {
        quote = null
      } else {
        current += ch
      }
    } else if (ch === '"' || ch === "'") {
      quote = ch
      hasToken = true
    } else if (ch === ' ' || ch === '\t') {
      if (current || hasToken) {
        args.push(current)
        current = ''
        hasToken = false
      }
    } else {
      current += ch
    }
  }

  if (current || hasToken) {
    args.push(current)
  }

  return args
}

// Joins arguments into a command string, quoting any containing whitespace
// or quotes so the result round-trips through parseArgs
export function formatCommand(argv: string[] | undefined): string {
  if (!argv) return ''
  return argv
    .map(arg => (arg === '' || /[\s"']/.test(arg) ? (arg.includes('"') ? `'${arg}'` : `"${arg}"`) : arg))
    .join(' ')
}
