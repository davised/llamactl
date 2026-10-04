// Joins arguments into a command string that parseArgs splits back into
// the same arguments. Arguments with whitespace or quotes (or empty ones)
// are double-quoted; a double quote inside one is written as "'"'" (close,
// single-quoted ", reopen), since parseArgs joins adjacent quoted pieces
// the way a shell does.
export function formatCommand(argv: string[] | undefined): string {
  if (!argv) return ''
  return argv
    .map(arg => (arg === '' || /[\s"']/.test(arg) ? `"${arg.split('"').join(`"'"'"`)}"` : arg))
    .join(' ')
}
