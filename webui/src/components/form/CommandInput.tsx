import type React from 'react'
import { useState } from 'react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { parseArgs } from '@/lib/parseArgs'
import { formatCommand } from '@/lib/formatCommand'

interface CommandInputProps {
  id: string
  label: string
  value: string[] | undefined
  onChange: (value: string[] | undefined) => void
  placeholder?: string
  description?: string
}

// Edits a command as text and stores it as an argv array. The raw text is
// kept locally so typing a space isn't immediately normalized away.
const CommandInput: React.FC<CommandInputProps> = ({
  id,
  label,
  value,
  onChange,
  placeholder,
  description
}) => {
  const [raw, setRaw] = useState(() => formatCommand(value))

  const handleChange = (text: string) => {
    setRaw(text)
    const argv = parseArgs(text)
    onChange(argv.length > 0 ? argv : undefined)
  }

  return (
    <div className="grid gap-2">
      <Label htmlFor={id}>{label}</Label>
      <Input
        id={id}
        type="text"
        value={raw}
        onChange={(e) => handleChange(e.target.value)}
        placeholder={placeholder}
        className="font-mono text-sm"
      />
      {description && (
        <p className="text-sm text-muted-foreground">{description}</p>
      )}
    </div>
  )
}

export default CommandInput
