import type { ConfidenceTier } from '@/types'

interface PickChipProps {
  direction: 'O' | 'U'
  line: number
  edge?: number | null
  confidence?: ConfidenceTier
  className?: string
}

export default function PickChip({
  direction,
  line,
  edge,
  confidence,
  className = '',
}: PickChipProps) {
  const isOver = direction === 'O'
  const label = isOver ? 'OVER' : 'UNDER'

  const confidenceStyles = {
    SUPERMAX: 'bg-gradient-to-r from-yellow-500/30 to-amber-500/30 border-yellow-500/60 text-yellow-400',
    HIGH: 'bg-purple-500/20 border-purple-500/50 text-purple-400',
    MEDIUM: 'bg-blue-500/20 border-blue-500/50 text-blue-400',
    LOW: 'bg-gray-500/20 border-gray-500/50 text-gray-400',
  }

  const baseStyle = confidence && confidenceStyles[confidence]
    ? confidenceStyles[confidence]
    : isOver
      ? 'bg-green-500/20 border-green-500/50 text-green-400'
      : 'bg-red-500/20 border-red-500/50 text-red-400'

  return (
    <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border ${baseStyle} ${className}`}>
      <span className="font-bold text-sm">{label}</span>
      <span className="font-mono text-sm">{line}</span>
      {edge !== undefined && edge !== null && (
        <span className="text-xs opacity-75">
          ({edge.toFixed(1)}%)
        </span>
      )}
    </div>
  )
}
