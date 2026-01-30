interface ProgressBarProps {
  value: number
  max?: number
  label?: string
  showValue?: boolean
  size?: 'sm' | 'md' | 'lg'
  color?: 'gold' | 'purple' | 'green' | 'red'
  className?: string
}

export default function ProgressBar({
  value,
  max = 100,
  label,
  showValue = true,
  size = 'md',
  color = 'gold',
  className = '',
}: ProgressBarProps) {
  const percentage = Math.min((value / max) * 100, 100)

  const heights = {
    sm: 'h-1.5',
    md: 'h-2.5',
    lg: 'h-4',
  }

  const colors = {
    gold: 'bg-[var(--gold)]',
    purple: 'bg-[var(--purple)]',
    green: 'bg-green-500',
    red: 'bg-red-500',
  }

  return (
    <div className={className}>
      {(label || showValue) && (
        <div className="flex justify-between items-center mb-1">
          {label && <span className="text-sm text-gray-400">{label}</span>}
          {showValue && (
            <span className="text-sm font-medium text-white">
              {percentage.toFixed(0)}%
            </span>
          )}
        </div>
      )}
      <div className={`w-full bg-[var(--border)] rounded-full ${heights[size]} overflow-hidden`}>
        <div
          className={`${heights[size]} ${colors[color]} rounded-full transition-all duration-500`}
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  )
}
