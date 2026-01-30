interface StatCardProps {
  label: string
  value: string | number
  subValue?: string
  icon?: React.ReactNode
  trend?: 'up' | 'down' | 'neutral'
  className?: string
}

export default function StatCard({
  label,
  value,
  subValue,
  icon,
  trend,
  className = '',
}: StatCardProps) {
  const trendColors = {
    up: 'text-green-400',
    down: 'text-red-400',
    neutral: 'text-gray-400',
  }

  return (
    <div className={`bg-[var(--card)] border border-[var(--border)] rounded-xl p-4 ${className}`}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-gray-400 mb-1">{label}</p>
          <p className="text-2xl font-bold text-white">{value}</p>
          {subValue && (
            <p className={`text-sm mt-1 ${trend ? trendColors[trend] : 'text-gray-500'}`}>
              {trend === 'up' && '+'}{subValue}
            </p>
          )}
        </div>
        {icon && (
          <div className="text-[var(--gold)] text-2xl">
            {icon}
          </div>
        )}
      </div>
    </div>
  )
}
