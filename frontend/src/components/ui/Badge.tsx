import type { ConfidenceTier, PickResult, League } from '@/types'
import { CONFIDENCE_COLORS, RESULT_COLORS, LEAGUE_COLORS } from '@/lib/constants'

interface BadgeProps {
  children: React.ReactNode
  variant?: 'default' | 'confidence' | 'result' | 'league' | 'live'
  confidence?: ConfidenceTier
  result?: PickResult
  league?: League
  className?: string
}

export default function Badge({
  children,
  variant = 'default',
  confidence,
  result,
  league,
  className = '',
}: BadgeProps) {
  const getStyles = () => {
    switch (variant) {
      case 'confidence':
        if (confidence && CONFIDENCE_COLORS[confidence]) {
          const colors = CONFIDENCE_COLORS[confidence]
          return `${colors.bg} ${colors.text} ${colors.border} border`
        }
        return 'bg-gray-500/20 text-gray-400 border border-gray-500/50'

      case 'result':
        if (result && RESULT_COLORS[result]) {
          const colors = RESULT_COLORS[result]
          return `${colors.bg} ${colors.text} ${colors.border} border`
        }
        return 'bg-gray-500/20 text-gray-400 border border-gray-500/50'

      case 'league':
        const leagueColor = league ? LEAGUE_COLORS[league] : '#6b7280'
        return `text-white`

      case 'live':
        return 'bg-red-500/20 text-red-400 border border-red-500/50'

      default:
        return 'bg-[var(--border)] text-gray-300'
    }
  }

  const leagueStyle = variant === 'league' && league
    ? { backgroundColor: `${LEAGUE_COLORS[league]}dd` }
    : {}

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getStyles()} ${className}`}
      style={leagueStyle}
    >
      {variant === 'live' && (
        <span className="w-1.5 h-1.5 rounded-full bg-red-500 mr-1.5 animate-pulse" />
      )}
      {children}
    </span>
  )
}
