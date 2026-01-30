import type { League } from '@/types'
import { LEAGUE_ICONS, LEAGUE_COLORS } from '@/lib/constants'

interface LeagueIconProps {
  league: League
  size?: 'sm' | 'md' | 'lg'
  showLabel?: boolean
  className?: string
}

export default function LeagueIcon({
  league,
  size = 'md',
  showLabel = false,
  className = '',
}: LeagueIconProps) {
  const sizes = {
    sm: 'text-base',
    md: 'text-xl',
    lg: 'text-2xl',
  }

  return (
    <div className={`inline-flex items-center gap-1.5 ${className}`}>
      <span className={sizes[size]}>{LEAGUE_ICONS[league]}</span>
      {showLabel && (
        <span
          className="text-xs font-bold px-1.5 py-0.5 rounded"
          style={{ backgroundColor: LEAGUE_COLORS[league], color: 'white' }}
        >
          {league}
        </span>
      )}
    </div>
  )
}
