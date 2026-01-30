import type { LiveScore as LiveScoreType } from '@/types'

interface LiveScoreProps {
  score: LiveScoreType
  awayTeam: string
  homeTeam: string
  className?: string
}

export default function LiveScore({
  score,
  awayTeam,
  homeTeam,
  className = '',
}: LiveScoreProps) {
  const { away_score, home_score, period, clock, is_final, is_live } = score

  const total = away_score + home_score

  return (
    <div className={`text-center ${className}`}>
      {/* Status */}
      {is_final ? (
        <span className="text-xs font-medium text-gray-500 uppercase">Final</span>
      ) : is_live ? (
        <div className="flex items-center justify-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
          <span className="text-xs font-medium text-red-400">
            {period} {clock}
          </span>
        </div>
      ) : null}

      {/* Scores */}
      <div className="flex items-center justify-center gap-4 mt-1">
        <div className="text-center">
          <span className="text-xs text-gray-500 block">{awayTeam}</span>
          <span className={`text-xl font-bold ${away_score > home_score ? 'text-white' : 'text-gray-400'}`}>
            {away_score}
          </span>
        </div>
        <span className="text-gray-600">-</span>
        <div className="text-center">
          <span className="text-xs text-gray-500 block">{homeTeam}</span>
          <span className={`text-xl font-bold ${home_score > away_score ? 'text-white' : 'text-gray-400'}`}>
            {home_score}
          </span>
        </div>
      </div>

      {/* Current Total */}
      <div className="mt-1">
        <span className="text-xs text-gray-500">Total: </span>
        <span className="text-sm font-mono text-[var(--gold)]">{total}</span>
      </div>
    </div>
  )
}
