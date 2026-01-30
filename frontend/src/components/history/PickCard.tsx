'use client'

import type { Pick } from '@/types'
import { Badge, LeagueIcon, Button } from '@/components/ui'

interface PickCardProps {
  pick: Pick
  onUpdateResult?: (result: 'W' | 'L' | 'P') => void
  loading?: boolean
  className?: string
}

export default function PickCard({
  pick,
  onUpdateResult,
  loading = false,
  className = '',
}: PickCardProps) {
  const {
    matchup,
    league,
    pick: pickText,
    line,
    edge,
    result,
    is_lock,
    date,
    confidence_tier,
  } = pick

  const resultStyles = {
    W: 'bg-green-500/20 border-green-500/50 text-green-400',
    L: 'bg-red-500/20 border-red-500/50 text-red-400',
    P: 'bg-gray-500/20 border-gray-500/50 text-gray-400',
  }

  return (
    <div className={`bg-[var(--card)] border border-[var(--border)] rounded-xl p-4 ${className}`}>
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <LeagueIcon league={league} size="sm" />
          <Badge variant="league" league={league}>{league}</Badge>
          {is_lock && (
            <Badge variant="confidence" confidence="SUPERMAX">LOCK</Badge>
          )}
          {confidence_tier && !is_lock && (
            <Badge variant="confidence" confidence={confidence_tier}>
              {confidence_tier}
            </Badge>
          )}
        </div>
        <span className="text-xs text-gray-500">{date}</span>
      </div>

      {/* Matchup */}
      <p className="font-medium text-white mb-2">{matchup}</p>

      {/* Pick Details */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className="text-[var(--gold)] font-bold">{pickText}</span>
          <span className="text-gray-400 font-mono">{line}</span>
        </div>
        <span className="text-sm text-gray-400">
          Edge: <span className="text-white">{edge.toFixed(1)}%</span>
        </span>
      </div>

      {/* Result */}
      <div className="flex items-center justify-between pt-3 border-t border-[var(--border)]">
        {result ? (
          <span className={`px-3 py-1 rounded-lg border font-bold ${resultStyles[result]}`}>
            {result === 'W' ? 'WIN' : result === 'L' ? 'LOSS' : 'PUSH'}
          </span>
        ) : (
          <span className="text-gray-500 text-sm">Pending</span>
        )}

        {/* Result Update Buttons */}
        {onUpdateResult && !result && (
          <div className="flex gap-2">
            <Button
              size="sm"
              variant="ghost"
              onClick={() => onUpdateResult('W')}
              disabled={loading}
              className="text-green-400 hover:bg-green-500/20"
            >
              W
            </Button>
            <Button
              size="sm"
              variant="ghost"
              onClick={() => onUpdateResult('L')}
              disabled={loading}
              className="text-red-400 hover:bg-red-500/20"
            >
              L
            </Button>
            <Button
              size="sm"
              variant="ghost"
              onClick={() => onUpdateResult('P')}
              disabled={loading}
              className="text-gray-400 hover:bg-gray-500/20"
            >
              P
            </Button>
          </div>
        )}
      </div>
    </div>
  )
}
