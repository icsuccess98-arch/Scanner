'use client'

import type { Game, LiveScore as LiveScoreType } from '@/types'
import { Badge, LeagueIcon } from '@/components/ui'
import PickChip from './PickChip'
import LiveScore from './LiveScore'

interface GameCardProps {
  game: Game
  liveScore?: LiveScoreType
  onClick?: () => void
  className?: string
}

export default function GameCard({
  game,
  liveScore,
  onClick,
  className = '',
}: GameCardProps) {
  const {
    league,
    away_team,
    home_team,
    away_logo,
    home_logo,
    game_time,
    line,
    edge,
    direction,
    confidence_tier,
    projected_total,
    bet_pct,
    money_pct,
    has_rlm,
  } = game

  const gameDate = new Date(game_time)
  const timeString = gameDate.toLocaleTimeString('en-US', {
    hour: 'numeric',
    minute: '2-digit',
  })

  const isLive = liveScore?.is_live || game.is_live
  const isFinal = liveScore?.is_final || game.is_final

  return (
    <div
      className={`bg-[var(--card)] border border-[var(--border)] rounded-xl p-4 transition-all hover:border-[var(--purple)] ${
        confidence_tier === 'SUPERMAX' ? 'ring-1 ring-yellow-500/30' : ''
      } ${onClick ? 'cursor-pointer' : ''} ${className}`}
      onClick={onClick}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <LeagueIcon league={league} size="sm" />
          <Badge variant="league" league={league}>{league}</Badge>
          {confidence_tier && (
            <Badge variant="confidence" confidence={confidence_tier}>
              {confidence_tier}
            </Badge>
          )}
        </div>
        <div className="flex items-center gap-2">
          {isLive && <Badge variant="live">LIVE</Badge>}
          {isFinal && <span className="text-xs text-gray-500">FINAL</span>}
          {!isLive && !isFinal && (
            <span className="text-xs text-gray-400">{timeString}</span>
          )}
        </div>
      </div>

      {/* Teams */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-2">
            {away_logo && (
              <img src={away_logo} alt={away_team} className="w-6 h-6 object-contain" />
            )}
            <span className="font-medium text-white">{away_team}</span>
          </div>
          <div className="flex items-center gap-2">
            {home_logo && (
              <img src={home_logo} alt={home_team} className="w-6 h-6 object-contain" />
            )}
            <span className="font-medium text-white">{home_team}</span>
          </div>
        </div>

        {/* Live Score or Line */}
        <div className="text-right">
          {liveScore && (isLive || isFinal) ? (
            <LiveScore
              score={liveScore}
              awayTeam={away_team}
              homeTeam={home_team}
            />
          ) : (
            <div>
              {line && (
                <div className="text-lg font-mono text-white">{line}</div>
              )}
              {projected_total && (
                <div className="text-xs text-gray-500">
                  Proj: {projected_total.toFixed(1)}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Pick */}
      {direction && line && (
        <div className="flex items-center justify-between pt-3 border-t border-[var(--border)]">
          <PickChip
            direction={direction}
            line={line}
            edge={edge}
            confidence={confidence_tier}
          />

          {/* Betting percentages */}
          {(bet_pct || money_pct) && (
            <div className="flex items-center gap-3 text-xs">
              {bet_pct && (
                <div>
                  <span className="text-gray-500">Bets: </span>
                  <span className="text-gray-300">{bet_pct}%</span>
                </div>
              )}
              {money_pct && (
                <div>
                  <span className="text-gray-500">Money: </span>
                  <span className="text-gray-300">{money_pct}%</span>
                </div>
              )}
              {has_rlm && (
                <span className="text-yellow-400 font-medium">RLM</span>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
