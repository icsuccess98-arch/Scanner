'use client'

import type { Game, LiveScore as LiveScoreType } from '@/types'
import { Badge, LeagueIcon } from '@/components/ui'
import PickChip from './PickChip'
import LiveScore from './LiveScore'

interface SupermaxHeroProps {
  game: Game
  liveScore?: LiveScoreType
  className?: string
}

export default function SupermaxHero({
  game,
  liveScore,
  className = '',
}: SupermaxHeroProps) {
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
    projected_total,
    ev,
    history_rate,
    bet_pct,
    money_pct,
    has_rlm,
  } = game

  const gameDate = new Date(game_time)
  const timeString = gameDate.toLocaleTimeString('en-US', {
    hour: 'numeric',
    minute: '2-digit',
  })
  const dateString = gameDate.toLocaleDateString('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
  })

  const isLive = liveScore?.is_live || game.is_live
  const isFinal = liveScore?.is_final || game.is_final

  return (
    <div className={`relative overflow-hidden bg-gradient-to-br from-[var(--card)] via-[#1a1528] to-[var(--card)] border border-yellow-500/30 rounded-2xl p-6 ${className}`}>
      {/* Gold shimmer effect */}
      <div className="absolute inset-0 animate-shimmer pointer-events-none" />

      {/* Header */}
      <div className="relative flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <span className="text-3xl">🔒</span>
          <div>
            <h2 className="text-xl font-bold text-[var(--gold)]">SUPERMAX LOCK</h2>
            <p className="text-sm text-gray-400">Highest Edge Pick of the Day</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <LeagueIcon league={league} size="lg" showLabel />
          {isLive && <Badge variant="live">LIVE</Badge>}
        </div>
      </div>

      {/* Main Content */}
      <div className="relative grid md:grid-cols-3 gap-6">
        {/* Matchup */}
        <div className="md:col-span-2">
          <div className="flex items-center justify-between mb-4">
            {/* Away Team */}
            <div className="flex items-center gap-3">
              {away_logo ? (
                <img src={away_logo} alt={away_team} className="w-12 h-12 object-contain" />
              ) : (
                <div className="w-12 h-12 bg-[var(--border)] rounded-full flex items-center justify-center text-xl">
                  {away_team.charAt(0)}
                </div>
              )}
              <span className="text-lg font-bold text-white">{away_team}</span>
            </div>

            {/* VS or Score */}
            <div className="px-4">
              {liveScore && (isLive || isFinal) ? (
                <div className="text-center">
                  <div className="flex items-center gap-3 text-2xl font-bold">
                    <span className={liveScore.away_score > liveScore.home_score ? 'text-white' : 'text-gray-500'}>
                      {liveScore.away_score}
                    </span>
                    <span className="text-gray-600">-</span>
                    <span className={liveScore.home_score > liveScore.away_score ? 'text-white' : 'text-gray-500'}>
                      {liveScore.home_score}
                    </span>
                  </div>
                  {isLive && (
                    <div className="text-xs text-red-400 mt-1">
                      {liveScore.period} {liveScore.clock}
                    </div>
                  )}
                </div>
              ) : (
                <span className="text-gray-600 font-medium">VS</span>
              )}
            </div>

            {/* Home Team */}
            <div className="flex items-center gap-3">
              <span className="text-lg font-bold text-white">{home_team}</span>
              {home_logo ? (
                <img src={home_logo} alt={home_team} className="w-12 h-12 object-contain" />
              ) : (
                <div className="w-12 h-12 bg-[var(--border)] rounded-full flex items-center justify-center text-xl">
                  {home_team.charAt(0)}
                </div>
              )}
            </div>
          </div>

          {/* Time */}
          {!isLive && !isFinal && (
            <p className="text-center text-gray-400 text-sm">
              {dateString} at {timeString}
            </p>
          )}

          {/* Pick */}
          {direction && line && (
            <div className="mt-4 flex justify-center">
              <PickChip
                direction={direction}
                line={line}
                edge={edge}
                confidence="SUPERMAX"
                className="text-lg px-4 py-2"
              />
            </div>
          )}
        </div>

        {/* Stats */}
        <div className="space-y-3">
          <div className="bg-black/20 rounded-lg p-3">
            <p className="text-xs text-gray-500 uppercase mb-1">Edge</p>
            <p className="text-2xl font-bold text-[var(--gold)]">
              {edge?.toFixed(1)}%
            </p>
          </div>

          {ev !== undefined && ev !== null && (
            <div className="bg-black/20 rounded-lg p-3">
              <p className="text-xs text-gray-500 uppercase mb-1">Expected Value</p>
              <p className={`text-xl font-bold ${ev >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                {ev >= 0 ? '+' : ''}{ev.toFixed(2)}
              </p>
            </div>
          )}

          {history_rate !== undefined && history_rate !== null && (
            <div className="bg-black/20 rounded-lg p-3">
              <p className="text-xs text-gray-500 uppercase mb-1">Historical Rate</p>
              <p className="text-xl font-bold text-purple-400">
                {(history_rate * 100).toFixed(0)}%
              </p>
            </div>
          )}

          {(bet_pct || money_pct) && (
            <div className="bg-black/20 rounded-lg p-3">
              <p className="text-xs text-gray-500 uppercase mb-1">Public Action</p>
              <div className="flex items-center gap-4 text-sm">
                {bet_pct && <span className="text-gray-300">{bet_pct}% Bets</span>}
                {money_pct && <span className="text-gray-300">{money_pct}% Money</span>}
              </div>
              {has_rlm && (
                <p className="text-yellow-400 text-xs mt-1 font-medium">
                  Reverse Line Movement Detected
                </p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
