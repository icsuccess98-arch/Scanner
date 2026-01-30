'use client'

import { useMemo } from 'react'
import { Container } from '@/components/layout'
import { Button, StatCard, Badge } from '@/components/ui'
import { GameCard, SupermaxHero } from '@/components/game'
import { useGames, useFetchGames, useFetchOdds, usePostDiscord, useLiveScores } from '@/hooks'
import type { Game, League } from '@/types'

export default function Dashboard() {
  const { data, isLoading, error } = useGames()
  const { data: liveScores } = useLiveScores()
  const fetchGames = useFetchGames()
  const fetchOdds = useFetchOdds()
  const postDiscord = usePostDiscord()

  // Group games by league
  const gamesByLeague = useMemo((): Record<League, Game[]> => {
    const grouped: Record<League, Game[]> = {
      NBA: [],
      CBB: [],
      NFL: [],
      CFB: [],
      NHL: [],
    }

    if (!data?.games) return grouped

    data.games.forEach((game) => {
      if (game.is_qualified && grouped[game.league]) {
        grouped[game.league].push(game)
      }
    })

    return grouped
  }, [data?.games])

  // Find supermax lock
  const supermaxGame = useMemo(() => {
    if (!data?.games) return null
    return data.games.find((g) => g.confidence_tier === 'SUPERMAX') || data.supermax
  }, [data])

  // Leagues with games (in display order)
  const leagueOrder: League[] = ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']
  const activeLeagues = leagueOrder.filter(
    (league) => gamesByLeague[league]?.length > 0
  )

  if (isLoading) {
    return (
      <Container className="py-8">
        <div className="flex items-center justify-center min-h-[50vh]">
          <div className="text-center">
            <div className="w-12 h-12 border-4 border-[var(--gold)] border-t-transparent rounded-full animate-spin mx-auto mb-4" />
            <p className="text-gray-400">Loading games...</p>
          </div>
        </div>
      </Container>
    )
  }

  if (error) {
    return (
      <Container className="py-8">
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-6 text-center">
          <h2 className="text-xl font-bold text-red-400 mb-2">Error Loading Data</h2>
          <p className="text-gray-400 mb-4">
            {error instanceof Error ? error.message : 'Failed to load games'}
          </p>
          <Button onClick={() => window.location.reload()}>Retry</Button>
        </div>
      </Container>
    )
  }

  const qualifiedCount = data?.games?.filter((g) => g.is_qualified).length || 0

  return (
    <Container className="py-6 space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white">Today's Picks</h1>
          <p className="text-gray-400 text-sm">
            {qualifiedCount} qualified {qualifiedCount === 1 ? 'game' : 'games'} found
          </p>
        </div>

        {/* Action Buttons */}
        <div className="flex flex-wrap gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => fetchGames.mutate()}
            loading={fetchGames.isPending}
          >
            Fetch Games
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => fetchOdds.mutate()}
            loading={fetchOdds.isPending}
          >
            Refresh Odds
          </Button>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => postDiscord.mutate()}
            loading={postDiscord.isPending}
            disabled={!supermaxGame}
          >
            Post to Discord
          </Button>
        </div>
      </div>

      {/* Stats Cards */}
      {data?.stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Total Games"
            value={data.stats.total_games || 0}
          />
          <StatCard
            label="Qualified"
            value={qualifiedCount}
          />
          <StatCard
            label="Win Rate"
            value={data.stats.win_rate ? `${(data.stats.win_rate * 100).toFixed(0)}%` : 'N/A'}
            trend={data.stats.win_rate && data.stats.win_rate >= 0.55 ? 'up' : 'neutral'}
          />
          <StatCard
            label="Record"
            value={
              data.stats.record
                ? `${data.stats.record.wins}-${data.stats.record.losses}-${data.stats.record.pushes}`
                : 'N/A'
            }
          />
        </div>
      )}

      {/* Supermax Hero */}
      {supermaxGame && (
        <SupermaxHero
          game={supermaxGame}
          liveScore={liveScores?.[supermaxGame.id.toString()]}
        />
      )}

      {/* Games by League */}
      {activeLeagues.length > 0 ? (
        activeLeagues.map((league) => (
          <section key={league}>
            <div className="flex items-center gap-2 mb-4">
              <h2 className="text-lg font-bold text-white">{league}</h2>
              <Badge variant="default">
                {gamesByLeague[league].length} {gamesByLeague[league].length === 1 ? 'pick' : 'picks'}
              </Badge>
            </div>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {gamesByLeague[league].map((game) => (
                <GameCard
                  key={game.id}
                  game={game}
                  liveScore={liveScores?.[game.id.toString()]}
                />
              ))}
            </div>
          </section>
        ))
      ) : (
        <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-8 text-center">
          <p className="text-gray-400 mb-4">No qualified picks found for today.</p>
          <Button onClick={() => fetchGames.mutate()} loading={fetchGames.isPending}>
            Fetch Today's Games
          </Button>
        </div>
      )}

      {/* Last Updated */}
      {data?.last_updated && (
        <p className="text-xs text-gray-500 text-center">
          Last updated: {new Date(data.last_updated).toLocaleString()}
        </p>
      )}
    </Container>
  )
}
