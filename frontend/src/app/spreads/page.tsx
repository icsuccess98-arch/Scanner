'use client'

import { useState, useMemo } from 'react'
import { Container } from '@/components/layout'
import { Button, Badge } from '@/components/ui'
import { GameCard } from '@/components/game'
import { useGames, useFetchOdds, useLiveScores } from '@/hooks'
import type { Game, League } from '@/types'

export default function SpreadsPage() {
  const { data, isLoading, error } = useGames()
  const { data: liveScores } = useLiveScores()
  const fetchOdds = useFetchOdds()

  const [selectedLeague, setSelectedLeague] = useState<League | 'all'>('all')

  // Group all games by league (not just qualified)
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
      if (grouped[game.league]) {
        grouped[game.league].push(game)
      }
    })

    return grouped
  }, [data?.games])

  // Leagues with games (in display order)
  const leagueOrder: League[] = ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']
  const activeLeagues = leagueOrder.filter(
    (league) => gamesByLeague[league]?.length > 0
  )

  // Filter games based on selected league
  const filteredGames = useMemo(() => {
    if (selectedLeague === 'all') {
      return data?.games || []
    }
    return gamesByLeague[selectedLeague] || []
  }, [data?.games, gamesByLeague, selectedLeague])

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
          <h2 className="text-xl font-bold text-red-400 mb-2">Error Loading Games</h2>
          <p className="text-gray-400 mb-4">
            {error instanceof Error ? error.message : 'Failed to load games'}
          </p>
          <Button onClick={() => window.location.reload()}>Retry</Button>
        </div>
      </Container>
    )
  }

  return (
    <Container className="py-6 space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white">All Games</h1>
          <p className="text-gray-400 text-sm">
            {filteredGames.length} games available
          </p>
        </div>

        <Button
          variant="outline"
          size="sm"
          onClick={() => fetchOdds.mutate()}
          loading={fetchOdds.isPending}
        >
          Refresh Odds
        </Button>
      </div>

      {/* League Tabs */}
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setSelectedLeague('all')}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            selectedLeague === 'all'
              ? 'bg-[var(--purple)] text-white'
              : 'bg-[var(--card)] border border-[var(--border)] text-gray-400 hover:text-white hover:border-[var(--purple)]'
          }`}
        >
          All
          <span className="ml-2 text-xs opacity-75">
            ({data?.games?.length || 0})
          </span>
        </button>
        {activeLeagues.map((league) => (
          <button
            key={league}
            onClick={() => setSelectedLeague(league)}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              selectedLeague === league
                ? 'bg-[var(--purple)] text-white'
                : 'bg-[var(--card)] border border-[var(--border)] text-gray-400 hover:text-white hover:border-[var(--purple)]'
            }`}
          >
            {league}
            <span className="ml-2 text-xs opacity-75">
              ({gamesByLeague[league].length})
            </span>
          </button>
        ))}
      </div>

      {/* Games Grid */}
      {filteredGames.length > 0 ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {filteredGames.map((game) => (
            <GameCard
              key={game.id}
              game={game}
              liveScore={liveScores?.[game.id.toString()]}
            />
          ))}
        </div>
      ) : (
        <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-8 text-center">
          <p className="text-gray-400">No games found for the selected league.</p>
        </div>
      )}
    </Container>
  )
}
