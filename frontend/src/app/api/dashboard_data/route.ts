import { NextResponse } from 'next/server'
import { fetchESPNScoreboard, fetchTeamStats } from '@/lib/espn-api'
import { fetchOdds, matchOddsToGame } from '@/lib/odds-api'
import { calculateProjectedTotal, calculateEdge, getDirection, getConfidenceTier, findSupermaxLock } from '@/lib/edge-calculator'
import { EDGE_THRESHOLDS, getTeamLogo } from '@/lib/constants'
import type { Game } from '@/lib/types'

const LEAGUES = ['NBA', 'CBB', 'NHL']

let cachedData: { games: Game[]; timestamp: number } | null = null
const CACHE_TTL = 30000

export async function GET() {
  try {
    if (cachedData && Date.now() - cachedData.timestamp < CACHE_TTL) {
      const qualifiedGames = cachedData.games.filter((g) => g.is_qualified)
      const supermaxLock = findSupermaxLock(cachedData.games)
      if (supermaxLock) supermaxLock.is_supermax = true

      return NextResponse.json({
        games: cachedData.games,
        supermax_lock: supermaxLock,
        total_games: cachedData.games.length,
        qualified_games: qualifiedGames.length,
        today: new Date().toISOString().split('T')[0],
      })
    }

    const today = new Date().toISOString().split('T')[0]
    const dateStr = today.replace(/-/g, '')

    const allGames: Game[] = []

    const leaguePromises = LEAGUES.map(async (league) => {
      try {
        const [games, odds] = await Promise.all([
          fetchESPNScoreboard(league, dateStr),
          fetchOdds(league),
        ])

        const enrichedGames = await Promise.all(
          games.map(async (game) => {
            try {
              const [awayStats, homeStats] = await Promise.all([
                fetchTeamStats(game.away_team, league).catch(() => null),
                fetchTeamStats(game.home_team, league).catch(() => null),
              ])

              const gameOdds = matchOddsToGame(game.away_team, game.home_team, odds)

              if (gameOdds?.total_line) {
                game.line = gameOdds.total_line
              }

              if (awayStats && homeStats && awayStats.ppg && homeStats.ppg) {
                game.away_ppg = awayStats.ppg
                game.home_ppg = homeStats.ppg
                game.away_opp_ppg = awayStats.opp_ppg
                game.home_opp_ppg = homeStats.opp_ppg

                const projected = calculateProjectedTotal(
                  awayStats.ppg,
                  homeStats.ppg,
                  awayStats.opp_ppg,
                  homeStats.opp_ppg,
                  undefined,
                  undefined,
                  league
                )
                game.projected_total = projected

                if (game.line) {
                  game.edge = calculateEdge(projected, game.line)
                  game.direction = getDirection(projected, game.line)
                  game.confidence_tier = getConfidenceTier(game.edge, league)

                  const threshold = EDGE_THRESHOLDS[league] || 8.0
                  game.is_qualified = game.edge >= threshold
                }
              }

              game.away_logo = game.away_logo || getTeamLogo(game.away_team, league)
              game.home_logo = game.home_logo || getTeamLogo(game.home_team, league)

              return game
            } catch (err) {
              console.error(`Error enriching game ${game.away_team}@${game.home_team}:`, err)
              game.away_logo = game.away_logo || getTeamLogo(game.away_team, league)
              game.home_logo = game.home_logo || getTeamLogo(game.home_team, league)
              return game
            }
          })
        )

        return enrichedGames
      } catch (err) {
        console.error(`Error fetching ${league} games:`, err)
        return []
      }
    })

    const leagueResults = await Promise.all(leaguePromises)
    leagueResults.forEach((games) => allGames.push(...games))

    cachedData = { games: allGames, timestamp: Date.now() }

    const qualifiedGames = allGames.filter((g) => g.is_qualified)
    const supermaxLock = findSupermaxLock(allGames)

    if (supermaxLock) {
      supermaxLock.is_supermax = true
    }

    return NextResponse.json({
      games: allGames,
      supermax_lock: supermaxLock,
      total_games: allGames.length,
      qualified_games: qualifiedGames.length,
      today,
    })
  } catch (error) {
    console.error('Dashboard data error:', error)
    return NextResponse.json(
      { error: 'Failed to fetch dashboard data', games: [], supermax_lock: null, total_games: 0, qualified_games: 0 },
      { status: 500 }
    )
  }
}
