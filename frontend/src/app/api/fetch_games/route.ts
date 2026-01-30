import { NextResponse } from 'next/server'
import { fetchESPNScoreboard, fetchTeamStats } from '@/lib/espn-api'
import { fetchOdds, fetchAltLines, matchOddsToGame } from '@/lib/odds-api'
import { calculateProjectedTotal, calculateEdge, getDirection, getConfidenceTier } from '@/lib/edge-calculator'
import { EDGE_THRESHOLDS, getTeamLogo } from '@/lib/constants'
import type { Game } from '@/lib/types'

const LEAGUES = ['NBA', 'CBB', 'NHL']

export async function POST() {
  try {
    const today = new Date().toISOString().split('T')[0]
    const dateStr = today.replace(/-/g, '')

    const allGames: Game[] = []
    let fetchedCount = 0
    let enrichedCount = 0

    for (const league of LEAGUES) {
      const games = await fetchESPNScoreboard(league, dateStr)
      const [odds, altLines] = await Promise.all([
        fetchOdds(league),
        fetchAltLines(league),
      ])

      fetchedCount += games.length

      for (const game of games) {
        const [awayStats, homeStats] = await Promise.all([
          fetchTeamStats(game.away_team, league),
          fetchTeamStats(game.home_team, league),
        ])

        const gameOdds = matchOddsToGame(game.away_team, game.home_team, odds)
        const gameAltLines = matchOddsToGame(game.away_team, game.home_team, altLines)

        if (gameOdds?.total_line) {
          game.line = gameOdds.total_line
        }

        if (gameAltLines?.alt_lines?.length) {
          const bestAlt = gameAltLines.alt_lines.reduce((best, current) => {
            const currentOdds = game.direction === 'O' ? current.over_odds : current.under_odds
            const bestOdds = game.direction === 'O' ? best.over_odds : best.under_odds
            return (currentOdds || 0) > (bestOdds || 0) ? current : best
          })
          if (bestAlt) {
            game.alt_line = bestAlt.line
            game.alt_odds = game.direction === 'O' ? bestAlt.over_odds : bestAlt.under_odds
          }
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
            enrichedCount++
          }
        }

        game.away_logo = game.away_logo || getTeamLogo(game.away_team, league)
        game.home_logo = game.home_logo || getTeamLogo(game.home_team, league)

        allGames.push(game)
      }
    }

    return NextResponse.json({
      success: true,
      message: `Fetched ${fetchedCount} games, enriched ${enrichedCount} with projections`,
      games_count: allGames.length,
      qualified_count: allGames.filter((g) => g.is_qualified).length,
    })
  } catch (error) {
    console.error('Fetch games error:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to fetch games' },
      { status: 500 }
    )
  }
}
