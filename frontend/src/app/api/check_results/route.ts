import { NextResponse } from 'next/server'
import { fetchLiveScores } from '@/lib/espn-api'

export async function POST() {
  try {
    const leagues = ['NBA', 'CBB', 'NHL']
    const allScores: Record<string, { away_score: number; home_score: number; is_final: boolean }> = {}

    for (const league of leagues) {
      const scores = await fetchLiveScores(league)
      Object.assign(allScores, scores)
    }

    const finals = Object.entries(allScores).filter(([, v]) => v.is_final)

    return NextResponse.json({
      success: true,
      checked: finals.length,
      finals: finals.map(([matchup, score]) => ({
        matchup,
        total: score.away_score + score.home_score,
        score: `${score.away_score}-${score.home_score}`,
      })),
    })
  } catch (error) {
    console.error('Check results error:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to check results' },
      { status: 500 }
    )
  }
}
