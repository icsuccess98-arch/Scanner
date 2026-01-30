import { NextResponse } from 'next/server'
import { fetchLiveScores } from '@/lib/espn-api'
import type { LiveScore } from '@/lib/types'

const LEAGUES = ['NBA', 'CBB', 'NHL']

export async function GET() {
  try {
    const allScores: Record<string, LiveScore> = {}

    const scorePromises = LEAGUES.map(async (league) => {
      const scores = await fetchLiveScores(league)
      return scores
    })

    const results = await Promise.all(scorePromises)
    results.forEach((scores) => {
      Object.assign(allScores, scores)
    })

    return NextResponse.json({
      live_scores: allScores,
      timestamp: Date.now(),
    })
  } catch (error) {
    console.error('Live scores error:', error)
    return NextResponse.json(
      { live_scores: {}, error: 'Failed to fetch live scores' },
      { status: 500 }
    )
  }
}
