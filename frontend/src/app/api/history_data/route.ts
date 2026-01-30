import { NextResponse } from 'next/server'
import type { Pick, HistoryData } from '@/lib/types'

let picksStore: Pick[] = []

export async function GET() {
  try {
    const wins = picksStore.filter((p) => p.result === 'W').length
    const losses = picksStore.filter((p) => p.result === 'L').length
    const pending = picksStore.filter((p) => p.result === null).length
    const total = picksStore.length
    const win_rate = wins + losses > 0 ? (wins / (wins + losses)) * 100 : 0

    const response: HistoryData = {
      picks: picksStore.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime()),
      stats: {
        total,
        wins,
        losses,
        pending,
        win_rate: Math.round(win_rate * 10) / 10,
      },
    }

    return NextResponse.json(response)
  } catch (error) {
    console.error('History data error:', error)
    return NextResponse.json(
      { picks: [], stats: { total: 0, wins: 0, losses: 0, pending: 0, win_rate: 0 } },
      { status: 500 }
    )
  }
}

export async function POST(request: Request) {
  try {
    const body = await request.json()

    if (body.action === 'add_pick') {
      const newPick: Pick = {
        id: Date.now(),
        date: body.date || new Date().toISOString().split('T')[0],
        matchup: body.matchup,
        league: body.league,
        pick: body.pick,
        line: body.line,
        edge: body.edge,
        result: null,
        is_lock: body.is_lock || false,
        confidence_tier: body.confidence_tier,
      }
      picksStore.push(newPick)
      return NextResponse.json({ success: true, pick: newPick })
    }

    if (body.action === 'update_result') {
      const pick = picksStore.find((p) => p.id === body.pick_id)
      if (pick) {
        pick.result = body.result
        pick.final_score = body.final_score
        pick.final_total = body.final_total
        return NextResponse.json({ success: true, pick })
      }
      return NextResponse.json({ success: false, error: 'Pick not found' }, { status: 404 })
    }

    return NextResponse.json({ success: false, error: 'Unknown action' }, { status: 400 })
  } catch (error) {
    console.error('History POST error:', error)
    return NextResponse.json({ success: false, error: 'Internal error' }, { status: 500 })
  }
}
