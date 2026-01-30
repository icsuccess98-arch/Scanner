import { NextResponse } from 'next/server'

export async function POST() {
  try {
    const response = await fetch(`${process.env.BACKEND_URL || 'http://0.0.0.0:5000'}/fetch_games`, {
      method: 'POST'
    })
    const data = await response.json()
    return NextResponse.json(data)
  } catch (error) {
    return NextResponse.json({ error: 'Failed to fetch games' }, { status: 500 })
  }
}
