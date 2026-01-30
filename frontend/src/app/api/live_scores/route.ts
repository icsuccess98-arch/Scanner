import { NextResponse } from 'next/server'

export async function GET() {
  try {
    const response = await fetch(`${process.env.BACKEND_URL || 'http://0.0.0.0:5000'}/api/live_scores`, {
      cache: 'no-store'
    })
    const data = await response.json()
    return NextResponse.json(data)
  } catch (error) {
    return NextResponse.json({ error: 'Failed to fetch live scores' }, { status: 500 })
  }
}
