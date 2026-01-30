import { NextResponse } from 'next/server'

export async function POST() {
  try {
    const response = await fetch(`${process.env.BACKEND_URL || 'http://0.0.0.0:5000'}/post_discord`, {
      method: 'POST'
    })
    const data = await response.json()
    return NextResponse.json(data)
  } catch (error) {
    return NextResponse.json({ error: 'Failed to post to discord' }, { status: 500 })
  }
}
