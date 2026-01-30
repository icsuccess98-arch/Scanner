import { NextResponse } from 'next/server'

export async function POST() {
  try {
    const response = await fetch(`${process.env.BACKEND_URL || 'http://0.0.0.0:5000'}/check_results`, {
      method: 'POST'
    })
    const data = await response.json()
    return NextResponse.json(data)
  } catch (error) {
    return NextResponse.json({ error: 'Failed to check results' }, { status: 500 })
  }
}
