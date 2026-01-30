import { NextResponse } from 'next/server'

export async function POST(request: Request) {
  try {
    const body = await request.json().catch(() => ({}))

    const webhookUrl = process.env.DISCORD_WEBHOOK_URL
    if (!webhookUrl) {
      return NextResponse.json(
        { success: false, error: 'Discord webhook not configured' },
        { status: 400 }
      )
    }

    const game = body.game || body.supermax_lock
    if (!game) {
      return NextResponse.json(
        { success: false, error: 'No game data provided' },
        { status: 400 }
      )
    }

    const direction = game.direction === 'O' ? 'OVER' : 'UNDER'
    const edgeText = game.edge?.toFixed(1) || '0.0'
    const tier = game.confidence_tier || 'SUPERMAX'

    const embed = {
      title: `🔒 ${tier} LOCK`,
      description: `**${game.away_team} @ ${game.home_team}**`,
      color: game.direction === 'O' ? 0x10B981 : 0xEF4444,
      fields: [
        {
          name: '📊 Pick',
          value: `**${direction} ${game.line}**`,
          inline: true,
        },
        {
          name: '📈 Edge',
          value: `**+${edgeText} pts**`,
          inline: true,
        },
        {
          name: '🏀 League',
          value: game.league,
          inline: true,
        },
        {
          name: '🎯 Projected Total',
          value: game.projected_total?.toFixed(1) || '--',
          inline: true,
        },
        {
          name: '⏰ Game Time',
          value: game.game_time || '--',
          inline: true,
        },
      ],
      footer: {
        text: `730's Locks | ${new Date().toLocaleDateString()}`,
      },
      timestamp: new Date().toISOString(),
    }

    const discordPayload = {
      username: "730's Locks",
      avatar_url: 'https://i.imgur.com/AfFp7pu.png',
      embeds: [embed],
    }

    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(discordPayload),
    })

    if (!response.ok) {
      throw new Error(`Discord API error: ${response.status}`)
    }

    return NextResponse.json({ success: true, message: 'Posted to Discord' })
  } catch (error) {
    console.error('Discord post error:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to post to Discord' },
      { status: 500 }
    )
  }
}
