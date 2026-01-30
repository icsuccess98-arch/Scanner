'use client'

import { useState, useEffect, useCallback } from 'react'
import api from '@/lib/api'
import { Header, MobileNav } from '@/components/layout'

interface Game {
  id: number
  league: string
  away_team: string
  home_team: string
  away_logo?: string
  home_logo?: string
  away_record?: string
  home_record?: string
  game_time: string
  line: number | null
  edge: number | null
  direction: 'O' | 'U' | null
  is_qualified: boolean
  projected_total: number | null
  is_supermax?: boolean
}

interface LiveScore {
  away_score: number
  home_score: number
  period: string
  clock: string
  status: string
  is_final: boolean
}

export default function Dashboard() {
  const [games, setGames] = useState<Game[]>([])
  const [supermaxLock, setSupermaxLock] = useState<Game | null>(null)
  const [totalGames, setTotalGames] = useState(0)
  const [qualifiedGames, setQualifiedGames] = useState(0)
  const [liveScores, setLiveScores] = useState<Record<string, LiveScore>>({})
  const [loading, setLoading] = useState(true)
  const [fetching, setFetching] = useState(false)
  const [posting, setPosting] = useState(false)
  const [today, setToday] = useState('')

  // Format today's date
  useEffect(() => {
    const date = new Date()
    setToday(date.toLocaleDateString('en-US', {
      weekday: 'long',
      month: 'long',
      day: 'numeric'
    }))
  }, [])

  // Fetch dashboard data
  const fetchDashboard = useCallback(async () => {
    try {
      const data = await api.getDashboardData()
      setGames(data.games || [])
      setSupermaxLock(data.supermax_lock || null)
      setTotalGames(data.total_games || 0)
      setQualifiedGames(data.qualified_games || 0)
    } catch (err) {
      console.error('Failed to fetch dashboard:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  // Fetch live scores
  const fetchLiveScores = useCallback(async () => {
    try {
      const data = await api.getLiveScores()
      setLiveScores(data.live_scores || {})
    } catch (err) {
      console.error('Failed to fetch live scores:', err)
    }
  }, [])

  // Initial load
  useEffect(() => {
    fetchDashboard()
    fetchLiveScores()
  }, [fetchDashboard, fetchLiveScores])

  // Poll live scores every 2.5 seconds
  useEffect(() => {
    const interval = setInterval(fetchLiveScores, 2500)
    return () => clearInterval(interval)
  }, [fetchLiveScores])

  // Poll dashboard every 30 seconds
  useEffect(() => {
    const interval = setInterval(fetchDashboard, 30000)
    return () => clearInterval(interval)
  }, [fetchDashboard])

  // Handle fetch games
  const handleFetchGames = async () => {
    setFetching(true)
    try {
      await api.fetchGames()
      await fetchDashboard()
    } catch (err) {
      console.error('Failed to fetch games:', err)
    } finally {
      setFetching(false)
    }
  }

  // Handle post to Discord
  const handlePostDiscord = async () => {
    setPosting(true)
    try {
      await api.postDiscord()
    } catch (err) {
      console.error('Failed to post to Discord:', err)
    } finally {
      setPosting(false)
    }
  }

  // Group games by league
  const gamesByLeague = games.reduce((acc, game) => {
    if (!acc[game.league]) acc[game.league] = []
    acc[game.league].push(game)
    return acc
  }, {} as Record<string, Game[]>)

  const leagueOrder = ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']

  // Get live score for a game
  const getLiveScore = (game: Game): LiveScore | null => {
    const key = `${game.away_team}@${game.home_team}`
    return liveScores[key] || null
  }

  if (loading) {
    return (
      <>
        <Header />
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '50vh' }}>
          <div className="spinner" />
        </div>
        <MobileNav />
      </>
    )
  }

  return (
    <>
      <Header />

      {/* Header Section with Control Panel */}
      <div className="header-section">
        <div className="control-panel">
          <div className="date-display">{today}</div>

          <div className="stats-grid">
            <div className="stat-card">
              <div className="value">{totalGames}</div>
              <div className="label">Total Games</div>
            </div>
            <div className="stat-card qualified">
              <div className="value">{qualifiedGames}</div>
              <div className="label">Qualified</div>
            </div>
          </div>

          <div className="action-buttons">
            <button
              className="btn-fetch"
              onClick={handleFetchGames}
              disabled={fetching}
            >
              <i className="bi bi-arrow-repeat" style={{ marginRight: '0.5rem' }} />
              {fetching ? 'Loading...' : 'Fetch Games'}
            </button>
            <button
              className="btn-discord"
              onClick={handlePostDiscord}
              disabled={posting || !supermaxLock}
            >
              <i className="bi bi-discord" style={{ marginRight: '0.5rem' }} />
              {posting ? 'Posting...' : 'Discord'}
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container" style={{ maxWidth: '1200px', margin: '0 auto', padding: '1rem' }}>

        {/* SUPERMAX Lock Hero */}
        {supermaxLock && (
          <LockHero game={supermaxLock} liveScore={getLiveScore(supermaxLock)} />
        )}

        {/* Games by League */}
        {leagueOrder.map(league => {
          const leagueGames = gamesByLeague[league]
          if (!leagueGames || leagueGames.length === 0) return null

          return (
            <div key={league} className="league-section">
              <div className="league-header">
                <div className={`league-icon ${league.toLowerCase()}`}>{league}</div>
                <div>
                  <div className="league-title">{league}</div>
                  <div className="league-count">{leagueGames.length} qualified picks</div>
                </div>
              </div>

              {leagueGames.map(game => (
                <GameCard key={game.id} game={game} liveScore={getLiveScore(game)} />
              ))}
            </div>
          )
        })}

        {games.length === 0 && (
          <div style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-muted)' }}>
            <p>No qualified picks for today.</p>
            <button className="btn-fetch" onClick={handleFetchGames} style={{ marginTop: '1rem' }}>
              Fetch Today's Games
            </button>
          </div>
        )}
      </div>

      <MobileNav />
    </>
  )
}

// Lock Hero Component
function LockHero({ game, liveScore }: { game: Game; liveScore: LiveScore | null }) {
  const isLive = liveScore && !liveScore.is_final && liveScore.away_score !== undefined
  const isFinal = liveScore?.is_final

  return (
    <div className="lock-hero">
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', position: 'relative', zIndex: 1 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <span className="lock-badge">
            <i className="bi bi-lightning-charge-fill" /> SUPERMAX
          </span>
          <span style={{ background: 'var(--bg-card)', color: 'var(--text-muted)', fontSize: '0.7rem', padding: '0.25rem 0.5rem', borderRadius: '4px' }}>
            {game.league}
          </span>
        </div>
        <div className="lock-edge-top">EDGE {game.edge?.toFixed(1)} PTS</div>
      </div>

      {/* Teams */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'relative', zIndex: 1 }}>
        {/* Away Team */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', width: '80px' }}>
          {game.away_logo ? (
            <img src={game.away_logo} alt={game.away_team} style={{ width: '56px', height: '56px', objectFit: 'contain' }} />
          ) : (
            <div style={{ width: '56px', height: '56px', background: 'rgba(139,92,246,0.2)', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '16px', fontWeight: 600, color: '#8B5CF6' }}>
              {game.away_team.substring(0, 3)}
            </div>
          )}
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '6px', textAlign: 'center' }}>{game.away_team}</div>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{game.away_record || '--'}</div>
        </div>

        {/* Center - Score or Time */}
        <div style={{ textAlign: 'center', flex: 1, padding: '0 8px' }}>
          {isFinal ? (
            <div>
              <div style={{ fontSize: '10px', fontWeight: 700, color: '#9ca3af', letterSpacing: '1px', marginBottom: '2px' }}>FINAL</div>
              <div style={{ fontSize: '22px', fontWeight: 700, color: '#fff' }}>
                {liveScore?.away_score} - {liveScore?.home_score}
              </div>
              <div style={{ fontSize: '10px', color: '#9ca3af', marginTop: '2px' }}>
                Total: {(liveScore?.away_score || 0) + (liveScore?.home_score || 0)}
              </div>
            </div>
          ) : isLive ? (
            <div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '4px', marginBottom: '4px' }}>
                <span className="pikkit-live-badge">LIVE</span>
                <span style={{ color: '#9ca3af', fontSize: '10px' }}>{liveScore?.period}</span>
              </div>
              <div style={{ fontSize: '22px', fontWeight: 700, color: '#fff' }}>
                {liveScore?.away_score} - {liveScore?.home_score}
              </div>
            </div>
          ) : (
            <div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '2px' }}>{game.game_time}</div>
              <div style={{ fontSize: '1.25rem', fontWeight: 500, color: 'var(--text-muted)' }}>@</div>
            </div>
          )}
        </div>

        {/* Home Team */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', width: '80px' }}>
          {game.home_logo ? (
            <img src={game.home_logo} alt={game.home_team} style={{ width: '56px', height: '56px', objectFit: 'contain' }} />
          ) : (
            <div style={{ width: '56px', height: '56px', background: 'rgba(139,92,246,0.2)', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '16px', fontWeight: 600, color: '#8B5CF6' }}>
              {game.home_team.substring(0, 3)}
            </div>
          )}
          <div style={{ fontSize: '0.85rem', fontWeight: 600, marginTop: '6px', textAlign: 'center' }}>{game.home_team}</div>
          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{game.home_record || '--'}</div>
        </div>
      </div>

      {/* Pick */}
      <div style={{ textAlign: 'center', marginTop: '1rem', position: 'relative', zIndex: 1 }}>
        <div className={`lock-pick ${game.direction === 'U' ? 'under' : ''}`}>
          {game.direction === 'O' ? 'OVER' : 'UNDER'} {game.line}
        </div>
      </div>
    </div>
  )
}

// Game Card Component (Pikkit Style)
function GameCard({ game, liveScore }: { game: Game; liveScore: LiveScore | null }) {
  const isLive = liveScore && !liveScore.is_final && liveScore.away_score !== undefined
  const isFinal = liveScore?.is_final

  const cardClass = `game-card ${game.league.toLowerCase()} ${game.is_qualified ? 'qualified' : ''} ${game.is_supermax ? 'lock-card' : ''} ${isLive ? 'game-live' : ''} ${isFinal ? 'game-final' : ''}`

  return (
    <div className={cardClass}>
      {/* Pikkit Header - Teams and Score */}
      <div className="pikkit-header">
        {/* Away Team */}
        <div className="pikkit-team">
          {game.away_logo ? (
            <img src={game.away_logo} alt={game.away_team} className="pikkit-team-logo" />
          ) : (
            <div style={{ width: '48px', height: '48px', background: 'rgba(139,92,246,0.2)', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '14px', fontWeight: 600, color: '#8B5CF6' }}>
              {game.away_team.substring(0, 3)}
            </div>
          )}
          <div className="pikkit-team-name">{game.away_team}</div>
          <div className="pikkit-team-record">{game.away_record || '--'}</div>
        </div>

        {/* Center */}
        <div className="pikkit-center">
          {isFinal ? (
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '10px', fontWeight: 700, color: '#9ca3af', letterSpacing: '1px' }}>FINAL</div>
              <div style={{ fontSize: '18px', fontWeight: 700, color: '#fff' }}>
                {liveScore?.away_score} - {liveScore?.home_score}
              </div>
              <div style={{ fontSize: '10px', color: '#9ca3af' }}>
                Total: {(liveScore?.away_score || 0) + (liveScore?.home_score || 0)}
              </div>
            </div>
          ) : isLive ? (
            <div style={{ textAlign: 'center' }}>
              <span className="pikkit-live-badge">LIVE</span>
              <div style={{ fontSize: '10px', color: '#9ca3af', marginTop: '2px' }}>{liveScore?.period}</div>
              <div style={{ fontSize: '18px', fontWeight: 700, color: '#fff', marginTop: '2px' }}>
                {liveScore?.away_score} - {liveScore?.home_score}
              </div>
            </div>
          ) : (
            <div className="pikkit-pregame">
              <div className="pikkit-game-time">{game.game_time}</div>
              <div className="pikkit-at-symbol">@</div>
            </div>
          )}
        </div>

        {/* Home Team */}
        <div className="pikkit-team">
          {game.home_logo ? (
            <img src={game.home_logo} alt={game.home_team} className="pikkit-team-logo" />
          ) : (
            <div style={{ width: '48px', height: '48px', background: 'rgba(139,92,246,0.2)', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '14px', fontWeight: 600, color: '#8B5CF6' }}>
              {game.home_team.substring(0, 3)}
            </div>
          )}
          <div className="pikkit-team-name">{game.home_team}</div>
          <div className="pikkit-team-record">{game.home_record || '--'}</div>
        </div>
      </div>

      {/* Totals Bar */}
      <div className="pikkit-totals-bar">
        <div className="totals-stat">
          <div className="totals-stat-label">Line</div>
          <div className="totals-stat-value">{game.line || '--'}</div>
        </div>
        <div className="totals-stat">
          <div className="totals-stat-label">Proj</div>
          <div className="totals-stat-value">{game.projected_total?.toFixed(1) || '--'}</div>
        </div>
        <div className="totals-stat">
          <div className="totals-stat-label">Edge</div>
          <div className={`totals-stat-value ${(game.edge || 0) >= 10 ? 'edge-high' : 'edge-low'}`}>
            {game.edge?.toFixed(1) || '--'}
          </div>
        </div>
        <div className="totals-stat">
          <div className="totals-stat-label">Pick</div>
          <span className={`pikkit-pick-chip ${game.direction === 'O' ? 'over' : game.direction === 'U' ? 'under' : 'no-bet'}`}>
            {game.direction === 'O' ? 'OVER' : game.direction === 'U' ? 'UNDER' : '--'} {game.line || ''}
          </span>
        </div>
      </div>
    </div>
  )
}
