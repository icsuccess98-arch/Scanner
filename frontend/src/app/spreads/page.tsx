'use client'

import { useState, useEffect, useCallback } from 'react'
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
}

interface LiveScore {
  away_score: number
  home_score: number
  period: string
  clock: string
  status: string
  is_final: boolean
}

export default function SpreadsPage() {
  const [games, setGames] = useState<Game[]>([])
  const [liveScores, setLiveScores] = useState<Record<string, LiveScore>>({})
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [selectedLeague, setSelectedLeague] = useState<string>('ALL')
  const [today, setToday] = useState('')
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date())

  useEffect(() => {
    const date = new Date()
    setToday(date.toLocaleDateString('en-US', {
      weekday: 'long',
      month: 'long',
      day: 'numeric'
    }))
  }, [])

  const fetchGames = useCallback(async () => {
    try {
      const res = await fetch('/api/dashboard_data')
      const data = await res.json()
      setGames(data.games || [])
    } catch (err) {
      console.error('Failed to fetch games:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchLiveScores = useCallback(async () => {
    try {
      const res = await fetch('/api/live_scores')
      const data = await res.json()
      setLiveScores(data.live_scores || {})
    } catch (err) {
      console.error('Failed to fetch live scores:', err)
    }
  }, [])

  const handleRefresh = async () => {
    setRefreshing(true)
    try {
      await fetch('/fetch_odds', { method: 'POST' })
      await fetchGames()
      await fetchLiveScores()
      setLastRefresh(new Date())
    } catch (err) {
      console.error('Refresh failed:', err)
    } finally {
      setRefreshing(false)
    }
  }

  useEffect(() => {
    fetchGames()
    fetchLiveScores()
  }, [fetchGames, fetchLiveScores])

  useEffect(() => {
    const interval = setInterval(fetchLiveScores, 2500)
    return () => clearInterval(interval)
  }, [fetchLiveScores])

  const leagues = ['ALL', 'NBA', 'CBB', 'NFL', 'CFB', 'NHL']
  const filteredGames = selectedLeague === 'ALL'
    ? games
    : games.filter(g => g.league === selectedLeague)

  const gamesByLeague = filteredGames.reduce((acc, game) => {
    if (!acc[game.league]) acc[game.league] = []
    acc[game.league].push(game)
    return acc
  }, {} as Record<string, Game[]>)

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

      {/* Header Section */}
      <div className="header-section">
        <div className="control-panel">
          <div className="date-display">{today}</div>

          <div className="stats-grid">
            <div className="stat-card">
              <div className="value">{games.length}</div>
              <div className="label">Total Games</div>
            </div>
            <div className="stat-card qualified">
              <div className="value">{games.filter(g => g.is_qualified).length}</div>
              <div className="label">With Lines</div>
            </div>
          </div>

          {/* League Filter */}
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', justifyContent: 'center', marginBottom: '1rem' }}>
            {leagues.map(league => (
              <button
                key={league}
                onClick={() => setSelectedLeague(league)}
                style={{
                  background: selectedLeague === league
                    ? 'linear-gradient(135deg, #7B2CBF 0%, #9D4EDD 100%)'
                    : 'var(--bg-input)',
                  border: selectedLeague === league ? 'none' : '1px solid var(--border)',
                  color: 'var(--text)',
                  padding: '0.5rem 1rem',
                  borderRadius: '8px',
                  fontSize: '0.8rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  transition: 'all 0.2s'
                }}
              >
                {league}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container" style={{ maxWidth: '1200px', margin: '0 auto', padding: '1rem' }}>
        {Object.entries(gamesByLeague).map(([league, leagueGames]) => (
          <div key={league} className="league-section">
            <div className="league-header">
              <div className={`league-icon ${league.toLowerCase()}`}>{league}</div>
              <div>
                <div className="league-title">{league}</div>
                <div className="league-count">{leagueGames.length} games</div>
              </div>
            </div>

            {leagueGames.map(game => (
              <GameCard key={game.id} game={game} liveScore={getLiveScore(game)} />
            ))}
          </div>
        ))}

        {filteredGames.length === 0 && (
          <div style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-muted)' }}>
            <p>No games found for {selectedLeague}.</p>
          </div>
        )}
      </div>

      {/* Floating Refresh Button */}
      <button
        onClick={handleRefresh}
        disabled={refreshing}
        className="floating-refresh"
        style={{
          position: 'fixed',
          bottom: '90px',
          right: '20px',
          width: '56px',
          height: '56px',
          borderRadius: '50%',
          background: 'linear-gradient(135deg, #7B2CBF, #9D4EDD)',
          border: 'none',
          color: '#fff',
          fontSize: '24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          boxShadow: '0 4px 15px rgba(123, 44, 191, 0.4)',
          cursor: 'pointer',
          zIndex: 1000
        }}
      >
        <i className={`bi bi-arrow-repeat ${refreshing ? 'spinning' : ''}`} style={{ animation: refreshing ? 'spin 1s linear infinite' : 'none' }} />
      </button>

      <MobileNav />

      <style jsx>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </>
  )
}

function GameCard({ game, liveScore }: { game: Game; liveScore: LiveScore | null }) {
  const isLive = liveScore && !liveScore.is_final && liveScore.away_score !== undefined
  const isFinal = liveScore?.is_final

  return (
    <div className={`game-card ${game.league.toLowerCase()} ${game.is_qualified ? 'qualified' : ''} ${isLive ? 'game-live' : ''} ${isFinal ? 'game-final' : ''}`}>
      <div className="pikkit-header">
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

        <div className="pikkit-center">
          {isFinal ? (
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '10px', fontWeight: 700, color: '#9ca3af', letterSpacing: '1px' }}>FINAL</div>
              <div style={{ fontSize: '18px', fontWeight: 700, color: '#fff' }}>
                {liveScore?.away_score} - {liveScore?.home_score}
              </div>
            </div>
          ) : isLive ? (
            <div style={{ textAlign: 'center' }}>
              <span className="pikkit-live-badge">LIVE</span>
              <div style={{ fontSize: '18px', fontWeight: 700, color: '#fff', marginTop: '4px' }}>
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
          <div className={`totals-stat-value ${(game.edge || 0) >= 10 ? 'edge-high' : ''}`}>
            {game.edge?.toFixed(1) || '--'}
          </div>
        </div>
        {game.direction && (
          <div className="totals-stat">
            <span className={`pikkit-pick-chip ${game.direction === 'O' ? 'over' : 'under'}`}>
              {game.direction === 'O' ? 'OVER' : 'UNDER'}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}
