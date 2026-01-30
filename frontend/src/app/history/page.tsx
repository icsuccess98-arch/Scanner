'use client'

import { useState, useEffect, useCallback } from 'react'
import { Header, MobileNav } from '@/components/layout'

interface Pick {
  id: number
  date: string
  matchup: string
  league: string
  pick: string
  edge: number
  result: 'W' | 'L' | 'P' | null
  is_lock: boolean
}

export default function HistoryPage() {
  const [picks, setPicks] = useState<Pick[]>([])
  const [loading, setLoading] = useState(true)
  const [updating, setUpdating] = useState<number | null>(null)

  const fetchHistory = useCallback(async () => {
    try {
      const res = await fetch('/api/history_data')
      const data = await res.json()
      setPicks(data.picks || [])
    } catch (err) {
      console.error('Failed to fetch history:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchHistory()
  }, [fetchHistory])

  // Auto-refresh every 30 seconds
  useEffect(() => {
    const interval = setInterval(fetchHistory, 30000)
    return () => clearInterval(interval)
  }, [fetchHistory])

  const handleUpdateResult = async (pickId: number, result: 'W' | 'L' | 'P') => {
    setUpdating(pickId)
    try {
      await fetch(`/update_result/${pickId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ result })
      })
      await fetchHistory()
    } catch (err) {
      console.error('Failed to update result:', err)
    } finally {
      setUpdating(null)
    }
  }

  // Calculate stats
  const wins = picks.filter(p => p.result === 'W').length
  const losses = picks.filter(p => p.result === 'L').length
  const pushes = picks.filter(p => p.result === 'P').length
  const decided = wins + losses
  const winRate = decided > 0 ? ((wins / decided) * 100).toFixed(1) : '0.0'

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

      <div className="container" style={{ maxWidth: '600px', margin: '0 auto', padding: '1rem' }}>
        {/* Stats Row */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(3, 1fr)',
          gap: '0.75rem',
          marginBottom: '1.5rem'
        }}>
          <div style={{
            background: 'linear-gradient(135deg, var(--bg-card) 0%, rgba(123, 44, 191, 0.08) 100%)',
            border: '1px solid var(--border)',
            borderRadius: '12px',
            padding: '1rem',
            textAlign: 'center'
          }}>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#FFD700' }}>{wins}</div>
            <div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginTop: '0.25rem' }}>Wins</div>
          </div>
          <div style={{
            background: 'linear-gradient(135deg, var(--bg-card) 0%, rgba(123, 44, 191, 0.08) 100%)',
            border: '1px solid var(--border)',
            borderRadius: '12px',
            padding: '1rem',
            textAlign: 'center'
          }}>
            <div style={{ fontSize: '1.75rem', fontWeight: 800, color: '#9D4EDD' }}>{losses}</div>
            <div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginTop: '0.25rem' }}>Losses</div>
          </div>
          <div style={{
            background: 'linear-gradient(135deg, var(--bg-card) 0%, rgba(123, 44, 191, 0.08) 100%)',
            border: '1px solid var(--border)',
            borderRadius: '12px',
            padding: '1rem',
            textAlign: 'center'
          }}>
            <div style={{
              fontSize: '1.75rem',
              fontWeight: 800,
              background: 'linear-gradient(135deg, #FFD700 0%, #FFF8DC 100%)',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent'
            }}>{winRate}%</div>
            <div style={{ fontSize: '0.65rem', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginTop: '0.25rem' }}>Win Rate</div>
          </div>
        </div>

        {/* Section Title */}
        <div style={{
          fontSize: '0.875rem',
          fontWeight: 600,
          color: 'var(--text-muted)',
          marginBottom: '0.75rem',
          display: 'flex',
          alignItems: 'center',
          gap: '0.5rem'
        }}>
          <i className="bi bi-trophy-fill" style={{ color: 'var(--gold)' }} />
          Lock of the Day History
        </div>

        {/* Picks List */}
        {picks.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '3rem', color: 'var(--text-muted)' }}>
            <p>No pick history yet.</p>
          </div>
        ) : (
          picks.map(pick => (
            <PickCard
              key={pick.id}
              pick={pick}
              onUpdateResult={handleUpdateResult}
              updating={updating === pick.id}
            />
          ))
        )}
      </div>

      <MobileNav />
    </>
  )
}

function PickCard({ pick, onUpdateResult, updating }: {
  pick: Pick
  onUpdateResult: (id: number, result: 'W' | 'L' | 'P') => void
  updating: boolean
}) {
  const isOver = pick.pick.toLowerCase().includes('over')
  const isUnder = pick.pick.toLowerCase().includes('under')

  const leagueGradients: Record<string, string> = {
    NBA: 'linear-gradient(135deg, #17408B, #C9082A)',
    CBB: 'linear-gradient(135deg, #1e40af, #7c3aed)',
    NFL: 'linear-gradient(135deg, #013369, #D50A0A)',
    CFB: 'linear-gradient(135deg, #7c3aed, #dc2626)',
    NHL: 'linear-gradient(135deg, #000000, #A2AAAD)',
  }

  return (
    <div style={{
      background: 'var(--bg-card)',
      border: '1px solid var(--border)',
      borderRadius: '12px',
      padding: '1rem',
      marginBottom: '0.75rem',
      position: 'relative',
      overflow: 'hidden',
      borderLeftWidth: '4px',
      borderLeftStyle: 'solid',
      borderLeftColor: pick.result === 'W' ? '#FFD700' : pick.result === 'L' ? '#9D4EDD' : 'var(--border)'
    }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
        <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{pick.date}</span>
        <span style={{
          fontSize: '0.6rem',
          fontWeight: 700,
          padding: '0.2rem 0.5rem',
          borderRadius: '4px',
          color: 'white',
          background: leagueGradients[pick.league] || 'var(--border)'
        }}>
          {pick.league}
        </span>
      </div>

      {/* Matchup */}
      <div style={{ fontWeight: 600, fontSize: '0.95rem', marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
        {pick.is_lock && <i className="bi bi-lock-fill" style={{ color: 'var(--gold)', fontSize: '0.875rem' }} />}
        {pick.matchup}
      </div>

      {/* Pick Details */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center' }}>
          <span style={{
            fontWeight: 700,
            fontSize: '0.8rem',
            padding: '0.25rem 0.5rem',
            borderRadius: '4px',
            background: isOver ? 'rgba(0, 230, 118, 0.2)' : isUnder ? 'rgba(255, 82, 82, 0.2)' : 'rgba(123, 44, 191, 0.2)',
            color: isOver ? '#00E676' : isUnder ? '#FF5252' : '#E0B0FF'
          }}>
            {pick.pick}
          </span>
          <span style={{ fontSize: '0.75rem', color: 'var(--gold)', fontWeight: 600 }}>
            {pick.edge.toFixed(1)} edge
          </span>
        </div>

        {/* Result Badge */}
        {pick.result && (
          <span style={{
            fontWeight: 700,
            fontSize: '0.7rem',
            padding: '0.3rem 0.6rem',
            borderRadius: '6px',
            textTransform: 'uppercase',
            background: pick.result === 'W' ? 'rgba(255, 215, 0, 0.2)' : pick.result === 'L' ? 'rgba(157, 78, 221, 0.2)' : 'rgba(218, 165, 32, 0.2)',
            color: pick.result === 'W' ? '#FFD700' : pick.result === 'L' ? '#9D4EDD' : '#DAA520'
          }}>
            {pick.result === 'W' ? 'WIN' : pick.result === 'L' ? 'LOSS' : 'PUSH'}
          </span>
        )}
      </div>

      {/* Result Buttons (if no result) */}
      {!pick.result && (
        <div style={{
          display: 'flex',
          gap: '0.375rem',
          marginTop: '0.75rem',
          paddingTop: '0.75rem',
          borderTop: '1px solid var(--border)'
        }}>
          <button
            onClick={() => onUpdateResult(pick.id, 'W')}
            disabled={updating}
            style={{
              flex: 1,
              background: 'rgba(255, 215, 0, 0.15)',
              border: '1px solid rgba(255, 215, 0, 0.3)',
              borderRadius: '6px',
              padding: '0.5rem',
              color: '#FFD700',
              fontSize: '0.75rem',
              fontWeight: 700,
              cursor: 'pointer',
              transition: 'all 0.2s'
            }}
          >
            WIN
          </button>
          <button
            onClick={() => onUpdateResult(pick.id, 'L')}
            disabled={updating}
            style={{
              flex: 1,
              background: 'rgba(157, 78, 221, 0.15)',
              border: '1px solid rgba(157, 78, 221, 0.3)',
              borderRadius: '6px',
              padding: '0.5rem',
              color: '#9D4EDD',
              fontSize: '0.75rem',
              fontWeight: 700,
              cursor: 'pointer',
              transition: 'all 0.2s'
            }}
          >
            LOSS
          </button>
          <button
            onClick={() => onUpdateResult(pick.id, 'P')}
            disabled={updating}
            style={{
              flex: 1,
              background: 'rgba(218, 165, 32, 0.15)',
              border: '1px solid rgba(218, 165, 32, 0.3)',
              borderRadius: '6px',
              padding: '0.5rem',
              color: '#DAA520',
              fontSize: '0.75rem',
              fontWeight: 700,
              cursor: 'pointer',
              transition: 'all 0.2s'
            }}
          >
            PUSH
          </button>
        </div>
      )}
    </div>
  )
}
