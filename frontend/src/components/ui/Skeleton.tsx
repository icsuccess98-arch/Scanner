'use client'

export function GameCardSkeleton() {
  return (
    <div className="game-card" style={{ opacity: 0.6 }}>
      <div className="pikkit-header">
        <div className="pikkit-team">
          <div style={{ width: 48, height: 48, borderRadius: '50%', background: 'var(--border)', animation: 'pulse 1.5s ease-in-out infinite' }} />
          <div style={{ width: 60, height: 12, background: 'var(--border)', borderRadius: 4, marginTop: 8, animation: 'pulse 1.5s ease-in-out infinite' }} />
        </div>
        <div className="pikkit-center">
          <div style={{ width: 40, height: 16, background: 'var(--border)', borderRadius: 4, animation: 'pulse 1.5s ease-in-out infinite' }} />
        </div>
        <div className="pikkit-team">
          <div style={{ width: 48, height: 48, borderRadius: '50%', background: 'var(--border)', animation: 'pulse 1.5s ease-in-out infinite' }} />
          <div style={{ width: 60, height: 12, background: 'var(--border)', borderRadius: 4, marginTop: 8, animation: 'pulse 1.5s ease-in-out infinite' }} />
        </div>
      </div>
      <div className="pikkit-totals-bar">
        {[1, 2, 3, 4].map(i => (
          <div key={i} className="totals-stat">
            <div style={{ width: 30, height: 10, background: 'var(--border)', borderRadius: 4, animation: 'pulse 1.5s ease-in-out infinite' }} />
            <div style={{ width: 40, height: 14, background: 'var(--border)', borderRadius: 4, marginTop: 4, animation: 'pulse 1.5s ease-in-out infinite' }} />
          </div>
        ))}
      </div>
    </div>
  )
}

export function StatCardSkeleton() {
  return (
    <div className="stat-card" style={{ opacity: 0.6 }}>
      <div style={{ width: 40, height: 24, background: 'var(--border)', borderRadius: 4, animation: 'pulse 1.5s ease-in-out infinite' }} />
      <div style={{ width: 60, height: 10, background: 'var(--border)', borderRadius: 4, marginTop: 8, animation: 'pulse 1.5s ease-in-out infinite' }} />
    </div>
  )
}
