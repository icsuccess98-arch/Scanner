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
  const [filters, setFilters] = useState({ league: 'all', date: 'all', result: 'all' })

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

  const handleCheckResults = async () => {
    try {
      await fetch('/check_results', { method: 'POST' })
      fetchHistory()
    } catch (err) {
      console.error('Check results failed:', err)
    }
  }

  const wins = picks.filter(p => p.result === 'W').length
  const losses = picks.filter(p => p.result === 'L').length
  const decided = wins + losses
  const winRate = decided > 0 ? ((wins / decided) * 100).toFixed(0) : '-'

  const filteredPicks = picks.filter(p => {
    if (filters.league !== 'all' && p.league !== filters.league) return false
    if (filters.result !== 'all') {
      if (filters.result === 'W' && p.result !== 'W') return false
      if (filters.result === 'L' && p.result !== 'L') return false
      if (filters.result === 'pending' && p.result !== null) return false
    }
    return true
  })

  if (loading) {
    return (
      <div className="min-h-screen bg-[#0a0a12] flex items-center justify-center">
        <div className="w-12 h-12 border-4 border-[#7B2CBF] border-t-transparent rounded-full animate-spin"></div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-[#0a0a12] text-[#F8F8FF] font-inter pb-20">
      <Header />

      <div className="max-w-[600px] mx-auto p-4">
        <div className="grid grid-cols-3 gap-3 mb-6">
          <div className="bg-gradient-to-br from-[#12101a] to-[rgba(123,44,191,0.08)] border border-[#2d2640] rounded-xl p-4 text-center">
            <div className="text-2xl font-extrabold text-[#FFD700]">{wins}</div>
            <div className="text-[10px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">Wins</div>
          </div>
          <div className="bg-gradient-to-br from-[#12101a] to-[rgba(123,44,191,0.08)] border border-[#2d2640] rounded-xl p-4 text-center">
            <div className="text-2xl font-extrabold text-[#9D4EDD]">{losses}</div>
            <div className="text-[10px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">Losses</div>
          </div>
          <div className="bg-gradient-to-br from-[#12101a] to-[rgba(123,44,191,0.08)] border border-[#2d2640] rounded-xl p-4 text-center">
            <div className="text-2xl font-extrabold bg-gradient-to-r from-[#FFD700] to-[#FFF8DC] bg-clip-text text-transparent">{winRate}{winRate !== '-' ? '%' : ''}</div>
            <div className="text-[10px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">Win Rate</div>
          </div>
        </div>

        <div className="flex flex-wrap gap-2 mb-6 items-center">
          <select 
            value={filters.league} 
            onChange={(e) => setFilters({...filters, league: e.target.value})}
            className="bg-[#1a1825] border border-[#2d2640] text-[#F8F8FF] text-xs font-bold rounded-lg px-3 py-2 outline-none"
          >
            <option value="all">All Leagues</option>
            <option value="NBA">NBA</option>
            <option value="CBB">CBB</option>
            <option value="NHL">NHL</option>
          </select>
          <select 
            value={filters.result} 
            onChange={(e) => setFilters({...filters, result: e.target.value})}
            className="bg-[#1a1825] border border-[#2d2640] text-[#F8F8FF] text-xs font-bold rounded-lg px-3 py-2 outline-none"
          >
            <option value="all">All Results</option>
            <option value="W">Wins</option>
            <option value="L">Losses</option>
            <option value="pending">Pending</option>
          </select>
          <button 
            onClick={handleCheckResults}
            className="ml-auto flex items-center gap-1 bg-[rgba(123,44,191,0.1)] border border-[#7B2CBF] text-xs font-bold px-3 py-2 rounded-lg active:scale-95 transition-all"
          >
            <i className="bi bi-arrow-repeat"></i> Sync
          </button>
        </div>

        <div className="space-y-3">
          {filteredPicks.map(pick => (
            <div key={pick.id} className={`bg-[#12101a] border border-[#2d2640] rounded-xl p-4 relative overflow-hidden ${pick.result === 'W' ? 'border-l-4 border-l-[#FFD700]' : pick.result === 'L' ? 'border-l-4 border-l-[#9D4EDD]' : ''}`}>
              <div className="flex justify-between items-center mb-2">
                <span className="text-[10px] text-[#9990B0]">{pick.date}</span>
                <span className={`text-[9px] font-bold px-2 py-0.5 rounded text-white ${
                  pick.league === 'NBA' ? 'bg-[#17408B]' : 'bg-[#1e40af]'
                }`}>{pick.league}</span>
              </div>
              <div className="font-bold text-sm flex items-center gap-2 mb-3">
                {pick.is_lock && <i className="bi bi-lock-fill text-[#FFD700] text-[10px]"></i>}
                {pick.matchup}
              </div>
              <div className="flex justify-between items-center">
                <div className="flex items-center gap-3">
                  <span className={`text-[10px] font-extrabold px-2 py-1 rounded ${pick.pick.toLowerCase().includes('over') ? 'bg-[#10B98133] text-[#10B981]' : 'bg-[#FF525233] text-[#FF5252]'}`}>{pick.pick}</span>
                  <span className="text-[10px] text-[#FFD700] font-bold">{pick.edge.toFixed(1)} edge</span>
                </div>
                {pick.result ? (
                  <span className={`text-[9px] font-bold px-2.5 py-1 rounded-md ${pick.result === 'W' ? 'bg-[#FFD70033] text-[#FFD700]' : 'bg-[#9D4EDD33] text-[#9D4EDD]'}`}>{pick.result === 'W' ? 'WIN' : 'LOSS'}</span>
                ) : (
                  <div className="flex gap-1.5">
                    <button onClick={() => handleUpdateResult(pick.id, 'W')} className="bg-[#FFD70026] border border-[#FFD7004d] text-[#FFD700] text-[9px] font-extrabold px-2 py-1 rounded active:scale-90">WIN</button>
                    <button onClick={() => handleUpdateResult(pick.id, 'L')} className="bg-[#9D4EDD26] border border-[#9D4EDD4d] text-[#9D4EDD] text-[9px] font-extrabold px-2 py-1 rounded active:scale-90">LOSS</button>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      <MobileNav />
    </div>
  )
}
