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

  useEffect(() => {
    const date = new Date()
    setToday(date.toLocaleDateString('en-US', {
      month: 'long',
      day: 'numeric',
      year: 'numeric'
    }))
  }, [])

  const fetchGames = useCallback(async () => {
    try {
      const data = await api.getDashboardData()
      setGames(data.games || [])
    } catch (err) {
      console.error('Failed to fetch games:', err)
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchLiveScores = useCallback(async () => {
    try {
      const data = await api.getLiveScores()
      setLiveScores(data.live_scores || {})
    } catch (err) {
      console.error('Failed to fetch live scores:', err)
    }
  }, [])

  useEffect(() => {
    fetchGames()
    fetchLiveScores()
    const interval = setInterval(fetchLiveScores, 2500)
    return () => clearInterval(interval)
  }, [fetchGames, fetchLiveScores])

  const handleRefresh = async () => {
    setRefreshing(true)
    try {
      await api.fetchOdds()
      await fetchGames()
      await fetchLiveScores()
    } catch (err) {
      console.error('Refresh failed:', err)
    } finally {
      setRefreshing(false)
    }
  }

  const leagues = ['ALL', 'NBA', 'CBB', 'NFL', 'CFB', 'NHL']
  const filteredGames = selectedLeague === 'ALL'
    ? games
    : games.filter(g => g.league === selectedLeague)

  const gamesByLeague = filteredGames.reduce((acc, game) => {
    if (!acc[game.league]) acc[game.league] = []
    acc[game.league].push(game)
    return acc
  }, {} as Record<string, Game[]>)

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

      <div className="bg-gradient-to-b from-[#0d0b12] via-[#12101a] to-[#0a0a12] border-b border-[#2d2640] py-8 px-4">
        <div className="max-w-[480px] mx-auto bg-gradient-to-br from-[rgba(26,24,37,0.95)] to-[rgba(18,16,26,0.98)] border border-[rgba(123,44,191,0.25)] rounded-[20px] p-6 shadow-[0_8px_32px_rgba(0,0,0,0.4),0_0_60px_rgba(123,44,191,0.08)]">
          <div className="text-2xl font-bold text-center mb-5 tracking-tight">{today}</div>
          
          <div className="grid grid-cols-2 gap-3 mb-5">
            <div className="bg-gradient-to-br from-[rgba(45,38,64,0.5)] to-[rgba(26,24,37,0.8)] border border-[rgba(255,215,0,0.15)] rounded-xl p-4 text-center">
              <div className="text-3xl font-extrabold bg-gradient-to-r from-[#FFD700] to-[#FFEC8B] bg-clip-text text-transparent leading-none">{games.length}</div>
              <div className="text-[11px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">Total Games</div>
            </div>
            <div className="bg-gradient-to-br from-[rgba(255,215,0,0.08)] to-[rgba(26,24,37,0.9)] border border-[rgba(255,215,0,0.35)] rounded-xl p-4 text-center">
              <div className="text-3xl font-extrabold bg-gradient-to-r from-[#FFD700] to-[#FFEC8B] bg-clip-text text-transparent leading-none">{games.filter(g => g.is_qualified).length}</div>
              <div className="text-[11px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">With Lines</div>
            </div>
          </div>

          <div className="flex gap-2 overflow-x-auto pb-1 no-scrollbar">
            {leagues.map(league => (
              <button
                key={league}
                onClick={() => setSelectedLeague(league)}
                className={`flex-shrink-0 px-4 py-2 rounded-full text-xs font-bold transition-all ${
                  selectedLeague === league 
                  ? 'bg-gradient-to-r from-[#7B2CBF] to-[#9D4EDD] text-white shadow-lg shadow-[rgba(123,44,191,0.3)]' 
                  : 'bg-[rgba(30,30,45,0.8)] text-[#9990B0] border border-[rgba(255,255,255,0.1)]'
                }`}
              >
                {league}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="max-w-[800px] mx-auto p-4">
        {Object.entries(gamesByLeague).map(([league, leagueGames]) => (
          <div key={league} className="mb-8">
            <div className="flex items-center gap-4 py-4 border-b border-[#2d2640] mb-4">
              <div className={`w-10 h-10 rounded-lg flex items-center justify-center font-bold text-[11px] text-white ${
                league === 'NBA' ? 'bg-gradient-to-br from-[#17408B] to-[#C9082A]' :
                league === 'CBB' ? 'bg-gradient-to-br from-[#1e40af] to-[#7c3aed]' :
                league === 'NFL' ? 'bg-gradient-to-br from-[#013369] to-[#D50A0A]' :
                league === 'CFB' ? 'bg-gradient-to-br from-[#7c3aed] to-[#dc2626]' :
                'bg-gradient-to-br from-[#000000] to-[#A2AAAD]'
              }`}>{league}</div>
              <div>
                <div className="text-xl font-bold">{league}</div>
                <div className="text-[#9990B0] text-sm">{leagueGames.length} games</div>
              </div>
            </div>

            {leagueGames.map(game => {
              const live = liveScores[`${game.away_team}@${game.home_team}`]
              return (
                <div key={game.id} className={`bg-[#12101a] border border-[#2d2640] rounded-xl mb-3 relative overflow-hidden transition-all hover:border-[#7B2CBF] ${game.is_qualified ? 'border-l-[5px] border-l-[#FFD700]' : ''}`}>
                  <div className="flex items-center justify-between p-4 bg-gradient-to-b from-[rgba(25,25,35,0.98)] to-[rgba(18,18,26,1)]">
                    <div className="flex-1 flex flex-col items-center">
                      <img src={game.away_logo} alt={game.away_team} className="w-10 h-10 object-contain" />
                      <div className="text-[11px] font-bold mt-1">{game.away_team}</div>
                    </div>
                    <div className="flex flex-col items-center min-w-[80px]">
                      {live ? (
                        <div className="text-center">
                          <span className="bg-red-600 text-white text-[9px] font-bold px-1.5 py-0.5 rounded animate-pulse">LIVE</span>
                          <div className="text-lg font-bold">{live.away_score} - {live.home_score}</div>
                        </div>
                      ) : (
                        <div className="text-center text-[#9ca3af]">
                          <div className="text-[10px] font-bold">{game.game_time}</div>
                          <div className="text-lg">@</div>
                        </div>
                      )}
                    </div>
                    <div className="flex-1 flex flex-col items-center">
                      <img src={game.home_logo} alt={game.home_team} className="w-10 h-10 object-contain" />
                      <div className="text-[11px] font-bold mt-1">{game.home_team}</div>
                    </div>
                  </div>
                  <div className="flex items-center justify-between p-2.5 bg-[rgba(15,23,42,0.8)] border-t border-[rgba(139,92,246,0.1)]">
                    <div className="flex-1 text-center"><div className="text-[8px] text-[#6b7280]">Line</div><div className="text-[13px] font-bold">{game.line || '--'}</div></div>
                    <div className="flex-1 text-center border-l border-[rgba(139,92,246,0.1)]"><div className="text-[8px] text-[#6b7280]">Proj</div><div className="text-[13px] font-bold">{game.projected_total?.toFixed(1) || '--'}</div></div>
                    <div className="flex-1 text-center border-l border-[rgba(139,92,246,0.1)]"><div className="text-[8px] text-[#6b7280]">Edge</div><div className="text-[13px] font-bold text-[#F59E0B]">{game.edge?.toFixed(1) || '--'}</div></div>
                    <div className="flex-1 flex justify-center border-l border-[rgba(139,92,246,0.1)]">
                      <span className={`px-2 py-1 rounded-full text-[10px] font-extrabold ${game.direction === 'O' ? 'text-[#10B981] bg-[#10B9811a]' : 'text-[#EF4444] bg-[#EF44441a]'}`}>
                        {game.direction === 'O' ? 'OVER' : 'UNDER'} {game.line}
                      </span>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        ))}
      </div>

      <button onClick={handleRefresh} disabled={refreshing} className="fixed bottom-[90px] right-5 w-14 h-14 rounded-full bg-gradient-to-br from-[#7B2CBF] to-[#9D4EDD] flex items-center justify-center text-white shadow-xl z-[1000] active:scale-95">
        <i className={`bi bi-arrow-repeat text-2xl ${refreshing ? 'animate-spin' : ''}`}></i>
      </button>

      <MobileNav />
    </div>
  )
}
