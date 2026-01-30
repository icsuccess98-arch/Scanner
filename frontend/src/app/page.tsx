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

  useEffect(() => {
    const date = new Date()
    setToday(date.toLocaleDateString('en-US', {
      month: 'long',
      day: 'numeric',
      year: 'numeric'
    }))
  }, [])

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

  const fetchLiveScores = useCallback(async () => {
    try {
      const data = await api.getLiveScores()
      setLiveScores(data.live_scores || {})
    } catch (err) {
      console.error('Failed to fetch live scores:', err)
    }
  }, [])

  useEffect(() => {
    fetchDashboard()
    fetchLiveScores()
    const scoreInterval = setInterval(fetchLiveScores, 2500)
    const dashInterval = setInterval(fetchDashboard, 30000)
    return () => {
      clearInterval(scoreInterval)
      clearInterval(dashInterval)
    }
  }, [fetchDashboard, fetchLiveScores])

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

  const getLiveScore = (game: Game): LiveScore | null => {
    const key = `${game.away_team}@${game.home_team}`
    return liveScores[key] || null
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-[#0a0a12] flex items-center justify-center">
        <div className="w-12 h-12 border-4 border-[#7B2CBF] border-t-transparent rounded-full animate-spin"></div>
      </div>
    )
  }

  const leagueOrder = ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']
  const gamesByLeague = games.reduce((acc, game) => {
    if (!acc[game.league]) acc[game.league] = []
    acc[game.league].push(game)
    return acc
  }, {} as Record<string, Game[]>)

  return (
    <div className="min-h-screen bg-[#0a0a12] text-[#F8F8FF] font-inter pb-20">
      <Header />

      <div className="bg-gradient-to-b from-[#0d0b12] via-[#12101a] to-[#0a0a12] border-b border-[#2d2640] py-8 px-4">
        <div className="max-w-[480px] mx-auto bg-gradient-to-br from-[rgba(26,24,37,0.95)] to-[rgba(18,16,26,0.98)] border border-[rgba(123,44,191,0.25)] rounded-[20px] p-6 shadow-[0_8px_32px_rgba(0,0,0,0.4),0_0_60px_rgba(123,44,191,0.08)]">
          <div className="text-2xl font-bold text-center mb-5 tracking-tight">{today}</div>
          
          <div className="grid grid-cols-2 gap-3 mb-5">
            <div className="bg-gradient-to-br from-[rgba(45,38,64,0.5)] to-[rgba(26,24,37,0.8)] border border-[rgba(255,215,0,0.15)] rounded-xl p-4 text-center">
              <div className="text-3xl font-extrabold bg-gradient-to-r from-[#FFD700] to-[#FFEC8B] bg-clip-text text-transparent leading-none">{totalGames}</div>
              <div className="text-[11px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">Total Games</div>
            </div>
            <div className="bg-gradient-to-br from-[rgba(255,215,0,0.08)] to-[rgba(26,24,37,0.9)] border border-[rgba(255,215,0,0.35)] rounded-xl p-4 text-center">
              <div className="text-3xl font-extrabold bg-gradient-to-r from-[#FFD700] to-[#FFEC8B] bg-clip-text text-transparent leading-none">{qualifiedGames}</div>
              <div className="text-[11px] text-[#9990B0] uppercase font-bold tracking-wider mt-1">Qualified Picks</div>
            </div>
          </div>

          <div className="flex gap-3">
            <button 
              onClick={handleFetchGames}
              disabled={fetching}
              className="flex-1 flex items-center justify-center gap-2 bg-[#1a1825] border border-[#2d2640] hover:border-[#7B2CBF] rounded-xl py-3 text-sm font-semibold transition-all active:scale-95"
            >
              <i className={`bi bi-arrow-repeat ${fetching ? 'animate-spin' : ''}`}></i>
              {fetching ? 'Syncing...' : 'Fetch Games'}
            </button>
            <button 
              onClick={handlePostDiscord}
              disabled={posting || !supermaxLock}
              className="flex-1 flex items-center justify-center gap-2 bg-gradient-to-r from-[#7B2CBF] to-[#9D4EDD] rounded-xl py-3 text-sm font-semibold shadow-[0_4px_15px_rgba(123,44,191,0.5)] active:scale-95 disabled:opacity-50"
            >
              <i className="bi bi-discord"></i>
              {posting ? 'Posting...' : 'Discord'}
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-[800px] mx-auto p-4">
        {supermaxLock && (
          <div className="relative overflow-hidden p-6 mb-8 rounded-2xl border-2 shadow-[0_0_40px_rgba(255,215,0,0.15)] bg-gradient-to-br from-[rgba(255,215,0,0.12)] via-[rgba(123,44,191,0.08)] to-[rgba(218,165,32,0.05)]" style={{ borderImage: 'linear-gradient(135deg, #FFD700, #9D4EDD, #DAA520) 1' }}>
            <div className="flex justify-between items-center mb-4 relative z-10">
              <div className="flex items-center gap-2">
                <span className="bg-gradient-to-r from-[#FFD700] via-[#FFF8DC] to-[#DAA520] text-[#0a0a12] px-3 py-1 rounded font-extrabold text-[11px] uppercase tracking-wider shadow-[0_2px_10px_rgba(255,215,0,0.4)]">
                  <i className="bi bi-lightning-charge-fill mr-1"></i> SUPERMAX
                </span>
                <span className="bg-[#12101a] text-[#9990B0] text-[10px] px-2 py-1 rounded font-bold">{supermaxLock.league}</span>
              </div>
              <div className="text-[#FFD700] font-bold text-sm tracking-widest uppercase">EDGE {supermaxLock.edge?.toFixed(1)} PTS</div>
            </div>

            <div className="flex justify-between items-center relative z-10">
              <div className="w-20 text-center">
                <img src={supermaxLock.away_logo} alt={supermaxLock.away_team} className="w-14 h-14 object-contain mx-auto drop-shadow-[0_2px_4px_rgba(0,0,0,0.3)]" />
                <div className="font-bold text-sm mt-2">{supermaxLock.away_team}</div>
                <div className="text-[10px] text-[#9990B0]">{supermaxLock.away_record || '--'}</div>
              </div>

              <div className="text-center flex-1">
                {getLiveScore(supermaxLock) ? (
                  <div>
                    <div className="text-[10px] font-bold text-[#9990B0] tracking-widest">{getLiveScore(supermaxLock)?.is_final ? 'FINAL' : 'LIVE'}</div>
                    <div className="text-2xl font-bold">{getLiveScore(supermaxLock)?.away_score} - {getLiveScore(supermaxLock)?.home_score}</div>
                  </div>
                ) : (
                  <div>
                    <div className="text-[11px] font-bold text-[#9990B0] mb-1">{supermaxLock.game_time}</div>
                    <div className="text-xl font-medium text-[#9990B0]">@</div>
                  </div>
                )}
              </div>

              <div className="w-20 text-center">
                <img src={supermaxLock.home_logo} alt={supermaxLock.home_team} className="w-14 h-14 object-contain mx-auto drop-shadow-[0_2px_4px_rgba(0,0,0,0.3)]" />
                <div className="font-bold text-sm mt-2">{supermaxLock.home_team}</div>
                <div className="text-[10px] text-[#9990B0]">{supermaxLock.home_record || '--'}</div>
              </div>
            </div>

            <div className="text-center mt-4 relative z-10">
              <div className={`text-3xl font-extrabold italic bg-clip-text text-transparent ${supermaxLock.direction === 'U' ? 'bg-gradient-to-r from-[#9D4EDD] via-[#E0B0FF] to-[#7B2CBF] drop-shadow-[0_0_10px_rgba(157,78,221,0.5)]' : 'bg-gradient-to-r from-[#FFD700] via-[#FFF8DC] to-[#DAA520] drop-shadow-[0_0_10px_rgba(255,215,0,0.5)]'}`}>
                {supermaxLock.direction === 'O' ? 'OVER' : 'UNDER'} {supermaxLock.line}
              </div>
            </div>
          </div>
        )}

        {leagueOrder.map(league => {
          const leagueGames = gamesByLeague[league]
          if (!leagueGames) return null
          return (
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
                  <div className="text-[#9990B0] text-sm">{leagueGames.length} qualified picks</div>
                </div>
              </div>

              {leagueGames.map(game => {
                const live = getLiveScore(game)
                return (
                  <div key={game.id} className={`bg-[#12101a] border border-[#2d2640] rounded-xl mb-3 relative overflow-hidden transition-all hover:-translate-y-0.5 hover:border-[#7B2CBF] hover:shadow-xl ${game.is_qualified ? 'border-l-[5px] border-l-[#FFD700]' : ''}`}>
                    <div className="flex items-center justify-between p-4 bg-gradient-to-b from-[rgba(25,25,35,0.98)] to-[rgba(18,18,26,1)] border-b border-[rgba(139,92,246,0.15)]">
                      <div className="flex-1 flex flex-col items-center">
                        <img src={game.away_logo} alt={game.away_team} className="w-10 h-10 object-contain drop-shadow-md" />
                        <div className="text-[11px] font-bold mt-1">{game.away_team}</div>
                        <div className="text-[10px] text-[#9ca3af]">{game.away_record || '--'}</div>
                      </div>

                      <div className="flex flex-col items-center min-w-[80px] px-2">
                        {live ? (
                          <div className="text-center">
                            <span className="bg-red-600 text-white text-[9px] font-bold px-1.5 py-0.5 rounded animate-pulse">LIVE</span>
                            <div className="text-[10px] text-[#9ca3af] mt-0.5">{live.period}</div>
                            <div className="text-lg font-bold">{live.away_score} - {live.home_score}</div>
                          </div>
                        ) : (
                          <div className="text-center">
                            <div className="text-[10px] font-bold text-[#9ca3af]">{game.game_time}</div>
                            <div className="text-lg font-medium text-[#9990B0]">@</div>
                          </div>
                        )}
                      </div>

                      <div className="flex-1 flex flex-col items-center">
                        <img src={game.home_logo} alt={game.home_team} className="w-10 h-10 object-contain drop-shadow-md" />
                        <div className="text-[11px] font-bold mt-1">{game.home_team}</div>
                        <div className="text-[10px] text-[#9ca3af]">{game.home_record || '--'}</div>
                      </div>
                    </div>

                    <div className="flex items-center justify-between p-2.5 bg-[rgba(15,23,42,0.8)] gap-1">
                      <div className="flex-1 flex flex-col items-center">
                        <div className="text-[8px] text-[#6b7280] uppercase">Line</div>
                        <div className="text-[13px] font-bold">{game.line || '--'}</div>
                      </div>
                      <div className="flex-1 flex flex-col items-center border-l border-[rgba(139,92,246,0.1)]">
                        <div className="text-[8px] text-[#6b7280] uppercase">Proj</div>
                        <div className="text-[13px] font-bold">{game.projected_total?.toFixed(1) || '--'}</div>
                      </div>
                      <div className="flex-1 flex flex-col items-center border-l border-[rgba(139,92,246,0.1)]">
                        <div className="text-[8px] text-[#6b7280] uppercase">Edge</div>
                        <div className={`text-[13px] font-bold ${(game.edge || 0) >= 10 ? 'text-[#10B981]' : 'text-[#F59E0B]'}`}>{game.edge?.toFixed(1) || '--'}</div>
                      </div>
                      <div className="flex-1 flex items-center justify-center border-l border-[rgba(139,92,246,0.1)]">
                        <span className={`px-2 py-1.5 rounded-full text-[10px] font-extrabold uppercase tracking-tight ${
                          game.direction === 'O' ? 'bg-[rgba(16,185,129,0.15)] text-[#10B981] border border-[rgba(16,185,129,0.4)]' : 
                          game.direction === 'U' ? 'bg-[rgba(239,68,68,0.15)] text-[#EF4444] border border-[rgba(239,68,68,0.4)]' : 
                          'bg-[rgba(107,114,128,0.2)] text-[#9CA3AF]'
                        }`}>
                          {game.direction === 'O' ? 'OVER' : game.direction === 'U' ? 'UNDER' : '--'} {game.line}
                        </span>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          )
        })}
      </div>

      <MobileNav />
    </div>
  )
}
