'use client'

import { useState, useEffect } from 'react'
import { Header, MobileNav } from '@/components/layout'

interface WeekData {
  week: number
  profit: number
  target: number
}

const GOAL_PRESETS = [10000, 20000, 40000, 52000, 100000]

export default function BankrollPage() {
  const [yearlyGoal, setYearlyGoal] = useState(20000)
  const [currentWeek, setCurrentWeek] = useState(1)
  const [weeks, setWeeks] = useState<WeekData[]>([])
  const [inputGoal, setInputGoal] = useState('20000')

  useEffect(() => {
    const saved = localStorage.getItem('bankroll_data_v2')
    if (saved) {
      const data = JSON.parse(saved)
      setYearlyGoal(data.yearlyGoal || 20000)
      setCurrentWeek(data.currentWeek || 1)
      setWeeks(data.weeks || [])
      setInputGoal((data.yearlyGoal || 20000).toString())
    }
  }, [])

  useEffect(() => {
    localStorage.setItem('bankroll_data_v2', JSON.stringify({ yearlyGoal, currentWeek, weeks }))
  }, [yearlyGoal, currentWeek, weeks])

  const weeklyTarget = Math.round(yearlyGoal / 52)
  const dailyTarget = Math.round(weeklyTarget / 7)
  const totalDeposited = weeks.reduce((sum, w) => sum + w.profit, 0)
  const progress = Math.min((weeks.length / 52) * 100, 100)

  const logWeek = (profit: number) => {
    if (currentWeek > 52) return
    const newWeek = { week: currentWeek, profit, target: weeklyTarget }
    setWeeks([...weeks, newWeek])
    setCurrentWeek(currentWeek + 1)
  }

  const undoLast = () => {
    if (weeks.length === 0) return
    const newWeeks = [...weeks]
    newWeeks.pop()
    setWeeks(newWeeks)
    setCurrentWeek(currentWeek - 1)
  }

  const resetAll = () => {
    if (confirm('Reset all bankroll data?')) {
      setYearlyGoal(20000)
      setCurrentWeek(1)
      setWeeks([])
      setInputGoal('20000')
    }
  }

  return (
    <div className="min-h-screen bg-[#0a0a12] text-[#F8F8FF] pb-24">
      <Header />

      <div className="bg-gradient-to-br from-[#7C3AED] via-[#8B5CF6] to-[#7B2CBF] p-5 relative overflow-hidden">
        <div className="relative z-10">
          <h1 className="text-2xl font-extrabold text-center mb-4 flex items-center justify-center gap-2 drop-shadow-lg">
            <i className="bi bi-trophy-fill text-[#FBBF24]"></i> 52 Week Bankroll Builder
          </h1>

          <div className="bg-[rgba(0,0,0,0.4)] rounded-xl p-4 mb-4 border border-[rgba(255,255,255,0.1)]">
            <div className="flex items-center justify-between mb-3 text-xs font-semibold text-[rgba(255,255,255,0.9)] uppercase">
              <span><i className="bi bi-gear-fill mr-1"></i> Yearly Goal</span>
            </div>
            <div className="flex items-center justify-center gap-2 mb-3">
              <span className="text-xl font-bold text-[#F59E0B]">$</span>
              <input 
                type="number" 
                value={inputGoal}
                onChange={(e) => {
                  setInputGoal(e.target.value)
                  const val = parseInt(e.target.value)
                  if (!isNaN(val)) setYearlyGoal(val)
                }}
                className="bg-[rgba(0,0,0,0.3)] border-2 border-[#F59E0B] rounded-lg text-xl font-bold p-2 w-32 text-center"
              />
              <span className="text-sm text-[rgba(255,255,255,0.7)]">/year</span>
            </div>
            <div className="flex gap-2 flex-wrap justify-center mb-4">
              {GOAL_PRESETS.map(goal => (
                <button key={goal} onClick={() => { setYearlyGoal(goal); setInputGoal(goal.toString()) }} className={`px-3 py-1.5 rounded-md text-[10px] font-bold border transition-all ${yearlyGoal === goal ? 'bg-[#F59E0B] text-[#0a0a12] border-[#F59E0B]' : 'bg-[rgba(255,255,255,0.1)] border-[rgba(255,255,255,0.2)]'}`}>
                  ${(goal/1000)}K
                </button>
              ))}
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div className="bg-[rgba(255,255,255,0.05)] rounded p-2 text-center">
                <div className="text-[10px] text-[rgba(255,255,255,0.6)] uppercase">Weekly Target</div>
                <div className="text-base font-bold text-[#F59E0B]">${weeklyTarget}</div>
              </div>
              <div className="bg-[rgba(255,255,255,0.05)] rounded p-2 text-center">
                <div className="text-[10px] text-[rgba(255,255,255,0.6)] uppercase">Daily Target</div>
                <div className="text-base font-bold text-[#F59E0B]">${dailyTarget}</div>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-3 gap-2 mb-4">
            <div className="bg-[rgba(0,0,0,0.3)] backdrop-blur-md border border-[rgba(255,255,255,0.1)] rounded-xl p-3 text-center">
              <div className="text-lg font-bold">{currentWeek}</div>
              <div className="text-[9px] text-[rgba(255,255,255,0.7)] uppercase font-bold mt-1">Week</div>
            </div>
            <div className="bg-[rgba(0,0,0,0.3)] backdrop-blur-md border border-[rgba(255,255,255,0.1)] rounded-xl p-3 text-center">
              <div className="text-lg font-bold">${totalDeposited}</div>
              <div className="text-[9px] text-[rgba(255,255,255,0.7)] uppercase font-bold mt-1">Deposited</div>
            </div>
            <div className="bg-[rgba(0,0,0,0.3)] backdrop-blur-md border border-[rgba(255,255,255,0.1)] rounded-xl p-3 text-center">
              <div className="text-lg font-bold">{weeks.length}</div>
              <div className="text-[9px] text-[rgba(255,255,255,0.7)] uppercase font-bold mt-1">Streak</div>
            </div>
          </div>

          <div className="bg-[rgba(0,0,0,0.3)] rounded-xl p-3 border border-[rgba(255,255,255,0.1)]">
            <div className="flex justify-between text-[11px] font-bold mb-2">
              <span>Goal Progress</span>
              <span>{Math.round(progress)}%</span>
            </div>
            <div className="w-full h-2.5 bg-[rgba(0,0,0,0.4)] rounded-full overflow-hidden">
              <div className="h-full bg-gradient-to-r from-[#10B981] to-[#06B6D4] shadow-[0_0_15px_rgba(16,185,129,0.5)] transition-all duration-700" style={{ width: `${progress}%` }}></div>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-[600px] mx-auto p-4">
        <div className="bg-[#13111c] border border-[#2d2640] rounded-2xl p-4 mb-6 shadow-xl">
          <h3 className="text-sm font-bold mb-4">Log Week {currentWeek} Result</h3>
          <div className="grid grid-cols-3 gap-2">
            {[-weeklyTarget, 0, Math.round(weeklyTarget*0.5), weeklyTarget, Math.round(weeklyTarget*1.5), weeklyTarget*2].map(p => (
              <button key={p} onClick={() => logWeek(p)} className={`py-3 rounded-xl text-white font-bold text-sm active:scale-95 transition-all ${p >= weeklyTarget ? 'bg-gradient-to-br from-[#10B981] to-[#059669]' : p > 0 ? 'bg-gradient-to-br from-[#F59E0B] to-[#D97706]' : p === 0 ? 'bg-[#1e1a2a]' : 'bg-gradient-to-br from-[#EF4444] to-[#DC2626]'}`}>
                {p >= 0 ? '+' : ''}${p}
              </button>
            ))}
          </div>
        </div>

        {weeks.length > 0 && (
          <div className="bg-[#13111c] border border-[#2d2640] rounded-2xl p-4">
            <h3 className="text-sm font-bold mb-4">Week History</h3>
            <div className="space-y-3">
              {[...weeks].reverse().map(w => (
                <div key={w.week} className="flex justify-between items-center py-2 border-b border-[#2d2640] last:border-0">
                  <div className="flex items-center gap-3">
                    <div className={`w-8 h-8 rounded-full flex items-center justify-center text-[10px] font-bold ${w.profit >= w.target ? 'bg-[#10B981] text-white' : 'bg-[#1e1a2a] text-[#9990B0]'}`}>{w.week}</div>
                    <span className="text-sm font-medium">Week {w.week}</span>
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-[10px] text-[#9990B0]">Target: ${w.target}</span>
                    <span className={`text-sm font-bold ${w.profit >= w.target ? 'text-[#10B981]' : w.profit > 0 ? 'text-[#F59E0B]' : 'text-[#EF4444]'}`}>
                      {w.profit >= 0 ? '+' : ''}${w.profit}
                    </span>
                    {w.profit >= w.target && <i className="bi bi-check-circle-fill text-[#10B981]"></i>}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex justify-center gap-4 mt-8">
          <button onClick={undoLast} disabled={weeks.length === 0} className="px-4 py-2 border border-red-500 text-red-500 rounded-lg text-xs font-bold active:scale-95 disabled:opacity-30">Undo Last</button>
          <button onClick={resetAll} className="px-4 py-2 border border-[#2d2640] text-[#9990B0] rounded-lg text-xs font-bold active:scale-95">Reset All</button>
        </div>
      </div>

      <MobileNav />
    </div>
  )
}
