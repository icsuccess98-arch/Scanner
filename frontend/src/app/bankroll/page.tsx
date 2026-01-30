'use client'

import { useState, useEffect } from 'react'
import { Container } from '@/components/layout'
import { Button, StatCard, ProgressBar } from '@/components/ui'

interface WeekData {
  week: number
  goal: number
  actual: number
  completed: boolean
}

const DEFAULT_GOAL = 100
const TOTAL_WEEKS = 52

export default function BankrollPage() {
  const [bankroll, setBankroll] = useState(1000)
  const [weeklyGoal, setWeeklyGoal] = useState(DEFAULT_GOAL)
  const [currentWeek, setCurrentWeek] = useState(1)
  const [weeks, setWeeks] = useState<WeekData[]>([])
  const [editingGoal, setEditingGoal] = useState(false)
  const [tempGoal, setTempGoal] = useState(weeklyGoal.toString())

  // Load from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('bankroll_data')
    if (saved) {
      const data = JSON.parse(saved)
      setBankroll(data.bankroll || 1000)
      setWeeklyGoal(data.weeklyGoal || DEFAULT_GOAL)
      setCurrentWeek(data.currentWeek || 1)
      setWeeks(data.weeks || [])
    }
  }, [])

  // Save to localStorage
  useEffect(() => {
    localStorage.setItem('bankroll_data', JSON.stringify({
      bankroll,
      weeklyGoal,
      currentWeek,
      weeks,
    }))
  }, [bankroll, weeklyGoal, currentWeek, weeks])

  const handleSaveGoal = () => {
    const goal = parseInt(tempGoal)
    if (!isNaN(goal) && goal > 0) {
      setWeeklyGoal(goal)
    }
    setEditingGoal(false)
  }

  const handleCompleteWeek = (profit: number) => {
    const newWeek: WeekData = {
      week: currentWeek,
      goal: weeklyGoal,
      actual: profit,
      completed: true,
    }
    setWeeks([...weeks, newWeek])
    setBankroll(bankroll + profit)
    setCurrentWeek(currentWeek + 1)
  }

  const totalProfit = weeks.reduce((sum, w) => sum + w.actual, 0)
  const goalsMet = weeks.filter(w => w.actual >= w.goal).length
  const successRate = weeks.length > 0 ? (goalsMet / weeks.length) * 100 : 0
  const projectedYear = bankroll + (weeklyGoal * (TOTAL_WEEKS - currentWeek + 1))

  const goalPresets = [50, 100, 150, 200, 250, 500]

  return (
    <Container className="py-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-white">Bankroll Builder</h1>
        <p className="text-gray-400 text-sm">
          52-Week Challenge - Week {currentWeek}
        </p>
      </div>

      {/* Main Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="Current Bankroll"
          value={`$${bankroll.toLocaleString()}`}
        />
        <StatCard
          label="Total Profit"
          value={`$${totalProfit >= 0 ? '+' : ''}${totalProfit.toLocaleString()}`}
          trend={totalProfit >= 0 ? 'up' : 'down'}
        />
        <StatCard
          label="Goals Met"
          value={`${goalsMet}/${weeks.length}`}
          subValue={`${successRate.toFixed(0)}%`}
        />
        <StatCard
          label="Projected EOY"
          value={`$${projectedYear.toLocaleString()}`}
        />
      </div>

      {/* Progress */}
      <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Year Progress</h2>
        <ProgressBar
          value={currentWeek - 1}
          max={TOTAL_WEEKS}
          label={`Week ${currentWeek} of ${TOTAL_WEEKS}`}
          color="gold"
        />
      </div>

      {/* Weekly Goal Setting */}
      <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-bold text-white">Weekly Goal</h2>
          {!editingGoal ? (
            <Button variant="ghost" size="sm" onClick={() => {
              setTempGoal(weeklyGoal.toString())
              setEditingGoal(true)
            }}>
              Edit
            </Button>
          ) : (
            <div className="flex gap-2">
              <Button variant="ghost" size="sm" onClick={() => setEditingGoal(false)}>
                Cancel
              </Button>
              <Button variant="primary" size="sm" onClick={handleSaveGoal}>
                Save
              </Button>
            </div>
          )}
        </div>

        {editingGoal ? (
          <div className="space-y-4">
            <div>
              <label className="block text-sm text-gray-400 mb-2">
                Enter custom goal
              </label>
              <input
                type="number"
                value={tempGoal}
                onChange={(e) => setTempGoal(e.target.value)}
                className="w-full bg-[var(--background)] border border-[var(--border)] rounded-lg px-4 py-2 text-white focus:outline-none focus:border-[var(--purple)]"
                placeholder="Weekly goal amount"
              />
            </div>
            <div>
              <p className="text-sm text-gray-400 mb-2">Quick presets</p>
              <div className="flex flex-wrap gap-2">
                {goalPresets.map((preset) => (
                  <button
                    key={preset}
                    onClick={() => setTempGoal(preset.toString())}
                    className={`px-3 py-1 rounded-lg text-sm font-medium transition-colors ${
                      tempGoal === preset.toString()
                        ? 'bg-[var(--gold)] text-black'
                        : 'bg-[var(--border)] text-gray-400 hover:text-white'
                    }`}
                  >
                    ${preset}
                  </button>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="text-center">
            <p className="text-4xl font-bold text-[var(--gold)]">
              ${weeklyGoal}
            </p>
            <p className="text-gray-400 text-sm mt-1">per week</p>
          </div>
        )}
      </div>

      {/* Record Week Profit */}
      <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-6">
        <h2 className="text-lg font-bold text-white mb-4">Record Week {currentWeek} Results</h2>
        <div className="grid grid-cols-3 gap-3">
          {[-weeklyGoal, 0, weeklyGoal / 2, weeklyGoal, weeklyGoal * 1.5, weeklyGoal * 2].map((profit) => (
            <Button
              key={profit}
              variant={profit >= weeklyGoal ? 'secondary' : profit < 0 ? 'danger' : 'outline'}
              onClick={() => handleCompleteWeek(profit)}
            >
              {profit >= 0 ? '+' : ''}{profit}
            </Button>
          ))}
        </div>
      </div>

      {/* Recent Weeks */}
      {weeks.length > 0 && (
        <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-6">
          <h2 className="text-lg font-bold text-white mb-4">Recent Weeks</h2>
          <div className="space-y-2">
            {[...weeks].reverse().slice(0, 10).map((week) => (
              <div
                key={week.week}
                className="flex items-center justify-between py-2 border-b border-[var(--border)] last:border-0"
              >
                <span className="text-gray-400">Week {week.week}</span>
                <div className="flex items-center gap-4">
                  <span className="text-gray-500 text-sm">
                    Goal: ${week.goal}
                  </span>
                  <span className={`font-bold ${
                    week.actual >= week.goal
                      ? 'text-green-400'
                      : week.actual >= 0
                        ? 'text-yellow-400'
                        : 'text-red-400'
                  }`}>
                    {week.actual >= 0 ? '+' : ''}${week.actual}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Reset Button */}
      <div className="text-center">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => {
            if (confirm('Are you sure you want to reset all bankroll data?')) {
              setBankroll(1000)
              setWeeklyGoal(DEFAULT_GOAL)
              setCurrentWeek(1)
              setWeeks([])
            }
          }}
          className="text-red-400 hover:text-red-300"
        >
          Reset All Data
        </Button>
      </div>
    </Container>
  )
}
