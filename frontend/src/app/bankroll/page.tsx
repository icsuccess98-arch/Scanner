'use client'

import { useState, useEffect } from 'react'
import { Header, MobileNav } from '@/components/layout'

interface WeekData {
  week: number
  profit: number
  target: number
}

const GOAL_PRESETS = [50, 100, 150, 200, 250, 300]

export default function BankrollPage() {
  const [weeklyGoal, setWeeklyGoal] = useState(100)
  const [currentWeek, setCurrentWeek] = useState(1)
  const [weeks, setWeeks] = useState<WeekData[]>([])
  const [inputGoal, setInputGoal] = useState('100')

  // Load from localStorage
  useEffect(() => {
    const saved = localStorage.getItem('bankroll_data')
    if (saved) {
      const data = JSON.parse(saved)
      setWeeklyGoal(data.weeklyGoal || 100)
      setCurrentWeek(data.currentWeek || 1)
      setWeeks(data.weeks || [])
      setInputGoal((data.weeklyGoal || 100).toString())
    }
  }, [])

  // Save to localStorage
  useEffect(() => {
    localStorage.setItem('bankroll_data', JSON.stringify({
      weeklyGoal,
      currentWeek,
      weeks
    }))
  }, [weeklyGoal, currentWeek, weeks])

  const handleGoalChange = (value: number) => {
    setWeeklyGoal(value)
    setInputGoal(value.toString())
  }

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setInputGoal(e.target.value)
    const val = parseInt(e.target.value)
    if (!isNaN(val) && val > 0) {
      setWeeklyGoal(val)
    }
  }

  const logWeek = (profit: number) => {
    const newWeek: WeekData = {
      week: currentWeek,
      profit,
      target: weeklyGoal
    }
    setWeeks([...weeks, newWeek])
    setCurrentWeek(currentWeek + 1)
  }

  const resetAll = () => {
    if (confirm('Reset all bankroll data?')) {
      setWeeklyGoal(100)
      setCurrentWeek(1)
      setWeeks([])
      setInputGoal('100')
    }
  }

  // Calculate stats
  const totalProfit = weeks.reduce((sum, w) => sum + w.profit, 0)
  const weeksHit = weeks.filter(w => w.profit >= w.target).length
  const hitRate = weeks.length > 0 ? ((weeksHit / weeks.length) * 100).toFixed(0) : '0'
  const yearlyProjection = weeklyGoal * 52
  const weeksRemaining = 52 - currentWeek + 1
  const projectedTotal = totalProfit + (weeklyGoal * weeksRemaining)

  return (
    <>
      <Header />

      {/* Hero Section */}
      <div style={{
        background: 'linear-gradient(135deg, #7C3AED 0%, #8B5CF6 50%, #7B2CBF 100%)',
        padding: '1.25rem 1rem',
        position: 'relative',
        overflow: 'hidden'
      }}>
        <div style={{ position: 'relative', zIndex: 1 }}>
          <h1 style={{
            fontSize: '1.5rem',
            fontWeight: 800,
            textAlign: 'center',
            marginBottom: '1rem',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: '0.5rem',
            textShadow: '0 2px 10px rgba(0,0,0,0.3)'
          }}>
            <i className="bi bi-trophy-fill" style={{ color: '#FBBF24', fontSize: '1.25rem' }} />
            52 Week Challenge
          </h1>

          {/* Goal Editor */}
          <div style={{
            background: 'rgba(0,0,0,0.4)',
            borderRadius: '12px',
            padding: '1rem',
            marginBottom: '1rem',
            border: '1px solid rgba(255,255,255,0.1)'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
              <span style={{ fontSize: '0.85rem', color: 'rgba(255,255,255,0.9)', fontWeight: 600 }}>
                Weekly Target
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span style={{ color: 'rgba(255,255,255,0.7)' }}>$</span>
                <input
                  type="number"
                  value={inputGoal}
                  onChange={handleInputChange}
                  style={{
                    background: 'rgba(0,0,0,0.3)',
                    border: '2px solid #F59E0B',
                    borderRadius: '8px',
                    color: 'var(--text)',
                    fontSize: '1.25rem',
                    fontWeight: 700,
                    padding: '0.5rem 0.75rem',
                    width: '100px',
                    textAlign: 'center'
                  }}
                />
              </div>
            </div>

            {/* Presets */}
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              {GOAL_PRESETS.map(preset => (
                <button
                  key={preset}
                  onClick={() => handleGoalChange(preset)}
                  style={{
                    background: weeklyGoal === preset ? '#F59E0B' : 'rgba(255,255,255,0.1)',
                    border: '1px solid ' + (weeklyGoal === preset ? '#F59E0B' : 'rgba(255,255,255,0.2)'),
                    borderRadius: '6px',
                    color: weeklyGoal === preset ? '#0a0a12' : 'var(--text)',
                    padding: '0.35rem 0.6rem',
                    fontSize: '0.7rem',
                    cursor: 'pointer',
                    transition: 'all 0.2s'
                  }}
                >
                  ${preset}
                </button>
              ))}
            </div>

            {/* Summary */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '0.5rem', marginTop: '0.75rem' }}>
              <div style={{ background: 'rgba(255,255,255,0.05)', borderRadius: '6px', padding: '0.5rem', textAlign: 'center' }}>
                <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.6)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                  Yearly Goal
                </div>
                <div style={{ fontSize: '1rem', fontWeight: 700, color: '#F59E0B' }}>
                  ${yearlyProjection.toLocaleString()}
                </div>
              </div>
              <div style={{ background: 'rgba(255,255,255,0.05)', borderRadius: '6px', padding: '0.5rem', textAlign: 'center' }}>
                <div style={{ fontSize: '0.65rem', color: 'rgba(255,255,255,0.6)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                  Week
                </div>
                <div style={{ fontSize: '1rem', fontWeight: 700, color: '#F59E0B' }}>
                  {currentWeek} / 52
                </div>
              </div>
            </div>
          </div>

          {/* Stats Row */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem' }}>
            <div style={{
              background: 'rgba(0,0,0,0.3)',
              borderRadius: '8px',
              padding: '0.75rem 0.5rem',
              textAlign: 'center'
            }}>
              <div style={{ fontSize: '1.25rem', fontWeight: 800, color: totalProfit >= 0 ? '#10B981' : '#EF4444' }}>
                {totalProfit >= 0 ? '+' : ''}${totalProfit}
              </div>
              <div style={{ fontSize: '0.6rem', color: 'rgba(255,255,255,0.7)', textTransform: 'uppercase' }}>
                Total P/L
              </div>
            </div>
            <div style={{
              background: 'rgba(0,0,0,0.3)',
              borderRadius: '8px',
              padding: '0.75rem 0.5rem',
              textAlign: 'center'
            }}>
              <div style={{ fontSize: '1.25rem', fontWeight: 800, color: '#FBBF24' }}>
                {hitRate}%
              </div>
              <div style={{ fontSize: '0.6rem', color: 'rgba(255,255,255,0.7)', textTransform: 'uppercase' }}>
                Hit Rate
              </div>
            </div>
            <div style={{
              background: 'rgba(0,0,0,0.3)',
              borderRadius: '8px',
              padding: '0.75rem 0.5rem',
              textAlign: 'center'
            }}>
              <div style={{ fontSize: '1.25rem', fontWeight: 800, color: '#06B6D4' }}>
                ${projectedTotal}
              </div>
              <div style={{ fontSize: '0.6rem', color: 'rgba(255,255,255,0.7)', textTransform: 'uppercase' }}>
                Projected
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container" style={{ maxWidth: '600px', margin: '0 auto', padding: '1rem' }}>

        {/* Log Week Section */}
        <div style={{
          background: 'var(--bg-card)',
          border: '1px solid var(--border)',
          borderRadius: '12px',
          padding: '1rem',
          marginBottom: '1rem'
        }}>
          <div style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--text)', marginBottom: '0.75rem' }}>
            Log Week {currentWeek} Result
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem' }}>
            {[-weeklyGoal, 0, Math.round(weeklyGoal * 0.5), weeklyGoal, Math.round(weeklyGoal * 1.5), weeklyGoal * 2].map(profit => (
              <button
                key={profit}
                onClick={() => logWeek(profit)}
                style={{
                  background: profit >= weeklyGoal
                    ? 'linear-gradient(135deg, #10B981, #059669)'
                    : profit > 0
                      ? 'linear-gradient(135deg, #F59E0B, #D97706)'
                      : profit === 0
                        ? 'var(--bg-input)'
                        : 'linear-gradient(135deg, #EF4444, #DC2626)',
                  border: 'none',
                  borderRadius: '8px',
                  padding: '0.75rem',
                  color: 'white',
                  fontSize: '0.9rem',
                  fontWeight: 700,
                  cursor: 'pointer',
                  transition: 'transform 0.2s'
                }}
              >
                {profit >= 0 ? '+' : ''}${profit}
              </button>
            ))}
          </div>
        </div>

        {/* Week History */}
        {weeks.length > 0 && (
          <div style={{
            background: 'var(--bg-card)',
            border: '1px solid var(--border)',
            borderRadius: '12px',
            padding: '1rem'
          }}>
            <div style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--text)', marginBottom: '0.75rem' }}>
              Week History
            </div>
            {[...weeks].reverse().slice(0, 10).map(week => (
              <div
                key={week.week}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '0.5rem 0',
                  borderBottom: '1px solid var(--border)'
                }}
              >
                <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>Week {week.week}</span>
                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                  <span style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>
                    Target: ${week.target}
                  </span>
                  <span style={{
                    fontWeight: 700,
                    fontSize: '0.9rem',
                    color: week.profit >= week.target ? '#10B981' : week.profit > 0 ? '#F59E0B' : '#EF4444'
                  }}>
                    {week.profit >= 0 ? '+' : ''}${week.profit}
                  </span>
                  {week.profit >= week.target && (
                    <i className="bi bi-check-circle-fill" style={{ color: '#10B981' }} />
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Reset Button */}
        <div style={{ textAlign: 'center', marginTop: '1.5rem' }}>
          <button
            onClick={resetAll}
            style={{
              background: 'transparent',
              border: '1px solid var(--border)',
              borderRadius: '8px',
              padding: '0.5rem 1rem',
              color: 'var(--text-muted)',
              fontSize: '0.8rem',
              cursor: 'pointer'
            }}
          >
            Reset All Data
          </button>
        </div>
      </div>

      <MobileNav />
    </>
  )
}
