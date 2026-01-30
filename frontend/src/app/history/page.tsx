'use client'

import { useState, useMemo } from 'react'
import { Container } from '@/components/layout'
import { Button, StatCard, Badge } from '@/components/ui'
import { PickCard } from '@/components/history'
import { useHistory, useUpdateResult, useCheckResults } from '@/hooks'
import type { League, PickResult } from '@/types'

type FilterResult = PickResult | 'all'
type FilterLeague = League | 'all'

export default function HistoryPage() {
  const { data, isLoading, error } = useHistory()
  const updateResult = useUpdateResult()
  const checkResults = useCheckResults()

  const [filterResult, setFilterResult] = useState<FilterResult>('all')
  const [filterLeague, setFilterLeague] = useState<FilterLeague>('all')

  // Filter picks
  const filteredPicks = useMemo(() => {
    if (!data?.picks) return []

    return data.picks.filter((pick) => {
      if (filterResult !== 'all' && pick.result !== filterResult) return false
      if (filterLeague !== 'all' && pick.league !== filterLeague) return false
      return true
    })
  }, [data?.picks, filterResult, filterLeague])

  const handleUpdateResult = (pickId: number, result: 'W' | 'L' | 'P') => {
    updateResult.mutate({ pickId, result })
  }

  if (isLoading) {
    return (
      <Container className="py-8">
        <div className="flex items-center justify-center min-h-[50vh]">
          <div className="text-center">
            <div className="w-12 h-12 border-4 border-[var(--gold)] border-t-transparent rounded-full animate-spin mx-auto mb-4" />
            <p className="text-gray-400">Loading history...</p>
          </div>
        </div>
      </Container>
    )
  }

  if (error) {
    return (
      <Container className="py-8">
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-6 text-center">
          <h2 className="text-xl font-bold text-red-400 mb-2">Error Loading History</h2>
          <p className="text-gray-400 mb-4">
            {error instanceof Error ? error.message : 'Failed to load history'}
          </p>
          <Button onClick={() => window.location.reload()}>Retry</Button>
        </div>
      </Container>
    )
  }

  const stats = data?.stats

  return (
    <Container className="py-6 space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white">Pick History</h1>
          <p className="text-gray-400 text-sm">
            {filteredPicks.length} of {data?.picks?.length || 0} picks
          </p>
        </div>

        <Button
          variant="outline"
          size="sm"
          onClick={() => checkResults.mutate()}
          loading={checkResults.isPending}
        >
          Check Results
        </Button>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <StatCard label="Total Picks" value={stats.total} />
          <StatCard
            label="Wins"
            value={stats.wins}
            className="border-green-500/30"
          />
          <StatCard
            label="Losses"
            value={stats.losses}
            className="border-red-500/30"
          />
          <StatCard label="Pushes" value={stats.pushes} />
          <StatCard
            label="Win Rate"
            value={`${(stats.win_rate * 100).toFixed(1)}%`}
            trend={stats.win_rate >= 0.55 ? 'up' : stats.win_rate < 0.5 ? 'down' : 'neutral'}
          />
        </div>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-4">
        {/* Result Filter */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-400">Result:</span>
          <div className="flex gap-1">
            {(['all', 'W', 'L', 'P', null] as const).map((r) => (
              <button
                key={r ?? 'pending'}
                onClick={() => setFilterResult(r)}
                className={`px-3 py-1 rounded-lg text-sm font-medium transition-colors ${
                  filterResult === r
                    ? 'bg-[var(--purple)] text-white'
                    : 'bg-[var(--border)] text-gray-400 hover:text-white'
                }`}
              >
                {r === 'all' ? 'All' : r === null ? 'Pending' : r}
              </button>
            ))}
          </div>
        </div>

        {/* League Filter */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-400">League:</span>
          <div className="flex gap-1">
            {(['all', 'NBA', 'CBB', 'NFL', 'CFB', 'NHL'] as const).map((l) => (
              <button
                key={l}
                onClick={() => setFilterLeague(l)}
                className={`px-3 py-1 rounded-lg text-sm font-medium transition-colors ${
                  filterLeague === l
                    ? 'bg-[var(--purple)] text-white'
                    : 'bg-[var(--border)] text-gray-400 hover:text-white'
                }`}
              >
                {l === 'all' ? 'All' : l}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Picks List */}
      {filteredPicks.length > 0 ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {filteredPicks.map((pick) => (
            <PickCard
              key={pick.id}
              pick={pick}
              onUpdateResult={(result) => handleUpdateResult(pick.id, result)}
              loading={updateResult.isPending}
            />
          ))}
        </div>
      ) : (
        <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-8 text-center">
          <p className="text-gray-400">No picks match your filters.</p>
        </div>
      )}
    </Container>
  )
}
