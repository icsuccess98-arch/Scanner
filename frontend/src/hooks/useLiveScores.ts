'use client'

import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { POLLING_INTERVALS } from '@/lib/constants'
import type { LiveScoresResponse } from '@/types'

export function useLiveScores(enabled: boolean = true) {
  return useQuery({
    queryKey: ['liveScores'],
    queryFn: api.getLiveScores,
    refetchInterval: enabled ? POLLING_INTERVALS.liveScores : false,
    enabled,
  })
}

export function useLiveScore(gameId: number, enabled: boolean = true) {
  const { data: scores, ...rest } = useLiveScores(enabled)

  return {
    ...rest,
    data: scores?.[gameId.toString()] || null,
  }
}
