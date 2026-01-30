'use client'

import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { POLLING_INTERVALS } from '@/lib/constants'
import type { League } from '@/types'

export function useLiveLines(league: League, enabled: boolean = true) {
  return useQuery({
    queryKey: ['liveLines', league],
    queryFn: () => api.getLiveLines(league),
    refetchInterval: enabled ? POLLING_INTERVALS.liveLines : false,
    enabled,
  })
}
