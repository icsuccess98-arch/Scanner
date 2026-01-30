'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { POLLING_INTERVALS } from '@/lib/constants'

export function useGames() {
  return useQuery({
    queryKey: ['games'],
    queryFn: api.getDashboardData,
    refetchInterval: POLLING_INTERVALS.dashboard,
  })
}

export function useFetchGames() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: api.fetchGames,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['games'] })
    },
  })
}

export function useFetchOdds() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: api.fetchOdds,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['games'] })
      queryClient.invalidateQueries({ queryKey: ['liveLines'] })
    },
  })
}

export function usePostDiscord() {
  return useMutation({
    mutationFn: api.postDiscord,
  })
}

export function usePostDiscordWindow() {
  return useMutation({
    mutationFn: (window: 'EARLY' | 'MID' | 'LATE') => api.postDiscordWindow(window),
  })
}
