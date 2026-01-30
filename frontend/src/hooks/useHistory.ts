'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { POLLING_INTERVALS } from '@/lib/constants'

export function useHistory() {
  return useQuery({
    queryKey: ['history'],
    queryFn: api.getHistoryData,
    refetchInterval: POLLING_INTERVALS.history,
  })
}

export function useUpdateResult() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ pickId, result }: { pickId: number; result: 'W' | 'L' | 'P' }) =>
      api.updateResult(pickId, result),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['history'] })
      queryClient.invalidateQueries({ queryKey: ['games'] })
    },
  })
}

export function useCheckResults() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: api.checkResults,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['history'] })
    },
  })
}
