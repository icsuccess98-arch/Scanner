export type League = 'NBA' | 'CBB' | 'NFL' | 'CFB' | 'NHL'
export type GameStatus = 'pregame' | 'live' | 'final'
export type PickResult = 'W' | 'L' | 'P' | null

export interface Game {
  id: number
  league: League
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
  confidence_tier?: string
  ev?: number | null
  history_rate?: number | null
  bet_pct?: number | null
  money_pct?: number | null
  has_rlm?: boolean
  is_live?: boolean
  is_final?: boolean
  away_score?: number
  home_score?: number
  period?: string
  clock?: string
  is_supermax?: boolean
}

export interface LiveScore {
  away_score: number
  home_score: number
  period: string
  clock: string
  status: string
  is_final: boolean
  is_live: boolean
}

export interface DashboardData {
  games: Game[]
  supermax_lock: Game | null
  total_games: number
  qualified_games: number
  last_updated: string
}

export interface Pick {
  id: number
  date: string
  matchup: string
  league: League
  pick: string
  line: number
  edge: number
  result: PickResult
  is_lock: boolean
}
