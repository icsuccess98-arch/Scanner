export type League = 'NBA' | 'CBB' | 'NFL' | 'CFB' | 'NHL'
export type GameStatus = 'pregame' | 'live' | 'final'
export type PickResult = 'W' | 'L' | 'P' | null
export type ConfidenceTier = 'SUPERMAX' | 'HIGH' | 'MEDIUM' | 'LOW' | null

export interface Game {
  id: number
  league: League
  away_team: string
  home_team: string
  away_logo?: string
  home_logo?: string
  game_time: string
  line: number | null
  edge: number | null
  direction: 'O' | 'U' | null
  is_qualified: boolean
  projected_total: number | null
  confidence_tier?: ConfidenceTier
  ev?: number | null
  history_rate?: number | null
  bet_pct?: number | null
  money_pct?: number | null
  has_rlm?: boolean
  is_live?: boolean
  is_final?: boolean
}

export interface LiveScore {
  game_id: number
  away_score: number
  home_score: number
  period: string
  clock: string
  status: string
  is_final: boolean
  is_live: boolean
}

export interface LiveScoresResponse {
  [key: string]: LiveScore
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
  confidence_tier?: ConfidenceTier
}

export interface DashboardData {
  games: Game[]
  supermax: Game | null
  stats: DashboardStats
  last_updated: string
}

export interface DashboardStats {
  total_games: number
  qualified_games: number
  supermax_count: number
  win_rate?: number
  record?: {
    wins: number
    losses: number
    pushes: number
  }
}

export interface HistoryData {
  picks: Pick[]
  stats: {
    total: number
    wins: number
    losses: number
    pushes: number
    win_rate: number
    roi?: number
  }
}

export interface MatchupData {
  game: Game
  team_stats: {
    away: TeamStats
    home: TeamStats
  }
  h2h?: HeadToHead
  trends?: string[]
}

export interface TeamStats {
  ppg: number
  oppg: number
  pace?: number
  off_rating?: number
  def_rating?: number
  ats_record?: string
  ou_record?: string
}

export interface HeadToHead {
  games: number
  away_wins: number
  home_wins: number
  avg_total: number
  over_pct: number
}

export interface ApiResponse<T> {
  success: boolean
  data?: T
  error?: string
  message?: string
}
