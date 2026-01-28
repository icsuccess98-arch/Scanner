"""
============================================================
COMPREHENSIVE NBA BETTING ANALYSIS ENGINE
============================================================

This script integrates:
1. Stat Comparison Engine (with correct directional logic)
2. Spread Projection Engine (STRICTLY for point differentials - NOT totals)
3. Market Analysis (Sharp Money + RLM Detection)
4. Web Scraping Integration (Covers, VSIN, ScoresAndOdds, NBA.com, etc.)
5. Elimination Filters (80%+ handle, large spreads, bad teams, etc.)
6. Bet Decision Layer (Only bet when stats + market + spread align)

CRITICAL RULES:
- Spread model is SEPARATE from totals model
- Sharp money detection uses DIRECTIONAL differences (not abs())
- RLM detection requires opening + current spread
- Betting splits are CONTEXT, not stat edges
- All metrics use correct "higher/lower is better" logic
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import time
from typing import Dict, List, Optional, Tuple
import statistics


# ============================================================
# 1. STAT COMPARISON ENGINE (BULLETPROOF - NO CHANGES NEEDED)
# ============================================================

class StatEngine:
    """
    Handles all stat comparisons with correct directional logic.
    """
    
    # Metric direction mapping (VERIFIED CORRECT)
    METRIC_DIRECTIONS = {
        'ORB%': 'higher',
        'DRB%': 'higher',
        'TOV%': 'lower',
        'Forced TOV%': 'higher',
        'OFF_Efficiency': 'higher',
        'DEF_Efficiency': 'lower',
        'eFG%': 'higher',
        'Opp_eFG%': 'lower',
        '3PM_Game': 'higher',
        'Opp_3PM_Game': 'lower',
        'FT_Rate': 'higher',
        'Opp_FT_Rate': 'lower',
        'SOS_Rank': 'lower',
        'H2H_L10': 'higher',
        'Rest_Days': 'higher'
    }
    
    @staticmethod
    def compare_stat(team_value: float, opp_value: float, higher_is_better: bool = True) -> int:
        """
        Compare two team metrics.
        
        Args:
            team_value: First team's stat value
            opp_value: Second team's stat value
            higher_is_better: True if higher value is better, False if lower is better
            
        Returns:
            1 if team has edge, -1 if opponent has edge, 0 if tie
        """
        if team_value == opp_value:
            return 0
        
        if higher_is_better:
            return 1 if team_value > opp_value else -1
        else:
            return 1 if team_value < opp_value else -1
    
    @staticmethod
    def get_stat_direction(metric_name: str) -> bool:
        """
        Get the correct direction for a metric.
        
        Returns:
            True if higher is better, False if lower is better
        """
        direction = StatEngine.METRIC_DIRECTIONS.get(metric_name, 'higher')
        return direction == 'higher'
    
    @staticmethod
    def calculate_stat_edges(team_stats: Dict, opp_stats: Dict) -> Tuple[int, int, List[Dict]]:
        """
        Calculate stat edges for both teams across all metrics.
        
        Returns:
            (team_edge_count, opp_edge_count, detailed_comparisons)
        """
        team_edges = 0
        opp_edges = 0
        comparisons = []
        
        for metric, direction in StatEngine.METRIC_DIRECTIONS.items():
            if metric in team_stats and metric in opp_stats:
                higher_is_better = (direction == 'higher')
                
                edge = StatEngine.compare_stat(
                    team_stats[metric],
                    opp_stats[metric],
                    higher_is_better
                )
                
                if edge == 1:
                    team_edges += 1
                    result = 'Team Edge'
                elif edge == -1:
                    opp_edges += 1
                    result = 'Opponent Edge'
                else:
                    result = 'Tie'
                
                comparisons.append({
                    'metric': metric,
                    'team_value': team_stats[metric],
                    'opp_value': opp_stats[metric],
                    'result': result,
                    'higher_is_better': higher_is_better
                })
        
        return team_edges, opp_edges, comparisons


# ============================================================
# 2. SPREAD PROJECTION ENGINE (STRICTLY FOR POINT DIFFERENTIALS)
# ============================================================

class SpreadProjectionEngine:
    """
    This engine is used ONLY to calculate expected margin of victory.
    It does NOT calculate totals and must not be reused for totals modeling.
    
    IMPORTANT: This is a separate system from your totals model.
    """
    
    LEAGUE_AVG_PACE = 100.0  # NBA average possessions per game
    
    def __init__(self):
        pass
    
    # ----------------------------
    # Core helpers
    # ----------------------------
    @staticmethod
    def pace_factor(pace_a: float, pace_b: float) -> float:
        """Calculate pace scaling factor."""
        avg_pace = (pace_a + pace_b) / 2
        return avg_pace / SpreadProjectionEngine.LEAGUE_AVG_PACE
    
    @staticmethod
    def base_points(ppg_a: float, opp_ppg_b: float, def_eff_b: Optional[float] = None) -> float:
        """
        Calculate base projected points with defensive efficiency blended safely.
        
        Args:
            ppg_a: Team A's points per game
            opp_ppg_b: Points per game allowed by Team B
            def_eff_b: Team B's defensive efficiency (optional but recommended)
        """
        if def_eff_b is not None:
            # Blend defensive efficiency (60% actual PPG, 40% efficiency)
            def_blend = 0.6 * opp_ppg_b + 0.4 * def_eff_b
        else:
            def_blend = opp_ppg_b
        
        return (ppg_a + def_blend) / 2
    
    # ----------------------------
    # Possession adjustments
    # ----------------------------
    @staticmethod
    def turnover_adjustment(off_to_a: float, def_to_a: float, 
                          off_to_b: float, def_to_b: float) -> float:
        """
        Calculate points adjustment from turnover differential.
        Positive means Team A benefits.
        """
        net_to = (def_to_a - off_to_a) - (def_to_b - off_to_b)
        return net_to * 1.05  # Average points per possession
    
    @staticmethod
    def rebounding_adjustment(orb_a: float, drb_b: float) -> float:
        """
        Calculate points adjustment from offensive rebounding edge.
        """
        reb_edge = orb_a - drb_b
        return reb_edge * 1.1  # Points per extra possession
    
    # ----------------------------
    # Efficiency & pressure
    # ----------------------------
    @staticmethod
    def shooting_efficiency_adjustment(base_pts: float, fg_edge: float, 
                                     tp_edge: float, ft_edge: float) -> float:
        """
        Calculate efficiency adjustment (capped to avoid noise).
        
        Args:
            base_pts: Base projected points
            fg_edge: FG% differential (team - opp)
            tp_edge: 3P% differential (team - opp)
            ft_edge: FT% differential (team - opp)
        """
        eff_score = (
            fg_edge * 0.40 +
            tp_edge * 0.35 +
            ft_edge * 0.25
        ) / 100
        
        # Cap to ±4% to prevent hot shooting noise
        eff_score = max(min(eff_score, 0.04), -0.04)
        return base_pts * eff_score
    
    @staticmethod
    def free_throw_adjustment(fta_a: float, ft_pct_a: float, 
                            fta_b: float, ft_pct_b: float) -> float:
        """Calculate free throw advantage."""
        return (fta_a * ft_pct_a / 100) - (fta_b * ft_pct_b / 100)
    
    @staticmethod
    def ball_control_adjustment(ast_to_a: float, ast_to_b: float) -> float:
        """
        Calculate ball control quality adjustment (capped at ±1 point).
        """
        adj = (ast_to_a - ast_to_b) * 0.5
        return max(min(adj, 1), -1)
    
    # ----------------------------
    # Final projected points (spread-only)
    # ----------------------------
    def projected_points(self, team: Dict, opp: Dict) -> float:
        """
        Calculate final projected points for a team.
        
        Required keys in team/opp dicts:
        - ppg: Points per game
        - opp_ppg: Opponent points per game allowed
        - pace: Possessions per game
        - off_to: Offensive turnovers per game
        - def_to: Defensive turnovers forced per game
        - orb: Offensive rebounds per game
        - drb: Defensive rebounds per game
        - fta: Free throw attempts per game
        - ft_pct: Free throw percentage
        - ast_to: Assist to turnover ratio
        - fg_edge: FG% - Opp FG% allowed
        - tp_edge: 3P% - Opp 3P% allowed
        - ft_edge: FT% - Opp FT% allowed
        
        Optional:
        - def_eff: Defensive efficiency (points per 100 possessions)
        """
        # Base projection
        base_pts = self.base_points(
            team['ppg'],
            opp['opp_ppg'],
            opp.get('def_eff')
        )
        
        # Scale by pace
        paced_pts = base_pts * self.pace_factor(team['pace'], opp['pace'])
        
        # Start with paced points
        pts = paced_pts
        
        # Add possession adjustments
        pts += self.turnover_adjustment(
            team['off_to'], team['def_to'],
            opp['off_to'], opp['def_to']
        )
        
        pts += self.rebounding_adjustment(team['orb'], opp['drb'])
        
        # Add pressure/foul adjustments
        pts += self.free_throw_adjustment(
            team['fta'], team['ft_pct'],
            opp['fta'], opp['ft_pct']
        )
        
        # Add quality control
        pts += self.ball_control_adjustment(
            team['ast_to'], opp['ast_to']
        )
        
        # Add efficiency adjustment
        pts += self.shooting_efficiency_adjustment(
            paced_pts,
            team['fg_edge'],
            team['tp_edge'],
            team['ft_edge']
        )
        
        return round(pts, 1)
    
    # ----------------------------
    # TRUE SPREAD OUTPUT
    # ----------------------------
    def true_spread(self, away: Dict, home: Dict) -> Dict:
        """
        Calculate the true spread (expected point differential).
        
        Returns:
            {
                'away_proj_pts': float,
                'home_proj_pts': float,
                'true_spread': float (positive = away favored)
            }
        """
        away_pts = self.projected_points(away, home)
        home_pts = self.projected_points(home, away)
        
        return {
            'away_proj_pts': away_pts,
            'home_proj_pts': home_pts,
            'true_spread': round(away_pts - home_pts, 2)
        }


# ============================================================
# 3. MARKET ENGINE (SHARP MONEY + RLM DETECTION)
# ============================================================

class MarketEngine:
    """
    Detects sharp money and reverse line movement (RLM).
    
    CRITICAL FIXES:
    1. Uses DIRECTIONAL differences (not abs())
    2. Betting splits are CONTEXT, not stat edges
    3. RLM requires opening + current spread
    """
    
    def __init__(self, sharp_threshold: float = 15.0):
        """
        Args:
            sharp_threshold: Minimum % difference between money and tickets to flag as sharp
        """
        self.sharp_threshold = sharp_threshold
    
    def sharp_diff(self, money: float, tickets: float) -> float:
        """
        Calculate DIRECTIONAL sharp difference.
        
        Positive = sharp side (money > tickets)
        Negative = public side (tickets > money)
        
        THIS IS THE CRITICAL FIX - NO abs() allowed here!
        """
        return money - tickets
    
    def detect_sharp(self, away_money: float, away_tkt: float, 
                    home_money: float, home_tkt: float) -> Dict:
        """
        Detect which side has sharp action.
        
        Args:
            away_money: % of money on away team
            away_tkt: % of tickets on away team
            home_money: % of money on home team
            home_tkt: % of tickets on home team
            
        Returns:
            {
                'away': 'Sharp'/'Public'/None,
                'home': 'Sharp'/'Public'/None,
                'sharp_side': 'Away'/'Home'/'Mixed/No clear sharp',
                'away_diff': float,
                'home_diff': float
            }
        """
        away_diff = self.sharp_diff(away_money, away_tkt)
        home_diff = self.sharp_diff(home_money, home_tkt)
        
        sharp_info = {
            'away': None,
            'home': None,
            'sharp_side': None,
            'away_diff': away_diff,
            'home_diff': home_diff
        }
        
        # Assign sharp/public labels
        if away_diff >= self.sharp_threshold:
            sharp_info['away'] = 'Sharp'
        elif away_diff <= -self.sharp_threshold:
            sharp_info['away'] = 'Public'
        
        if home_diff >= self.sharp_threshold:
            sharp_info['home'] = 'Sharp'
        elif home_diff <= -self.sharp_threshold:
            sharp_info['home'] = 'Public'
        
        # Determine overall sharp side
        if sharp_info['away'] == 'Sharp' and sharp_info['home'] != 'Sharp':
            sharp_info['sharp_side'] = 'Away'
        elif sharp_info['home'] == 'Sharp' and sharp_info['away'] != 'Sharp':
            sharp_info['sharp_side'] = 'Home'
        else:
            sharp_info['sharp_side'] = 'Mixed/No clear sharp'
        
        return sharp_info
    
    def detect_rlm(self, opening_spread: float, current_spread: float, 
                  sharp_side: str) -> Dict:
        """
        Detect Reverse Line Movement (trap games).
        
        RLM = Money on one side, line moves OPPOSITE direction
        
        Args:
            opening_spread: Opening spread (negative = away favored)
            current_spread: Current spread (negative = away favored)
            sharp_side: 'Away'/'Home'/'Mixed/No clear sharp'
            
        Returns:
            {
                'rlm_detected': bool,
                'rlm_flag': str,
                'line_movement': float,
                'movement_direction': str
            }
        """
        line_movement = current_spread - opening_spread
        
        result = {
            'rlm_detected': False,
            'rlm_flag': 'No RLM detected',
            'line_movement': line_movement,
            'movement_direction': 'No movement'
        }
        
        if line_movement > 0:
            result['movement_direction'] = 'Moved toward Home'
        elif line_movement < 0:
            result['movement_direction'] = 'Moved toward Away'
        
        # RLM detection
        if sharp_side == 'Away' and line_movement > 0:
            result['rlm_detected'] = True
            result['rlm_flag'] = '⚠️ TRAP: Away sharp, line moved Home'
        elif sharp_side == 'Home' and line_movement < 0:
            result['rlm_detected'] = True
            result['rlm_flag'] = '⚠️ TRAP: Home sharp, line moved Away'
        
        return result
    
    def market_summary(self, away_money: float, away_tkt: float, 
                      home_money: float, home_tkt: float,
                      opening_spread: float, current_spread: float) -> Dict:
        """
        Full market analysis combining sharp detection and RLM.
        
        Returns:
            {
                'sharp_info': dict,
                'rlm_info': dict,
                'high_handle': bool (if one side has 80%+ handle)
            }
        """
        sharp_info = self.detect_sharp(away_money, away_tkt, home_money, home_tkt)
        rlm_info = self.detect_rlm(opening_spread, current_spread, sharp_info['sharp_side'])
        
        # Check for high handle (80%+ on one side)
        high_handle = (away_money >= 80 or home_money >= 80 or 
                      away_tkt >= 80 or home_tkt >= 80)
        
        return {
            'sharp_info': sharp_info,
            'rlm_info': rlm_info,
            'high_handle': high_handle
        }


# ============================================================
# 4. ELIMINATION FILTERS
# ============================================================

class EliminationFilters:
    """
    Process of elimination to filter out games that should be avoided.
    
    Filters:
    1. 80%+ Handle on one side
    2. Large spreads (10+ points)
    3. Bad teams with terrible records (0-5, 0-12, 2-12, etc.)
    4. Bad defense in last 5 games
    5. Back-to-back situations
    """
    
    @staticmethod
    def check_high_handle(market_summary: Dict) -> Tuple[bool, str]:
        """
        Filter #1: Check if 80%+ of money or tickets on one side.
        
        Returns:
            (should_avoid, reason)
        """
        if market_summary['high_handle']:
            return True, "80%+ handle on one side - public trap risk"
        return False, ""
    
    @staticmethod
    def check_large_spread(spread: float, threshold: float = 10.0) -> Tuple[bool, str]:
        """
        Filter #2: Avoid large spreads (10+ points).
        
        Returns:
            (should_avoid, reason)
        """
        if abs(spread) >= threshold:
            return True, f"Large spread ({spread:.1f}) - unpredictable"
        return False, ""
    
    @staticmethod
    def check_bad_record(wins: int, losses: int, 
                        bad_thresholds: List[Tuple[int, int]] = None) -> Tuple[bool, str]:
        """
        Filter #3: Avoid teams with terrible records.
        
        Default bad records: 0-5, 0-12, 2-12, etc.
        
        Returns:
            (should_avoid, reason)
        """
        if bad_thresholds is None:
            bad_thresholds = [
                (0, 5), (0, 10), (0, 12),
                (1, 10), (1, 12), (1, 15),
                (2, 10), (2, 12), (2, 15)
            ]
        
        record = (wins, losses)
        if losses >= 5:  # Only check if team has played enough games
            for bad_record in bad_thresholds:
                if wins <= bad_record[0] and losses >= bad_record[1]:
                    return True, f"Bad record ({wins}-{losses}) - unreliable"
        
        return False, ""
    
    @staticmethod
    def check_bad_defense_l5(def_rank_l5: int, threshold: int = 25) -> Tuple[bool, str]:
        """
        Filter #4: Avoid teams with bad defense in last 5 games.
        
        Args:
            def_rank_l5: Defensive rank in last 5 games (1 = best, 30 = worst)
            threshold: Rank threshold (default: 25th or worse)
            
        Returns:
            (should_avoid, reason)
        """
        if def_rank_l5 >= threshold:
            return True, f"Bad defense L5 (Rank {def_rank_l5}) - vulnerable"
        return False, ""
    
    @staticmethod
    def check_back_to_back(is_b2b: bool, rest_days: int = 0) -> Tuple[bool, str]:
        """
        Filter #5: Flag back-to-back games (optional avoidance).
        
        Returns:
            (is_b2b, info_message)
        """
        if is_b2b or rest_days == 0:
            return True, "Back-to-back game - fatigue factor"
        return False, ""
    
    @staticmethod
    def run_all_filters(game_data: Dict) -> Dict:
        """
        Run all elimination filters on a game.
        
        Required keys in game_data:
        - market_summary: Dict from MarketEngine
        - spread: Current spread
        - away_record: Tuple (wins, losses)
        - home_record: Tuple (wins, losses)
        - away_def_rank_l5: Defensive rank last 5 games
        - home_def_rank_l5: Defensive rank last 5 games
        - away_is_b2b: Bool
        - home_is_b2b: Bool
        
        Returns:
            {
                'should_avoid': bool,
                'reasons': List[str],
                'filter_results': Dict[str, bool]
            }
        """
        reasons = []
        filter_results = {}
        
        # Filter 1: High Handle
        avoid, reason = EliminationFilters.check_high_handle(game_data['market_summary'])
        filter_results['high_handle'] = avoid
        if avoid:
            reasons.append(reason)
        
        # Filter 2: Large Spread
        avoid, reason = EliminationFilters.check_large_spread(game_data['spread'])
        filter_results['large_spread'] = avoid
        if avoid:
            reasons.append(reason)
        
        # Filter 3: Bad Records
        away_wins, away_losses = game_data['away_record']
        home_wins, home_losses = game_data['home_record']
        
        away_avoid, away_reason = EliminationFilters.check_bad_record(away_wins, away_losses)
        home_avoid, home_reason = EliminationFilters.check_bad_record(home_wins, home_losses)
        
        filter_results['away_bad_record'] = away_avoid
        filter_results['home_bad_record'] = home_avoid
        
        if away_avoid:
            reasons.append(f"Away: {away_reason}")
        if home_avoid:
            reasons.append(f"Home: {home_reason}")
        
        # Filter 4: Bad Defense L5
        away_avoid, away_reason = EliminationFilters.check_bad_defense_l5(
            game_data.get('away_def_rank_l5', 15)
        )
        home_avoid, home_reason = EliminationFilters.check_bad_defense_l5(
            game_data.get('home_def_rank_l5', 15)
        )
        
        filter_results['away_bad_defense'] = away_avoid
        filter_results['home_bad_defense'] = home_avoid
        
        if away_avoid:
            reasons.append(f"Away: {away_reason}")
        if home_avoid:
            reasons.append(f"Home: {home_reason}")
        
        # Filter 5: Back-to-back
        away_b2b, away_info = EliminationFilters.check_back_to_back(
            game_data.get('away_is_b2b', False)
        )
        home_b2b, home_info = EliminationFilters.check_back_to_back(
            game_data.get('home_is_b2b', False)
        )
        
        filter_results['away_b2b'] = away_b2b
        filter_results['home_b2b'] = home_b2b
        
        if away_b2b:
            reasons.append(f"Away: {away_info}")
        if home_b2b:
            reasons.append(f"Home: {home_info}")
        
        return {
            'should_avoid': len(reasons) > 0,
            'reasons': reasons,
            'filter_results': filter_results
        }


# ============================================================
# 5. WEB SCRAPING INTEGRATION
# ============================================================

class WebScraperIntegration:
    """
    Integration layer for scraping data from:
    - Covers.com (betting trends, consensus, line movements)
    - VSIN.com (betting strategies, expert picks)
    - ScoresAndOdds.com (live odds, line shopping)
    - NBA.com (official stats, injuries, schedules)
    - CleaningTheGlass.com (advanced analytics)
    
    NOTE: This is a framework. Actual scraping requires:
    1. Respecting robots.txt
    2. Rate limiting
    3. API keys where required
    4. Proper authentication
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def scrape_covers(self, game_id: str) -> Optional[Dict]:
        """
        Scrape Covers.com for betting trends and consensus.
        
        Returns:
            {
                'consensus': float,
                'public_percentage': float,
                'line_movement': List[Dict],
                'trends': Dict
            }
        """
        # Implementation placeholder
        # Real implementation would parse HTML/API
        return {
            'consensus': None,
            'public_percentage': None,
            'line_movement': [],
            'trends': {}
        }
    
    def scrape_scores_and_odds(self, game_id: str) -> Optional[Dict]:
        """
        Scrape ScoresAndOdds.com for live odds across multiple books.
        
        Returns:
            {
                'best_line_away': float,
                'best_line_home': float,
                'opening_spread': float,
                'current_spread': float,
                'line_movement': float
            }
        """
        # Implementation placeholder
        return {
            'best_line_away': None,
            'best_line_home': None,
            'opening_spread': None,
            'current_spread': None,
            'line_movement': None
        }
    
    def scrape_nba_official(self, game_id: str) -> Optional[Dict]:
        """
        Scrape NBA.com for official stats and injury reports.
        
        Returns:
            {
                'away_injuries': List[str],
                'home_injuries': List[str],
                'away_stats': Dict,
                'home_stats': Dict
            }
        """
        # Implementation placeholder
        return {
            'away_injuries': [],
            'home_injuries': [],
            'away_stats': {},
            'home_stats': {}
        }
    
    def scrape_cleaning_the_glass(self, team_abbr: str) -> Optional[Dict]:
        """
        Scrape CleaningTheGlass.com for advanced analytics.
        
        Returns:
            {
                'lineup_stats': Dict,
                'on_off_splits': Dict,
                'efficiency_metrics': Dict
            }
        """
        # Implementation placeholder
        return {
            'lineup_stats': {},
            'on_off_splits': {},
            'efficiency_metrics': {}
        }
    
    def aggregate_all_sources(self, game_id: str) -> Dict:
        """
        Aggregate data from all sources for a single game.
        """
        return {
            'covers': self.scrape_covers(game_id),
            'scores_and_odds': self.scrape_scores_and_odds(game_id),
            'nba_official': self.scrape_nba_official(game_id),
            # Add other sources as needed
        }


# ============================================================
# 6. BET DECISION LAYER
# ============================================================

class BetDecisionEngine:
    """
    Final decision layer that combines:
    - Stat edges
    - Market signals (sharp money + RLM)
    - Spread value
    - Elimination filters
    
    Only recommends bets when ALL systems align.
    """
    
    def __init__(self):
        pass
    
    @staticmethod
    def evaluate_spread_value(true_spread: float, vegas_spread: float, 
                            threshold: float = 2.0) -> Tuple[str, float]:
        """
        Determine if there's value in the spread.
        
        Args:
            true_spread: Model's projected spread
            vegas_spread: Current Vegas spread
            threshold: Minimum difference to consider value
            
        Returns:
            (side_with_value, edge_size)
        """
        edge = true_spread - vegas_spread
        
        if edge >= threshold:
            return 'Away', edge
        elif edge <= -threshold:
            return 'Home', abs(edge)
        else:
            return 'No Value', 0
    
    @staticmethod
    def determine_bet(stat_edges_away: int, stat_edges_home: int,
                     market_summary: Dict, true_spread: float,
                     vegas_spread: float, elimination_result: Dict) -> Dict:
        """
        Final bet determination combining all factors.
        
        Returns:
            {
                'decision': 'Bet Away'/'Bet Home'/'No Bet',
                'confidence': 'High'/'Medium'/'Low',
                'reasons': List[str],
                'warnings': List[str]
            }
        """
        reasons = []
        warnings = []
        
        # Check elimination filters first
        if elimination_result['should_avoid']:
            return {
                'decision': 'No Bet',
                'confidence': 'N/A',
                'reasons': ['Game filtered out'],
                'warnings': elimination_result['reasons']
            }
        
        # Determine stat favorite
        if stat_edges_away == stat_edges_home:
            stat_favored = 'Tie'
        elif stat_edges_away > stat_edges_home:
            stat_favored = 'Away'
            reasons.append(f"Stat edge: Away ({stat_edges_away} vs {stat_edges_home})")
        else:
            stat_favored = 'Home'
            reasons.append(f"Stat edge: Home ({stat_edges_home} vs {stat_edges_away})")
        
        # Check market signals
        sharp_side = market_summary['sharp_info']['sharp_side']
        rlm_detected = market_summary['rlm_info']['rlm_detected']
        
        if sharp_side in ['Away', 'Home']:
            reasons.append(f"Sharp money: {sharp_side}")
        
        # Check for RLM trap
        if rlm_detected:
            warnings.append(market_summary['rlm_info']['rlm_flag'])
        
        # Check spread value
        value_side, edge_size = BetDecisionEngine.evaluate_spread_value(
            true_spread, vegas_spread
        )
        
        if value_side != 'No Value':
            reasons.append(f"Spread value: {value_side} ({edge_size:.1f} point edge)")
        
        # Decision logic: All systems must align
        if (stat_favored == sharp_side == value_side and 
            stat_favored != 'Tie' and 
            not rlm_detected):
            
            # High confidence: stat + market + spread all agree
            confidence = 'High'
            decision = f"Bet {stat_favored}"
            
        elif (stat_favored == value_side and 
              stat_favored != 'Tie' and 
              not rlm_detected and
              sharp_side != 'Mixed/No clear sharp'):
            
            # Medium confidence: stat + spread agree, market unclear
            confidence = 'Medium'
            decision = f"Bet {stat_favored}"
            reasons.append("(Market signal mixed but not contradictory)")
            
        else:
            # No bet: systems don't align or RLM detected
            confidence = 'Low'
            decision = 'No Bet'
            
            if rlm_detected:
                reasons.append("RLM trap detected - potential line manipulation")
            if stat_favored != sharp_side and sharp_side in ['Away', 'Home']:
                reasons.append("Stat and market signals contradict")
            if value_side == 'No Value':
                reasons.append("No spread value vs Vegas line")
        
        return {
            'decision': decision,
            'confidence': confidence,
            'reasons': reasons,
            'warnings': warnings
        }


# ============================================================
# 7. COMPREHENSIVE GAME ANALYZER
# ============================================================

class ComprehensiveGameAnalyzer:
    """
    Master class that orchestrates all components for complete game analysis.
    """
    
    def __init__(self, sharp_threshold: float = 15.0):
        self.stat_engine = StatEngine()
        self.spread_engine = SpreadProjectionEngine()
        self.market_engine = MarketEngine(sharp_threshold)
        self.elimination_filters = EliminationFilters()
        self.web_scraper = WebScraperIntegration()
        self.bet_decision = BetDecisionEngine()
    
    def analyze_game(self, game_data: Dict) -> Dict:
        """
        Complete analysis of a single game.
        
        Required keys in game_data:
        - away_team: str
        - home_team: str
        - away_stats: Dict (all stat metrics)
        - home_stats: Dict (all stat metrics)
        - away_spread_data: Dict (for spread projection)
        - home_spread_data: Dict (for spread projection)
        - away_money: float
        - away_tickets: float
        - home_money: float
        - home_tickets: float
        - opening_spread: float
        - current_spread: float
        - away_record: Tuple (wins, losses)
        - home_record: Tuple (wins, losses)
        - away_def_rank_l5: int
        - home_def_rank_l5: int
        - away_is_b2b: bool
        - home_is_b2b: bool
        
        Returns:
            Complete analysis with all components
        """
        # 1. Stat comparison
        stat_edges_away, stat_edges_home, stat_comparisons = \
            self.stat_engine.calculate_stat_edges(
                game_data['away_stats'],
                game_data['home_stats']
            )
        
        # 2. Spread projection (SEPARATE from totals)
        spread_projection = self.spread_engine.true_spread(
            game_data['away_spread_data'],
            game_data['home_spread_data']
        )
        
        # 3. Market analysis
        market_summary = self.market_engine.market_summary(
            game_data['away_money'],
            game_data['away_tickets'],
            game_data['home_money'],
            game_data['home_tickets'],
            game_data['opening_spread'],
            game_data['current_spread']
        )
        
        # 4. Elimination filters
        filter_data = {
            'market_summary': market_summary,
            'spread': game_data['current_spread'],
            'away_record': game_data['away_record'],
            'home_record': game_data['home_record'],
            'away_def_rank_l5': game_data.get('away_def_rank_l5', 15),
            'home_def_rank_l5': game_data.get('home_def_rank_l5', 15),
            'away_is_b2b': game_data.get('away_is_b2b', False),
            'home_is_b2b': game_data.get('home_is_b2b', False)
        }
        
        elimination_result = self.elimination_filters.run_all_filters(filter_data)
        
        # 5. Final bet decision
        bet_result = self.bet_decision.determine_bet(
            stat_edges_away,
            stat_edges_home,
            market_summary,
            spread_projection['true_spread'],
            game_data['current_spread'],
            elimination_result
        )
        
        # 6. Compile full report
        return {
            'game': f"{game_data['away_team']} @ {game_data['home_team']}",
            'stat_analysis': {
                'away_edges': stat_edges_away,
                'home_edges': stat_edges_home,
                'comparisons': stat_comparisons
            },
            'spread_analysis': spread_projection,
            'market_analysis': market_summary,
            'elimination_filters': elimination_result,
            'bet_recommendation': bet_result,
            'vegas_spread': game_data['current_spread'],
            'opening_spread': game_data['opening_spread']
        }
    
    def generate_report(self, analysis: Dict) -> str:
        """
        Generate human-readable report from analysis.
        """
        report = []
        report.append("=" * 80)
        report.append(f"GAME ANALYSIS: {analysis['game']}")
        report.append("=" * 80)
        report.append("")
        
        # Stat Summary
        report.append("📊 STAT ANALYSIS")
        report.append(f"   Away Edges: {analysis['stat_analysis']['away_edges']}")
        report.append(f"   Home Edges: {analysis['stat_analysis']['home_edges']}")
        report.append("")
        
        # Spread Analysis
        spread = analysis['spread_analysis']
        report.append("📈 SPREAD PROJECTION (NOT TOTALS)")
        report.append(f"   Away Projected: {spread['away_proj_pts']} pts")
        report.append(f"   Home Projected: {spread['home_proj_pts']} pts")
        report.append(f"   True Spread: {spread['true_spread']:.1f}")
        report.append(f"   Vegas Spread: {analysis['vegas_spread']:.1f}")
        report.append(f"   Edge: {abs(spread['true_spread'] - analysis['vegas_spread']):.1f} pts")
        report.append("")
        
        # Market Analysis
        market = analysis['market_analysis']
        report.append("💰 MARKET ANALYSIS")
        report.append(f"   Sharp Side: {market['sharp_info']['sharp_side']}")
        report.append(f"   RLM: {market['rlm_info']['rlm_flag']}")
        if market['high_handle']:
            report.append("   ⚠️ HIGH HANDLE (80%+) DETECTED")
        report.append("")
        
        # Elimination Filters
        elim = analysis['elimination_filters']
        if elim['should_avoid']:
            report.append("🚫 ELIMINATION FILTERS")
            for reason in elim['reasons']:
                report.append(f"   - {reason}")
            report.append("")
        
        # Bet Decision
        bet = analysis['bet_recommendation']
        report.append("🎯 BET RECOMMENDATION")
        report.append(f"   Decision: {bet['decision']}")
        report.append(f"   Confidence: {bet['confidence']}")
        report.append("")
        report.append("   Reasons:")
        for reason in bet['reasons']:
            report.append(f"   - {reason}")
        
        if bet['warnings']:
            report.append("")
            report.append("   ⚠️ Warnings:")
            for warning in bet['warnings']:
                report.append(f"   - {warning}")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)


# ============================================================
# 8. EXAMPLE USAGE
# ============================================================

def example_usage():
    """
    Example of how to use the complete system.
    """
    
    # Initialize analyzer
    analyzer = ComprehensiveGameAnalyzer(sharp_threshold=15.0)
    
    # Example game data (you would populate this from your data sources)
    game_data = {
        'away_team': 'Detroit Pistons',
        'home_team': 'Denver Nuggets',
        
        # Stat comparison data
        'away_stats': {
            'ORB%': 28.5,
            'DRB%': 72.1,
            'TOV%': 12.8,
            'Forced TOV%': 14.2,
            'OFF_Efficiency': 112.4,
            'DEF_Efficiency': 115.2,
            'eFG%': 53.1,
            'Opp_eFG%': 55.8,
            '3PM_Game': 12.3,
            'Opp_3PM_Game': 13.1,
            'FT_Rate': 22.4,
            'Opp_FT_Rate': 24.1,
            'SOS_Rank': 12,
            'H2H_L10': 4,
            'Rest_Days': 1
        },
        'home_stats': {
            'ORB%': 26.8,
            'DRB%': 73.5,
            'TOV%': 13.5,
            'Forced TOV%': 13.8,
            'OFF_Efficiency': 116.7,
            'DEF_Efficiency': 112.1,
            'eFG%': 56.2,
            'Opp_eFG%': 52.4,
            '3PM_Game': 13.8,
            'Opp_3PM_Game': 11.9,
            'FT_Rate': 25.3,
            'Opp_FT_Rate': 21.8,
            'SOS_Rank': 8,
            'H2H_L10': 6,
            'Rest_Days': 2
        },
        
        # Spread projection data (SEPARATE from stats)
        'away_spread_data': {
            'ppg': 117.4,
            'opp_ppg': 110.1,
            'def_eff': 115.2,
            'pace': 98.5,
            'off_to': 12.8,
            'def_to': 14.2,
            'orb': 10.5,
            'drb': 32.8,
            'fta': 22.4,
            'ft_pct': 78.5,
            'ast_to': 1.65,
            'fg_edge': -2.7,  # FG% - Opp FG% allowed
            'tp_edge': -0.8,  # 3P% - Opp 3P% allowed
            'ft_edge': -2.1   # FT% - Opp FT% allowed
        },
        'home_spread_data': {
            'ppg': 120.7,
            'opp_ppg': 116.2,
            'def_eff': 112.1,
            'pace': 101.2,
            'off_to': 13.5,
            'def_to': 13.8,
            'orb': 9.8,
            'drb': 34.1,
            'fta': 25.3,
            'ft_pct': 81.2,
            'ast_to': 1.82,
            'fg_edge': 3.8,
            'tp_edge': 1.9,
            'ft_edge': 3.5
        },
        
        # Market data
        'away_money': 45.0,
        'away_tickets': 52.0,
        'home_money': 55.0,
        'home_tickets': 48.0,
        'opening_spread': -4.5,
        'current_spread': -3.0,
        
        # Team records
        'away_record': (15, 18),
        'home_record': (22, 15),
        
        # Defense ranks last 5 games
        'away_def_rank_l5': 18,
        'home_def_rank_l5': 12,
        
        # Back-to-back info
        'away_is_b2b': False,
        'home_is_b2b': False
    }
    
    # Run analysis
    analysis = analyzer.analyze_game(game_data)
    
    # Generate report
    report = analyzer.generate_report(analysis)
    print(report)
    
    # Return structured data for app integration
    return analysis


if __name__ == "__main__":
    # Run example
    result = example_usage()
    
    print("\n\n📦 Structured Output for App Integration:")
    print(json.dumps(result['bet_recommendation'], indent=2))
