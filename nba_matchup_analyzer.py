"""
NBA MATCHUP ANALYSIS TOOL
=========================

Pulls data from:
- TeamRankings.com
- NBA.com (official stats)
- Covers.com (betting data)
- CleaningTheGlass.com (advanced analytics)

Generates comprehensive matchup analysis with:
- Power Ratings
- Efficiency Metrics
- Full Season Stats
- Last 5 Games Stats
- Edge Analysis
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional, Tuple
import time


# ============================================================
# DATA SCRAPER - TEAM RANKINGS
# ============================================================

class TeamRankingsScraper:
    """
    Scrapes data from TeamRankings.com for:
    - Power Ratings
    - Offensive/Defensive Efficiency Rankings
    - Statistical Rankings
    """
    
    BASE_URL = "https://www.teamrankings.com/ncb"  # or /nba for NBA
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_power_ratings(self, team_name: str) -> Dict:
        """
        Get team power rating and national rank.
        
        Returns:
            {
                'team': str,
                'power_rating': float,
                'rank': int,
                'percentile': str (e.g., "Top 12%")
            }
        """
        try:
            # Example endpoint - adjust based on actual site structure
            url = f"{self.BASE_URL}/teams/{team_name.lower().replace(' ', '-')}"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Parse power rating (adjust selectors based on actual HTML)
            # This is a placeholder - you'll need to inspect the actual page
            
            return {
                'team': team_name,
                'power_rating': 0.0,  # Parse from page
                'rank': 0,  # Parse from page
                'percentile': "Top 0%"  # Calculate from rank
            }
        except Exception as e:
            print(f"Error fetching power rating for {team_name}: {e}")
            return None
    
    def get_efficiency_rankings(self, team_name: str) -> Dict:
        """
        Get offensive and defensive efficiency rankings.
        
        Returns:
            {
                'offensive_efficiency': {
                    'value': float,
                    'rank': int,
                    'percentile': str
                },
                'defensive_efficiency': {
                    'value': float,
                    'rank': int,
                    'percentile': str
                }
            }
        """
        # Implementation similar to power_ratings
        pass


# ============================================================
# DATA SCRAPER - NBA.COM (OFFICIAL)
# ============================================================

class NBAOfficialScraper:
    """
    Uses NBA.com's official stats API for accurate current season data.
    """
    
    BASE_URL = "https://stats.nba.com/stats"
    
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com'
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def get_team_stats(self, team_abbr: str, season: str = "2024-25") -> Dict:
        """
        Get comprehensive team stats from NBA.com.
        
        Args:
            team_abbr: Team abbreviation (e.g., 'LAL', 'BOS')
            season: Season in format "YYYY-YY"
        
        Returns:
            Dictionary with all team stats
        """
        try:
            # Team Dashboard endpoint
            endpoint = f"{self.BASE_URL}/teamdashboardbygeneralsplits"
            
            params = {
                'Season': season,
                'SeasonType': 'Regular Season',
                'TeamID': self._get_team_id(team_abbr),
                'MeasureType': 'Advanced',
                'PerMode': 'PerGame'
            }
            
            response = self.session.get(endpoint, params=params, timeout=10)
            data = response.json()
            
            # Parse the response
            if 'resultSets' in data:
                stats = self._parse_team_dashboard(data)
                return stats
            
            return None
            
        except Exception as e:
            print(f"Error fetching NBA.com stats for {team_abbr}: {e}")
            return None
    
    def get_last_n_games_stats(self, team_abbr: str, n: int = 5) -> Dict:
        """
        Get team stats for last N games.
        """
        try:
            endpoint = f"{self.BASE_URL}/teamdashboardbygeneralsplits"
            
            params = {
                'Season': '2024-25',
                'SeasonType': 'Regular Season',
                'TeamID': self._get_team_id(team_abbr),
                'MeasureType': 'Advanced',
                'LastNGames': n
            }
            
            response = self.session.get(endpoint, params=params, timeout=10)
            data = response.json()
            
            if 'resultSets' in data:
                return self._parse_team_dashboard(data)
            
            return None
            
        except Exception as e:
            print(f"Error fetching last {n} games for {team_abbr}: {e}")
            return None
    
    def _get_team_id(self, team_abbr: str) -> int:
        """Convert team abbreviation to NBA.com team ID."""
        # NBA.com team ID mapping
        team_ids = {
            'ATL': 1610612737, 'BOS': 1610612738, 'BKN': 1610612751,
            'CHA': 1610612766, 'CHI': 1610612741, 'CLE': 1610612739,
            'DAL': 1610612742, 'DEN': 1610612743, 'DET': 1610612765,
            'GSW': 1610612744, 'HOU': 1610612745, 'IND': 1610612754,
            'LAC': 1610612746, 'LAL': 1610612747, 'MEM': 1610612763,
            'MIA': 1610612748, 'MIL': 1610612749, 'MIN': 1610612750,
            'NOP': 1610612740, 'NYK': 1610612752, 'OKC': 1610612760,
            'ORL': 1610612753, 'PHI': 1610612755, 'PHX': 1610612756,
            'POR': 1610612757, 'SAC': 1610612758, 'SAS': 1610612759,
            'TOR': 1610612761, 'UTA': 1610612762, 'WAS': 1610612764
        }
        return team_ids.get(team_abbr.upper(), 0)
    
    def _parse_team_dashboard(self, data: Dict) -> Dict:
        """Parse NBA.com team dashboard response."""
        try:
            headers = data['resultSets'][0]['headers']
            values = data['resultSets'][0]['rowSet'][0]
            
            # Create dictionary from headers and values
            stats = dict(zip(headers, values))
            
            return {
                'ORB%': stats.get('OREB_PCT', 0) * 100,
                'DRB%': stats.get('DREB_PCT', 0) * 100,
                'TOV%': stats.get('TOV_PCT', 0) * 100,
                'OFF_Efficiency': stats.get('OFF_RATING', 0),
                'DEF_Efficiency': stats.get('DEF_RATING', 0),
                'eFG%': stats.get('EFG_PCT', 0) * 100,
                'Opp_eFG%': stats.get('OPP_EFG_PCT', 0) * 100,
                'FT_Rate': stats.get('FTA_RATE', 0),
                'Pace': stats.get('PACE', 0),
                '3PM_Game': stats.get('FG3M', 0),
                '2PT%': stats.get('FG2_PCT', 0) * 100,
                '3PT%': stats.get('FG3_PCT', 0) * 100,
                '3PT_Rate': stats.get('FG3A_RATE', 0) * 100,
                'AST': stats.get('AST', 0),
                'Assists_per_TO': stats.get('AST_TO', 0)
            }
            
        except Exception as e:
            print(f"Error parsing team dashboard: {e}")
            return {}


# ============================================================
# DATA SCRAPER - CLEANING THE GLASS
# ============================================================

class CleaningTheGlassScraper:
    """
    Scrapes advanced analytics from CleaningTheGlass.com
    Note: This site requires a subscription for full access
    """
    
    BASE_URL = "https://cleaningtheglass.com"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session = requests.Session()
    
    def get_advanced_stats(self, team_name: str) -> Dict:
        """
        Get advanced analytics like:
        - Garbage time adjusted stats
        - On/Off splits
        - Lineup data
        """
        # Implementation depends on access level
        pass


# ============================================================
# MATCHUP ANALYZER
# ============================================================

class MatchupAnalyzer:
    """
    Main class that combines all data sources and generates matchup analysis.
    """
    
    def __init__(self):
        self.nba_scraper = NBAOfficialScraper()
        self.team_rankings_scraper = TeamRankingsScraper()
    
    def analyze_matchup(self, team1_abbr: str, team2_abbr: str,
                       team1_name: str, team2_name: str) -> Dict:
        """
        Generate complete matchup analysis.
        
        Args:
            team1_abbr: Team 1 abbreviation (e.g., 'LAL')
            team2_abbr: Team 2 abbreviation (e.g., 'BOS')
            team1_name: Team 1 full name (e.g., 'Lakers')
            team2_name: Team 2 full name (e.g., 'Celtics')
        
        Returns:
            Complete matchup analysis dictionary
        """
        
        # Get full season stats
        team1_stats = self.nba_scraper.get_team_stats(team1_abbr)
        team2_stats = self.nba_scraper.get_team_stats(team2_abbr)
        
        # Get last 5 games stats
        team1_l5 = self.nba_scraper.get_last_n_games_stats(team1_abbr, 5)
        team2_l5 = self.nba_scraper.get_last_n_games_stats(team2_abbr, 5)
        
        # Get power ratings
        team1_power = self.team_rankings_scraper.get_power_ratings(team1_name)
        team2_power = self.team_rankings_scraper.get_power_ratings(team2_name)
        
        # Calculate edges
        analysis = {
            'matchup': f"{team1_name} vs {team2_name}",
            'date': datetime.now().strftime('%m/%d/%Y'),
            
            'power_ratings': self._compare_power_ratings(team1_power, team2_power),
            
            'season_stats': self._compare_stats(team1_stats, team2_stats, team1_name, team2_name),
            
            'last_5_games': self._compare_stats(team1_l5, team2_l5, team1_name, team2_name),
            
            'efficiency_comparison': self._efficiency_comparison(team1_stats, team2_stats, team1_name, team2_name),
            
            'edge_summary': self._generate_edge_summary(team1_stats, team2_stats, team1_name, team2_name)
        }
        
        return analysis
    
    def _compare_power_ratings(self, team1: Dict, team2: Dict) -> Dict:
        """Compare power ratings and calculate edge."""
        if not team1 or not team2:
            return {}
        
        edge = abs(team1['rank'] - team2['rank'])
        leader = team1['team'] if team1['rank'] < team2['rank'] else team2['team']
        
        return {
            'team1': {
                'rank': team1['rank'],
                'percentile': team1['percentile'],
                'has_edge': team1['rank'] < team2['rank']
            },
            'team2': {
                'rank': team2['rank'],
                'percentile': team2['percentile'],
                'has_edge': team2['rank'] < team1['rank']
            },
            'edge': f"+{edge}",
            'leader': leader
        }
    
    def _compare_stats(self, team1: Dict, team2: Dict, 
                      team1_name: str, team2_name: str) -> List[Dict]:
        """
        Compare all stats and determine edges.
        
        Returns list of metric comparisons.
        """
        if not team1 or not team2:
            return []
        
        # Define metrics and their "better" direction
        metrics = {
            'ORB%': 'higher',
            'DRB%': 'higher',
            'TOV%': 'lower',
            'OFF_Efficiency': 'higher',
            'DEF_Efficiency': 'lower',
            'eFG%': 'higher',
            'Opp_eFG%': 'lower',
            '3PM_Game': 'higher',
            'FT_Rate': 'higher',
            '2PT%': 'higher',
            '3PT%': 'higher',
            '3PT_Rate': 'neutral',  # Context dependent
            'Pace': 'neutral',
            'Assists_per_TO': 'higher'
        }
        
        comparisons = []
        
        for metric, direction in metrics.items():
            if metric in team1 and metric in team2:
                team1_val = team1[metric]
                team2_val = team2[metric]
                
                # Determine edge
                if direction == 'higher':
                    edge = team1_name if team1_val > team2_val else team2_name if team2_val > team1_val else 'No Edge'
                elif direction == 'lower':
                    edge = team1_name if team1_val < team2_val else team2_name if team2_val < team1_val else 'No Edge'
                else:
                    edge = 'No Edge'
                
                comparisons.append({
                    'metric': metric,
                    'team1_value': round(team1_val, 1),
                    'team2_value': round(team2_val, 1),
                    'edge': edge,
                    'direction': direction
                })
        
        return comparisons
    
    def _efficiency_comparison(self, team1: Dict, team2: Dict,
                              team1_name: str, team2_name: str) -> Dict:
        """
        Calculate efficiency matchup (Team1 Offense vs Team2 Defense).
        """
        if not team1 or not team2:
            return {}
        
        # Team 1 offense vs Team 2 defense
        team1_off = team1.get('OFF_Efficiency', 0)
        team2_def = team2.get('DEF_Efficiency', 0)
        team1_expected_pts = team1_off - (team2_def - 110)  # Adjust vs league avg
        
        # Team 2 offense vs Team 1 defense
        team2_off = team2.get('OFF_Efficiency', 0)
        team1_def = team1.get('DEF_Efficiency', 0)
        team2_expected_pts = team2_off - (team1_def - 110)
        
        edge = team1_expected_pts - team2_expected_pts
        
        return {
            'team1_offense': round(team1_off, 1),
            'team2_defense': round(team2_def, 1),
            'team1_expected_pts': round(team1_expected_pts, 1),
            
            'team2_offense': round(team2_off, 1),
            'team1_defense': round(team1_def, 1),
            'team2_expected_pts': round(team2_expected_pts, 1),
            
            'efficiency_edge': round(edge, 1),
            'edge_holder': team1_name if edge > 0 else team2_name if edge < 0 else 'Even'
        }
    
    def _generate_edge_summary(self, team1: Dict, team2: Dict,
                              team1_name: str, team2_name: str) -> str:
        """
        Generate analyst-style edge summary.
        """
        comparisons = self._compare_stats(team1, team2, team1_name, team2_name)
        
        team1_edges = sum(1 for c in comparisons if c['edge'] == team1_name)
        team2_edges = sum(1 for c in comparisons if c['edge'] == team2_name)
        
        if team1_edges > team2_edges + 2:
            return f"{team1_name} has clear statistical advantages across multiple categories."
        elif team2_edges > team1_edges + 2:
            return f"{team2_name} has clear statistical advantages across multiple categories."
        else:
            return "Closely matched teams. Execution and game flow will be decisive."


# ============================================================
# TABLE GENERATOR
# ============================================================

class MatchupTableGenerator:
    """
    Generates formatted tables for display.
    """
    
    @staticmethod
    def generate_comparison_table(analysis: Dict) -> pd.DataFrame:
        """
        Generate main comparison table.
        """
        comparisons = analysis.get('season_stats', [])
        
        if not comparisons:
            return pd.DataFrame()
        
        # Get team names from analysis
        matchup = analysis['matchup'].split(' vs ')
        team1_name = matchup[0] if len(matchup) > 0 else 'Team1'
        team2_name = matchup[1] if len(matchup) > 1 else 'Team2'
        
        df = pd.DataFrame(comparisons)
        
        # Rename columns for display
        df = df.rename(columns={
            'metric': 'Metric',
            'team1_value': team1_name,
            'team2_value': team2_name,
            'edge': 'Edge'
        })
        
        return df[['Metric', team1_name, team2_name, 'Edge']]
    
    @staticmethod
    def generate_last5_table(analysis: Dict) -> pd.DataFrame:
        """
        Generate Last 5 Games comparison table.
        """
        comparisons = analysis.get('last_5_games', [])
        
        if not comparisons:
            return pd.DataFrame()
        
        matchup = analysis['matchup'].split(' vs ')
        team1_name = f"{matchup[0]} (L5)" if len(matchup) > 0 else 'Team1 (L5)'
        team2_name = f"{matchup[1]} (L5)" if len(matchup) > 1 else 'Team2 (L5)'
        
        df = pd.DataFrame(comparisons)
        
        df = df.rename(columns={
            'metric': 'Metric',
            'team1_value': team1_name,
            'team2_value': team2_name,
            'edge': 'Edge'
        })
        
        return df[['Metric', team1_name, team2_name, 'Edge']]
    
    @staticmethod
    def format_power_rating_display(analysis: Dict) -> str:
        """
        Format power rating section.
        """
        pr = analysis.get('power_ratings', {})
        
        if not pr:
            return "Power ratings unavailable"
        
        output = "POWER RATING - Overall Team Strength\n"
        output += f"#{pr['team1']['rank']} {pr.get('leader', 'Team1')} ✓ {pr['team1']['percentile']}\n"
        output += f"  vs\n"
        output += f"#{pr['team2']['rank']} {pr.get('leader', 'Team2')} {pr['team2']['percentile']}\n"
        output += f"Edge: {pr.get('edge', 'N/A')}\n"
        
        return output


# ============================================================
# EXAMPLE USAGE
# ============================================================

def main():
    """
    Example usage of the matchup analyzer.
    """
    
    # Initialize analyzer
    analyzer = MatchupAnalyzer()
    
    # Analyze a matchup
    print("Fetching matchup data...")
    analysis = analyzer.analyze_matchup(
        team1_abbr='CHI',
        team2_abbr='IND',
        team1_name='Bulls',
        team2_name='Pacers'
    )
    
    # Generate tables
    table_gen = MatchupTableGenerator()
    
    # Power Ratings
    print("\n" + "="*80)
    print(table_gen.format_power_rating_display(analysis))
    
    # Season Stats Comparison
    print("\n" + "="*80)
    print("SEASON STATS COMPARISON")
    print("="*80)
    season_table = table_gen.generate_comparison_table(analysis)
    print(season_table.to_string(index=False))
    
    # Last 5 Games Comparison
    print("\n" + "="*80)
    print("LAST 5 GAMES COMPARISON")
    print("="*80)
    last5_table = table_gen.generate_last5_table(analysis)
    print(last5_table.to_string(index=False))
    
    # Efficiency Comparison
    print("\n" + "="*80)
    print("EFFICIENCY COMPARISON")
    print("="*80)
    eff = analysis.get('efficiency_comparison', {})
    if eff:
        matchup = analysis['matchup'].split(' vs ')
        print(f"{matchup[0]} OFFENSE: {eff['team1_offense']} pts/100 poss")
        print(f"{matchup[1]} DEFENSE: {eff['team2_defense']} pts/100 allowed")
        print(f"Expected Points: {eff['team1_expected_pts']}")
        print()
        print(f"{matchup[1]} OFFENSE: {eff['team2_offense']} pts/100 poss")
        print(f"{matchup[0]} DEFENSE: {eff['team1_defense']} pts/100 allowed")
        print(f"Expected Points: {eff['team2_expected_pts']}")
        print()
        print(f"Efficiency Edge: {eff['efficiency_edge']} ({eff['edge_holder']})")
    
    # Edge Summary
    print("\n" + "="*80)
    print("EDGE SUMMARY")
    print("="*80)
    print(analysis.get('edge_summary', 'No summary available'))
    
    # Export to JSON
    with open('matchup_analysis.json', 'w') as f:
        json.dump(analysis, f, indent=2)
    
    print("\n\nFull analysis exported to matchup_analysis.json")


if __name__ == "__main__":
    main()
