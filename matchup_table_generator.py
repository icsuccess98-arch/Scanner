"""
NBA MATCHUP TABLE - EXACT FORMAT GENERATOR
==========================================

Generates tables matching your screenshot requirements:
1. Power Rating section
2. Offensive/Defensive Efficiency sections  
3. Main stats comparison table
4. Last 5 Games comparison table
5. Edge summary (no "Savant Analytics")

"""

import pandas as pd
from typing import Dict, List, Tuple
import json


# ============================================================
# COMPLETE MATCHUP TABLE GENERATOR
# ============================================================

class CompleteMatchupTable:
    """
    Generates complete matchup analysis table with:
    - Power Ratings
    - Offensive/Defensive Efficiency
    - Full season stats
    - Last 5 games stats
    - Edge summary
    """
    
    # Define all metrics with their proper names and directions
    METRICS_CONFIG = {
        # Core metrics (from your current table)
        'ORB%': {'display': 'ORB%', 'direction': 'higher', 'format': '{:.1f}%'},
        'DRB%': {'display': 'DRB%', 'direction': 'higher', 'format': '{:.1f}%'},
        'TOV%': {'display': 'TOV%', 'direction': 'lower', 'format': '{:.1f}%'},
        'Forced TOV%': {'display': 'Forced TOV%', 'direction': 'higher', 'format': '{:.1f}%'},
        'OFF_Efficiency': {'display': 'OFF Efficiency', 'direction': 'higher', 'format': '{:.1f}'},
        'DEF_Efficiency': {'display': 'DEF Efficiency', 'direction': 'lower', 'format': '{:.1f}'},
        'eFG%': {'display': 'eFG%', 'direction': 'higher', 'format': '{:.1f}%'},
        'Opp_eFG%': {'display': 'Opp eFG%', 'direction': 'lower', 'format': '{:.1f}%'},
        '3PM_Game': {'display': '3PM/Game', 'direction': 'higher', 'format': '{:.1f}'},
        'FT_Rate': {'display': 'FT Rate', 'direction': 'higher', 'format': '{:.1f}'},
        'SOS': {'display': 'SOS', 'direction': 'lower', 'format': '{:.1f}'},
        'Edge_Count': {'display': 'Edge Count', 'direction': 'higher', 'format': '{:.0f}'},
    }
    
    def __init__(self, team1_name: str, team2_name: str):
        """
        Args:
            team1_name: First team name (e.g., 'Bulls')
            team2_name: Second team name (e.g., 'Pacers')
        """
        self.team1_name = team1_name
        self.team2_name = team2_name
    
    def generate_complete_table(self, 
                               team1_stats: Dict,
                               team2_stats: Dict,
                               team1_l5_stats: Dict,
                               team2_l5_stats: Dict,
                               team1_power: Dict,
                               team2_power: Dict) -> str:
        """
        Generate complete formatted table.
        
        Args:
            team1_stats: Full season stats for team 1
            team2_stats: Full season stats for team 2
            team1_l5_stats: Last 5 games stats for team 1
            team2_l5_stats: Last 5 games stats for team 2
            team1_power: Power rating data for team 1
            team2_power: Power rating data for team 2
        
        Returns:
            Formatted string with complete table
        """
        output = []
        
        # Title
        output.append("="*100)
        output.append(f"NBA MATCHUP ANALYSIS".center(100))
        output.append("="*100)
        output.append("")
        
        # Power Rating Section
        output.append(self._format_power_rating(team1_power, team2_power))
        output.append("")
        
        # Offensive Efficiency Section
        output.append(self._format_offensive_efficiency(team1_power, team2_power))
        output.append("")
        
        # Defensive Efficiency Section
        output.append(self._format_defensive_efficiency(team1_power, team2_power))
        output.append("")
        
        # Main Stats Table
        output.append("-"*100)
        output.append("SEASON STATS COMPARISON".center(100))
        output.append("-"*100)
        stats_table = self._create_comparison_table(team1_stats, team2_stats)
        output.append(stats_table.to_string(index=False))
        output.append("")
        
        # Last 5 Games Table
        output.append("-"*100)
        output.append("LAST 5 GAMES COMPARISON".center(100))
        output.append("-"*100)
        l5_table = self._create_comparison_table(team1_l5_stats, team2_l5_stats)
        output.append(l5_table.to_string(index=False))
        output.append("")
        
        # Edge Summary
        output.append("-"*100)
        output.append("EDGE SUMMARY".center(100))
        output.append("-"*100)
        edge_summary = self._generate_edge_summary(stats_table)
        output.append(edge_summary)
        output.append("")
        
        output.append("="*100)
        
        return "\n".join(output)
    
    def _format_power_rating(self, team1_power: Dict, team2_power: Dict) -> str:
        """
        Format power rating section matching your screenshot.
        
        Expected format:
        POWER RATING
        Overall Team Strength
        
        #44 UCLA ✓        vs        #77 Oregon
        Top 12%                     Top 21%
                        +33
        """
        output = []
        output.append("┌─ POWER RATING ─────────────────────────────────────────────────────────────┐")
        output.append("│ Overall Team Strength                                                       │")
        output.append("│                                                                             │")
        
        # Format team comparison
        team1_str = f"#{team1_power.get('rank', 'N/A')} {self.team1_name}"
        if team1_power.get('has_edge', False):
            team1_str += " ✓"
        
        team2_str = f"#{team2_power.get('rank', 'N/A')} {self.team2_name}"
        if team2_power.get('has_edge', False):
            team2_str += " ✓"
        
        # Center with "vs"
        line = f"│ {team1_str:^35} vs {team2_str:^35} │"
        output.append(line)
        
        # Percentiles
        team1_pct = team1_power.get('percentile', 'N/A')
        team2_pct = team2_power.get('percentile', 'N/A')
        line = f"│ {team1_pct:^35}    {team2_pct:^35} │"
        output.append(line)
        
        # Edge
        edge = team1_power.get('edge', '+0')
        line = f"│ {edge:^77} │"
        output.append(line)
        
        output.append("└─────────────────────────────────────────────────────────────────────────────┘")
        
        return "\n".join(output)
    
    def _format_offensive_efficiency(self, team1_power: Dict, team2_power: Dict) -> str:
        """Format offensive efficiency section."""
        output = []
        output.append("┌─ OFFENSIVE EFFICIENCY ─────────────────────────────────────────────────────┐")
        output.append("│ Scoring Ability (pts/100)                                                   │")
        output.append("│                                                                             │")
        
        team1_str = f"#{team1_power.get('off_rank', 'N/A')} {self.team1_name}"
        if team1_power.get('off_edge', False):
            team1_str += " ✓"
        
        team2_str = f"#{team2_power.get('off_rank', 'N/A')} {self.team2_name}"
        if team2_power.get('off_edge', False):
            team2_str += " ✓"
        
        line = f"│ {team1_str:^35} vs {team2_str:^35} │"
        output.append(line)
        
        team1_pct = team1_power.get('off_percentile', 'N/A')
        team2_pct = team2_power.get('off_percentile', 'N/A')
        line = f"│ {team1_pct:^35}    {team2_pct:^35} │"
        output.append(line)
        
        edge = team1_power.get('off_diff', '+0')
        line = f"│ {edge:^77} │"
        output.append(line)
        
        output.append("└─────────────────────────────────────────────────────────────────────────────┘")
        
        return "\n".join(output)
    
    def _format_defensive_efficiency(self, team1_power: Dict, team2_power: Dict) -> str:
        """Format defensive efficiency section."""
        output = []
        output.append("┌─ DEFENSIVE EFFICIENCY ─────────────────────────────────────────────────────┐")
        output.append("│ Points Allowed (pts/100)                                                    │")
        output.append("│                                                                             │")
        
        team1_str = f"#{team1_power.get('def_rank', 'N/A')} {self.team1_name}"
        if team1_power.get('def_edge', False):
            team1_str += " ✓"
        
        team2_str = f"#{team2_power.get('def_rank', 'N/A')} {self.team2_name}"
        if team2_power.get('def_edge', False):
            team2_str += " ✓"
        
        line = f"│ {team1_str:^35} vs {team2_str:^35} │"
        output.append(line)
        
        team1_pct = team1_power.get('def_percentile', 'N/A')
        team2_pct = team2_power.get('def_percentile', 'N/A')
        line = f"│ {team1_pct:^35}    {team2_pct:^35} │"
        output.append(line)
        
        edge = team1_power.get('def_diff', '+0')
        line = f"│ {edge:^77} │"
        output.append(line)
        
        output.append("└─────────────────────────────────────────────────────────────────────────────┘")
        
        return "\n".join(output)
    
    def _create_comparison_table(self, team1_stats: Dict, team2_stats: Dict) -> pd.DataFrame:
        """
        Create comparison table for stats.
        """
        rows = []
        
        for metric_key, config in self.METRICS_CONFIG.items():
            if metric_key in team1_stats and metric_key in team2_stats:
                team1_val = team1_stats[metric_key]
                team2_val = team2_stats[metric_key]
                
                # Determine edge
                direction = config['direction']
                if direction == 'higher':
                    if team1_val > team2_val:
                        edge = self.team1_name
                    elif team2_val > team1_val:
                        edge = self.team2_name
                    else:
                        edge = 'No Edge'
                elif direction == 'lower':
                    if team1_val < team2_val:
                        edge = self.team1_name
                    elif team2_val < team1_val:
                        edge = self.team2_name
                    else:
                        edge = 'No Edge'
                else:
                    edge = 'No Edge'
                
                # Format values
                fmt = config.get('format', '{:.1f}')
                team1_display = fmt.format(team1_val)
                team2_display = fmt.format(team2_val)
                
                rows.append({
                    'Metric': config['display'],
                    self.team1_name: team1_display,
                    self.team2_name: team2_display,
                    'Edge': edge
                })
        
        return pd.DataFrame(rows)
    
    def _generate_edge_summary(self, stats_df: pd.DataFrame) -> str:
        """
        Generate edge summary (without "Savant Analytics" branding).
        """
        team1_edges = (stats_df['Edge'] == self.team1_name).sum()
        team2_edges = (stats_df['Edge'] == self.team2_name).sum()
        no_edges = (stats_df['Edge'] == 'No Edge').sum()
        
        summary = []
        
        # Overall edge count
        summary.append(f"{self.team1_name}: {team1_edges} edges")
        summary.append(f"{self.team2_name}: {team2_edges} edges")
        summary.append(f"Even: {no_edges} metrics")
        summary.append("")
        
        # Analysis
        if team1_edges > team2_edges + 3:
            summary.append(f"EDGE: {self.team1_name}")
            summary.append(f"{self.team1_name} has clear statistical advantages across key categories.")
        elif team2_edges > team1_edges + 3:
            summary.append(f"EDGE: {self.team2_name}")
            summary.append(f"{self.team2_name} has clear statistical advantages across key categories.")
        else:
            summary.append("EDGE: Even")
            summary.append("Closely matched teams statistically. Execution will be decisive.")
        
        return "\n".join(summary)


# ============================================================
# SAMPLE DATA GENERATOR (for testing)
# ============================================================

def generate_sample_data():
    """
    Generate sample data matching your Bulls vs Pacers screenshot.
    """
    
    # Bulls stats (Team 1)
    bulls_stats = {
        'ORB%': 26.3,
        'DRB%': 78.8,
        'TOV%': 12.5,
        'Forced TOV%': 7.8,
        'OFF_Efficiency': 113.6,
        'DEF_Efficiency': 113.2,
        'eFG%': 56.6,
        'Opp_eFG%': 52.0,
        '3PM_Game': 14.2,
        'FT_Rate': 22.5,
        'SOS': 18.0,
        'Edge_Count': 5.0
    }
    
    # Pacers stats (Team 2)
    pacers_stats = {
        'ORB%': 27.0,
        'DRB%': 78.3,
        'TOV%': 8.7,
        'Forced TOV%': 8.7,
        'OFF_Efficiency': 113.2,
        'DEF_Efficiency': 113.6,
        'eFG%': 49.6,
        'Opp_eFG%': 51.5,
        '3PM_Game': 12.0,
        'FT_Rate': 27.9,
        'SOS': 12.0,
        'Edge_Count': 6.0
    }
    
    # Last 5 games stats (slightly different)
    bulls_l5 = {
        'ORB%': 25.5,
        'DRB%': 79.2,
        'TOV%': 13.1,
        'Forced TOV%': 8.2,
        'OFF_Efficiency': 115.3,
        'DEF_Efficiency': 112.1,
        'eFG%': 57.8,
        'Opp_eFG%': 50.9,
        '3PM_Game': 15.1,
        'FT_Rate': 23.4,
        'SOS': 18.0,
        'Edge_Count': 6.0
    }
    
    pacers_l5 = {
        'ORB%': 28.1,
        'DRB%': 77.9,
        'TOV%': 9.2,
        'Forced TOV%': 9.1,
        'OFF_Efficiency': 114.8,
        'DEF_Efficiency': 114.2,
        'eFG%': 51.2,
        'Opp_eFG%': 52.3,
        '3PM_Game': 11.8,
        'FT_Rate': 28.5,
        'SOS': 12.0,
        'Edge_Count': 5.0
    }
    
    # Power ratings
    bulls_power = {
        'rank': 44,
        'percentile': 'Top 12%',
        'has_edge': True,
        'off_rank': 47,
        'off_percentile': 'Top 13%',
        'off_edge': True,
        'off_diff': '+26',
        'def_rank': 64,
        'def_percentile': 'Top 17%',
        'def_edge': True,
        'def_diff': '+33',
        'edge': '+33'
    }
    
    pacers_power = {
        'rank': 77,
        'percentile': 'Top 21%',
        'has_edge': False,
        'off_rank': 73,
        'off_percentile': 'Top 20%',
        'off_edge': False,
        'def_rank': 97,
        'def_percentile': 'Top 26%',
        'def_edge': False
    }
    
    return {
        'bulls_stats': bulls_stats,
        'pacers_stats': pacers_stats,
        'bulls_l5': bulls_l5,
        'pacers_l5': pacers_l5,
        'bulls_power': bulls_power,
        'pacers_power': pacers_power
    }


# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """
    Generate complete matchup table.
    """
    
    print("Generating NBA Matchup Analysis Table...\n")
    
    # Get sample data
    data = generate_sample_data()
    
    # Create table generator
    generator = CompleteMatchupTable(
        team1_name='Bulls',
        team2_name='Pacers'
    )
    
    # Generate complete table
    complete_table = generator.generate_complete_table(
        team1_stats=data['bulls_stats'],
        team2_stats=data['pacers_stats'],
        team1_l5_stats=data['bulls_l5'],
        team2_l5_stats=data['pacers_l5'],
        team1_power=data['bulls_power'],
        team2_power=data['pacers_power']
    )
    
    # Print table
    print(complete_table)
    
    # Export to file
    with open('matchup_table.txt', 'w') as f:
        f.write(complete_table)
    
    print("\n\nTable exported to: matchup_table.txt")
    
    # Also export structured data as JSON
    export_data = {
        'matchup': 'Bulls vs Pacers',
        'date': '1/28/2026',
        'teams': {
            'team1': {
                'name': 'Bulls',
                'power_rating': data['bulls_power'],
                'season_stats': data['bulls_stats'],
                'last_5_games': data['bulls_l5']
            },
            'team2': {
                'name': 'Pacers',
                'power_rating': data['pacers_power'],
                'season_stats': data['pacers_stats'],
                'last_5_games': data['pacers_l5']
            }
        }
    }
    
    with open('matchup_data.json', 'w') as f:
        json.dump(export_data, f, indent=2)
    
    print("Structured data exported to: matchup_data.json")


if __name__ == "__main__":
    main()
