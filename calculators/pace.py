"""
PaceCalculator - Tempo analysis for OVER/UNDER tendency.
Fast pace teams trend OVER, slow pace teams trend UNDER.
"""


class PaceCalculator:
    """Pace/tempo analysis - fast pace = OVER, slow pace = UNDER."""
    
    LEAGUE_AVG_PACE = {
        'NBA': 100.0,   # Possessions per game
        'CBB': 68.0,    # Possessions per game
        'NFL': 64.0,    # Plays per game
        'CFB': 70.0,    # Plays per game
        'NHL': 30.0     # Shots per period
    }
    
    PACE_IMPACT = {
        'NBA': 1.5,     # Points per possession differential
        'CBB': 1.2,
        'NFL': 0.8,
        'CFB': 1.0,
        'NHL': 0.5
    }
    
    @staticmethod
    def calculate_projected_pace(away_pace: float, home_pace: float) -> float:
        """
        Calculate projected game pace (weighted toward home team).
        Home team controls pace ~60% of time.
        """
        if not away_pace or not home_pace:
            return 0.0
        return round((away_pace * 0.4) + (home_pace * 0.6), 1)
    
    @staticmethod
    def pace_impact_on_total(projected_pace: float, league: str) -> float:
        """
        Calculate pace impact on projected total.
        
        Returns:
            Points to add/subtract from projection
        """
        league_avg = PaceCalculator.LEAGUE_AVG_PACE.get(league, 70.0)
        impact_factor = PaceCalculator.PACE_IMPACT.get(league, 1.0)
        pace_diff = projected_pace - league_avg
        return round(pace_diff * impact_factor, 1)
    
    @staticmethod
    def get_pace_trend(away_pace: float, home_pace: float, league: str) -> str:
        """
        Get descriptive pace trend for game.
        
        Returns:
            'FAST' (OVER lean), 'SLOW' (UNDER lean), or 'NEUTRAL'
        """
        projected = PaceCalculator.calculate_projected_pace(away_pace, home_pace)
        league_avg = PaceCalculator.LEAGUE_AVG_PACE.get(league, 70.0)
        
        diff_pct = ((projected - league_avg) / league_avg) * 100
        
        if diff_pct >= 5:
            return 'FAST'
        elif diff_pct <= -5:
            return 'SLOW'
        else:
            return 'NEUTRAL'
