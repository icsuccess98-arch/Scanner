"""
RestDayCalculator - Fatigue modeling for back-to-back and rest situations.
B2B games significantly suppress scoring, especially in NBA/NHL.
"""


class RestDayCalculator:
    """Rest day fatigue modeling - B2B kills scoring."""
    
    REST_IMPACT = {
        'NBA': {
            'b2b': -4.0,        # Back-to-back severe penalty
            'one_day': -2.0,    # One day rest
            'two_days': 0.0,    # Normal rest
            'three_plus': 1.5   # Well rested bonus
        },
        'CBB': {
            'b2b': -3.0,
            'one_day': -1.5,
            'two_days': 0.0,
            'three_plus': 1.0
        },
        'NHL': {
            'b2b': -2.5,        # B2B penalty
            'one_day': -1.0,    # Light penalty
            'two_plus': 0.0     # Normal
        },
        'NFL': {
            'thursday': -3.0,   # Short week (Thursday game)
            'normal': 0.0,      # Sunday/Monday normal week
            'bye': 2.5          # Coming off bye week
        },
        'CFB': {
            'short_week': -2.0,
            'normal': 0.0,
            'bye': 1.5
        }
    }
    
    @staticmethod
    def calculate_rest_impact(days_rest: int, is_b2b: bool, league: str) -> float:
        """
        Calculate rest day impact on team performance.
        
        Args:
            days_rest: Days since last game
            is_b2b: Is this a back-to-back game?
            league: League name (NBA, NFL, etc.)
        
        Returns:
            Points adjustment (negative = fatigue, positive = rest bonus)
        """
        if league not in RestDayCalculator.REST_IMPACT:
            return 0.0
        
        impacts = RestDayCalculator.REST_IMPACT[league]
        
        if is_b2b:
            return impacts.get('b2b', -2.0)
        elif days_rest == 1:
            return impacts.get('one_day', -1.0)
        elif days_rest == 2:
            return impacts.get('two_days', 0.0)
        elif days_rest >= 3:
            return impacts.get('three_plus', 1.0)
        
        return 0.0
    
    @staticmethod
    def get_rest_advantage(away_days: int, home_days: int, league: str) -> dict:
        """
        Calculate rest advantage between teams.
        
        Returns:
            dict with advantage_team, advantage_points
        """
        away_impact = RestDayCalculator.calculate_rest_impact(
            away_days, away_days == 1, league
        )
        home_impact = RestDayCalculator.calculate_rest_impact(
            home_days, home_days == 1, league
        )
        
        diff = home_impact - away_impact
        
        if abs(diff) < 1.0:
            return {'advantage_team': None, 'advantage_points': 0.0}
        elif diff > 0:
            return {'advantage_team': 'HOME', 'advantage_points': round(diff, 1)}
        else:
            return {'advantage_team': 'AWAY', 'advantage_points': round(abs(diff), 1)}
    
    @staticmethod
    def is_fatigue_spot(days_rest: int, is_b2b: bool, league: str) -> bool:
        """
        Check if team is in a fatigue spot (should avoid).
        """
        impact = RestDayCalculator.calculate_rest_impact(days_rest, is_b2b, league)
        return impact <= -2.0
