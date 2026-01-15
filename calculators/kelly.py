"""
KellyCalculator - Optimal bet sizing using Kelly Criterion.
Uses fractional Kelly (default 25%) with max cap for bankroll protection.
"""


class KellyCalculator:
    """Kelly Criterion for optimal bet sizing."""
    
    DEFAULT_FRACTION = 0.25  # Quarter Kelly for safety
    MAX_BET_PERCENT = 0.05   # Never risk more than 5% of bankroll
    
    @staticmethod
    def calculate_kelly(win_prob: float, odds: int, fraction: float = 0.25) -> float:
        """
        Calculate Kelly bet size as percentage of bankroll.
        
        Args:
            win_prob: Estimated probability of winning (0-1)
            odds: American odds (-110, +150, etc.)
            fraction: Kelly fraction (0.25 = quarter Kelly)
        
        Returns:
            Recommended bet size as percentage (0-5%)
        """
        if win_prob <= 0 or win_prob >= 1 or not odds:
            return 0.0
        
        if odds > 0:
            decimal_odds = (odds / 100) + 1
        else:
            decimal_odds = (100 / abs(odds)) + 1
        
        b = decimal_odds - 1  # Net odds (profit per unit risked)
        q = 1 - win_prob      # Probability of losing
        
        kelly = ((b * win_prob) - q) / b
        kelly = kelly * fraction
        kelly = min(kelly, KellyCalculator.MAX_BET_PERCENT)
        kelly = max(kelly, 0.0)
        
        return round(kelly * 100, 2)
    
    @staticmethod
    def get_unit_size(edge: float, confidence_tier: str) -> float:
        """
        Get recommended unit size based on edge and tier.
        
        Returns:
            Unit size (0.5, 1, 2, or 3 units)
        """
        if confidence_tier == 'ELITE' and edge >= 12:
            return 3.0
        elif confidence_tier == 'HIGH' and edge >= 10:
            return 2.0
        elif confidence_tier == 'MEDIUM' and edge >= 8:
            return 1.0
        else:
            return 0.5
    
    @staticmethod
    def calculate_ev(win_prob: float, odds: int, bet_amount: float = 100) -> float:
        """
        Calculate expected value of a bet.
        
        Returns:
            Expected value in dollars (positive = +EV)
        """
        if not odds or win_prob <= 0 or win_prob >= 1:
            return 0.0
        
        if odds > 0:
            profit_if_win = bet_amount * (odds / 100)
        else:
            profit_if_win = bet_amount * (100 / abs(odds))
        
        loss_if_lose = bet_amount
        
        ev = (win_prob * profit_if_win) - ((1 - win_prob) * loss_if_lose)
        return round(ev, 2)
