"""
ProVigCalc - Calculate vig and fair lines.
Critical for determining true edge after juice removal.
"""


class ProVigCalc:
    """Calculate vig and fair lines - critical for true edge."""
    
    @staticmethod
    def american_to_decimal(odds: int) -> float:
        """Convert American odds to decimal odds."""
        if odds > 0:
            return (odds / 100) + 1
        else:
            return (100 / abs(odds)) + 1
    
    @staticmethod
    def decimal_to_american(decimal_odds: float) -> int:
        """Convert decimal odds to American odds."""
        if decimal_odds >= 2.0:
            return int((decimal_odds - 1) * 100)
        else:
            return int(-100 / (decimal_odds - 1))
    
    @staticmethod
    def calculate_vig_pct(over_odds: int, under_odds: int) -> float:
        """
        Calculate the vig percentage from a two-way market.
        Standard -110/-110 has ~4.76% vig.
        """
        if not over_odds or not under_odds:
            return 0.0
        over_decimal = ProVigCalc.american_to_decimal(over_odds)
        under_decimal = ProVigCalc.american_to_decimal(under_odds)
        implied_over = 1 / over_decimal
        implied_under = 1 / under_decimal
        total_prob = implied_over + implied_under
        vig_pct = ((total_prob - 1.0) / total_prob) * 100
        return round(vig_pct, 2)
    
    @staticmethod
    def calculate_true_probability(odds: int, market_vig: float = 4.76) -> float:
        """
        Calculate true probability after removing vig.
        """
        if not odds:
            return 0.5
        decimal_odds = ProVigCalc.american_to_decimal(odds)
        implied_prob = 1 / decimal_odds
        vig_factor = 1 + (market_vig / 100)
        true_prob = implied_prob / vig_factor
        return round(true_prob, 4)
    
    @staticmethod
    def calculate_fair_line(over_odds: int, under_odds: int) -> float:
        """
        Calculate the fair (no-vig) line from market odds.
        Returns the true probability for the over side.
        """
        if not over_odds or not under_odds:
            return 0.5
        over_decimal = ProVigCalc.american_to_decimal(over_odds)
        under_decimal = ProVigCalc.american_to_decimal(under_odds)
        implied_over = 1 / over_decimal
        implied_under = 1 / under_decimal
        total_prob = implied_over + implied_under
        fair_over_prob = implied_over / total_prob
        return round(fair_over_prob, 4)
