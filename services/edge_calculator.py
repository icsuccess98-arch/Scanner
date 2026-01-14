"""
Edge calculation service - Handles all edge-related math in one place
"""
from typing import Tuple, Optional
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.thresholds import THRESHOLDS


class EdgeCalculator:
    """
    Centralized edge calculation.
    
    Responsibilities:
    - Calculate raw edge (before vig removal)
    - Calculate true edge (after vig removal)
    - Determine if edge qualifies
    - Calculate margin of victory
    """
    
    @staticmethod
    def calculate_raw_edge(projected: float, line: float) -> float:
        """Calculate raw edge (absolute difference)."""
        return abs(projected - line)
    
    @staticmethod
    def calculate_true_edge(
        projected: float, 
        line: float, 
        over_odds: int = -110, 
        under_odds: int = -110
    ) -> Tuple[float, float]:
        """
        Calculate true edge after vig removal.
        
        Returns: (true_edge, fair_line)
        """
        vig = EdgeCalculator._calculate_vig(over_odds, under_odds)
        fair_line = line + (vig * 0.5)
        true_edge = abs(projected - fair_line)
        return true_edge, fair_line
    
    @staticmethod
    def _calculate_vig(over_odds: int, under_odds: int) -> float:
        """Calculate vig from odds."""
        def implied_prob(odds: int) -> float:
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)
        
        over_prob = implied_prob(over_odds)
        under_prob = implied_prob(under_odds)
        total_prob = over_prob + under_prob
        vig = (total_prob - 1) * 100
        return vig
    
    @staticmethod
    def qualifies_raw(edge: float, league: str) -> bool:
        """Check if raw edge meets threshold."""
        threshold = THRESHOLDS.get_raw_edge_threshold(league)
        return edge >= threshold
    
    @staticmethod
    def qualifies_true(true_edge: float, league: str) -> bool:
        """Check if true edge meets threshold."""
        threshold = THRESHOLDS.get_true_edge_threshold(league)
        return true_edge >= threshold
    
    @staticmethod
    def calculate_direction(projected: float, line: float) -> Optional[str]:
        """Determine bet direction (OVER or UNDER)."""
        if projected > line:
            return 'OVER'
        elif projected < line:
            return 'UNDER'
        return None
    
    @staticmethod
    def calculate_spread_direction(expected_margin: float, spread: float) -> Optional[str]:
        """Determine spread bet direction (HOME or AWAY)."""
        if expected_margin > spread:
            return 'AWAY'
        elif expected_margin < spread:
            return 'HOME'
        return None
    
    @staticmethod
    def calculate_margin(team_ppg: float, team_opp_ppg: float) -> float:
        """Calculate expected margin of victory."""
        return team_ppg - team_opp_ppg
    
    @staticmethod
    def calculate_edge_percentage(edge: float, line: float) -> float:
        """Calculate edge as percentage of line."""
        if line == 0:
            return 0.0
        return (edge / line) * 100
    
    @staticmethod
    def get_confidence_tier(edge: float, ev: float, history_pct: float) -> str:
        """Get confidence tier from metrics."""
        return THRESHOLDS.calculate_tier(edge, ev, history_pct)
