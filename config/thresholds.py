"""
Betting thresholds configuration - Single source of truth for ALL threshold decisions
"""
from dataclasses import dataclass, field
from typing import Dict

@dataclass(frozen=True)
class BettingThresholds:
    """
    All betting thresholds in one place.
    
    Philosophy:
    - Raw edge: Before vig removal (what user sees)
    - True edge: After vig removal (what actually matters)
    - Tiers: Universal across leagues (consistency)
    """
    
    RAW_EDGE: Dict[str, float] = field(default_factory=lambda: {
        'NBA': 8.0,
        'CBB': 8.0,
        'NFL': 6.0,
        'CFB': 7.0,
        'NHL': 0.3,
    })
    
    TRUE_EDGE: Dict[str, float] = field(default_factory=lambda: {
        'NBA': 3.5,
        'CBB': 4.0,
        'NFL': 2.0,
        'CFB': 2.5,
        'NHL': 0.3,
    })
    
    TIERS: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        'SUPERMAX': {
            'edge': 12.0,
            'ev': 3.0,
            'history': 70.0,
            'kelly': 5.0,
        },
        'HIGH': {
            'edge': 10.0,
            'ev': 1.0,
            'history': 65.0,
            'kelly': 3.0,
        },
        'MEDIUM': {
            'edge': 8.0,
            'ev': -1.0,
            'history': 60.0,
            'kelly': 2.0,
        },
        'LOW': {
            'edge': 6.0,
            'ev': -2.0,
            'history': 55.0,
            'kelly': 1.0,
        }
    })
    
    MIN_HISTORY_PCT: float = 55.0
    MIN_SAMPLE_SIZE: int = 5
    IDEAL_SAMPLE_SIZE: int = 10
    
    MIN_EV_FOR_MEDIUM: float = -1.0
    MIN_EV_FOR_LOW: float = -2.0
    TARGET_EV: float = 3.0
    
    KELLY_FRACTION: float = 0.25
    MAX_KELLY: float = 5.0
    MIN_KELLY: float = 0.5
    
    MAX_VIG_PCT: float = 5.0
    IDEAL_VIG_PCT: float = 2.5
    
    def get_raw_edge_threshold(self, league: str) -> float:
        return self.RAW_EDGE.get(league, 8.0)
    
    def get_true_edge_threshold(self, league: str) -> float:
        return self.TRUE_EDGE.get(league, 3.5)
    
    def get_tier_requirements(self, tier: str) -> Dict[str, float]:
        return self.TIERS.get(tier, self.TIERS['LOW'])
    
    def calculate_tier(self, edge: float, ev: float, history_pct: float) -> str:
        """Determine confidence tier from metrics."""
        ev = ev if ev is not None else 0
        if history_pct is not None and history_pct < 1:
            history_pct = history_pct * 100
        history_pct = history_pct or 0
        
        for tier in ['SUPERMAX', 'HIGH', 'MEDIUM', 'LOW']:
            req = self.TIERS[tier]
            if (edge >= req['edge'] and 
                ev >= req['ev'] and 
                history_pct >= req['history']):
                return tier
        return 'NONE'
    
    def get_kelly_bet_size(self, tier: str, bankroll: float = 1000.0) -> float:
        """Calculate recommended bet size based on tier."""
        tier_reqs = self.get_tier_requirements(tier)
        kelly_pct = tier_reqs.get('kelly', 1.0) * self.KELLY_FRACTION
        kelly_pct = min(kelly_pct, self.MAX_KELLY)
        kelly_pct = max(kelly_pct, self.MIN_KELLY)
        return bankroll * (kelly_pct / 100)


THRESHOLDS = BettingThresholds()
