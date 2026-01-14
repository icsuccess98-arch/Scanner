"""
BULLETPROOF PICK QUALIFICATION WITH ROTOWIRE INTEGRATION
Professional betting qualification system with injury tracking
"""

import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from rotowire_integration import (
    fetch_team_injuries,
    fetch_team_lineup,
    check_game_injuries,
    InjuryImpactCalculator,
    InjuredPlayer,
    LineupData
)

logger = logging.getLogger(__name__)


class DisqualificationReason(Enum):
    """Reasons a pick can be disqualified."""
    INJURY_AWAY_KEY = "AWAY_KEY_INJURIES"
    INJURY_HOME_KEY = "HOME_KEY_INJURIES"
    INJURY_BOTH = "BOTH_TEAMS_INJURIES"
    INJURY_ASYMMETRIC = "INJURY_IMBALANCE"
    EDGE_INSUFFICIENT = "EDGE_TOO_LOW"
    EV_NEGATIVE = "NEGATIVE_EV"
    HISTORY_POOR = "HISTORY_WIN_RATE_LOW"
    DATA_INVALID = "INVALID_DATA"
    LINEUP_INCOMPLETE = "LINEUP_NOT_CONFIRMED"


@dataclass
class QualificationResult:
    """Complete qualification result with injury tracking."""
    qualified: bool
    confidence_tier: str
    true_edge: float
    ev_pct: float
    injury_check_passed: bool
    away_injury_impact: float
    home_injury_impact: float
    injury_source: str
    disqualification_reasons: list
    away_lineup_confirmed: bool
    home_lineup_confirmed: bool
    lineup_bonus_applied: bool
    recommendation: str
    should_post: bool
    
    def to_dict(self) -> Dict:
        """Convert to dict for JSON serialization."""
        return {
            'qualified': self.qualified,
            'confidence_tier': self.confidence_tier,
            'true_edge': self.true_edge,
            'ev_pct': self.ev_pct,
            'injury_check_passed': self.injury_check_passed,
            'away_injury_impact': self.away_injury_impact,
            'home_injury_impact': self.home_injury_impact,
            'injury_source': self.injury_source,
            'disqualification_reasons': self.disqualification_reasons,
            'away_lineup_confirmed': self.away_lineup_confirmed,
            'home_lineup_confirmed': self.home_lineup_confirmed,
            'lineup_bonus_applied': self.lineup_bonus_applied,
            'recommendation': self.recommendation,
            'should_post': self.should_post
        }


class InjuryQualificationGate:
    """
    FIRST GATE: Check injuries before doing any edge calculations.
    Saves computation by immediately disqualifying games with major injuries.
    """
    
    @staticmethod
    def check_injuries_preflight(away_team: str, home_team: str, 
                                 league: str) -> Tuple[bool, Dict]:
        """
        Pre-flight injury check - runs before edge calculation.
        Returns: (should_continue: bool, injury_data: dict)
        """
        try:
            injury_data = check_game_injuries(away_team, home_team, league)
            
            logger.info(
                f"{away_team} @ {home_team}: INJURY CHECK "
                f"[{injury_data['sources']}] - "
                f"Away: {injury_data['away']['out_count']} OUT, Q: {injury_data['away']['questionable_count']} | "
                f"Home: {injury_data['home']['out_count']} OUT, Q: {injury_data['home']['questionable_count']}"
            )
            
            if injury_data['should_disqualify']:
                logger.warning(
                    f"{away_team} @ {home_team}: DISQUALIFIED - {injury_data['recommendation']} "
                    f"(Away impact: {injury_data['away']['total_impact']}, "
                    f"Home impact: {injury_data['home']['total_impact']})"
                )
                return False, injury_data
            
            if injury_data['asymmetric_concern']:
                logger.warning(
                    f"{away_team} @ {home_team}: ASYMMETRIC INJURY WARNING - "
                    f"Impact difference: {abs(injury_data['away']['total_impact'] - injury_data['home']['total_impact']):.1f}"
                )
            
            return True, injury_data
            
        except Exception as e:
            logger.error(f"Error checking injuries for {away_team} @ {home_team}: {str(e)}")
            return True, {
                'away': {'total_impact': 0.0, 'source': 'error', 'out_count': 0, 'questionable_count': 0, 'should_disqualify': False},
                'home': {'total_impact': 0.0, 'source': 'error', 'out_count': 0, 'questionable_count': 0, 'should_disqualify': False},
                'should_disqualify': False,
                'asymmetric_concern': False,
                'recommendation': 'Error checking injuries - proceeding with caution',
                'sources': 'error/error'
            }


class LineupBonus:
    """Add confidence bonus for confirmed starting lineups."""
    
    LINEUP_BONUS_MULTIPLIER = 1.05
    
    @staticmethod
    def check_lineups(away_team: str, home_team: str, league: str) -> Dict:
        """Check lineup confirmation for both teams."""
        try:
            away_lineup = fetch_team_lineup(away_team, league)
            home_lineup = fetch_team_lineup(home_team, league)
            
            away_confirmed = away_lineup is not None and away_lineup.confirmed
            home_confirmed = home_lineup is not None and home_lineup.confirmed
            
            both_confirmed = away_confirmed and home_confirmed
            
            if both_confirmed:
                logger.info(f"{away_team} @ {home_team}: LINEUPS CONFIRMED - "
                           f"Away: {len(away_lineup.starters)} starters, "
                           f"Home: {len(home_lineup.starters)} starters")
            
            return {
                'away_confirmed': away_confirmed,
                'home_confirmed': home_confirmed,
                'both_confirmed': both_confirmed,
                'bonus_applicable': both_confirmed
            }
            
        except Exception as e:
            logger.error(f"Error checking lineups: {str(e)}")
            return {
                'away_confirmed': False,
                'home_confirmed': False,
                'both_confirmed': False,
                'bonus_applicable': False
            }
    
    @classmethod
    def apply_lineup_bonus(cls, true_edge: float, lineup_data: Dict) -> float:
        """Apply lineup confirmation bonus to edge."""
        if lineup_data.get('bonus_applicable', False):
            bonus_edge = true_edge * cls.LINEUP_BONUS_MULTIPLIER
            logger.debug(f"Lineup bonus applied: {true_edge:.2f} -> {bonus_edge:.2f}")
            return bonus_edge
        return true_edge


class RotoWireQualificationSystem:
    """
    Complete qualification system integrating RotoWire data.
    
    Qualification Flow:
    1. Pre-flight injury check (GATE 1) -> Disqualify major injuries
    2. Calculate edge and EV
    3. Check historical win rate
    4. Check lineup confirmations -> Apply bonus if applicable
    5. Final validation -> Return complete result
    """
    
    EDGE_THRESHOLDS = {
        "NBA": 8.0,
        "CBB": 8.0,
        "NFL": 3.5,
        "CFB": 3.5,
        "NHL": 0.5
    }
    
    MIN_EV = 0.0
    HISTORY_QUALIFY_RATE = 0.60
    
    @classmethod
    def qualify_game(cls, game_data: Dict, away_team: str, home_team: str, 
                     league: str, pick_type: str = 'total') -> QualificationResult:
        """Master qualification function with RotoWire integration."""
        disqualifications = []
        
        injury_passed, injury_data = InjuryQualificationGate.check_injuries_preflight(
            away_team, home_team, league
        )
        
        if not injury_passed:
            if injury_data['away'].get('should_disqualify'):
                disqualifications.append(DisqualificationReason.INJURY_AWAY_KEY)
            if injury_data['home'].get('should_disqualify'):
                disqualifications.append(DisqualificationReason.INJURY_HOME_KEY)
            if injury_data.get('asymmetric_concern'):
                disqualifications.append(DisqualificationReason.INJURY_ASYMMETRIC)
            
            return QualificationResult(
                qualified=False,
                confidence_tier='NONE',
                true_edge=0.0,
                ev_pct=0.0,
                injury_check_passed=False,
                away_injury_impact=injury_data['away']['total_impact'],
                home_injury_impact=injury_data['home']['total_impact'],
                injury_source=injury_data['away'].get('source', 'none'),
                disqualification_reasons=[r.value for r in disqualifications],
                away_lineup_confirmed=False,
                home_lineup_confirmed=False,
                lineup_bonus_applied=False,
                recommendation=injury_data['recommendation'],
                should_post=False
            )
        
        try:
            if pick_type == 'total':
                true_edge = cls._calculate_total_edge(game_data)
            else:
                true_edge = cls._calculate_spread_edge(game_data)
            
            ev_pct = cls._calculate_ev(game_data)
            
        except Exception as e:
            logger.error(f"Error calculating edge/EV: {str(e)}")
            disqualifications.append(DisqualificationReason.DATA_INVALID)
            true_edge = 0.0
            ev_pct = 0.0
        
        threshold = cls.EDGE_THRESHOLDS.get(league, 8.0)
        if true_edge < threshold:
            disqualifications.append(DisqualificationReason.EDGE_INSUFFICIENT)
        
        if ev_pct < cls.MIN_EV:
            disqualifications.append(DisqualificationReason.EV_NEGATIVE)
        
        historical_win_rate = game_data.get('historical_win_rate', 0.0)
        sample_size = game_data.get('sample_size', 0)
        
        if sample_size >= 8 and historical_win_rate < cls.HISTORY_QUALIFY_RATE:
            disqualifications.append(DisqualificationReason.HISTORY_POOR)
        
        lineup_data = LineupBonus.check_lineups(away_team, home_team, league)
        
        if lineup_data['bonus_applicable']:
            true_edge = LineupBonus.apply_lineup_bonus(true_edge, lineup_data)
        
        qualified = len(disqualifications) == 0
        
        if qualified:
            if true_edge >= 12.0 and ev_pct >= 3.0:
                confidence = 'SUPERMAX'
            elif true_edge >= 10.0 and ev_pct >= 2.0:
                confidence = 'HIGH'
            else:
                confidence = 'STANDARD'
        else:
            confidence = 'NONE'
        
        if qualified:
            if confidence == 'SUPERMAX':
                recommendation = f"STRONG PLAY - Edge: {true_edge:.1f}, EV: {ev_pct:.1f}%"
            elif confidence == 'HIGH':
                recommendation = f"GOOD PLAY - Edge: {true_edge:.1f}, EV: {ev_pct:.1f}%"
            else:
                recommendation = f"STANDARD PLAY - Edge: {true_edge:.1f}, EV: {ev_pct:.1f}%"
            
            if lineup_data['bonus_applicable']:
                recommendation += " (Lineups confirmed)"
        else:
            reasons = ', '.join([r.value for r in disqualifications])
            recommendation = f"SKIP - {reasons}"
        
        result = QualificationResult(
            qualified=qualified,
            confidence_tier=confidence,
            true_edge=true_edge,
            ev_pct=ev_pct,
            injury_check_passed=injury_passed,
            away_injury_impact=injury_data['away']['total_impact'],
            home_injury_impact=injury_data['home']['total_impact'],
            injury_source=injury_data['away'].get('source', 'none'),
            disqualification_reasons=[r.value for r in disqualifications],
            away_lineup_confirmed=lineup_data['away_confirmed'],
            home_lineup_confirmed=lineup_data['home_confirmed'],
            lineup_bonus_applied=lineup_data['bonus_applicable'],
            recommendation=recommendation,
            should_post=qualified and confidence in ['SUPERMAX', 'HIGH']
        )
        
        if qualified:
            logger.info(
                f"{away_team} @ {home_team}: QUALIFIED - {confidence} "
                f"(Edge: {true_edge:.2f}, EV: {ev_pct:.2f}%, "
                f"Injury source: {result.injury_source})"
            )
        else:
            logger.info(
                f"{away_team} @ {home_team}: REJECTED - {recommendation}"
            )
        
        return result
    
    @staticmethod
    def _calculate_total_edge(game_data: Dict) -> float:
        """Calculate true edge for totals."""
        projected = game_data.get('projected_total', 0.0)
        line = game_data.get('line', 0.0)
        return abs(projected - line)
    
    @staticmethod
    def _calculate_spread_edge(game_data: Dict) -> float:
        """Calculate true edge for spreads."""
        projected_margin = game_data.get('projected_margin', 0.0)
        spread_line = game_data.get('spread_line', 0.0)
        return abs(projected_margin - spread_line)
    
    @staticmethod
    def _calculate_ev(game_data: Dict) -> float:
        """Calculate expected value percentage."""
        pinnacle_odds = game_data.get('pinnacle_odds')
        bovada_odds = game_data.get('bovada_odds', -110)
        
        if not pinnacle_odds:
            return 0.0
        
        def american_to_implied(odds: int) -> float:
            if odds > 0:
                return 100 / (odds + 100)
            return abs(odds) / (abs(odds) + 100)
        
        def american_to_decimal(odds: int) -> float:
            if odds > 0:
                return (odds / 100) + 1
            return (100 / abs(odds)) + 1
        
        pinnacle_implied = american_to_implied(pinnacle_odds)
        true_prob = pinnacle_implied / 1.025
        
        bovada_decimal = american_to_decimal(bovada_odds)
        ev = (true_prob * bovada_decimal) - 1
        
        return round(ev * 100, 2)


def qualify_game_with_injuries(game_data: Dict, away_team: str, home_team: str,
                               league: str, pick_type: str = 'total') -> Dict:
    """Main qualification function to use in your app."""
    qual_result = RotoWireQualificationSystem.qualify_game(
        game_data, away_team, home_team, league, pick_type
    )
    return qual_result.to_dict()


def quick_injury_check(away_team: str, home_team: str, league: str) -> Dict:
    """Quick injury check without full qualification."""
    injury_data = check_game_injuries(away_team, home_team, league)
    
    return {
        'should_play': not injury_data['should_disqualify'],
        'away_impact': injury_data['away']['total_impact'],
        'home_impact': injury_data['home']['total_impact'],
        'source': injury_data['sources'],
        'recommendation': injury_data['recommendation'],
        'details': injury_data
    }
