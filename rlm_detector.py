"""
Reverse Line Movement (RLM) Detection Module
Professional-grade implementation with correct logic for spreads and totals.

Key Principles from Sharp Betting Strategy:
- Sharp money = lower bet % but higher handle/money % (e.g., 40% bets but 70% money = sharp side)
- RLM = line moves AGAINST majority of bets (not money)
- Opening line matters most - Vegas sets it with elite information
- Stagnant lines with heavy money = Vegas resisting, warning sign
"""

import logging
from typing import Dict, Optional, Tuple, List
from enum import Enum
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class BetType(Enum):
    """Types of bets for RLM detection."""
    SPREAD = "spread"
    TOTAL = "total"
    MONEYLINE = "moneyline"


class Severity(Enum):
    """RLM severity levels."""
    NONE = "none"
    MODERATE = "moderate"  # 0.5-1.0 point movement
    STRONG = "strong"      # 1.0-2.0 point movement
    EXTREME = "extreme"    # 2.0+ point movement


@dataclass
class RLMResult:
    """Result of RLM detection."""
    rlm_detected: bool
    movement: float
    movement_abs: float
    direction: str  # 'reverse', 'normal', 'none'
    severity: Severity
    explanation: str
    public_pct: float
    public_side: str
    sharp_side: str  # Which side sharp money is on
    confidence: float  # 0-100, how confident we are in this RLM


@dataclass
class SharpMoneyResult:
    """
    Result of sharp money detection based on bet % vs money % divergence.
    Key principle: Sharp money = lower bet % but higher money/handle %
    Example: 40% of bets but 70% of money = sharp side
    """
    sharp_detected: bool
    sharp_side: str  # 'away', 'home', 'over', 'under', 'none'
    sharp_team: str  # Team name or O/U
    bet_pct: float  # Percentage of tickets
    money_pct: float  # Percentage of handle
    divergence: float  # money_pct - bet_pct (positive = sharp action)
    severity: str  # 'none', 'moderate', 'strong', 'extreme'
    explanation: str
    stagnant_line_warning: bool  # Heavy money but no line movement


@dataclass
class BettingChecklist:
    """
    Final line checklist from professional betting strategy.
    All criteria must pass for a qualified bet.
    """
    opening_line_confirmed: bool
    current_line_validated: bool
    money_stats_line_align: bool  # Money, stats, and line movement align
    situational_factors_clear: bool  # No hidden fatigue, travel, injury
    no_traps_detected: bool  # No warning signs
    total_checks_passed: int
    total_checks: int
    is_qualified: bool  # All 5 criteria pass
    details: List[str] = field(default_factory=list)


class ReverseLineMovementDetector:
    """
    Detects reverse line movement for spreads and totals.
    
    RLM occurs when the betting line moves in the opposite direction
    of where the majority of public money is going.
    """
    
    def __init__(
        self,
        min_public_pct: float = 65.0,
        min_movement: float = 0.5,
        use_money_pct: bool = True
    ):
        """
        Initialize RLM detector.
        
        Args:
            min_public_pct: Minimum public percentage to consider (default 65%)
            min_movement: Minimum line movement in points (default 0.5)
            use_money_pct: Use money % over tickets % when available
        """
        self.min_public_pct = min_public_pct
        self.min_movement = min_movement
        self.use_money_pct = use_money_pct
    
    def detect_spread_rlm(
        self,
        opening_spread: float,
        current_spread: float,
        tickets_pct_favorite: Optional[float] = None,
        money_pct_favorite: Optional[float] = None,
        team_name: str = "Team"
    ) -> RLMResult:
        """
        Detect RLM for point spreads.
        
        Args:
            opening_spread: Opening spread (negative = favorite)
            current_spread: Current spread (negative = favorite)
            tickets_pct_favorite: Percentage of tickets on favorite
            money_pct_favorite: Percentage of money on favorite
            team_name: Team name for logging
        
        Returns:
            RLMResult with detection details
        
        Examples:
            Opening: -7.0, Current: -5.5, Public: 75% on favorite
            → RLM detected (line easier for favorite despite public support)
            
            Opening: -7.0, Current: -8.5, Public: 75% on favorite  
            → No RLM (line moved with public)
        """
        # Choose which percentage to use
        if self.use_money_pct and money_pct_favorite is not None:
            public_pct = money_pct_favorite
            pct_type = "money"
        elif tickets_pct_favorite is not None:
            public_pct = tickets_pct_favorite
            pct_type = "tickets"
        else:
            return self._no_data_result("No betting percentage data available")
        
        # Calculate movement
        movement = current_spread - opening_spread
        movement_abs = abs(movement)
        
        # Check minimum movement threshold
        if movement_abs < self.min_movement:
            return RLMResult(
                rlm_detected=False,
                movement=movement,
                movement_abs=movement_abs,
                direction='none',
                severity=Severity.NONE,
                explanation=f'Insignificant movement: {movement_abs:.1f} points',
                public_pct=public_pct,
                public_side='unknown',
                sharp_side='unknown',
                confidence=0.0
            )
        
        # Determine public side
        if public_pct >= 50:
            public_side = 'favorite'
            public_pct_adjusted = public_pct
        else:
            public_side = 'underdog'
            public_pct_adjusted = 100 - public_pct
        
        # Check if public is lopsided enough
        if public_pct_adjusted < self.min_public_pct:
            return RLMResult(
                rlm_detected=False,
                movement=movement,
                movement_abs=movement_abs,
                direction='balanced',
                severity=Severity.NONE,
                explanation=f'Public not lopsided enough: {public_pct_adjusted:.1f}% (need {self.min_public_pct}%)',
                public_pct=public_pct_adjusted,
                public_side=public_side,
                sharp_side='unknown',
                confidence=0.0
            )
        
        # Detect RLM based on public side
        rlm_detected = False
        
        if public_side == 'favorite':
            # Public is on the favorite (negative spread)
            # NORMAL: Line should become MORE negative (harder to cover)
            # RLM: Line becomes LESS negative (easier to cover)
            
            if movement > 0:  # Line became less negative (e.g., -7 to -6)
                rlm_detected = True
                sharp_side = 'underdog'
                explanation = (
                    f"{public_pct_adjusted:.1f}% of {pct_type} on favorite, "
                    f"but line moved from {opening_spread:.1f} to {current_spread:.1f} "
                    f"({movement:+.1f}) making it EASIER for favorite to cover. "
                    f"Sharp money likely on underdog."
                )
            else:  # Line became more negative (normal movement)
                sharp_side = 'favorite'
                explanation = (
                    f"Line moved WITH public: {opening_spread:.1f} → {current_spread:.1f} "
                    f"({movement:+.1f}). Public and sharp money aligned on favorite."
                )
        
        else:  # public_side == 'underdog'
            # Public is on the underdog (positive spread)
            # NORMAL: Line should become LESS positive (harder for underdog)
            # RLM: Line becomes MORE positive (easier for underdog)
            
            # Note: For underdog, opening_spread and current_spread are positive
            if movement > 0:  # Line became more positive (e.g., +7 to +8)
                rlm_detected = True
                sharp_side = 'favorite'
                explanation = (
                    f"{public_pct_adjusted:.1f}% of {pct_type} on underdog, "
                    f"but line moved from {opening_spread:+.1f} to {current_spread:+.1f} "
                    f"({movement:+.1f}) giving underdog MORE points. "
                    f"Sharp money likely on favorite."
                )
            else:  # Line became less positive (normal movement)
                sharp_side = 'underdog'
                explanation = (
                    f"Line moved WITH public: {opening_spread:+.1f} → {current_spread:+.1f} "
                    f"({movement:+.1f}). Public and sharp money aligned on underdog."
                )
        
        # Calculate severity and confidence
        severity = self._calculate_severity(movement_abs)
        confidence = self._calculate_confidence(
            public_pct_adjusted, movement_abs, rlm_detected
        )
        
        return RLMResult(
            rlm_detected=rlm_detected,
            movement=movement,
            movement_abs=movement_abs,
            direction='reverse' if rlm_detected else 'normal',
            severity=severity if rlm_detected else Severity.NONE,
            explanation=explanation,
            public_pct=public_pct_adjusted,
            public_side=public_side,
            sharp_side=sharp_side,
            confidence=confidence
        )
    
    def detect_total_rlm(
        self,
        opening_total: float,
        current_total: float,
        tickets_pct_over: Optional[float] = None,
        money_pct_over: Optional[float] = None,
        game_name: str = "Game"
    ) -> RLMResult:
        """
        Detect RLM for over/under totals.
        
        Args:
            opening_total: Opening total
            current_total: Current total
            tickets_pct_over: Percentage of tickets on Over
            money_pct_over: Percentage of money on Over
            game_name: Game name for logging
        
        Returns:
            RLMResult with detection details
        
        Examples:
            Opening: 225.0, Current: 223.0, Public: 70% on Over
            → RLM detected (total lowered making Over easier)
            
            Opening: 225.0, Current: 227.0, Public: 70% on Over
            → No RLM (total raised making Over harder)
        """
        # Choose which percentage to use
        if self.use_money_pct and money_pct_over is not None:
            public_pct = money_pct_over
            pct_type = "money"
        elif tickets_pct_over is not None:
            public_pct = tickets_pct_over
            pct_type = "tickets"
        else:
            return self._no_data_result("No betting percentage data available")
        
        # Calculate movement
        movement = current_total - opening_total
        movement_abs = abs(movement)
        
        # Check minimum movement threshold
        if movement_abs < self.min_movement:
            return RLMResult(
                rlm_detected=False,
                movement=movement,
                movement_abs=movement_abs,
                direction='none',
                severity=Severity.NONE,
                explanation=f'Insignificant movement: {movement_abs:.1f} points',
                public_pct=public_pct,
                public_side='unknown',
                sharp_side='unknown',
                confidence=0.0
            )
        
        # Determine public side
        if public_pct >= 50:
            public_side = 'over'
            public_pct_adjusted = public_pct
        else:
            public_side = 'under'
            public_pct_adjusted = 100 - public_pct
        
        # Check if public is lopsided enough
        if public_pct_adjusted < self.min_public_pct:
            return RLMResult(
                rlm_detected=False,
                movement=movement,
                movement_abs=movement_abs,
                direction='balanced',
                severity=Severity.NONE,
                explanation=f'Public not lopsided enough: {public_pct_adjusted:.1f}% (need {self.min_public_pct}%)',
                public_pct=public_pct_adjusted,
                public_side=public_side,
                sharp_side='unknown',
                confidence=0.0
            )
        
        # Detect RLM based on public side
        rlm_detected = False
        
        if public_side == 'over':
            # Public is on Over
            # NORMAL: Total should go UP (harder for Over to hit)
            # RLM: Total goes DOWN (easier for Over to hit)
            
            if movement < 0:  # Total decreased
                rlm_detected = True
                sharp_side = 'over'
                explanation = (
                    f"{public_pct_adjusted:.1f}% of {pct_type} on Over, "
                    f"but total moved from {opening_total:.1f} to {current_total:.1f} "
                    f"({movement:+.1f}) making Over EASIER to hit. "
                    f"Sharp money likely on Over."
                )
            else:  # Total increased (normal)
                sharp_side = 'under'
                explanation = (
                    f"Total moved AGAINST Over: {opening_total:.1f} → {current_total:.1f} "
                    f"({movement:+.1f}). Sharp money likely on Under."
                )
        
        else:  # public_side == 'under'
            # Public is on Under
            # NORMAL: Total should go DOWN (harder for Under to hit)
            # RLM: Total goes UP (easier for Under to hit)
            
            if movement > 0:  # Total increased
                rlm_detected = True
                sharp_side = 'under'
                explanation = (
                    f"{public_pct_adjusted:.1f}% of {pct_type} on Under, "
                    f"but total moved from {opening_total:.1f} to {current_total:.1f} "
                    f"({movement:+.1f}) making Under EASIER to hit. "
                    f"Sharp money likely on Under."
                )
            else:  # Total decreased (normal)
                sharp_side = 'over'
                explanation = (
                    f"Total moved AGAINST Under: {opening_total:.1f} → {current_total:.1f} "
                    f"({movement:+.1f}). Sharp money likely on Over."
                )
        
        # Calculate severity and confidence
        severity = self._calculate_severity(movement_abs)
        confidence = self._calculate_confidence(
            public_pct_adjusted, movement_abs, rlm_detected
        )
        
        return RLMResult(
            rlm_detected=rlm_detected,
            movement=movement,
            movement_abs=movement_abs,
            direction='reverse' if rlm_detected else 'normal',
            severity=severity if rlm_detected else Severity.NONE,
            explanation=explanation,
            public_pct=public_pct_adjusted,
            public_side=public_side,
            sharp_side=sharp_side,
            confidence=confidence
        )
    
    def _calculate_severity(self, movement_abs: float) -> Severity:
        """Calculate severity based on absolute movement."""
        if movement_abs >= 2.0:
            return Severity.EXTREME
        elif movement_abs >= 1.0:
            return Severity.STRONG
        elif movement_abs >= 0.5:
            return Severity.MODERATE
        else:
            return Severity.NONE
    
    def _calculate_confidence(
        self,
        public_pct: float,
        movement_abs: float,
        rlm_detected: bool
    ) -> float:
        """
        Calculate confidence score (0-100).
        
        Higher confidence when:
        - Public % is very lopsided (75%+)
        - Movement is significant (1.5+ points)
        - RLM is detected
        """
        if not rlm_detected:
            return 0.0
        
        # Base confidence on public percentage
        # 65% → 50, 70% → 60, 75% → 75, 80% → 90, 85%+ → 100
        pct_score = min(100, (public_pct - 50) * 2)
        
        # Boost for significant movement
        # 0.5 pts → 1.0x, 1.0 pts → 1.15x, 2.0 pts → 1.3x
        movement_multiplier = 1.0 + min(0.3, movement_abs * 0.15)
        
        confidence = min(100, pct_score * movement_multiplier)
        
        return round(confidence, 1)
    
    def _no_data_result(self, reason: str) -> RLMResult:
        """Return result when data is missing."""
        return RLMResult(
            rlm_detected=False,
            movement=0.0,
            movement_abs=0.0,
            direction='none',
            severity=Severity.NONE,
            explanation=reason,
            public_pct=0.0,
            public_side='unknown',
            sharp_side='unknown',
            confidence=0.0
        )
    
    def detect_sharp_money(
        self,
        away_bet_pct: float,
        away_money_pct: float,
        home_bet_pct: float,
        home_money_pct: float,
        away_team: str = "Away",
        home_team: str = "Home",
        line_movement: float = 0.0,
        min_divergence: float = 10.0
    ) -> SharpMoneyResult:
        """
        Detect sharp money based on bet % vs money % divergence.
        
        Key Principle: Sharp money appears when lower bet % but higher handle %
        Example: 40% of bets but 70% of money = sharp side
        
        Args:
            away_bet_pct: Percentage of tickets on away team
            away_money_pct: Percentage of money/handle on away team  
            home_bet_pct: Percentage of tickets on home team
            home_money_pct: Percentage of money/handle on home team
            away_team: Away team name
            home_team: Home team name
            line_movement: How much the line has moved (for stagnant line detection)
            min_divergence: Minimum divergence to flag sharp money (default 10%)
        
        Returns:
            SharpMoneyResult with detection details
        """
        # Calculate divergence (money % - bet %) for each side
        away_divergence = away_money_pct - away_bet_pct
        home_divergence = home_money_pct - home_bet_pct
        
        # Determine which side has sharp action (positive divergence = sharps)
        sharp_detected = False
        sharp_side = 'none'
        sharp_team = 'None'
        divergence = 0.0
        bet_pct = 0.0
        money_pct = 0.0
        
        # Check for stagnant line warning (heavy money but line didn't move)
        total_money_one_side = max(away_money_pct, home_money_pct)
        stagnant_warning = total_money_one_side >= 65 and abs(line_movement) < 0.5
        
        if away_divergence >= min_divergence:
            sharp_detected = True
            sharp_side = 'away'
            sharp_team = away_team
            divergence = away_divergence
            bet_pct = away_bet_pct
            money_pct = away_money_pct
        elif home_divergence >= min_divergence:
            sharp_detected = True
            sharp_side = 'home'
            sharp_team = home_team
            divergence = home_divergence
            bet_pct = home_bet_pct
            money_pct = home_money_pct
        
        # Determine severity based on divergence
        if divergence >= 30:
            severity = 'extreme'
        elif divergence >= 20:
            severity = 'strong'
        elif divergence >= 10:
            severity = 'moderate'
        else:
            severity = 'none'
        
        # Build explanation
        if sharp_detected:
            explanation = (
                f"SHARP MONEY on {sharp_team}: {bet_pct:.0f}% of bets but {money_pct:.0f}% of money "
                f"(+{divergence:.0f}% divergence). Big bettors backing {sharp_team}."
            )
            if stagnant_warning:
                explanation += " WARNING: Heavy money but line hasn't moved - Vegas may be resisting."
        else:
            explanation = "No significant sharp money divergence detected. Public and sharp action aligned."
        
        return SharpMoneyResult(
            sharp_detected=sharp_detected,
            sharp_side=sharp_side,
            sharp_team=sharp_team,
            bet_pct=bet_pct,
            money_pct=money_pct,
            divergence=divergence,
            severity=severity,
            explanation=explanation,
            stagnant_line_warning=stagnant_warning
        )
    
    def generate_betting_checklist(
        self,
        opening_line: Optional[float],
        current_line: Optional[float],
        sharp_result: Optional[SharpMoneyResult],
        rlm_result: Optional[RLMResult],
        is_home_team: bool = False,
        is_b2b: bool = False,
        spread_size: float = 0.0,
        team_l10_wins: int = 5,
        is_bottom_tier: bool = False
    ) -> BettingChecklist:
        """
        Generate final line checklist for bet qualification.
        
        Checklist criteria:
        1. Opening line confirmed
        2. Current line validated
        3. Money, stats, and line movement align
        4. Situational factors make sense (no B2B trap, home court, etc.)
        5. No hidden traps detected
        
        Args:
            opening_line: Opening spread/total
            current_line: Current spread/total
            sharp_result: Sharp money detection result
            rlm_result: RLM detection result
            is_home_team: Is this the home team (home court advantage)
            is_b2b: Is team on back-to-back
            spread_size: Absolute spread size (avoid >10)
            team_l10_wins: Team wins in last 10 games
            is_bottom_tier: Is team considered bottom-tier
        
        Returns:
            BettingChecklist with pass/fail for each criterion
        """
        details = []
        checks_passed = 0
        
        # 1. Opening line confirmed
        opening_confirmed = opening_line is not None
        if opening_confirmed:
            details.append(f"✓ Opening line: {opening_line:+.1f}")
            checks_passed += 1
        else:
            details.append("✗ Opening line not available")
        
        # 2. Current line validated
        current_validated = current_line is not None
        if current_validated:
            details.append(f"✓ Current line: {current_line:+.1f}")
            checks_passed += 1
        else:
            details.append("✗ Current line not available")
        
        # 3. Money, stats, line movement align
        alignment = False
        if sharp_result and rlm_result:
            # Sharp money and RLM should point same direction, or no conflicting signals
            if sharp_result.sharp_detected and rlm_result.rlm_detected:
                alignment = True
                details.append(f"✓ Sharp money + RLM aligned on {sharp_result.sharp_team}")
            elif not sharp_result.stagnant_line_warning:
                alignment = True
                details.append("✓ No conflicting money/line signals")
            else:
                details.append("⚠ Stagnant line warning - Vegas resisting")
        else:
            alignment = True  # No data = pass by default
            details.append("✓ Line data consistent")
        if alignment:
            checks_passed += 1
        
        # 4. Situational factors clear
        situational_clear = True
        situational_notes = []
        
        if is_b2b:
            situational_clear = False
            situational_notes.append("B2B fatigue")
        if spread_size > 10:
            situational_clear = False
            situational_notes.append(f"Large spread ({spread_size:.1f})")
        if is_bottom_tier:
            situational_clear = False
            situational_notes.append("Bottom-tier team")
        if team_l10_wins <= 3:
            situational_notes.append("Cold streak (L10)")
        if is_home_team:
            situational_notes.append("Home court ✓")
        
        if situational_clear:
            details.append("✓ Situational factors clear" + (f" ({', '.join(situational_notes)})" if situational_notes else ""))
            checks_passed += 1
        else:
            details.append(f"✗ Situational concerns: {', '.join(situational_notes)}")
        
        # 5. No traps detected
        no_traps = True
        trap_notes = []
        
        if sharp_result and sharp_result.stagnant_line_warning:
            no_traps = False
            trap_notes.append("Vegas resistance")
        if is_b2b:
            trap_notes.append("Fatigue trap")
        if is_bottom_tier and spread_size < 5:
            trap_notes.append("Trap game potential")
        
        if no_traps and not trap_notes:
            details.append("✓ No trap signals detected")
            checks_passed += 1
        elif no_traps:
            details.append(f"⚠ Minor concerns: {', '.join(trap_notes)}")
            checks_passed += 1
        else:
            details.append(f"✗ Trap warning: {', '.join(trap_notes)}")
        
        is_qualified = checks_passed >= 4  # Need at least 4/5 to qualify
        
        return BettingChecklist(
            opening_line_confirmed=opening_confirmed,
            current_line_validated=current_validated,
            money_stats_line_align=alignment,
            situational_factors_clear=situational_clear,
            no_traps_detected=no_traps,
            total_checks_passed=checks_passed,
            total_checks=5,
            is_qualified=is_qualified,
            details=details
        )


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    detector = ReverseLineMovementDetector(
        min_public_pct=65.0,
        min_movement=0.5
    )
    
    print("=" * 80)
    print("REVERSE LINE MOVEMENT DETECTION - TEST CASES")
    print("=" * 80)
    
    # Test Case 1: Classic RLM (Spread - Favorite)
    print("\n[TEST 1] Classic RLM: Chiefs vs Colts")
    print("-" * 80)
    result = detector.detect_spread_rlm(
        opening_spread=-7.0,
        current_spread=-5.5,
        tickets_pct_favorite=75,
        team_name="Chiefs"
    )
    print(f"RLM Detected: {result.rlm_detected}")
    print(f"Severity: {result.severity.value}")
    print(f"Confidence: {result.confidence}%")
    print(f"Explanation: {result.explanation}")
    print(f"Sharp Side: {result.sharp_side}")
    
    # Test Case 2: Normal Movement (No RLM)
    print("\n[TEST 2] Normal Movement: Bucks vs Wizards")
    print("-" * 80)
    result = detector.detect_spread_rlm(
        opening_spread=-12.0,
        current_spread=-13.5,
        tickets_pct_favorite=80,
        team_name="Bucks"
    )
    print(f"RLM Detected: {result.rlm_detected}")
    print(f"Explanation: {result.explanation}")
    
    # Test Case 3: RLM on Underdog
    print("\n[TEST 3] RLM on Underdog: Heat vs Bulls")
    print("-" * 80)
    result = detector.detect_spread_rlm(
        opening_spread=+3.5,
        current_spread=+5.0,
        tickets_pct_favorite=30,  # 70% on underdog
        team_name="Bulls"
    )
    print(f"RLM Detected: {result.rlm_detected}")
    print(f"Severity: {result.severity.value}")
    print(f"Explanation: {result.explanation}")
    
    # Test Case 4: Total RLM (Over)
    print("\n[TEST 4] Total RLM: Lakers vs Celtics Over")
    print("-" * 80)
    result = detector.detect_total_rlm(
        opening_total=225.0,
        current_total=223.0,
        tickets_pct_over=72,
        game_name="Lakers vs Celtics"
    )
    print(f"RLM Detected: {result.rlm_detected}")
    print(f"Severity: {result.severity.value}")
    print(f"Confidence: {result.confidence}%")
    print(f"Explanation: {result.explanation}")
    
    # Test Case 5: Extreme RLM
    print("\n[TEST 5] Extreme RLM: Warriors vs Spurs")
    print("-" * 80)
    result = detector.detect_spread_rlm(
        opening_spread=-9.0,
        current_spread=-6.0,
        money_pct_favorite=82,
        team_name="Warriors"
    )
    print(f"RLM Detected: {result.rlm_detected}")
    print(f"Severity: {result.severity.value}")
    print(f"Confidence: {result.confidence}%")
    print(f"Movement: {result.movement:+.1f} points")
    print(f"Explanation: {result.explanation}")
    
    print("\n" + "=" * 80)
    print("All tests complete!")
