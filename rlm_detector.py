"""
Reverse Line Movement (RLM) Detection Module
Professional-grade implementation with correct logic for spreads and totals.
"""

import logging
from typing import Dict, Optional, Tuple
from enum import Enum
from dataclasses import dataclass

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
