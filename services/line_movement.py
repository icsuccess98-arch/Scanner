"""
Line Movement Tracking Service - Track sharp money and CLV
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List
from enum import Enum


class MovementDirection(Enum):
    TOWARD_PICK = "toward_pick"
    AGAINST_PICK = "against_pick"
    NEUTRAL = "neutral"


class SharpIndicator(Enum):
    SHARP_AGREES = "sharp_agrees"
    SHARP_DISAGREES = "sharp_disagrees"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"


@dataclass
class LineMovement:
    """Represents line movement for a game."""
    opening_line: float
    current_line: float
    movement: float
    direction: MovementDirection
    sharp_indicator: SharpIndicator
    timestamp: datetime
    
    @property
    def moved_significantly(self) -> bool:
        return abs(self.movement) >= 0.5
    
    @property
    def reverse_line_movement(self) -> bool:
        return self.sharp_indicator == SharpIndicator.SHARP_AGREES


class LineMovementTracker:
    """
    Tracks line movement and detects sharp money.
    
    Key Concepts:
    - Opening line: First line posted
    - Current line: Live line now
    - Movement: How much it moved
    - RLM: Reverse Line Movement (sharp indicator)
    """
    
    def __init__(self):
        self._cache: Dict[str, LineMovement] = {}
    
    def record_opening(self, game_id: str, line: float, line_type: str = 'total') -> None:
        """Record opening line for a game."""
        key = f"{game_id}_{line_type}"
        if key not in self._cache:
            self._cache[key] = LineMovement(
                opening_line=line,
                current_line=line,
                movement=0.0,
                direction=MovementDirection.NEUTRAL,
                sharp_indicator=SharpIndicator.UNKNOWN,
                timestamp=datetime.now()
            )
    
    def update_current(
        self, 
        game_id: str, 
        current_line: float, 
        pick_direction: str,
        public_pct: Optional[float] = None,
        line_type: str = 'total'
    ) -> Optional[LineMovement]:
        """
        Update current line and analyze movement.
        
        Args:
            game_id: Unique game identifier
            current_line: Current line value
            pick_direction: 'OVER', 'UNDER', 'HOME', 'AWAY'
            public_pct: Public betting percentage on pick side (0-100)
            line_type: 'total' or 'spread'
        """
        key = f"{game_id}_{line_type}"
        
        if key not in self._cache:
            self.record_opening(game_id, current_line, line_type)
            return self._cache[key]
        
        movement_data = self._cache[key]
        opening = movement_data.opening_line
        movement = current_line - opening
        
        if line_type == 'total':
            direction = self._analyze_total_movement(movement, pick_direction)
        else:
            direction = self._analyze_spread_movement(movement, pick_direction)
        
        sharp = self._detect_sharp_money(movement, pick_direction, public_pct, line_type)
        
        self._cache[key] = LineMovement(
            opening_line=opening,
            current_line=current_line,
            movement=movement,
            direction=direction,
            sharp_indicator=sharp,
            timestamp=datetime.now()
        )
        
        return self._cache[key]
    
    def _analyze_total_movement(self, movement: float, pick_direction: str) -> MovementDirection:
        """Analyze total line movement direction."""
        if abs(movement) < 0.5:
            return MovementDirection.NEUTRAL
        
        if pick_direction == 'OVER':
            return MovementDirection.TOWARD_PICK if movement > 0 else MovementDirection.AGAINST_PICK
        else:
            return MovementDirection.TOWARD_PICK if movement < 0 else MovementDirection.AGAINST_PICK
    
    def _analyze_spread_movement(self, movement: float, pick_direction: str) -> MovementDirection:
        """Analyze spread line movement direction."""
        if abs(movement) < 0.5:
            return MovementDirection.NEUTRAL
        
        if pick_direction in ['AWAY', 'OVER']:
            return MovementDirection.TOWARD_PICK if movement < 0 else MovementDirection.AGAINST_PICK
        else:
            return MovementDirection.TOWARD_PICK if movement > 0 else MovementDirection.AGAINST_PICK
    
    def _detect_sharp_money(
        self, 
        movement: float, 
        pick_direction: str, 
        public_pct: Optional[float],
        line_type: str
    ) -> SharpIndicator:
        """
        Detect if sharp money is on our side.
        
        Reverse Line Movement (RLM): Line moves against public betting.
        - 75% public on OVER, line moves DOWN = sharps on UNDER
        """
        if public_pct is None:
            return SharpIndicator.UNKNOWN
        
        if abs(movement) < 0.5:
            return SharpIndicator.NEUTRAL
        
        public_on_pick = public_pct > 50
        
        if line_type == 'total':
            line_moved_toward_pick = (
                (pick_direction == 'OVER' and movement > 0) or
                (pick_direction == 'UNDER' and movement < 0)
            )
        else:
            line_moved_toward_pick = (
                (pick_direction in ['AWAY'] and movement < 0) or
                (pick_direction in ['HOME'] and movement > 0)
            )
        
        if public_on_pick and not line_moved_toward_pick:
            return SharpIndicator.SHARP_AGREES
        elif not public_on_pick and line_moved_toward_pick:
            return SharpIndicator.SHARP_DISAGREES
        elif public_on_pick and line_moved_toward_pick:
            return SharpIndicator.NEUTRAL
        else:
            return SharpIndicator.SHARP_AGREES
    
    def get_movement(self, game_id: str, line_type: str = 'total') -> Optional[LineMovement]:
        """Get line movement for a game."""
        key = f"{game_id}_{line_type}"
        return self._cache.get(key)
    
    def calculate_clv(self, bet_line: float, closing_line: float, direction: str) -> float:
        """
        Calculate Closing Line Value.
        
        Positive CLV = you beat the closing line (sharp bettor)
        Negative CLV = you lost to the closing line (square bettor)
        """
        if direction in ['OVER', 'AWAY']:
            return bet_line - closing_line
        else:
            return closing_line - bet_line


LINE_TRACKER = LineMovementTracker()
