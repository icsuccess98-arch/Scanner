"""
Real-Time Line Movement Service - WebSocket and SSE support.
Provides live line movement alerts and heat map data.
"""
import logging
import json
import threading
from datetime import datetime
from typing import Optional, Dict, List, Callable
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class AlertType(Enum):
    STEAM_MOVE = "steam_move"         # >1.5 points in <15 min
    SHARP_ACTION = "sharp_action"     # RLM detected
    LINE_FREEZE = "line_freeze"       # Line stuck despite action
    CLOSING_SOON = "closing_soon"     # Game starting in <30 min


@dataclass
class LineSnapshot:
    """Point-in-time line capture for tracking."""
    game_id: str
    league: str
    line: float
    timestamp: datetime
    source: str = 'bovada'


@dataclass
class LineMovementAlert:
    """Real-time alert for significant line movement."""
    game_id: str
    matchup: str
    league: str
    alert_type: AlertType
    message: str
    old_line: float
    new_line: float
    movement: float
    direction: str  # 'up', 'down'
    is_favorable: bool  # True if move benefits our pick
    timestamp: datetime
    priority: int  # 1=high, 2=medium, 3=low
    
    def to_dict(self) -> dict:
        return {
            'game_id': self.game_id,
            'matchup': self.matchup,
            'league': self.league,
            'alert_type': self.alert_type.value,
            'message': self.message,
            'old_line': self.old_line,
            'new_line': self.new_line,
            'movement': self.movement,
            'direction': self.direction,
            'is_favorable': self.is_favorable,
            'timestamp': self.timestamp.isoformat(),
            'priority': self.priority
        }


class LineMovementService:
    """
    Real-time line movement tracking with alerts.
    
    Features:
    - Store line snapshots every update
    - Detect steam moves (rapid large movement)
    - Track movement direction vs pick direction
    - Generate alerts for significant moves
    - Provide heat map data for dashboard
    """
    
    STEAM_THRESHOLD = 1.5  # Points moved to trigger steam alert
    STEAM_WINDOW = 900     # 15 minutes in seconds
    
    def __init__(self):
        self._snapshots: Dict[str, List[LineSnapshot]] = {}
        self._alerts: List[LineMovementAlert] = []
        self._subscribers: List[Callable] = []
        self._lock = threading.Lock()
        self._picks: Dict[str, dict] = {}  # game_id -> {direction, line}
    
    def record_snapshot(self, game_id: str, league: str, line: float, source: str = 'bovada') -> None:
        """Record a line snapshot."""
        with self._lock:
            if game_id not in self._snapshots:
                self._snapshots[game_id] = []
            
            snapshot = LineSnapshot(
                game_id=game_id,
                league=league,
                line=line,
                timestamp=datetime.now(),
                source=source
            )
            
            self._snapshots[game_id].append(snapshot)
            
            if len(self._snapshots[game_id]) > 100:
                self._snapshots[game_id] = self._snapshots[game_id][-50:]
            
            self._check_for_steam_move(game_id, league)
    
    def register_pick(self, game_id: str, direction: str, line: float, matchup: str) -> None:
        """Register a qualified pick for movement tracking."""
        with self._lock:
            self._picks[game_id] = {
                'direction': direction,
                'line': line,
                'matchup': matchup
            }
    
    def _check_for_steam_move(self, game_id: str, league: str) -> None:
        """Check if recent movement qualifies as a steam move."""
        snapshots = self._snapshots.get(game_id, [])
        if len(snapshots) < 2:
            return
        
        current = snapshots[-1]
        now = datetime.now()
        
        for old in reversed(snapshots[:-1]):
            age = (now - old.timestamp).total_seconds()
            if age > self.STEAM_WINDOW:
                break
            
            movement = abs(current.line - old.line)
            if movement >= self.STEAM_THRESHOLD:
                self._create_steam_alert(game_id, league, old, current)
                break
    
    def _create_steam_alert(
        self, 
        game_id: str, 
        league: str,
        old_snapshot: LineSnapshot,
        new_snapshot: LineSnapshot
    ) -> None:
        """Create a steam move alert."""
        movement = new_snapshot.line - old_snapshot.line
        direction = 'up' if movement > 0 else 'down'
        
        pick_info = self._picks.get(game_id, {})
        matchup = pick_info.get('matchup', f'Game {game_id}')
        pick_direction = pick_info.get('direction', '')
        
        is_favorable = (
            (pick_direction == 'O' and direction == 'up') or
            (pick_direction == 'U' and direction == 'down')
        )
        
        alert = LineMovementAlert(
            game_id=game_id,
            matchup=matchup,
            league=league,
            alert_type=AlertType.STEAM_MOVE,
            message=f"Steam move: {old_snapshot.line} → {new_snapshot.line} ({movement:+.1f})",
            old_line=old_snapshot.line,
            new_line=new_snapshot.line,
            movement=abs(movement),
            direction=direction,
            is_favorable=is_favorable,
            timestamp=datetime.now(),
            priority=1 if is_favorable else 2
        )
        
        self._alerts.append(alert)
        
        if len(self._alerts) > 50:
            self._alerts = self._alerts[-25:]
        
        self._notify_subscribers(alert)
        
        logger.info(f"STEAM ALERT: {matchup} moved {movement:+.1f} points")
    
    def subscribe(self, callback: Callable[[LineMovementAlert], None]) -> None:
        """Subscribe to line movement alerts."""
        with self._lock:
            self._subscribers.append(callback)
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from alerts."""
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)
    
    def _notify_subscribers(self, alert: LineMovementAlert) -> None:
        """Notify all subscribers of new alert."""
        for callback in self._subscribers:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert subscriber error: {e}")
    
    def get_heat_map_data(self) -> List[dict]:
        """
        Get heat map data for all tracked games.
        
        Returns list of:
        {
            game_id, matchup, league, 
            current_line, opening_line, movement,
            direction, color (green/red/gray),
            last_updated
        }
        """
        result = []
        
        with self._lock:
            for game_id, snapshots in self._snapshots.items():
                if not snapshots:
                    continue
                
                pick_info = self._picks.get(game_id, {})
                if not pick_info:
                    continue
                
                opening = snapshots[0]
                current = snapshots[-1]
                movement = current.line - opening.line
                
                pick_direction = pick_info.get('direction', '')
                is_favorable = (
                    (pick_direction == 'O' and movement > 0) or
                    (pick_direction == 'U' and movement < 0)
                )
                
                if abs(movement) < 0.5:
                    color = '#94a3b8'  # gray
                elif is_favorable:
                    color = '#22c55e'  # green
                else:
                    color = '#ef4444'  # red
                
                result.append({
                    'game_id': game_id,
                    'matchup': pick_info.get('matchup', ''),
                    'league': opening.league,
                    'current_line': current.line,
                    'opening_line': opening.line,
                    'movement': round(movement, 1),
                    'direction': 'up' if movement > 0 else 'down' if movement < 0 else 'flat',
                    'color': color,
                    'is_favorable': is_favorable,
                    'pick_direction': pick_direction,
                    'last_updated': current.timestamp.isoformat()
                })
        
        result.sort(key=lambda x: abs(x['movement']), reverse=True)
        return result
    
    def get_recent_alerts(self, limit: int = 10) -> List[dict]:
        """Get recent alerts."""
        with self._lock:
            return [a.to_dict() for a in self._alerts[-limit:]]
    
    def get_movement_history(self, game_id: str) -> List[dict]:
        """Get line movement history for a specific game."""
        with self._lock:
            snapshots = self._snapshots.get(game_id, [])
            return [
                {
                    'line': s.line,
                    'timestamp': s.timestamp.isoformat(),
                    'source': s.source
                }
                for s in snapshots
            ]


_line_service: Optional[LineMovementService] = None


def get_line_movement_service() -> LineMovementService:
    """Get the global line movement service instance."""
    global _line_service
    if _line_service is None:
        _line_service = LineMovementService()
    return _line_service
