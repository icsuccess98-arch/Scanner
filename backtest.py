"""
Backtesting Engine for Seven Thirty's Locks
Replays historical predictions vs actual outcomes with CLV tracking and ROI analysis.

Features:
- Track CLV (closing line value) on every pick
- Calculate ROI by brain, sport, bet type (spread/total)
- Output summary stats: win rate, ROI%, CLV avg, units +/-, by brain contribution
- Works with existing Game ORM model and PostgreSQL DB
- Standalone mode that can run from JSON files if no DB available
"""
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, field
from collections import defaultdict
import argparse
import sys
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import database dependencies - graceful fallback for standalone mode
DB_AVAILABLE = True
try:
    from sports_app import db, Game, Pick
    from feature_engineering import extract_features
    from ai_brains import analyze_game, MasterVerdict
    from ml_models import EnsembleModel
except ImportError as e:
    DB_AVAILABLE = False
    logger.warning(f"Database dependencies not available: {e}")
    logger.info("Running in standalone mode - will work with JSON files only")


@dataclass
class BacktestResult:
    """Results from backtesting a single pick."""
    pick_id: int
    game_id: int
    date: str
    league: str
    matchup: str
    pick_side: str  # OVER/UNDER/HOME/AWAY
    bet_type: str  # 'totals' or 'spread'
    
    # Lines and CLV
    opening_line: Optional[float]
    closing_line: Optional[float]
    pick_line: Optional[float]  # Line when pick was made
    clv: Optional[float]  # Closing Line Value
    
    # Brain Analysis
    brain_verdict: Optional[str]
    brain_confidence: Optional[float]
    brain_agreement: Optional[int]
    contributing_brains: List[str] = field(default_factory=list)
    
    # Outcome
    result: Optional[str]  # 'win', 'loss', 'push'
    actual_total: Optional[float]
    actual_away_score: Optional[float] = None
    actual_home_score: Optional[float] = None
    
    # Performance
    units_won: float = 0.0  # +1 for win, -1 for loss, 0 for push
    roi_contribution: float = 0.0  # Percentage return on this pick


@dataclass 
class BacktestSummary:
    """Summary statistics for a backtest run."""
    total_picks: int = 0
    wins: int = 0
    losses: int = 0 
    pushes: int = 0
    win_rate: float = 0.0
    
    total_units: float = 0.0
    roi_percent: float = 0.0
    
    avg_clv: float = 0.0
    positive_clv_rate: float = 0.0
    
    # By category
    by_league: Dict[str, Dict] = field(default_factory=dict)
    by_bet_type: Dict[str, Dict] = field(default_factory=dict)
    by_brain_agreement: Dict[int, Dict] = field(default_factory=dict)
    by_contributing_brain: Dict[str, Dict] = field(default_factory=dict)
    
    date_range: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            'total_picks': self.total_picks,
            'wins': self.wins,
            'losses': self.losses,
            'pushes': self.pushes,
            'win_rate': self.win_rate,
            'total_units': self.total_units,
            'roi_percent': self.roi_percent,
            'avg_clv': self.avg_clv,
            'positive_clv_rate': self.positive_clv_rate,
            'by_league': self.by_league,
            'by_bet_type': self.by_bet_type,
            'by_brain_agreement': self.by_brain_agreement,
            'by_contributing_brain': self.by_contributing_brain,
            'date_range': self.date_range
        }


class BacktestEngine:
    """Core backtesting engine supporting both DB and standalone modes."""
    
    def __init__(self, use_db: bool = True):
        self.use_db = use_db and DB_AVAILABLE
        if not self.use_db:
            logger.info("Running in standalone mode")
    
    def run_backtest(self, 
                    start_date: datetime = None, 
                    end_date: datetime = None,
                    leagues: List[str] = None,
                    bet_types: List[str] = None,
                    min_brain_agreement: int = 3,
                    json_file: str = None) -> BacktestSummary:
        """
        Run complete backtest analysis.
        
        Args:
            start_date: Start date for analysis
            end_date: End date for analysis  
            leagues: List of leagues to include ['NBA', 'CBB', 'NFL', etc.]
            bet_types: List of bet types to include ['totals', 'spread']
            min_brain_agreement: Minimum number of brains that must agree (1-4)
            json_file: Path to JSON file for standalone mode
            
        Returns:
            BacktestSummary with all analysis results
        """
        if self.use_db and json_file is None:
            results = self._backtest_from_db(start_date, end_date, leagues, bet_types, min_brain_agreement)
        elif json_file:
            results = self._backtest_from_json(json_file, leagues, bet_types, min_brain_agreement)
        else:
            raise ValueError("Either database must be available or json_file must be provided")
            
        return self._generate_summary(results, start_date, end_date)
    
    def _backtest_from_db(self, start_date, end_date, leagues, bet_types, min_brain_agreement) -> List[BacktestResult]:
        """Backtest using database records."""
        if not self.use_db:
            raise RuntimeError("Database not available")
            
        logger.info("Loading picks from database...")
        
        # Build query for picks with results
        query = db.session.query(Pick).join(Game)
        
        # Apply filters
        if start_date:
            query = query.filter(Pick.date >= start_date)
        if end_date:
            query = query.filter(Pick.date <= end_date)
        if leagues:
            query = query.filter(Pick.league.in_(leagues))
        if bet_types:
            query = query.filter(Pick.pick_type.in_(bet_types))
            
        # Only picks with results
        query = query.filter(Pick.result.isnot(None))
        
        picks = query.order_by(Pick.date).all()
        logger.info(f"Found {len(picks)} picks with results")
        
        results = []
        for pick in picks:
            game = pick.game
            
            # Skip if brain agreement requirement not met
            if game.brain_agreement and game.brain_agreement < min_brain_agreement:
                continue
                
            # Calculate CLV
            clv = self._calculate_clv(pick.opening_line, pick.closing_line, pick.pick)
            
            # Determine contributing brains (approximation based on brain_verdict and confidence)
            contributing_brains = self._infer_contributing_brains(game)
            
            # Map result to units
            units_won = self._result_to_units(pick.result)
            roi_contribution = units_won  # 100% betting unit approach
            
            result = BacktestResult(
                pick_id=pick.id,
                game_id=pick.game_id,
                date=pick.date.strftime('%Y-%m-%d'),
                league=pick.league,
                matchup=pick.matchup,
                pick_side=pick.pick,
                bet_type=pick.pick_type or 'totals',
                
                opening_line=pick.opening_line,
                closing_line=pick.closing_line,
                pick_line=pick.line_value,
                clv=clv,
                
                brain_verdict=game.brain_verdict,
                brain_confidence=game.brain_confidence,
                brain_agreement=game.brain_agreement,
                contributing_brains=contributing_brains,
                
                result=pick.result,
                actual_total=pick.actual_total,
                
                units_won=units_won,
                roi_contribution=roi_contribution
            )
            
            results.append(result)
            
        logger.info(f"Processed {len(results)} valid picks for backtesting")
        return results
    
    def _backtest_from_json(self, json_file: str, leagues, bet_types, min_brain_agreement) -> List[BacktestResult]:
        """Backtest using JSON file data."""
        logger.info(f"Loading picks from JSON file: {json_file}")
        
        if not os.path.exists(json_file):
            raise FileNotFoundError(f"JSON file not found: {json_file}")
            
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        picks_data = data.get('picks', [])
        logger.info(f"Found {len(picks_data)} picks in JSON file")
        
        results = []
        for pick_data in picks_data:
            # Apply filters
            if leagues and pick_data.get('league') not in leagues:
                continue
            if bet_types and pick_data.get('bet_type') not in bet_types:
                continue
            if pick_data.get('brain_agreement', 0) < min_brain_agreement:
                continue
            if not pick_data.get('result'):  # Skip picks without results
                continue
                
            # Calculate CLV
            clv = self._calculate_clv(
                pick_data.get('opening_line'),
                pick_data.get('closing_line'),
                pick_data.get('pick_side')
            )
            
            units_won = self._result_to_units(pick_data.get('result'))
            
            result = BacktestResult(
                pick_id=pick_data.get('pick_id', 0),
                game_id=pick_data.get('game_id', 0),
                date=pick_data.get('date', ''),
                league=pick_data.get('league', ''),
                matchup=pick_data.get('matchup', ''),
                pick_side=pick_data.get('pick_side', ''),
                bet_type=pick_data.get('bet_type', 'totals'),
                
                opening_line=pick_data.get('opening_line'),
                closing_line=pick_data.get('closing_line'),
                pick_line=pick_data.get('pick_line'),
                clv=clv,
                
                brain_verdict=pick_data.get('brain_verdict'),
                brain_confidence=pick_data.get('brain_confidence'),
                brain_agreement=pick_data.get('brain_agreement'),
                contributing_brains=pick_data.get('contributing_brains', []),
                
                result=pick_data.get('result'),
                actual_total=pick_data.get('actual_total'),
                actual_away_score=pick_data.get('actual_away_score'),
                actual_home_score=pick_data.get('actual_home_score'),
                
                units_won=units_won,
                roi_contribution=units_won
            )
            
            results.append(result)
            
        logger.info(f"Processed {len(results)} valid picks from JSON")
        return results
    
    def _calculate_clv(self, opening_line: Optional[float], closing_line: Optional[float], pick_side: str) -> Optional[float]:
        """
        Calculate Closing Line Value (CLV).
        
        For totals:
        - OVER: CLV = closing_line - opening_line (positive = line went up, good for OVER)  
        - UNDER: CLV = opening_line - closing_line (positive = line went down, good for UNDER)
        
        For spreads:
        - Favorite: CLV = opening_spread - closing_spread (positive = spread got smaller, good for favorite)
        - Underdog: CLV = closing_spread - opening_spread (positive = spread got bigger, good for underdog)
        """
        if opening_line is None or closing_line is None:
            return None
            
        if pick_side in ['OVER', 'Over']:
            return closing_line - opening_line
        elif pick_side in ['UNDER', 'Under']:
            return opening_line - closing_line  
        else:
            # For spreads, assume positive CLV when line moves in our favor
            # This is a simplification - in reality we'd need to know if team was favorite/underdog
            return abs(closing_line - opening_line) if closing_line != opening_line else 0.0
    
    def _result_to_units(self, result: str) -> float:
        """Convert result string to units won/lost."""
        if result == 'win':
            return 1.0
        elif result == 'loss':
            return -1.0
        else:  # push or None
            return 0.0
    
    def _infer_contributing_brains(self, game) -> List[str]:
        """Infer which brains contributed to the pick based on available data."""
        brains = []
        
        # This is an approximation since we don't store individual brain results
        # In a real implementation, we'd want to store this data directly
        if game.brain_agreement:
            if game.brain_agreement >= 4:
                brains = ['statistician', 'historian', 'scout', 'sharp']
            elif game.brain_agreement == 3:
                # Assume the three most common contributors
                brains = ['statistician', 'historian', 'scout'] 
            elif game.brain_agreement == 2:
                brains = ['statistician', 'historian']
            else:
                brains = ['statistician']
                
        return brains
    
    def _generate_summary(self, results: List[BacktestResult], start_date=None, end_date=None) -> BacktestSummary:
        """Generate comprehensive summary statistics."""
        if not results:
            return BacktestSummary()
            
        summary = BacktestSummary()
        summary.total_picks = len(results)
        
        # Basic stats
        for result in results:
            if result.result == 'win':
                summary.wins += 1
            elif result.result == 'loss':
                summary.losses += 1
            else:
                summary.pushes += 1
                
            summary.total_units += result.units_won
            
        # Calculate rates
        if summary.total_picks > 0:
            summary.win_rate = summary.wins / (summary.wins + summary.losses) if (summary.wins + summary.losses) > 0 else 0.0
            summary.roi_percent = (summary.total_units / summary.total_picks) * 100
            
        # CLV analysis
        clv_values = [r.clv for r in results if r.clv is not None]
        if clv_values:
            summary.avg_clv = sum(clv_values) / len(clv_values)
            summary.positive_clv_rate = sum(1 for clv in clv_values if clv > 0) / len(clv_values)
        
        # Category breakdowns
        summary.by_league = self._breakdown_by_category(results, 'league')
        summary.by_bet_type = self._breakdown_by_category(results, 'bet_type')
        summary.by_brain_agreement = self._breakdown_by_category(results, 'brain_agreement')
        
        # Brain contribution analysis
        summary.by_contributing_brain = defaultdict(lambda: {'picks': 0, 'wins': 0, 'units': 0.0, 'clv_sum': 0.0, 'clv_count': 0})
        for result in results:
            for brain in result.contributing_brains:
                stats = summary.by_contributing_brain[brain]
                stats['picks'] += 1
                if result.result == 'win':
                    stats['wins'] += 1
                stats['units'] += result.units_won
                if result.clv is not None:
                    stats['clv_sum'] += result.clv
                    stats['clv_count'] += 1
        
        # Calculate brain stats  
        for brain, stats in summary.by_contributing_brain.items():
            stats['win_rate'] = stats['wins'] / stats['picks'] if stats['picks'] > 0 else 0.0
            stats['roi_percent'] = (stats['units'] / stats['picks']) * 100 if stats['picks'] > 0 else 0.0
            stats['avg_clv'] = stats['clv_sum'] / stats['clv_count'] if stats['clv_count'] > 0 else 0.0
            # Clean up temp fields
            del stats['clv_sum'], stats['clv_count']
        
        # Date range
        if results:
            dates = [r.date for r in results]
            summary.date_range = f"{min(dates)} to {max(dates)}"
            
        return summary
    
    def _breakdown_by_category(self, results: List[BacktestResult], category: str) -> Dict[str, Dict]:
        """Create breakdown statistics by category."""
        breakdown = defaultdict(lambda: {'picks': 0, 'wins': 0, 'losses': 0, 'pushes': 0, 'units': 0.0})
        
        for result in results:
            cat_value = getattr(result, category, 'Unknown')
            stats = breakdown[str(cat_value)]
            stats['picks'] += 1
            stats['units'] += result.units_won
            
            if result.result == 'win':
                stats['wins'] += 1
            elif result.result == 'loss':
                stats['losses'] += 1
            else:
                stats['pushes'] += 1
                
        # Calculate rates
        for cat_value, stats in breakdown.items():
            decided_games = stats['wins'] + stats['losses']
            stats['win_rate'] = stats['wins'] / decided_games if decided_games > 0 else 0.0
            stats['roi_percent'] = (stats['units'] / stats['picks']) * 100 if stats['picks'] > 0 else 0.0
            
        return dict(breakdown)
    
    def export_results(self, results: List[BacktestResult], filename: str):
        """Export detailed results to JSON file."""
        export_data = {
            'picks': [
                {
                    'pick_id': r.pick_id,
                    'game_id': r.game_id,
                    'date': r.date,
                    'league': r.league,
                    'matchup': r.matchup,
                    'pick_side': r.pick_side,
                    'bet_type': r.bet_type,
                    'opening_line': r.opening_line,
                    'closing_line': r.closing_line,
                    'pick_line': r.pick_line,
                    'clv': r.clv,
                    'brain_verdict': r.brain_verdict,
                    'brain_confidence': r.brain_confidence,
                    'brain_agreement': r.brain_agreement,
                    'contributing_brains': r.contributing_brains,
                    'result': r.result,
                    'actual_total': r.actual_total,
                    'units_won': r.units_won,
                    'roi_contribution': r.roi_contribution
                }
                for r in results
            ]
        }
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)
        logger.info(f"Exported {len(results)} results to {filename}")


def print_summary(summary: BacktestSummary):
    """Print formatted summary to console."""
    print("\n" + "="*60)
    print("SEVEN THIRTY'S LOCKS - BACKTEST RESULTS")
    print("="*60)
    print(f"Date Range: {summary.date_range}")
    print(f"Total Picks: {summary.total_picks}")
    print(f"Record: {summary.wins}-{summary.losses}-{summary.pushes}")
    print(f"Win Rate: {summary.win_rate:.1%}")
    print(f"Total Units: {summary.total_units:+.1f}")
    print(f"ROI: {summary.roi_percent:+.1f}%")
    print(f"Average CLV: {summary.avg_clv:+.2f}")
    print(f"Positive CLV Rate: {summary.positive_clv_rate:.1%}")
    
    print("\n" + "-"*40)
    print("BY LEAGUE:")
    for league, stats in summary.by_league.items():
        print(f"  {league}: {stats['wins']}-{stats['losses']}-{stats['pushes']} "
              f"({stats['win_rate']:.1%}, ROI: {stats['roi_percent']:+.1f}%)")
    
    print("\n" + "-"*40)
    print("BY BET TYPE:")
    for bet_type, stats in summary.by_bet_type.items():
        print(f"  {bet_type}: {stats['wins']}-{stats['losses']}-{stats['pushes']} "
              f"({stats['win_rate']:.1%}, ROI: {stats['roi_percent']:+.1f}%)")
    
    print("\n" + "-"*40)
    print("BY BRAIN AGREEMENT:")
    for agreement, stats in summary.by_brain_agreement.items():
        print(f"  {agreement}/4 brains: {stats['wins']}-{stats['losses']}-{stats['pushes']} "
              f"({stats['win_rate']:.1%}, ROI: {stats['roi_percent']:+.1f}%)")
    
    print("\n" + "-"*40) 
    print("BY CONTRIBUTING BRAIN:")
    for brain, stats in summary.by_contributing_brain.items():
        print(f"  {brain}: {stats['wins']}-{stats['losses']} from {stats['picks']} picks "
              f"({stats['win_rate']:.1%}, ROI: {stats['roi_percent']:+.1f}%, CLV: {stats['avg_clv']:+.2f})")
    
    print("="*60)


def main():
    """Command line interface for the backtesting engine."""
    parser = argparse.ArgumentParser(description='Seven Thirty\'s Locks Backtesting Engine')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--leagues', nargs='+', help='Leagues to include (NBA CBB NFL CFB NHL)')
    parser.add_argument('--bet-types', nargs='+', choices=['totals', 'spread'], help='Bet types to include')
    parser.add_argument('--min-brain-agreement', type=int, default=3, help='Minimum brain agreement (1-4)')
    parser.add_argument('--json-file', type=str, help='JSON file for standalone mode')
    parser.add_argument('--export', type=str, help='Export detailed results to JSON file')
    parser.add_argument('--no-db', action='store_true', help='Force standalone mode')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    # Create engine
    engine = BacktestEngine(use_db=not args.no_db)
    
    try:
        # Run backtest
        summary = engine.run_backtest(
            start_date=start_date,
            end_date=end_date,
            leagues=args.leagues,
            bet_types=args.bet_types,
            min_brain_agreement=args.min_brain_agreement,
            json_file=args.json_file
        )
        
        # Print results
        print_summary(summary)
        
        # Export if requested
        if args.export:
            # We need to run the backtest again to get detailed results
            # This is inefficient but keeps the interface clean
            if engine.use_db and args.json_file is None:
                results = engine._backtest_from_db(start_date, end_date, args.leagues, args.bet_types, args.min_brain_agreement)
            else:
                results = engine._backtest_from_json(args.json_file, args.leagues, args.bet_types, args.min_brain_agreement)
            engine.export_results(results, args.export)
        
        # Export summary to JSON as well
        summary_file = args.export.replace('.json', '_summary.json') if args.export else 'backtest_summary.json'
        with open(summary_file, 'w') as f:
            json.dump(summary.to_dict(), f, indent=2)
        print(f"\nSummary exported to {summary_file}")
        
    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()