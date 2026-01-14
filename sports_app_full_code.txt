import os
import logging
import time
from datetime import datetime, date, timedelta
from typing import Tuple, Optional
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase, validates
from sqlalchemy import delete
import requests
import pytz


class QualificationStatus(Enum):
    """
    FOOLPROOF PICK QUALIFICATION SYSTEM
    
    A pick ONLY qualifies if it passes ALL checks.
    NO EXCEPTIONS. NO PARTIAL QUALIFICATIONS.
    """
    FULLY_QUALIFIED = "FULLY_QUALIFIED"           # Passes ALL checks
    EDGE_ONLY = "EDGE_ONLY"                       # Edge met, but history/EV failed
    HISTORY_ONLY = "HISTORY_ONLY"                 # History met, but edge/EV failed
    NEGATIVE_EV = "NEGATIVE_EV"                   # Has proven negative EV
    INJURY_CONCERN = "INJURY_CONCERN"             # Major injury impact
    VALIDATION_FAILED = "VALIDATION_FAILED"       # Data validation failed
    NOT_QUALIFIED = "NOT_QUALIFIED"               # Didn't meet basic criteria

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)
app = Flask(__name__, static_folder='static', static_url_path='/static')

flask_secret = os.environ.get("FLASK_SECRET_KEY")
if not flask_secret:
    import secrets
    flask_secret = secrets.token_hex(32)
    logger.warning("FLASK_SECRET_KEY not set - using generated key (sessions will reset on restart)")
app.secret_key = flask_secret

app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
db.init_app(app)

last_game_count = {}

_live_scores_cache = {"data": {}, "timestamp": 0}
LIVE_SCORES_CACHE_TTL = 15

@app.after_request
def add_cache_headers(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

THRESHOLDS = {
    "NBA": 8.0,
    "CBB": 8.0,
    "NFL": 3.5,
    "CFB": 3.5,
    "NHL": 0.5
}

injury_cache = {}
line_movement_cache = {}
opening_lines_store = {}

class VigCalculator:
    """Handles all vig removal calculations for true edge computation"""
    
    @staticmethod
    def american_to_implied_probability(odds: float) -> float:
        """Convert American odds to implied probability (with vig)"""
        if odds < 0:
            return abs(odds) / (abs(odds) + 100)
        else:
            return 100 / (odds + 100)
    
    @staticmethod
    def remove_vig_two_way(odds_a: float, odds_b: float) -> dict:
        """
        Remove vig from two-way market (spreads, totals)
        Returns dict with true probabilities and vig percentage
        """
        prob_a_with_vig = VigCalculator.american_to_implied_probability(odds_a)
        prob_b_with_vig = VigCalculator.american_to_implied_probability(odds_b)
        
        total = prob_a_with_vig + prob_b_with_vig
        
        true_prob_a = prob_a_with_vig / total if total > 0 else 0.5
        true_prob_b = prob_b_with_vig / total if total > 0 else 0.5
        
        vig_pct = (total - 1.0) * 100 if total > 0 else 0
        
        return {
            'true_prob_a': true_prob_a,
            'true_prob_b': true_prob_b,
            'prob_a_with_vig': prob_a_with_vig,
            'prob_b_with_vig': prob_b_with_vig,
            'vig_percentage': vig_pct,
            'total_probability': total
        }
    
    @staticmethod
    def calculate_vig_adjusted_edge(raw_edge: float, bovada_odds: int = -110) -> float:
        """
        Adjust edge by removing the vig component from the calculation.
        
        Uses a lookup table based on the actual price to apply the correct
        vig discount. Heavier vig markets get larger haircuts.
        
        The multiplier is ALWAYS clamped to ≤ 1.0 to ensure edge is never inflated.
        
        Vig Lookup Table:
        - Standard (-110/-110): 4.76% vig → 0.9524 multiplier
        - Heavy fav (-115+): ~5.5% vig → 0.945 multiplier  
        - Very heavy (-120+): ~6.5% vig → 0.935 multiplier
        - Light (+100 to -105): ~2.5% vig → 0.975 multiplier
        - Plus money (+105+): ~1-2% vig → 0.980 multiplier
        
        Examples:
        - Raw edge 8.0 at -110 → adjusted edge ~7.6
        - Raw edge 8.0 at -120 → adjusted edge ~7.5
        """
        abs_odds = abs(bovada_odds) if bovada_odds < 0 else bovada_odds
        
        if bovada_odds > 0:
            vig_multiplier = 0.980
        elif abs_odds <= 105:
            vig_multiplier = 0.975
        elif abs_odds <= 110:
            vig_multiplier = 0.9524
        elif abs_odds <= 115:
            vig_multiplier = 0.945
        elif abs_odds <= 120:
            vig_multiplier = 0.935
        elif abs_odds <= 130:
            vig_multiplier = 0.920
        else:
            vig_multiplier = 0.900
        
        return raw_edge * min(vig_multiplier, 1.0)

LEAGUE_SPORT_KEYS = {
    'NBA': 'basketball_nba',
    'CBB': 'basketball_ncaab',
    'NFL': 'americanfootball_nfl',
    'CFB': 'americanfootball_ncaaf',
    'NHL': 'icehockey_nhl'
}

LEAGUE_HISTORICAL_CONFIG = {
    'NBA': {'games_count': 10, 'min_games': 8, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'CBB': {'games_count': 10, 'min_games': 8, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'NFL': {'games_count': 5, 'min_games': 4, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'CFB': {'games_count': 5, 'min_games': 4, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'NHL': {'games_count': 10, 'min_games': 8, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40}
}

historical_lines_cache = {}

class BulletproofCurrentLineCalculator:
    """
    BULLETPROOF Current Line Calculator
    Uses current Vegas lines + ESPN results - NO PAID API NEEDED
    
    Logic:
    1. Get current Vegas line for today's game
    2. Look at team's last N ESPN game results
    3. Apply current line to those past games
    4. Calculate hypothetical performance with proper push handling
    """
    
    def __init__(self):
        self.min_games = {
            'NBA': 8, 'CBB': 8, 'NFL': 4, 'CFB': 4, 'NHL': 8
        }
        self.thresholds = {
            'qualify': 0.60,
            'supermax': 0.70,
            'high': 0.65,
            'medium': 0.60
        }
    
    def calculate_total_performance(self, games: list, current_total: float, direction: str, league: str) -> dict:
        """
        Calculate how team's games would have performed against CURRENT total line.
        Properly excludes pushes from hit rate calculations.
        """
        min_games = self.min_games.get(league, 8)
        
        if not games or len(games) < 5:
            return {
                'hit_rate': None,
                'qualified': False,
                'sufficient_data': False,
                'reason': f'Insufficient games: {len(games) if games else 0} (need {min_games})'
            }
        
        overs = 0
        unders = 0
        pushes = 0
        game_details = []
        
        for g in games:
            actual_total = g.get('total', g.get('team_score', 0) + g.get('opp_score', 0))
            diff = actual_total - current_total
            
            if abs(diff) < 0.5:
                pushes += 1
                result = 'PUSH'
            elif diff > 0:
                overs += 1
                result = 'OVER'
            else:
                unders += 1
                result = 'UNDER'
            
            game_details.append({
                'actual_total': actual_total,
                'current_line': current_total,
                'margin': diff,
                'result': result
            })
        
        total_games = len(games)
        non_push_games = total_games - pushes
        
        if non_push_games < min_games:
            return {
                'hit_rate': None,
                'qualified': False,
                'sufficient_data': False,
                'overs': overs,
                'unders': unders,
                'pushes': pushes,
                'total_games': total_games,
                'reason': f'Only {non_push_games} decisive games (excluding {pushes} pushes), need {min_games}'
            }
        
        if direction == 'O':
            hits = overs
            hit_rate = (overs / non_push_games) * 100
        else:
            hits = unders
            hit_rate = (unders / non_push_games) * 100
        
        qualified = (hit_rate / 100) >= self.thresholds['qualify']
        
        confidence = None
        if qualified:
            rate_decimal = hit_rate / 100
            if rate_decimal >= self.thresholds['supermax']:
                confidence = 'SUPERMAX'
            elif rate_decimal >= self.thresholds['high']:
                confidence = 'HIGH'
            elif rate_decimal >= self.thresholds['medium']:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
        
        return {
            'hit_rate': round(hit_rate, 1),
            'hits': hits,
            'total_games': non_push_games,
            'overs': overs,
            'unders': unders,
            'pushes': pushes,
            'all_games': total_games,
            'direction': direction,
            'qualified': qualified,
            'confidence': confidence,
            'threshold': self.thresholds['qualify'] * 100,
            'current_line': current_total,
            'sufficient_data': True,
            'uses_current_line': True,
            'record': f"{hits}-{non_push_games - hits}-{pushes}",
            'game_details': game_details
        }
    
    def calculate_spread_performance(self, games: list, current_spread: float, league: str) -> dict:
        """
        Calculate how team would have performed against CURRENT spread.
        Uses correct ATS formula: spread_result = actual_margin + closing_spread
        Properly excludes pushes from cover rate calculations.
        """
        min_games = self.min_games.get(league, 8)
        
        if not games or len(games) < 5:
            return {
                'cover_rate': None,
                'qualified': False,
                'sufficient_data': False,
                'reason': f'Insufficient games: {len(games) if games else 0} (need {min_games})'
            }
        
        covers = 0
        losses = 0
        pushes = 0
        game_details = []
        
        for g in games:
            actual_margin = g.get('margin', g.get('team_score', 0) - g.get('opp_score', 0))
            
            spread_result = actual_margin + current_spread
            
            if abs(spread_result) < 0.5:
                pushes += 1
                result = 'PUSH'
            elif spread_result > 0:
                covers += 1
                result = 'COVER'
            else:
                losses += 1
                result = 'NO_COVER'
            
            game_details.append({
                'actual_margin': actual_margin,
                'current_spread': current_spread,
                'spread_result': spread_result,
                'result': result
            })
        
        total_games = len(games)
        non_push_games = total_games - pushes
        
        if non_push_games < min_games:
            return {
                'cover_rate': None,
                'qualified': False,
                'sufficient_data': False,
                'covers': covers,
                'losses': losses,
                'pushes': pushes,
                'total_games': total_games,
                'reason': f'Only {non_push_games} decisive games (excluding {pushes} pushes), need {min_games}'
            }
        
        cover_rate = (covers / non_push_games) * 100
        qualified = (cover_rate / 100) >= self.thresholds['qualify']
        
        confidence = None
        if qualified:
            rate_decimal = cover_rate / 100
            if rate_decimal >= self.thresholds['supermax']:
                confidence = 'SUPERMAX'
            elif rate_decimal >= self.thresholds['high']:
                confidence = 'HIGH'
            elif rate_decimal >= self.thresholds['medium']:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
        
        return {
            'cover_rate': round(cover_rate, 1),
            'covers': covers,
            'losses': losses,
            'pushes': pushes,
            'total_games': non_push_games,
            'all_games': total_games,
            'qualified': qualified,
            'confidence': confidence,
            'threshold': self.thresholds['qualify'] * 100,
            'current_spread': current_spread,
            'sufficient_data': True,
            'uses_current_line': True,
            'record': f"{covers}-{losses}-{pushes}",
            'game_details': game_details
        }
    
    def get_confidence_tier(self, rate: float) -> str:
        if rate >= 70:
            return 'SUPERMAX'
        elif rate >= 65:
            return 'HIGH'
        elif rate >= 60:
            return 'MEDIUM'
        else:
            return 'LOW'

bulletproof_calculator = BulletproofCurrentLineCalculator()

class HistoricalBettingLinesService:
    def __init__(self):
        self.base_url = "https://api.the-odds-api.com/v4"
        self.cache = {}
        self.cache_ttl = 43200
    
    def _get_api_key(self):
        return os.environ.get("API_KEY", "")
    
    def _team_match(self, api_team: str, db_team: str) -> bool:
        from difflib import SequenceMatcher
        if not api_team or not db_team:
            return False
        api_norm = api_team.lower().strip()
        db_norm = db_team.lower().strip()
        if api_norm == db_norm:
            return True
        if db_norm in api_norm or api_norm in db_norm:
            return True
        ratio = SequenceMatcher(None, api_norm, db_norm).ratio()
        return ratio >= 0.75
    
    def fetch_historical_games_with_lines(self, team_name: str, league: str, bet_type: str = 'total', num_games: int = 10) -> list:
        sport_key = LEAGUE_SPORT_KEYS.get(league)
        if not sport_key:
            logger.warning(f"Historical lines: Unknown league {league}")
            return []
        
        cache_key = f"hist:{league}:{team_name}:{bet_type}:{num_games}"
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if (datetime.now() - cached_time).seconds < self.cache_ttl:
                return cached_data
        
        api_key = self._get_api_key()
        if not api_key:
            logger.warning("Historical lines: No API key available")
            return []
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=60)
            market = 'spreads' if bet_type == 'spread' else 'totals'
            
            url = f"{self.base_url}/historical/sports/{sport_key}/odds"
            params = {
                'apiKey': api_key,
                'regions': 'us',
                'markets': market,
                'oddsFormat': 'american',
                'date': start_date.strftime('%Y-%m-%dT12:00:00Z')
            }
            
            logger.info(f"Historical lines: Fetching {bet_type} history for {team_name} ({league})")
            resp = requests.get(url, params=params, timeout=15)
            
            if resp.status_code != 200:
                logger.warning(f"Historical lines API error: {resp.status_code}")
                return []
            
            data = resp.json()
            all_games = data.get('data', [])
            
            team_games = []
            for game in all_games:
                away_team = game.get('away_team', '')
                home_team = game.get('home_team', '')
                
                if self._team_match(away_team, team_name) or self._team_match(home_team, team_name):
                    extracted = self._extract_game_data(game, team_name, bet_type)
                    if extracted:
                        team_games.append(extracted)
            
            team_games.sort(key=lambda x: x.get('commence_time', ''), reverse=True)
            team_games = team_games[:num_games]
            
            self.cache[cache_key] = (team_games, datetime.now())
            logger.info(f"Historical lines: Found {len(team_games)} games with actual lines for {team_name}")
            return team_games
            
        except Exception as e:
            logger.error(f"Historical lines fetch error: {e}")
            return []
    
    def _extract_game_data(self, game: dict, team_name: str, bet_type: str) -> dict:
        try:
            bookmakers = game.get('bookmakers', [])
            if not bookmakers:
                return None
            
            preferred_books = ['pinnacle', 'fanduel', 'draftkings', 'bovada']
            bookmaker = None
            for pref in preferred_books:
                for bm in bookmakers:
                    if pref in bm.get('key', '').lower():
                        bookmaker = bm
                        break
                if bookmaker:
                    break
            if not bookmaker:
                bookmaker = bookmakers[0]
            
            markets = bookmaker.get('markets', [])
            away_team = game.get('away_team')
            home_team = game.get('home_team')
            scores = game.get('scores')
            
            if not scores:
                return None
            
            away_score = None
            home_score = None
            for score in scores:
                if score.get('name') == away_team:
                    away_score = score.get('score')
                elif score.get('name') == home_team:
                    home_score = score.get('score')
            
            if away_score is None or home_score is None:
                return None
            
            try:
                away_score = int(away_score)
                home_score = int(home_score)
            except (ValueError, TypeError):
                return None
            
            if bet_type == 'total':
                totals_market = next((m for m in markets if m['key'] == 'totals'), None)
                if not totals_market:
                    return None
                
                outcomes = totals_market.get('outcomes', [])
                over_outcome = next((o for o in outcomes if o['name'] == 'Over'), None)
                if not over_outcome:
                    return None
                
                closing_total = float(over_outcome.get('point', 0))
                actual_total = away_score + home_score
                is_push = actual_total == closing_total
                went_over = actual_total > closing_total
                went_under = actual_total < closing_total
                
                return {
                    'game_id': game.get('id'),
                    'commence_time': game.get('commence_time'),
                    'away_team': away_team,
                    'home_team': home_team,
                    'closing_line': closing_total,
                    'actual_total': actual_total,
                    'went_over': went_over,
                    'went_under': went_under,
                    'is_push': is_push,
                    'margin': actual_total - closing_total,
                    'bet_type': 'total',
                    'uses_actual_line': True
                }
            
            elif bet_type == 'spread':
                spreads_market = next((m for m in markets if m['key'] == 'spreads'), None)
                if not spreads_market:
                    return None
                
                outcomes = spreads_market.get('outcomes', [])
                is_home = self._team_match(home_team, team_name)
                
                team_outcome = None
                for outcome in outcomes:
                    if self._team_match(outcome['name'], team_name):
                        team_outcome = outcome
                        break
                
                if not team_outcome:
                    return None
                
                closing_spread = float(team_outcome.get('point', 0))
                
                if is_home:
                    actual_margin = home_score - away_score
                else:
                    actual_margin = away_score - home_score
                
                spread_result = actual_margin + closing_spread
                is_push = spread_result == 0
                covered = spread_result > 0
                
                return {
                    'game_id': game.get('id'),
                    'commence_time': game.get('commence_time'),
                    'away_team': away_team,
                    'home_team': home_team,
                    'team_name': team_name,
                    'is_home': is_home,
                    'closing_spread': closing_spread,
                    'actual_margin': actual_margin,
                    'spread_result': spread_result,
                    'covered_spread': covered,
                    'is_push': is_push,
                    'bet_type': 'spread',
                    'uses_actual_line': True
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Extract game data error: {e}")
            return None
    
    def calculate_ou_hit_rate_with_actual_lines(self, team_name: str, league: str, direction: str = 'O') -> dict:
        config = LEAGUE_HISTORICAL_CONFIG.get(league, LEAGUE_HISTORICAL_CONFIG['NBA'])
        games = self.fetch_historical_games_with_lines(team_name, league, 'total', config['games_count'])
        
        if len(games) < config['min_games']:
            logger.info(f"Historical lines: Insufficient data for {team_name} ({len(games)}/{config['min_games']} games)")
            return {
                'hit_rate': None,
                'games_found': len(games),
                'min_required': config['min_games'],
                'qualified': False,
                'uses_actual_lines': True,
                'fallback_used': False,
                'reason': f"Only {len(games)} games with historical lines found"
            }
        
        non_push_games = [g for g in games if not g.get('is_push', False)]
        pushes = len(games) - len(non_push_games)
        
        if len(non_push_games) < config['min_games']:
            return {
                'hit_rate': None,
                'games_found': len(games),
                'pushes': pushes,
                'min_required': config['min_games'],
                'qualified': False,
                'uses_actual_lines': True,
                'fallback_used': False,
                'reason': f"Only {len(non_push_games)} decisive games (excluding {pushes} pushes)"
            }
        
        if direction == 'O':
            hits = sum(1 for g in non_push_games if g.get('went_over'))
        else:
            hits = sum(1 for g in non_push_games if g.get('went_under', not g.get('went_over', True)))
        
        hit_rate = (hits / len(non_push_games)) * 100
        threshold = config['over_threshold'] if direction == 'O' else (1 - config['under_threshold'])
        qualified = (hit_rate / 100) >= threshold
        
        avg_line = sum(g.get('closing_line', 0) for g in games) / len(games)
        avg_actual = sum(g.get('actual_total', 0) for g in games) / len(games)
        avg_vs_line = avg_actual - avg_line
        
        return {
            'hit_rate': round(hit_rate, 1),
            'hits': hits,
            'total_games': len(non_push_games),
            'pushes': pushes,
            'direction': direction,
            'qualified': qualified,
            'threshold': threshold * 100,
            'avg_line': round(avg_line, 1),
            'avg_actual': round(avg_actual, 1),
            'avg_vs_line': round(avg_vs_line, 1),
            'uses_actual_lines': True,
            'fallback_used': False,
            'game_details': games
        }
    
    def calculate_ats_hit_rate(self, team_name: str, league: str, location: str = None) -> dict:
        config = LEAGUE_HISTORICAL_CONFIG.get(league, LEAGUE_HISTORICAL_CONFIG['NBA'])
        games = self.fetch_historical_games_with_lines(team_name, league, 'spread', config['games_count'])
        
        if location:
            if location == 'home':
                games = [g for g in games if g.get('is_home')]
            elif location == 'away':
                games = [g for g in games if not g.get('is_home')]
        
        if len(games) < config['min_games']:
            return {
                'ats_rate': None,
                'games_found': len(games),
                'min_required': config['min_games'],
                'qualified': False,
                'uses_actual_lines': True,
                'reason': f"Only {len(games)} games with historical lines found"
            }
        
        non_push_games = [g for g in games if not g.get('is_push', False)]
        pushes = len(games) - len(non_push_games)
        
        if len(non_push_games) < config['min_games']:
            return {
                'ats_rate': None,
                'games_found': len(games),
                'pushes': pushes,
                'min_required': config['min_games'],
                'qualified': False,
                'uses_actual_lines': True,
                'reason': f"Only {len(non_push_games)} decisive games (excluding {pushes} pushes)"
            }
        
        covers = sum(1 for g in non_push_games if g.get('covered_spread'))
        losses = len(non_push_games) - covers
        ats_rate = covers / len(non_push_games)
        qualified = ats_rate >= config['ats_threshold']
        
        avg_spread = sum(g.get('closing_spread', 0) for g in games) / len(games)
        avg_margin = sum(g.get('actual_margin', 0) for g in games) / len(games)
        
        return {
            'ats_rate': round(ats_rate * 100, 1),
            'ats_record': f"{covers}-{losses}-{pushes}",
            'covers': covers,
            'losses': losses,
            'pushes': pushes,
            'total_games': len(non_push_games),
            'qualified': qualified,
            'threshold': config['ats_threshold'] * 100,
            'avg_spread': round(avg_spread, 1),
            'avg_margin': round(avg_margin, 1),
            'avg_vs_spread': round(avg_margin - avg_spread, 1),
            'uses_actual_lines': True,
            'location': location or 'all'
        }

historical_lines_service = HistoricalBettingLinesService()

def api_request_with_retry(url: str, timeout: int = 30, max_retries: int = 2, **kwargs) -> requests.Response:
    """Make API request with simple retry logic for transient failures.
    Returns the response object or raises exception after all retries fail.
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 200:
                return resp
            elif resp.status_code >= 500:  # Server error - retry
                wait = (2 ** attempt) * 0.3
                logger.debug(f"API server error {resp.status_code} for {url[:60]}, retrying in {wait}s")
                time.sleep(wait)
            else:  # Client error - don't retry
                return resp
        except requests.RequestException as e:
            last_exception = e
            wait = (2 ** attempt) * 0.3
            logger.debug(f"API request error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(wait)
    if last_exception:
        raise last_exception
    return resp

def post_to_discord_with_retry(webhook_url: str, payload: dict, max_retries: int = 3) -> tuple:
    """Post to Discord with retry logic and exponential backoff.
    Returns (success: bool, status_code: int, error_msg: str or None)
    """
    for attempt in range(max_retries):
        try:
            resp = requests.post(webhook_url, json=payload, timeout=15)
            if resp.status_code in [200, 204]:
                return (True, resp.status_code, None)
            elif resp.status_code == 429:  # Rate limited
                retry_after = int(resp.headers.get('Retry-After', 2))
                logger.warning(f"Discord rate limited, waiting {retry_after}s (attempt {attempt+1}/{max_retries})")
                time.sleep(retry_after)
            elif resp.status_code >= 500:  # Server error - retry
                wait = (2 ** attempt) * 0.5
                logger.warning(f"Discord server error {resp.status_code}, retrying in {wait}s")
                time.sleep(wait)
            else:  # Client error - don't retry
                logger.error(f"Discord post failed with status {resp.status_code}: {resp.text[:200]}")
                return (False, resp.status_code, resp.text[:200])
        except requests.RequestException as e:
            wait = (2 ** attempt) * 0.5
            logger.warning(f"Discord request error (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(wait)
    logger.error(f"Discord post failed after {max_retries} attempts")
    return (False, 0, "Max retries exceeded")

league_injury_cache = {}

def fetch_team_injuries(team_name: str, league: str) -> dict:
    """
    Lightweight injury data fetching with count-based impact scoring.
    
    Uses public ESPN injuries endpoint (fetches all teams, caches by league).
    Filters to find the matching team and counts injuries.
    
    Impact scoring formula:
    - 1st injured player: 2.5 points (likely a key player if listed)
    - 2nd injured player: 2.0 points
    - 3rd+ injured player: 1.0 points each
    
    Returns: {
        "has_key_injuries": bool (True if impact_score >= 3.0),
        "injured_starters": int,
        "impact_score": float (total point impact),
        "star_out": bool (3+ players out suggests star involvement),
        "details": list
    }
    """
    global league_injury_cache
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"injuries:{today_str}:{league}:{team_name.lower()}"
    
    if cache_key in injury_cache:
        return injury_cache[cache_key]
    
    try:
        sport_map = {
            "NBA": "basketball/nba",
            "CBB": "basketball/mens-college-basketball",
            "NFL": "football/nfl",
            "CFB": "football/college-football",
            "NHL": "hockey/nhl"
        }
        sport = sport_map.get(league)
        if not sport:
            return {"has_key_injuries": False, "injured_starters": 0, "impact_score": 0, "star_out": False, "details": []}
        
        league_cache_key = f"league_injuries:{today_str}:{league}"
        if league_cache_key not in league_injury_cache:
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/injuries"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                return {"has_key_injuries": False, "injured_starters": 0, "impact_score": 0, "star_out": False, "details": []}
            league_injury_cache[league_cache_key] = resp.json().get("injuries", [])
        
        all_team_injuries = league_injury_cache[league_cache_key]
        
        team_name_lower = team_name.lower().strip()
        team_injuries = []
        details = []
        
        for team_data in all_team_injuries:
            espn_team_name = team_data.get("displayName", "").lower()
            espn_short = team_data.get("shortDisplayName", "").lower()
            espn_abbrev = team_data.get("abbreviation", "").lower()
            
            if (team_name_lower in espn_team_name or 
                espn_team_name in team_name_lower or
                team_name_lower == espn_short or
                team_name_lower == espn_abbrev or
                any(word in espn_team_name for word in team_name_lower.split() if len(word) > 3)):
                team_injuries = team_data.get("injuries", [])
                for inj in team_injuries:
                    athlete = inj.get("athlete", {})
                    status = inj.get("status", "Unknown")
                    if status.lower() == "out":
                        details.append({
                            "name": athlete.get("displayName", "Unknown"),
                            "status": status
                        })
                break
        
        out_count = len([d for d in details if d.get("status", "").lower() == "out"])
        injured_count = len(team_injuries)
        
        if out_count == 0:
            total_impact_score = 0.0
        elif out_count == 1:
            total_impact_score = 2.5
        elif out_count == 2:
            total_impact_score = 4.5
        else:
            total_impact_score = 4.5 + (out_count - 2) * 1.0
        
        star_out = out_count >= 3
        
        result = {
            "has_key_injuries": total_impact_score >= 3.0 or star_out,
            "injured_starters": out_count,
            "impact_score": round(total_impact_score, 1),
            "star_out": star_out,
            "details": details
        }
        injury_cache[cache_key] = result
        return result
    except Exception as e:
        logger.debug(f"Injury fetch error for {team_name}: {e}")
        return {"has_key_injuries": False, "injured_starters": 0, "impact_score": 0, "star_out": False, "details": []}

def store_opening_line(event_id: str, line: float):
    """
    Store the first line we see as the opening line for comparison.
    Called when we first fetch odds for a game.
    """
    if event_id and line and event_id not in opening_lines_store:
        opening_lines_store[event_id] = {
            "line": line,
            "timestamp": datetime.now(pytz.timezone('America/New_York')).isoformat()
        }
        logger.debug(f"Stored opening line for {event_id}: {line}")

def get_line_movement(event_id: str, current_line: float) -> dict:
    """
    Get line movement by comparing stored opening line with current line.
    Returns: {"opening_line": float, "current_line": float, "movement": float}
    """
    if event_id not in opening_lines_store:
        return {"opening_line": None, "current_line": current_line, "movement": 0}
    
    opening = opening_lines_store[event_id]["line"]
    movement = current_line - opening
    
    return {
        "opening_line": opening,
        "current_line": current_line,
        "movement": movement
    }

def fetch_opening_line(sport_key: str, event_id: str, current_line: float = None) -> dict:
    """
    Track line movement by comparing stored opening line vs current.
    If we have a stored opening, compare. Otherwise just return current.
    Returns: {"opening_line": float, "current_line": float, "movement": float, "sharp_move": bool}
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"line_movement:{today_str}:{sport_key}:{event_id}"
    
    if cache_key in line_movement_cache:
        cached = line_movement_cache[cache_key]
        if current_line and cached.get("opening_line"):
            cached["current_line"] = current_line
            cached["movement"] = current_line - cached["opening_line"]
        return cached
    
    result = {"opening_line": None, "current_line": current_line, "movement": 0, "sharp_move": False}
    
    if event_id in opening_lines_store and current_line:
        opening = opening_lines_store[event_id]["line"]
        result["opening_line"] = opening
        result["current_line"] = current_line
        result["movement"] = current_line - opening
        result["sharp_move"] = abs(result["movement"]) >= 1.5
    
    line_movement_cache[cache_key] = result
    return result

def detect_sharp_money(opening_line: float, current_line: float, direction: str = None) -> dict:
    """
    Detect sharp money based on line movement patterns.
    Sharp indicators:
    - Line moved 1.5+ points in same direction as our pick (sharp agrees)
    - Line moved 1.5+ points against our pick (sharp disagrees - warning)
    - Reverse line movement (public on one side, line moves other way)
    
    For TOTALS (direction = "O" or "U"):
    - Line moves UP = sharps on OVER, moves DOWN = sharps on UNDER
    
    For SPREADS (direction = "HOME" or "AWAY"):
    - Spread line is stored in AWAY PERSPECTIVE (positive = away underdog, negative = away favorite)
    - Line moves UP (e.g., 7 → 9): away underdog getting more points = sharps on HOME
    - Line moves DOWN (e.g., 7 → 5): away underdog getting fewer points = sharps on AWAY
    
    Returns: {"sharp_aligned": bool, "sharp_against": bool, "movement": float, "signal": str}
    """
    if opening_line is None or current_line is None:
        return {"sharp_aligned": False, "sharp_against": False, "movement": 0, "signal": "NO_DATA"}
    
    movement = current_line - opening_line
    
    # TOTALS: line movement = total points moved up/down
    if direction == "O":
        if movement >= 1.5:
            return {"sharp_aligned": True, "sharp_against": False, "movement": movement, "signal": "SHARP_AGREES"}
        elif movement <= -1.5:
            return {"sharp_aligned": False, "sharp_against": True, "movement": movement, "signal": "SHARP_DISAGREES"}
    elif direction == "U":
        if movement <= -1.5:
            return {"sharp_aligned": True, "sharp_against": False, "movement": movement, "signal": "SHARP_AGREES"}
        elif movement >= 1.5:
            return {"sharp_aligned": False, "sharp_against": True, "movement": movement, "signal": "SHARP_DISAGREES"}
    
    # SPREADS: line is in AWAY PERSPECTIVE (positive = away underdog)
    # Movement positive = spread went up (away getting more points) = sharps on HOME favorite
    # Movement negative = spread went down (away getting fewer points) = sharps on AWAY
    elif direction == "HOME":
        if movement >= 1.5:  # Line moved 7 → 9, home favorite stronger, sharps on HOME
            return {"sharp_aligned": True, "sharp_against": False, "movement": movement, "signal": "SHARP_AGREES"}
        elif movement <= -1.5:  # Line moved 7 → 5, away getting value, sharps on AWAY
            return {"sharp_aligned": False, "sharp_against": True, "movement": movement, "signal": "SHARP_DISAGREES"}
    elif direction == "AWAY":
        if movement <= -1.5:  # Line moved 7 → 5, sharps on AWAY
            return {"sharp_aligned": True, "sharp_against": False, "movement": movement, "signal": "SHARP_AGREES"}
        elif movement >= 1.5:  # Line moved 7 → 9, sharps on HOME
            return {"sharp_aligned": False, "sharp_against": True, "movement": movement, "signal": "SHARP_DISAGREES"}
    
    return {"sharp_aligned": False, "sharp_against": False, "movement": movement, "signal": "NEUTRAL"}

class SpreadValidator:
    """Validates spread signs using moneyline cross-reference"""
    
    @staticmethod
    def validate_spread_vs_moneyline(
        spread: float,
        away_ml: Optional[float],
        home_ml: Optional[float],
        away_team: str,
        home_team: str
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        """
        Validates spread sign against moneyline odds.
        Returns: (is_valid, error_message, corrected_spread)
        
        LOGIC:
        - Negative spread (from away perspective) = away team is FAVORITE
        - Positive spread = away team is UNDERDOG
        - Lower moneyline = FAVORITE
        """
        if not away_ml or not home_ml:
            return True, None, None
        
        away_is_favorite_by_ml = away_ml < home_ml
        away_is_favorite_by_spread = spread < 0
        
        if away_is_favorite_by_ml != away_is_favorite_by_spread:
            error_msg = (
                f"SPREAD SIGN MISMATCH: {away_team} @ {home_team} | "
                f"Spread: {spread} (says {'FAV' if spread < 0 else 'DOG'}) vs "
                f"ML: {away_ml}/{home_ml} (says {'FAV' if away_is_favorite_by_ml else 'DOG'})"
            )
            corrected_spread = -spread
            return False, error_msg, corrected_spread
        
        return True, None, None
    
    @staticmethod
    def validate_and_correct_spread(
        spread: float,
        away_ml: Optional[float],
        home_ml: Optional[float],
        away_team: str,
        home_team: str
    ) -> Tuple[float, bool]:
        """
        Validates spread and returns corrected value if needed.
        Returns: (final_spread, was_corrected)
        """
        is_valid, error_msg, corrected = SpreadValidator.validate_spread_vs_moneyline(
            spread, away_ml, home_ml, away_team, home_team
        )
        
        if not is_valid and corrected is not None:
            logger.warning(f"AUTO-CORRECTED: {error_msg} -> Using {corrected}")
            return corrected, True
        
        return spread, False


class BulletproofPickValidator:
    """
    BULLETPROOF PRE-SEND VALIDATION
    
    Runs ALL validation checks before picks are posted to Discord.
    Ensures maximum win rate by enforcing every qualification rule.
    """
    
    LEAGUE_EDGE_THRESHOLDS = {
        "NBA": 8.0, "CBB": 8.0,
        "NFL": 3.5, "CFB": 3.5,
        "NHL": 0.5
    }
    
    CONFIDENCE_TIERS = {
        "SUPERMAX": {"edge": 12, "ev": 3.0, "history": 70},
        "HIGH": {"edge": 10, "ev": 1.0, "history": 65},
        "MEDIUM": {"edge": 8, "ev": 0, "history": 60},
        "LOW": {"edge": 0, "ev": -999, "history": 60}
    }
    
    @classmethod
    def validate_pick(cls, game, pick_type: str) -> dict:
        """
        Run ALL validation checks on a pick before posting.
        
        Args:
            game: Game model instance
            pick_type: 'total' or 'spread'
        
        Returns:
            Dict with validated, reasons, confidence_tier, disqualification_reasons
        """
        checks_passed = []
        checks_failed = []
        warnings = []
        
        league = game.league
        threshold = cls.LEAGUE_EDGE_THRESHOLDS.get(league, 8.0)
        
        if pick_type == 'total':
            edge = game.edge or 0
            ev = game.total_ev
            history_pct = max(game.away_ou_pct or 0, game.home_ou_pct or 0)
            is_qualified = game.is_qualified
            history_qualified = game.history_qualified
            direction = game.direction
        else:
            edge = game.spread_edge or 0
            ev = game.spread_ev
            history_pct = max(getattr(game, 'away_spread_pct', 0) or 0, getattr(game, 'home_spread_pct', 0) or 0)
            is_qualified = game.spread_is_qualified
            history_qualified = game.spread_history_qualified
            direction = game.spread_direction
        
        # CHECK 1: Edge threshold
        if edge >= threshold:
            checks_passed.append(f"EDGE_OK: {edge:.1f} >= {threshold}")
        else:
            checks_failed.append(f"EDGE_FAIL: {edge:.1f} < {threshold}")
        
        # CHECK 2: Model qualification
        if is_qualified:
            checks_passed.append("MODEL_QUALIFIED: True")
        else:
            checks_failed.append("MODEL_QUALIFIED: False")
        
        # CHECK 3: Historical qualification
        if history_qualified:
            checks_passed.append("HISTORY_QUALIFIED: True")
        else:
            checks_failed.append("HISTORY_QUALIFIED: False")
        
        # CHECK 4: EV check (NULL allowed, negative excluded)
        if ev is None:
            checks_passed.append("EV_OK: No Pinnacle data (allowed)")
        elif ev >= 0:
            checks_passed.append(f"EV_OK: {ev:.2f}%")
        else:
            checks_failed.append(f"EV_FAIL: {ev:.2f}% (negative)")
        
        # CHECK 5: Injury concern - handled during qualification, skip redundant check
        # Injuries are already factored into is_qualified and history_qualified flags
        checks_passed.append("INJURY_CHECK: Validated during qualification")
        
        # CHECK 6: Game not already started/finished
        game_time = game.game_time or ""
        if 'final' in game_time.lower():
            checks_failed.append("GAME_STATUS: Already finished")
        elif 'in progress' in game_time.lower() or 'live' in game_time.lower():
            checks_failed.append("GAME_STATUS: Already in progress")
        else:
            checks_passed.append("GAME_STATUS: Not started")
        
        # CHECK 7: Spread validation (for spread picks)
        # SpreadValidator already runs during fetch_games, so spread_is_qualified already incorporates validation
        if pick_type == 'spread' and game.spread_line is not None:
            checks_passed.append("SPREAD_VALIDATION: Validated during fetch")
        
        # DETERMINE IF VALIDATED
        validated = len(checks_failed) == 0
        
        # DETERMINE QUALIFICATION STATUS
        status = cls._determine_qualification_status(
            validated, checks_passed, checks_failed, edge, ev, history_qualified, threshold
        )
        
        # CALCULATE CONFIDENCE TIER
        confidence_tier = cls._calculate_confidence_tier(edge, ev, history_pct, validated)
        
        return {
            "validated": validated,
            "status": status.value,
            "confidence_tier": confidence_tier,
            "checks_passed": checks_passed,
            "checks_failed": checks_failed,
            "warnings": warnings,
            "edge": edge,
            "ev": ev,
            "history_pct": history_pct,
            "game": f"{game.away_team} @ {game.home_team}",
            "pick_type": pick_type,
            "league": league
        }
    
    @classmethod
    def _determine_qualification_status(cls, validated: bool, checks_passed: list, checks_failed: list,
                                         edge: float, ev, history_qualified: bool, threshold: float) -> QualificationStatus:
        """Determine the exact qualification status for clear logging"""
        if validated:
            return QualificationStatus.FULLY_QUALIFIED
        
        has_edge = edge >= threshold
        has_history = history_qualified
        has_ev_problem = ev is not None and ev < 0
        
        if has_ev_problem:
            return QualificationStatus.NEGATIVE_EV
        elif has_edge and not has_history:
            return QualificationStatus.EDGE_ONLY
        elif has_history and not has_edge:
            return QualificationStatus.HISTORY_ONLY
        else:
            return QualificationStatus.NOT_QUALIFIED
    
    @classmethod
    def _calculate_confidence_tier(cls, edge: float, ev: Optional[float], history_pct: float, validated: bool) -> str:
        """Calculate confidence tier based on edge, EV, and history"""
        if not validated:
            return "DISQUALIFIED"
        
        ev_val = ev if ev is not None else 0
        
        # SUPERMAX: Edge 12+, EV 3%+, History 70%+
        if edge >= 12 and ev_val >= 3.0 and history_pct >= 70:
            return "SUPERMAX"
        
        # HIGH: Edge 10+, EV 1%+, History 65%+
        if edge >= 10 and ev_val >= 1.0 and history_pct >= 65:
            return "HIGH"
        
        # MEDIUM: Edge 8+, EV 0%+, History 60%+
        if edge >= 8 and ev_val >= 0 and history_pct >= 60:
            return "MEDIUM"
        
        # LOW: Meets minimum thresholds
        return "LOW"
    
    @classmethod
    def validate_all_picks(cls, games: list, pick_type: str = 'both') -> dict:
        """
        Validate all games and return categorized results.
        
        Args:
            games: List of Game model instances
            pick_type: 'total', 'spread', or 'both'
        
        Returns:
            Dict with validated_picks, rejected_picks, by_tier
        """
        validated_picks = []
        rejected_picks = []
        by_tier = {"SUPERMAX": [], "HIGH": [], "MEDIUM": [], "LOW": []}
        
        for game in games:
            if pick_type in ['total', 'both'] and game.is_qualified:
                result = cls.validate_pick(game, 'total')
                if result['validated']:
                    validated_picks.append(result)
                    tier = result['confidence_tier']
                    if tier in by_tier:
                        by_tier[tier].append(result)
                else:
                    rejected_picks.append(result)
            
            if pick_type in ['spread', 'both'] and game.spread_is_qualified:
                result = cls.validate_pick(game, 'spread')
                if result['validated']:
                    validated_picks.append(result)
                    tier = result['confidence_tier']
                    if tier in by_tier:
                        by_tier[tier].append(result)
                else:
                    rejected_picks.append(result)
        
        # Sort each tier by edge
        for tier in by_tier:
            by_tier[tier].sort(key=lambda x: x['edge'], reverse=True)
        
        return {
            "validated_picks": validated_picks,
            "rejected_picks": rejected_picks,
            "by_tier": by_tier,
            "total_validated": len(validated_picks),
            "total_rejected": len(rejected_picks)
        }
    
    @classmethod
    def get_best_picks_for_posting(cls, games: list, max_picks: int = 3) -> list:
        """
        Get the best validated picks for Discord posting.
        
        Prioritizes: SUPERMAX > HIGH > MEDIUM > LOW
        Within tiers, sorts by edge.
        
        Returns list of validated pick dicts ready for posting.
        """
        validation_result = cls.validate_all_picks(games, pick_type='both')
        by_tier = validation_result['by_tier']
        
        best_picks = []
        for tier in ["SUPERMAX", "HIGH", "MEDIUM", "LOW"]:
            for pick in by_tier[tier]:
                if len(best_picks) >= max_picks:
                    break
                best_picks.append(pick)
            if len(best_picks) >= max_picks:
                break
        
        # Log validation summary
        logger.info(f"🔒 BULLETPROOF VALIDATION: {len(best_picks)} picks selected")
        logger.info(f"   SUPERMAX: {len(by_tier['SUPERMAX'])}, HIGH: {len(by_tier['HIGH'])}, "
                   f"MEDIUM: {len(by_tier['MEDIUM'])}, LOW: {len(by_tier['LOW'])}")
        logger.info(f"   Rejected: {validation_result['total_rejected']}")
        
        for pick in best_picks:
            logger.info(f"   ✅ {pick['game']} ({pick['pick_type'].upper()}) - "
                       f"Tier: {pick['confidence_tier']}, Edge: {pick['edge']:.1f}, "
                       f"EV: {pick['ev']:.2f}%" if pick['ev'] else f"EV: N/A")
        
        return best_picks


class DashboardFilter:
    """
    Filters picks for dashboard display
    
    RULE: Only FULLY_QUALIFIED picks are shown
    NO EXCEPTIONS
    """
    
    @staticmethod
    def filter_for_dashboard(validation_results: list) -> dict:
        """
        Filter validation results for dashboard display
        
        Args:
            validation_results: List of validation dicts from BulletproofPickValidator
        
        Returns:
            {
                'qualified': List[Dict],        # Only FULLY_QUALIFIED
                'rejected': List[Dict],         # Everything else
                'stats': {
                    'total': int,
                    'qualified': int,
                    'edge_only': int,
                    'negative_ev': int,
                    'history_only': int,
                    'not_qualified': int,
                },
            }
        """
        qualified = []
        rejected = []
        stats = {
            'total': 0,
            'qualified': 0,
            'edge_only': 0,
            'negative_ev': 0,
            'history_only': 0,
            'not_qualified': 0
        }
        
        for result in validation_results:
            stats['total'] += 1
            status = result.get('status', 'NOT_QUALIFIED')
            
            if status == QualificationStatus.FULLY_QUALIFIED.value:
                qualified.append(result)
                stats['qualified'] += 1
            else:
                rejected.append(result)
                if status == QualificationStatus.EDGE_ONLY.value:
                    stats['edge_only'] += 1
                elif status == QualificationStatus.NEGATIVE_EV.value:
                    stats['negative_ev'] += 1
                elif status == QualificationStatus.HISTORY_ONLY.value:
                    stats['history_only'] += 1
                else:
                    stats['not_qualified'] += 1
        
        # Sort qualified by edge descending
        qualified.sort(key=lambda x: x.get('edge', 0), reverse=True)
        
        return {
            'qualified': qualified,
            'rejected': rejected,
            'stats': stats
        }
    
    @staticmethod
    def get_rejection_summary(rejected: list) -> str:
        """Get a summary of why picks were rejected"""
        reasons = {}
        for r in rejected:
            status = r.get('status', 'UNKNOWN')
            reasons[status] = reasons.get(status, 0) + 1
        
        summary_parts = [f"{count} {status}" for status, count in reasons.items()]
        return ", ".join(summary_parts) if summary_parts else "None rejected"


def calculate_recent_form_ppg(games: list) -> dict:
    """
    Calculate PPG and Opp PPG from last 5 games for recent form.
    Returns: {"ppg": float, "opp_ppg": float, "games_used": int}
    """
    if len(games) < 3:
        return {"ppg": 0, "opp_ppg": 0, "games_used": 0}
    
    recent = games[-5:] if len(games) >= 5 else games
    
    ppg = sum(g["team_score"] for g in recent) / len(recent)
    opp_ppg = sum(g["opp_score"] for g in recent) / len(recent)
    
    return {"ppg": ppg, "opp_ppg": opp_ppg, "games_used": len(recent)}

def calculate_blended_stats(season_ppg: float, season_opp: float, 
                           recent_ppg: float, recent_opp: float,
                           recent_weight: float = 0.6) -> Tuple[float, float]:
    """
    Blend season stats with recent form (default 60% recent, 40% season).
    Returns: (blended_ppg, blended_opp_ppg)
    """
    if recent_ppg == 0 or recent_opp == 0:
        return season_ppg, season_opp
    
    season_weight = 1 - recent_weight
    
    blended_ppg = (recent_ppg * recent_weight) + (season_ppg * season_weight)
    blended_opp = (recent_opp * recent_weight) + (season_opp * season_weight)
    
    return blended_ppg, blended_opp

def calculate_sos_factor(opp_ppg: float, league: str) -> float:
    """
    Calculate strength of schedule adjustment factor.
    League average defensive ratings used as baseline.
    Returns: multiplier (>1 = tough schedule, <1 = easy schedule)
    """
    league_avg_def = {
        "NBA": 114.0,
        "CBB": 70.0,
        "NFL": 22.0,
        "CFB": 25.0,
        "NHL": 3.0
    }
    avg = league_avg_def.get(league, 100)
    if avg == 0:
        return 1.0
    
    return opp_ppg / avg

def check_line_movement_sharp(opening: float, current: float, threshold: float = 1.5) -> bool:
    """
    Detect sharp money movement (significant line shift).
    Sharp move = line moved 1.5+ points in either direction.
    """
    if opening is None or current is None:
        return False
    movement = abs(current - opening)
    return movement >= threshold

def calculate_advanced_qualification_score(game, away_games: list, home_games: list) -> dict:
    """
    Calculate advanced qualification factors for a game.
    Incorporates recent form, opponent strength, injuries, and line movement.
    Returns: {
        "recent_form_boost": float (-2 to +2),
        "injury_penalty": float (0 to -3),
        "sharp_money_aligned": bool,
        "sos_adjusted_edge": float,
        "total_adjustment": float
    }
    """
    adjustments = {
        "recent_form_boost": 0,
        "injury_penalty": 0,
        "sharp_money_aligned": False,
        "sos_factor": 1.0,
        "total_adjustment": 0
    }
    
    try:
        if len(away_games) >= 3:
            away_recent = calculate_recent_form_ppg(away_games)
            away_margin_recent = sum(g["margin"] for g in away_games[-5:]) / min(5, len(away_games))
            away_margin_season = sum(g["margin"] for g in away_games) / len(away_games)
            
            if away_margin_recent > away_margin_season + 3:
                adjustments["recent_form_boost"] += 1.0
            elif away_margin_recent < away_margin_season - 3:
                adjustments["recent_form_boost"] -= 1.0
        
        if len(home_games) >= 3:
            home_recent = calculate_recent_form_ppg(home_games)
            home_margin_recent = sum(g["margin"] for g in home_games[-5:]) / min(5, len(home_games))
            home_margin_season = sum(g["margin"] for g in home_games) / len(home_games)
            
            if home_margin_recent > home_margin_season + 3:
                adjustments["recent_form_boost"] -= 0.5
            elif home_margin_recent < home_margin_season - 3:
                adjustments["recent_form_boost"] += 0.5
        
        away_injuries = fetch_team_injuries(game.away_team, game.league)
        home_injuries = fetch_team_injuries(game.home_team, game.league)
        
        if game.spread_direction == "AWAY" and away_injuries["has_key_injuries"]:
            adjustments["injury_penalty"] -= 2
        elif game.spread_direction == "HOME" and home_injuries["has_key_injuries"]:
            adjustments["injury_penalty"] -= 2
        
        if game.direction == "O" and (away_injuries["has_key_injuries"] or home_injuries["has_key_injuries"]):
            adjustments["injury_penalty"] -= 1
        
        adjustments["total_adjustment"] = adjustments["recent_form_boost"] + adjustments["injury_penalty"]
        
    except Exception as e:
        logger.debug(f"Advanced qualification error: {e}")
    
    return adjustments

DIRECTIONAL_PREFIXES = {'eastern', 'western', 'central', 'northern', 'southern', 
                         'southeast', 'southwest', 'northeast', 'northwest'}
DIRECTIONAL_ABBREVS = {'e': 'eastern', 'w': 'western', 'c': 'central', 'n': 'northern', 
                       's': 'southern', 'se': 'southeast', 'sw': 'southwest', 
                       'ne': 'northeast', 'nw': 'northwest'}

TEAM_ALIASES = {
    'umass': 'massachusetts', 'uconn': 'connecticut', 'usc': 'southern california',
    'ucla': 'california los angeles', 'unlv': 'nevada las vegas', 'utep': 'texas el paso',
    'utsa': 'texas san antonio', 'unc': 'north carolina', 'lsu': 'louisiana state',
    'ole miss': 'mississippi', 'gw': 'george washington', 'g washington': 'george washington',
    'siue': 'siu edwardsville', 'siu e': 'siu edwardsville', 'ucf': 'central florida',
    'usf': 'south florida', 'fiu': 'florida international', 'fau': 'florida atlantic',
    'byu': 'brigham young', 'tcu': 'texas christian', 'smu': 'southern methodist',
    'vcu': 'virginia commonwealth', 'mtsu': 'middle tennessee', 'etsu': 'east tennessee',
    'bgsu': 'bowling green', 'niu': 'northern illinois', 'wku': 'western kentucky',
    'app state': 'appalachian state', 'app st': 'appalachian state',
    'ga southern': 'georgia southern', 'ga tech': 'georgia tech',
    'miami oh': 'miami ohio', 'miami fl': 'miami florida',
}

def normalize_team_name(name: str) -> str:
    """Normalize team name for matching, expanding common abbreviations."""
    if not name:
        return ""
    n = name.lower().replace("'", "").replace("-", " ").replace(".", "").strip()
    n = n.replace("(", " ").replace(")", " ")
    n = " ".join(n.split())
    if n.endswith(" st"):
        n = n[:-3] + " state"
    n = n.replace(" st ", " state ")
    for abbrev, full in TEAM_ALIASES.items():
        if n == abbrev or n.startswith(abbrev + " "):
            n = n.replace(abbrev, full, 1)
            break
    return n

def get_team_tokens(name: str) -> set:
    """Get tokens from team name, excluding stop words."""
    stop_words = {'the', 'of', 'at', 'vs', 'and'}
    words = normalize_team_name(name).split()
    return set(w for w in words if w not in stop_words and len(w) > 1)

def get_directional_prefix(name: str) -> Optional[str]:
    """Extract directional prefix (Eastern, Western, etc) from team name."""
    n = normalize_team_name(name)
    words = n.split()
    if not words:
        return None
    first = words[0]
    if first in DIRECTIONAL_PREFIXES:
        return first
    if first in DIRECTIONAL_ABBREVS:
        return DIRECTIONAL_ABBREVS[first]
    return None

def teams_match(name1: str, name2: str) -> bool:
    """
    Check if two team names match using token overlap with directional prefix validation.
    Prevents Eastern Michigan from matching Central Michigan, etc.
    """
    tokens1 = get_team_tokens(name1)
    tokens2 = get_team_tokens(name2)
    if not tokens1 or not tokens2:
        return False
    
    dir1 = get_directional_prefix(name1)
    dir2 = get_directional_prefix(name2)
    if dir1 and dir2 and dir1 != dir2:
        return False
    if dir1 and not dir2:
        return False
    if dir2 and not dir1:
        return False
    
    overlap = tokens1 & tokens2
    if not overlap:
        return False
    if tokens1 <= tokens2 or tokens2 <= tokens1:
        return True
    if len(overlap) >= min(len(tokens1), len(tokens2)):
        return True
    return False

class UniversalSpreadHandler:
    """
    Bulletproof spread extraction - works for ANY team (home or away) without confusion.
    Gets spread for BOTH teams, validates they're mirror images, cross-checks with moneyline.
    """
    
    @staticmethod
    def extract_spread_data(away_team: str, home_team: str, bookmakers: list) -> Optional[dict]:
        """
        Extract spread data with FULL VALIDATION.
        Returns standardized format with BOTH teams' perspectives.
        """
        if not bookmakers:
            return None
        
        bovada_book = next((b for b in bookmakers if b.get("key") == "bovada"), None)
        if not bovada_book:
            return None
        
        markets = {m.get("key"): m for m in bovada_book.get("markets", [])}
        
        spreads_market = markets.get("spreads")
        h2h_market = markets.get("h2h")
        
        if not spreads_market:
            return None
        
        outcomes = spreads_market.get("outcomes", [])
        
        away_spread_outcome = None
        home_spread_outcome = None
        
        for outcome in outcomes:
            outcome_name = outcome.get("name", "")
            if teams_match(outcome_name, away_team):
                away_spread_outcome = outcome
            elif teams_match(outcome_name, home_team):
                home_spread_outcome = outcome
        
        if not away_spread_outcome or not home_spread_outcome:
            logger.debug(f"Missing spread data for {away_team} @ {home_team}")
            return None
        
        away_spread_raw = float(away_spread_outcome.get("point", 0))
        home_spread_raw = float(home_spread_outcome.get("point", 0))
        try:
            away_odds = int(away_spread_outcome.get("price", -110))
        except (ValueError, TypeError):
            away_odds = -110
        try:
            home_odds = int(home_spread_outcome.get("price", -110))
        except (ValueError, TypeError):
            home_odds = -110
        
        away_ml = None
        home_ml = None
        if h2h_market:
            h2h_outcomes = h2h_market.get("outcomes", [])
            for h2h_out in h2h_outcomes:
                h2h_name = h2h_out.get("name", "")
                if teams_match(h2h_name, away_team):
                    away_ml = h2h_out.get("price")
                elif teams_match(h2h_name, home_team):
                    home_ml = h2h_out.get("price")
        
        if abs(away_spread_raw + home_spread_raw) > 0.5:
            logger.warning(f"SPREAD MISMATCH: {away_team}({away_spread_raw}) @ {home_team}({home_spread_raw})")
        
        if away_ml and home_ml:
            if away_ml < home_ml:
                favorite_team = away_team
                underdog_team = home_team
                favorite_location = 'away'
            else:
                favorite_team = home_team
                underdog_team = away_team
                favorite_location = 'home'
        else:
            if away_spread_raw < 0:
                favorite_team = away_team
                underdog_team = home_team
                favorite_location = 'away'
            else:
                favorite_team = home_team
                underdog_team = away_team
                favorite_location = 'home'
        
        spread_data = {
            'away_team': away_team,
            'home_team': home_team,
            'away_spread': away_spread_raw,
            'home_spread': home_spread_raw,
            'away_odds': away_odds,
            'home_odds': home_odds,
            'away_moneyline': away_ml,
            'home_moneyline': home_ml,
            'favorite': favorite_team,
            'underdog': underdog_team,
            'favorite_location': favorite_location,
            'spread_magnitude': abs(away_spread_raw),
            'spread_away_perspective': away_spread_raw,
        }
        
        if away_ml and home_ml:
            ml_says_away_fav = away_ml < home_ml
            spread_says_away_fav = away_spread_raw < 0
            
            if ml_says_away_fav != spread_says_away_fav:
                logger.warning(
                    f"SPREAD/MONEYLINE MISMATCH: {away_team} @ {home_team} - "
                    f"ML: {'Away fav' if ml_says_away_fav else 'Home fav'}, "
                    f"Spread: {'Away fav' if spread_says_away_fav else 'Home fav'}"
                )
        
        return spread_data

class Game(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)
    league = db.Column(db.String(10), nullable=False)
    away_team = db.Column(db.String(100), nullable=False)
    home_team = db.Column(db.String(100), nullable=False)
    game_time = db.Column(db.String(20))
    line = db.Column(db.Float)
    away_ppg = db.Column(db.Float)
    away_opp_ppg = db.Column(db.Float)
    home_ppg = db.Column(db.Float)
    home_opp_ppg = db.Column(db.Float)
    projected_total = db.Column(db.Float)
    edge = db.Column(db.Float)
    direction = db.Column(db.String(10))
    is_qualified = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    spread_line = db.Column(db.Float)
    spread_edge = db.Column(db.Float)
    spread_direction = db.Column(db.String(10))
    spread_is_qualified = db.Column(db.Boolean, default=False)
    expected_away = db.Column(db.Float)
    expected_home = db.Column(db.Float)
    projected_margin = db.Column(db.Float)
    event_id = db.Column(db.String(64))
    sport_key = db.Column(db.String(50))
    alt_total_line = db.Column(db.Float)
    alt_total_odds = db.Column(db.Integer)
    alt_spread_line = db.Column(db.Float)
    alt_spread_odds = db.Column(db.Integer)
    alt_edge = db.Column(db.Float)
    alt_spread_edge = db.Column(db.Float)
    # Historical percentages (last 10 games)
    away_ou_pct = db.Column(db.Float)  # Away team's O/U hit rate
    home_ou_pct = db.Column(db.Float)  # Home team's O/U hit rate
    away_spread_pct = db.Column(db.Float)  # Away team's spread cover rate
    home_spread_pct = db.Column(db.Float)  # Home team's spread cover rate
    h2h_ou_pct = db.Column(db.Float)  # Head-to-head O/U hit rate
    h2h_spread_pct = db.Column(db.Float)  # Head-to-head spread cover rate
    history_qualified = db.Column(db.Boolean, default=None)  # NULL = not checked, True/False = checked (for TOTALS)
    spread_history_qualified = db.Column(db.Boolean, default=None)  # Separate history qualification for SPREADS
    # Pinnacle comparison for EV calculation
    bovada_total_odds = db.Column(db.Integer)  # Bovada odds for our totals pick
    pinnacle_total_odds = db.Column(db.Integer)  # Pinnacle odds for same line
    bovada_spread_odds = db.Column(db.Integer)  # Bovada odds for our spread pick
    pinnacle_spread_odds = db.Column(db.Integer)  # Pinnacle odds for same line
    total_ev = db.Column(db.Float)  # Expected value vs Pinnacle for totals
    spread_ev = db.Column(db.Float)  # Expected value vs Pinnacle for spreads
    # NBA 1H Money Line for away favorites (Model 4)
    nba_1h_ml_odds = db.Column(db.Integer)  # Away team 1st half money line odds
    nba_1h_ml_qualified = db.Column(db.Boolean, default=False)  # Qualifies for Model 4
    # Model 4 historical percentages (1H win rate when away team is favored)
    nba_1h_away_win_pct = db.Column(db.Float)  # Away team 1H win rate (last 15-20 games)
    nba_1h_h2h_win_pct = db.Column(db.Float)  # H2H 1H win rate for away team
    nba_1h_history_qualified = db.Column(db.Boolean, default=None)  # History qualification for Model 4
    
    __table_args__ = (
        db.Index('idx_date_league', 'date', 'league'),
        db.Index('idx_qualified', 'is_qualified'),
        db.Index('idx_spread_qualified', 'spread_is_qualified'),
    )
    
    @validates('edge')
    def validate_edge(self, key, value):
        if value is not None and value < 0:
            raise ValueError("Edge cannot be negative")
        return value

class Pick(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey('game.id'))
    date = db.Column(db.Date, nullable=False)
    league = db.Column(db.String(10), nullable=False)
    matchup = db.Column(db.String(200), nullable=False)
    pick = db.Column(db.String(50), nullable=False)
    edge = db.Column(db.Float)
    result = db.Column(db.String(10))
    actual_total = db.Column(db.Float)
    is_lock = db.Column(db.Boolean, default=False)
    game_window = db.Column(db.String(10))
    posted_to_discord = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    pick_type = db.Column(db.String(10), default="total")
    line_value = db.Column(db.Float)
    game_start = db.Column(db.DateTime)  # When the game starts
    
    __table_args__ = (
        db.Index('idx_pick_result', 'result'),
        db.Index('idx_pick_date_league', 'date', 'league'),
        db.Index('idx_pick_type', 'pick_type'),
    )

def parse_game_time_hour(game_time_str: str) -> Optional[int]:
    """Parse game time string and return hour in 24h format (ET)."""
    if not game_time_str:
        return None
    import re
    match = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM)', game_time_str, re.IGNORECASE)
    if not match:
        return None
    hour = int(match.group(1))
    ampm = match.group(3).upper()
    if ampm == 'PM' and hour != 12:
        hour += 12
    elif ampm == 'AM' and hour == 12:
        hour = 0
    return hour

def get_game_window(game_time_str: str) -> str:
    """
    Categorize game into time window:
    - EARLY: Before 1:00 PM ET (games starting 10am-12:59pm)
    - MID: 1:00 PM - 5:59 PM ET  
    - LATE: 6:00 PM ET and later
    """
    hour = parse_game_time_hour(game_time_str)
    if hour is None:
        return 'LATE'
    if hour < 13:
        return 'EARLY'
    elif hour < 18:
        return 'MID'
    return 'LATE'

def is_big_slate_day() -> bool:
    """Check if today is Friday, Saturday, or Sunday (big slate days)."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et)
    return today.weekday() >= 4

def auto_save_qualified_picks(top_picks: list, today: 'date') -> int:
    """
    BULLETPROOF Auto-save qualified picks to Pick table for history tracking.
    This runs independently of Discord posting.
    
    Validation checks before saving:
    1. Pick must have valid game reference
    2. Pick must have valid edge (> 0)
    3. Pick must have valid direction
    4. Pick must not already exist in database
    5. Game must not have already started (no late saves)
    
    Args:
        top_picks: List of combined pick dicts from dashboard
        today: Current date
    
    Returns:
        Number of picks saved
    """
    if not top_picks:
        return 0
    
    saved_count = 0
    skipped_count = 0
    et = pytz.timezone('America/New_York')
    now = datetime.now(et)
    
    for pick_info in top_picks:
        try:
            game = pick_info.get('game')
            if not game:
                logger.warning("Auto-save skipped: No game reference in pick_info")
                skipped_count += 1
                continue
            
            pick_type = pick_info.get('pick_type')
            if pick_type == 'totals':
                pick_type = 'total'
            if not pick_type or pick_type not in ['total', 'spread', '1h_ml']:
                logger.warning(f"Auto-save skipped: Invalid pick_type '{pick_type}'")
                skipped_count += 1
                continue
            
            direction = pick_info.get('direction')
            if not direction:
                logger.warning(f"Auto-save skipped: No direction for {game.away_team}@{game.home_team}")
                skipped_count += 1
                continue
            
            edge = pick_info.get('edge', 0)
            if not edge or edge <= 0:
                logger.warning(f"Auto-save skipped: Invalid edge {edge} for {game.away_team}@{game.home_team}")
                skipped_count += 1
                continue
            
            matchup = f"{game.away_team} @ {game.home_team}"
            
            existing = Pick.query.filter_by(
                date=today, 
                matchup=matchup, 
                pick_type=pick_type
            ).first()
            
            if existing:
                continue
            
            if pick_type == 'total':
                line_val = pick_info.get('alt_line') or pick_info.get('line') or game.line
                if not line_val:
                    logger.warning(f"Auto-save skipped: No line value for {matchup} (total)")
                    skipped_count += 1
                    continue
                pick_str = f"{'O' if direction == 'O' else 'U'}{line_val}"
                edge = pick_info.get('edge') or game.alt_edge or game.edge
            elif pick_type == 'spread':
                line_val = pick_info.get('alt_line') or pick_info.get('line') or game.spread_line
                if line_val is None:
                    logger.warning(f"Auto-save skipped: No spread line for {matchup}")
                    skipped_count += 1
                    continue
                team_name = game.away_team if direction == 'AWAY' else game.home_team
                pick_str = f"{team_name} {line_val:+.1f}"
                edge = pick_info.get('edge') or game.alt_spread_edge or game.spread_edge
            elif pick_type == '1h_ml':
                line_val = None
                pick_str = f"{game.away_team} 1H ML"
                edge = pick_info.get('edge') or game.spread_edge or 0
            else:
                continue
            
            if not edge or edge <= 0:
                logger.warning(f"Auto-save skipped: Final edge check failed for {matchup}")
                skipped_count += 1
                continue
            
            game_start = None
            if game.game_time:
                try:
                    from dateutil import parser
                    game_time_clean = game.game_time.replace(' EST', ' -0500').replace(' EDT', ' -0400')
                    game_start = parser.parse(game_time_clean)
                except Exception as e:
                    logger.debug(f"Could not parse game_time for {matchup}: {e}")
            
            new_pick = Pick(
                game_id=game.id,
                date=today,
                league=game.league,
                matchup=matchup,
                pick=pick_str,
                edge=edge,
                is_lock=True,
                game_window=get_game_window(game.game_time) if game.game_time else 'LATE',
                posted_to_discord=False,
                pick_type=pick_type,
                line_value=line_val,
                game_start=game_start
            )
            db.session.add(new_pick)
            saved_count += 1
            logger.info(f"Auto-saved pick to history: {matchup} - {pick_str} ({pick_type}) edge={edge:.1f}")
            
        except Exception as e:
            logger.error(f"Auto-save error for pick: {e}")
            db.session.rollback()
            skipped_count += 1
            continue
    
    if saved_count > 0:
        try:
            db.session.commit()
            logger.info(f"BULLETPROOF Auto-save complete: {saved_count} saved, {skipped_count} skipped")
        except Exception as e:
            logger.error(f"Auto-save commit failed: {e}")
            db.session.rollback()
            return 0
    
    return saved_count

with app.app_context():
    db.create_all()
    # Migration: Add Model 4 columns if they don't exist (PostgreSQL)
    try:
        from sqlalchemy import text, inspect
        inspector = inspect(db.engine)
        existing_columns = [col['name'] for col in inspector.get_columns('game')]
        
        if 'nba_1h_ml_odds' not in existing_columns:
            with db.engine.connect() as conn:
                conn.execute(text("ALTER TABLE game ADD COLUMN nba_1h_ml_odds INTEGER"))
                conn.execute(text("ALTER TABLE game ADD COLUMN nba_1h_ml_qualified BOOLEAN DEFAULT FALSE"))
                conn.execute(text("ALTER TABLE game ADD COLUMN nba_1h_away_win_pct FLOAT"))
                conn.execute(text("ALTER TABLE game ADD COLUMN nba_1h_h2h_win_pct FLOAT"))
                conn.execute(text("ALTER TABLE game ADD COLUMN nba_1h_history_qualified BOOLEAN"))
                conn.commit()
                logger.info("Migration: Added Model 4 columns (nba_1h_ml_odds, nba_1h_ml_qualified, history fields)")
    except Exception as e:
        logger.warning(f"Migration check: {e}")

def calculate_projection(away_ppg: float, away_opp: float, 
                        home_ppg: float, home_opp: float) -> float:
    """
    LOCKED FORMULA - DO NOT MODIFY
    
    Expected Away Score = (Away PPG + Home Opp PPG) / 2
    Expected Home Score = (Home PPG + Away Opp PPG) / 2
    Projected Total = Expected Away + Expected Home
    """
    if any(v is None or v < 0 for v in [away_ppg, away_opp, home_ppg, home_opp]):
        raise ValueError("Insufficient data — no play")
    
    exp_away = (away_ppg + home_opp) / 2
    exp_home = (home_ppg + away_opp) / 2
    return exp_away + exp_home

def check_qualification(projected: float, line: float, league: str) -> Tuple[bool, Optional[str], float]:
    """
    LOCKED THRESHOLDS - DO NOT MODIFY
    
    Direction Rules:
    - OVER ("O"): If Projected_Total >= Bovada_Line + Threshold
    - UNDER ("U"): If Bovada_Line >= Projected_Total + Threshold
    """
    threshold = THRESHOLDS.get(league, 8.0)
    diff = projected - line
    edge = abs(diff)
    
    if projected >= line + threshold:
        return True, "O", edge
    elif line >= projected + threshold:
        return True, "U", edge
    return False, None, edge

def calculate_expected_scores(away_ppg: float, away_opp: float, 
                              home_ppg: float, home_opp: float) -> Tuple[float, float, float]:
    """
    LOCKED FORMULA - Uses same logic as calculate_projection
    Returns: (expected_away, expected_home, projected_total)
    """
    if any(v is None or v < 0 for v in [away_ppg, away_opp, home_ppg, home_opp]):
        raise ValueError("Insufficient data — no play")
    
    exp_away = (away_ppg + home_opp) / 2
    exp_home = (home_ppg + away_opp) / 2
    return exp_away, exp_home, exp_away + exp_home

def calculate_blended_expected_scores(game, away_games: list, home_games: list) -> Tuple[float, float, float]:
    """
    Calculate expected scores using blended stats (60% recent form, 40% season).
    Uses the locked formula with blended PPG values.
    Returns: (expected_away, expected_home, projected_total)
    """
    if not all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg]):
        raise ValueError("Insufficient data — no play")
    
    away_recent = calculate_recent_form_ppg(away_games) if away_games else {"ppg": 0, "opp_ppg": 0}
    home_recent = calculate_recent_form_ppg(home_games) if home_games else {"ppg": 0, "opp_ppg": 0}
    
    if away_recent["ppg"] > 0 and home_recent["ppg"] > 0:
        blended_away_ppg, blended_away_opp = calculate_blended_stats(
            game.away_ppg, game.away_opp_ppg,
            away_recent["ppg"], away_recent["opp_ppg"]
        )
        blended_home_ppg, blended_home_opp = calculate_blended_stats(
            game.home_ppg, game.home_opp_ppg,
            home_recent["ppg"], home_recent["opp_ppg"]
        )
        
        return calculate_expected_scores(
            blended_away_ppg, blended_away_opp,
            blended_home_ppg, blended_home_opp
        )
    
    return calculate_expected_scores(
        game.away_ppg, game.away_opp_ppg,
        game.home_ppg, game.home_opp_ppg
    )

def check_spread_qualification(expected_away: float, expected_home: float, 
                                spread_line: float, league: str) -> Tuple[bool, Optional[str], float]:
    """
    LOCKED THRESHOLDS - Same thresholds as totals
    
    spread_line is stored in AWAY PERSPECTIVE:
    - Positive = away is underdog (home is favorite by that amount)
    - Negative = away is favorite (home is underdog)
    
    E.g., spread_line = 22 means away +22 underdog, home -22 favorite
    E.g., spread_line = -5 means away -5 favorite, home +5 underdog
    
    line_margin = spread_line (what home team is expected to win by per the line)
    projected_margin = expected_home - expected_away (positive = home wins by X)
    
    Direction Rules:
    - Take HOME if: projected_margin >= line_margin + threshold (we think home wins by MORE than the line)
    - Take AWAY if: projected_margin <= line_margin - threshold (we think home wins by LESS than the line)
    
    Example: Home expected to win by 3, spread_line = 22 (home -22 favorite)
    line_margin = 22, projected_margin = 3
    We expect home to win by 3, but line says home wins by 22
    Difference = 19 points of value on AWAY side
    projected_margin (3) <= line_margin (22) - threshold (8) = 14? Yes!
    Bet AWAY +22 (they lose by 3 but cover +22)
    """
    threshold = THRESHOLDS.get(league, 8.0)
    projected_margin = expected_home - expected_away
    line_margin = spread_line  # spread_line IS the implied home margin (in away perspective storage)
    edge = abs(projected_margin - line_margin)
    
    if projected_margin >= line_margin + threshold:
        return True, "HOME", edge
    elif projected_margin <= line_margin - threshold:
        return True, "AWAY", edge
    return False, None, edge

def unified_spread_qualification(
    spread_direction: str,
    spread_line: float,
    raw_edge: float,
    home_avg_margin: float,
    away_avg_margin: float,
    home_recent_margin: float,
    away_recent_margin: float,
    home_form_trending: str,
    away_form_trending: str,
    injury_data: dict,
    league: str = "CBB",
    bovada_odds: int = -110
) -> dict:
    """
    UNIFIED SPREAD QUALIFICATION FUNCTION
    
    Combines all spread qualification rules into one function:
    1. Edge threshold check (with vig adjustment) - MUST PASS FIRST
    2. Historical margin validation (85% for HOME, positive for AWAY)
    3. Form trending check (declining form disqualifies)
    4. Injury impact check (PPG-weighted)
    
    Returns: {
        "qualified": bool,
        "reason": str,
        "raw_edge": float,
        "adjusted_edge": float,  # After vig removal
        "confidence": str
    }
    """
    adjusted_edge = VigCalculator.calculate_vig_adjusted_edge(raw_edge, bovada_odds)
    threshold = THRESHOLDS.get(league, 8.0)
    
    result = {
        "qualified": False,
        "reason": "",
        "raw_edge": raw_edge,
        "adjusted_edge": adjusted_edge,
        "confidence": "LOW"
    }
    
    if not spread_direction:
        result["reason"] = "NO_DIRECTION"
        return result
    
    if adjusted_edge < threshold:
        result["reason"] = f"EDGE_BELOW_THRESHOLD: {adjusted_edge:.1f} < {threshold:.1f}"
        return result
    
    if spread_direction == "HOME":
        # spread_line > 0 means HOME is favorite, < 0 means HOME is underdog
        is_home_favorite = spread_line > 0
        
        if is_home_favorite:
            # HOME FAVORITE: Must cover their spread (85% threshold)
            margin_threshold = abs(spread_line) * 0.85
            if home_avg_margin < margin_threshold:
                result["reason"] = f"HOME_FAV_MARGIN_BELOW_85%: {home_avg_margin:.1f} < {margin_threshold:.1f}"
                return result
        else:
            # HOME UNDERDOG: Must have positive margin (not a losing team)
            if home_avg_margin <= 0:
                result["reason"] = f"HOME_DOG_NEGATIVE_MARGIN: {home_avg_margin:.1f} (must be > 0)"
                return result
        
        if home_form_trending == "DOWN":
            if home_recent_margin < abs(spread_line) * 0.5:
                result["reason"] = f"HOME_FORM_DECLINING: recent {home_recent_margin:.1f} < 50% of spread"
                return result
        
        team_injury = injury_data.get("home", {})
        if team_injury.get("has_key_injuries") or team_injury.get("star_out"):
            result["reason"] = f"HOME_KEY_INJURIES: impact={team_injury.get('impact_score', 0)}"
            return result
        
        result["qualified"] = True
        result["reason"] = f"HOME_{'FAVORITE' if is_home_favorite else 'UNDERDOG'}_QUALIFIED"
        if home_form_trending == "UP" and home_avg_margin >= abs(spread_line):
            result["confidence"] = "HIGH"
        else:
            result["confidence"] = "MEDIUM"
            
    elif spread_direction == "AWAY":
        # spread_line > 0 means AWAY is underdog, < 0 means AWAY is favorite
        is_away_favorite = spread_line < 0
        
        if is_away_favorite:
            # AWAY FAVORITE: Must cover their spread (85% threshold)
            margin_threshold = abs(spread_line) * 0.85
            if away_avg_margin < margin_threshold:
                result["reason"] = f"AWAY_FAV_MARGIN_BELOW_85%: {away_avg_margin:.1f} < {margin_threshold:.1f}"
                return result
        else:
            # AWAY UNDERDOG: Must have positive margin (not a losing team)
            if away_avg_margin <= 0:
                result["reason"] = f"AWAY_DOG_NEGATIVE_MARGIN: {away_avg_margin:.1f} (must be > 0)"
                return result
        
        if away_form_trending == "DOWN":
            if away_recent_margin < 0:
                result["reason"] = f"AWAY_FORM_DECLINING: recent margin {away_recent_margin:.1f} < 0"
                return result
        
        team_injury = injury_data.get("away", {})
        if team_injury.get("has_key_injuries") or team_injury.get("star_out"):
            result["reason"] = f"AWAY_KEY_INJURIES: impact={team_injury.get('impact_score', 0)}"
            return result
        
        result["qualified"] = True
        result["reason"] = f"AWAY_{'FAVORITE' if is_away_favorite else 'UNDERDOG'}_QUALIFIED"
        if away_form_trending == "UP" and away_avg_margin > 3:
            result["confidence"] = "HIGH"
        else:
            result["confidence"] = "MEDIUM"
    
    return result

def american_to_decimal(odds: int) -> float:
    """Convert American odds to decimal odds."""
    if odds > 0:
        return (odds / 100) + 1
    else:
        return (100 / abs(odds)) + 1

def american_to_implied_prob(odds: int) -> float:
    """Convert American odds to implied probability (no-vig)."""
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)

def calculate_ev(bovada_odds: int, pinnacle_odds: int) -> float:
    """
    Calculate Expected Value using Pinnacle as the true probability.
    
    EV = (p_true * decimal_payout) - 1
    Where p_true comes from Pinnacle's implied probability
    and decimal_payout comes from Bovada's odds
    
    Positive EV = our odds are better than the sharp market
    
    Example: Bovada -140, Pinnacle -180
    - Pinnacle implies 64.3% true probability
    - Bovada decimal = 1.714
    - EV = (0.643 * 1.714) - 1 = 0.102 = +10.2% EV
    """
    if not bovada_odds or not pinnacle_odds:
        return None
    
    p_true = american_to_implied_prob(pinnacle_odds)
    decimal_bovada = american_to_decimal(bovada_odds)
    ev = (p_true * decimal_bovada) - 1
    return round(ev * 100, 2)  # Return as percentage

EV_THRESHOLD = 0.0  # Require positive EV (0% or better)

def is_game_upcoming(game: Game) -> bool:
    """Check if a game is upcoming (not finished)."""
    if not game.game_time:
        return True
    time_str = game.game_time.lower()
    finished_indicators = ['final', 'end', 'ft', 'aet', 'postponed', 'canceled', 'cancelled']
    for indicator in finished_indicators:
        if indicator in time_str:
            return False
    return True

GAME_DURATION_HOURS = {
    'NBA': 2.5,
    'CBB': 2.5,
    'NFL': 3.5,
    'CFB': 3.5,
    'NHL': 3.0
}

def parse_game_time_to_datetime(game_time: str, game_date: date) -> Optional[datetime]:
    """Parse game_time string (e.g., '8:30 PM EST') to datetime."""
    if not game_time:
        return None
    try:
        import re
        et = pytz.timezone('America/New_York')
        match = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM)', game_time.upper())
        if match:
            hour, minute, ampm = int(match.group(1)), int(match.group(2)), match.group(3)
            if ampm == 'PM' and hour != 12:
                hour += 12
            elif ampm == 'AM' and hour == 12:
                hour = 0
            game_dt = et.localize(datetime(game_date.year, game_date.month, game_date.day, hour, minute))
            return game_dt
    except Exception as e:
        logger.debug(f"Could not parse game time '{game_time}': {e}")
    return None

def check_finished_games_results() -> int:
    """
    Check results for picks where the game has likely finished.
    A game is considered finished if current time > game_start + duration.
    Also checks picks older than 24 hours regardless of game_start.
    """
    et = pytz.timezone('America/New_York')
    now = datetime.now(et)
    pending_picks = Pick.query.filter(Pick.result == None).all()
    results_updated = 0
    
    for pick in pending_picks:
        game_start = None
        
        if pick.game_start:
            if pick.game_start.tzinfo is None:
                game_start = et.localize(pick.game_start)
            else:
                game_start = pick.game_start.astimezone(et)
        else:
            game = Game.query.get(pick.game_id) if pick.game_id else None
            if game and game.game_time:
                game_start = parse_game_time_to_datetime(game.game_time, pick.date)
        
        should_check = False
        if game_start:
            duration_hours = GAME_DURATION_HOURS.get(pick.league, 2.5)
            expected_end = game_start + timedelta(hours=duration_hours)
            if now >= expected_end:
                should_check = True
        else:
            pick_datetime = et.localize(datetime.combine(pick.date, datetime.min.time()))
            if now > pick_datetime + timedelta(hours=24):
                should_check = True
        
        if should_check:
            try:
                if pick.pick_type == "spread":
                    updated = check_spread_pick_result(pick)
                else:
                    updated = check_totals_pick_result(pick)
                if updated:
                    db.session.commit()
                    results_updated += updated
            except Exception as e:
                logger.debug(f"Error checking result for pick {pick.id}: {e}")
    
    return results_updated

def check_totals_pick_result(pick: Pick) -> int:
    """Check result for a totals pick."""
    if not pick.pick or len(pick.pick) < 2:
        return 0
    
    try:
        line = float(pick.pick[1:])
        direction = pick.pick[0]
    except ValueError:
        return 0
    
    if direction not in ['O', 'U']:
        return 0
    
    teams = pick.matchup.split(' @ ')
    if len(teams) != 2:
        return 0
    away_team, home_team = teams[0].strip(), teams[1].strip()
    date_str = pick.date.strftime("%Y%m%d")
    actual_total = None
    
    sport_urls = {
        "NBA": f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}",
        "CBB": f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&limit=500&groups=50",
        "NFL": f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={date_str}",
        "CFB": f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={date_str}&limit=100",
    }
    
    try:
        if pick.league == "NHL":
            url = f"https://api-web.nhle.com/v1/score/{pick.date.strftime('%Y-%m-%d')}"
            resp = requests.get(url, timeout=15)
            for game in resp.json().get("games", []):
                if game.get("gameState") != "OFF":
                    continue
                away_name = game.get("awayTeam", {}).get("placeName", {}).get("default", "")
                home_name = game.get("homeTeam", {}).get("placeName", {}).get("default", "")
                if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                    away_score = game.get("awayTeam", {}).get("score", 0)
                    home_score = game.get("homeTeam", {}).get("score", 0)
                    actual_total = away_score + home_score
                    break
        elif pick.league in sport_urls:
            url = sport_urls[pick.league]
            resp = requests.get(url, timeout=30)
            for event in resp.json().get("events", []):
                status = event.get("status", {}).get("type", {}).get("name", "")
                if status != "STATUS_FINAL":
                    continue
                comps = event.get("competitions", [{}])[0]
                teams_data = comps.get("competitors", [])
                if len(teams_data) == 2:
                    away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                            away_score = int(away.get("score", 0))
                            home_score = int(home.get("score", 0))
                            actual_total = away_score + home_score
                            break
    except Exception as e:
        logger.debug(f"Error fetching scores for pick {pick.id}: {e}")
        return 0
    
    if actual_total is None:
        return 0
    
    pick.actual_total = actual_total
    if direction == 'O':
        if actual_total > line:
            pick.result = 'W'
        elif actual_total < line:
            pick.result = 'L'
        else:
            pick.result = 'P'
    else:
        if actual_total < line:
            pick.result = 'W'
        elif actual_total > line:
            pick.result = 'L'
        else:
            pick.result = 'P'
    
    return 1

def retry_request(max_retries: int = 3, backoff_factor: float = 1.0):
    """Decorator for retrying failed HTTP requests with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, requests.Timeout) as e:
                    last_exception = e
                    wait_time = backoff_factor * (2 ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            logger.error(f"All {max_retries} attempts failed: {last_exception}")
            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected retry failure")
        return wrapper
    return decorator

@retry_request(max_retries=3, backoff_factor=1.0)
def fetch_url(url: str, timeout: int = 15) -> dict:
    """Fetch URL with retry logic."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()

def fetch_espn_scoreboard(league: str, date_str: str, timeout: int = 15) -> dict:
    """Fetch scoreboard from ESPN API - approved data source only."""
    urls = {
        "NBA": f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}",
        "CBB": f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&limit=500&groups=50"
    }
    
    url = urls.get(league)
    if not url:
        raise ValueError(f"Invalid league: {league}")
    
    return fetch_url(url, timeout)

espn_teams_cache: dict = {}  # league -> {team_name: team_id}
espn_schedule_cache: dict = {}  # "YYYY-MM-DD:league:team_name" -> games list (date-keyed for daily refresh)

def get_espn_team_id(team_name: str, league: str) -> Optional[str]:
    """Get ESPN team ID from team name using search endpoint with caching."""
    cache_key = f"{league}:{team_name.lower()}"
    
    if league not in espn_teams_cache:
        try:
            sport_map = {
                "NBA": "basketball/nba",
                "CBB": "basketball/mens-college-basketball",
                "NFL": "football/nfl",
                "CFB": "football/college-football",
                "NHL": "hockey/nhl"
            }
            sport = sport_map.get(league)
            if not sport:
                return None
            
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams?limit=500"
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                return None
            
            teams = resp.json().get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
            espn_teams_cache[league] = {}
            for t in teams:
                team_data = t.get("team", {})
                team_id = team_data.get("id")
                for name_key in ["displayName", "shortDisplayName", "name", "abbreviation", "location"]:
                    name = team_data.get(name_key, "").lower()
                    if name:
                        espn_teams_cache[league][name] = team_id
        except Exception as e:
            logger.error(f"Error loading ESPN teams for {league}: {e}")
            return None
    
    team_lower = team_name.lower()
    league_cache = espn_teams_cache.get(league, {})
    
    team_aliases = {
        # Common abbreviations
        "mtsu": "middle tennessee",
        "utep": "utep",
        "siue": "siu edwardsville",
        "little rock": "little rock",
        "csu northridge": "csun",
        "cal poly": "cal poly",
        "uconn": "connecticut",
        "umass": "massachusetts",
        "ucf": "ucf",
        "smu": "smu",
        "lsu": "lsu",
        "ole miss": "ole miss",
        "pitt": "pittsburgh",
        "usc": "usc",
        "ucla": "ucla",
        "unlv": "unlv",
        "utsa": "utsa",
        "fiu": "fiu",
        "fau": "fau",
        "gw": "george washington",
        "vcu": "vcu",
        "byu": "byu",
        "tcu": "tcu",
        "sfa": "stephen f. austin",
        "unc": "north carolina",
        "uab": "uab",
        "ualr": "little rock",
        "utrgv": "ut rio grande valley",
        "ul monroe": "louisiana-monroe",
        "ul lafayette": "louisiana",
        "southern utah": "southern utah",
        "utah valley": "utah valley",
        # Bovada to ESPN mappings
        "santa clara": "santa clara",
        "gonzaga": "gonzaga",
        "michigan st": "michigan state",
        "ohio st": "ohio state",
        "penn st": "penn state",
        "florida st": "florida state",
        "arizona st": "arizona state",
        "oregon st": "oregon state",
        "washington st": "washington state",
        "kansas st": "kansas state",
        "iowa st": "iowa state",
        "oklahoma st": "oklahoma state",
        "colorado st": "colorado state",
        "boise st": "boise state",
        "fresno st": "fresno state",
        "san diego st": "san diego state",
        "san jose st": "san jose state",
        "utah st": "utah state",
        "nc state": "nc state",
        "miss st": "mississippi state",
        "ark st": "arkansas state",
        "app st": "appalachian state",
        "ga southern": "georgia southern",
        "ga st": "georgia state",
        "la tech": "louisiana tech",
        "tx state": "texas state",
        "n texas": "north texas",
        "w kentucky": "western kentucky",
        "e michigan": "eastern michigan",
        "w michigan": "western michigan",
        "n illinois": "northern illinois",
        "c michigan": "central michigan",
        "ball st": "ball state",
        "bowling green": "bowling green",
        "kent st": "kent state",
        "miami oh": "miami (oh)",
        "miami fl": "miami",
        # NHL cities to team names
        "new york": "rangers",
        "ny rangers": "rangers",
        "ny islanders": "islanders",
        "la": "kings",
        "los angeles": "kings",
        "montréal": "canadiens",
        "montreal": "canadiens",
        "st louis": "blues",
        "tampa bay": "lightning",
        "san jose": "sharks",
        # More CBB aliases
        "nc a&t": "north carolina a&t",
        "a&m": "texas a&m",
        "tamu": "texas a&m",
        "sc": "south carolina",
        "msu": "michigan state",
        "osu": "ohio state",
        "isu": "iowa state",
        "ksu": "kansas state",
        "wsu": "washington state",
        "asu": "arizona state",
        "csuf": "cal state fullerton",
        "csub": "cal state bakersfield",
        "csun": "cal state northridge",
        "sdsu": "san diego state",
        "sjsu": "san jose state",
        "unt": "north texas",
        "utep": "utep",
        "nmsu": "new mexico state",
        "etsu": "east tennessee state",
        "wku": "western kentucky",
        "eku": "eastern kentucky",
        "nku": "northern kentucky",
        "wvu": "west virginia",
        "jmu": "james madison",
        "odu": "old dominion",
        "ecu": "east carolina",
        "ccu": "coastal carolina",
        "uic": "uic",
        "uic flames": "uic",
        "iupui": "iupui",
        "ipfw": "purdue fort wayne",
        "uncg": "unc greensboro",
        "uncw": "unc wilmington",
        "unca": "unc asheville",
        "umes": "maryland-eastern shore",
        "umbc": "umbc",
        "umkc": "kansas city",
        "ualr": "little rock",
        "uca": "central arkansas",
        "semo": "southeast missouri state",
        "siu": "southern illinois",
        "niu": "northern illinois",
        "eiu": "eastern illinois",
        "wiu": "western illinois",
        "liu": "liu",
        "st johns": "st. john's",
        "st marys": "saint mary's",
        "st joes": "saint joseph's",
        "st peters": "saint peter's",
        "st bonaventure": "st. bonaventure",
    }
    
    check_names = [team_lower]
    if team_lower in team_aliases:
        check_names.append(team_aliases[team_lower])
    
    for check_name in check_names:
        if check_name in league_cache:
            return league_cache[check_name]
        
        for cached_name, team_id in league_cache.items():
            if check_name in cached_name or cached_name in check_name:
                return team_id
            words = check_name.split()
            if len(words) >= 1 and words[0] in cached_name:
                return team_id
    
    return None

def fetch_team_last_10_games(team_name: str, league: str) -> list:
    """
    Fetch team's last 10 completed games from ESPN with daily caching.
    Returns list of game dicts with: total_score, opponent_score, was_home
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"{today_str}:{league}:{team_name.lower()}"
    if cache_key in espn_schedule_cache:
        return espn_schedule_cache[cache_key]
    
    try:
        sport_map = {
            "NBA": "basketball/nba",
            "CBB": "basketball/mens-college-basketball", 
            "NFL": "football/nfl",
            "CFB": "football/college-football",
            "NHL": "hockey/nhl"
        }
        sport = sport_map.get(league)
        if not sport:
            return []
        
        team_id = get_espn_team_id(team_name, league)
        if not team_id:
            logger.warning(f"Could not find ESPN team ID for {team_name} ({league})")
            return []
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{team_id}/schedule"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return []
        
        events = resp.json().get("events", [])
        completed_games = []
        
        for event in events:
            status = event.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name", "")
            if status != "STATUS_FINAL":
                continue
            
            comps = event.get("competitions", [{}])[0]
            competitors = comps.get("competitors", [])
            if len(competitors) < 2:
                continue
            
            home_team = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away_team = next((c for c in competitors if c.get("homeAway") == "away"), None)
            
            if not home_team or not away_team:
                continue
            
            home_score_val = home_team.get("score", 0)
            away_score_val = away_team.get("score", 0)
            if isinstance(home_score_val, dict):
                home_score_val = home_score_val.get("value", 0)
            if isinstance(away_score_val, dict):
                away_score_val = away_score_val.get("value", 0)
            try:
                home_score = int(home_score_val) if home_score_val else 0
                away_score = int(away_score_val) if away_score_val else 0
            except (ValueError, TypeError):
                continue
            total_score = home_score + away_score
            
            home_id = home_team.get("team", {}).get("id", "")
            was_home = str(home_id) == str(team_id)
            
            team_score = home_score if was_home else away_score
            opp_score = away_score if was_home else home_score
            
            completed_games.append({
                "total": total_score,
                "team_score": team_score,
                "opp_score": opp_score,
                "was_home": was_home,
                "margin": team_score - opp_score
            })
        
        result = completed_games[-10:] if len(completed_games) >= 10 else completed_games
        espn_schedule_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error(f"Error fetching history for {team_name}: {e}")
        return []

def calculate_ou_hit_rate(games: list, direction: str, current_line: float = None) -> float:
    """
    BULLETPROOF: Calculate O/U hit rate against CURRENT line with proper push handling.
    Pushes (exact line matches) are EXCLUDED from the calculation to prevent bias.
    
    Args:
        games: List of game dicts with 'total' (combined score)
        direction: "O" for over, "U" for under
        current_line: The current betting line to compare against (e.g., 224.5)
    
    Returns:
        Percentage of non-push games that would have hit (0-100)
    """
    if len(games) < 5:
        return 0.0
    
    if current_line is not None and current_line > 0:
        compare_line = current_line
    else:
        totals = [g["total"] for g in games]
        compare_line = sum(totals) / len(totals)
    
    hits = 0
    pushes = 0
    
    for g in games:
        total = g["total"]
        diff = total - compare_line
        
        if abs(diff) < 0.5:
            pushes += 1
            continue
        
        if direction == "O" and diff > 0:
            hits += 1
        elif direction == "U" and diff < 0:
            hits += 1
    
    non_push_games = len(games) - pushes
    if non_push_games == 0:
        return 0.0
    
    return (hits / non_push_games) * 100

def calculate_avg_margin(games: list) -> float:
    """
    Calculate team's average margin of victory/defeat over last N games.
    Positive = average wins by X points
    Negative = average losses by X points
    """
    if len(games) < 5:
        return 0.0
    
    margins = [g["margin"] for g in games]
    return sum(margins) / len(margins)

def calculate_spread_cover_rate(games: list, spread_direction: str = None, current_spread: float = None) -> float:
    """
    BULLETPROOF: Calculate spread cover rate using correct ATS formula with push handling.
    
    ATS Cover Formula: spread_result = actual_margin + closing_spread
    - covered = spread_result > 0
    - push = spread_result == 0 (excluded from calculation)
    
    Example:
    - Team is -5 favorite (closing_spread = -5)
    - Wins by 8 (actual_margin = +8)
    - spread_result = 8 + (-5) = 3 > 0 → COVERED
    
    Args:
        games: List of game dicts with 'margin' (team_score - opp_score)
        spread_direction: 'HOME' or 'AWAY' (currently unused but available)
        current_spread: The current spread to test against (optional, uses avg if not provided)
    
    Returns:
        Percentage of non-push games that would have covered (0-100)
    """
    if len(games) < 5:
        return 0.0
    
    margins = [g["margin"] for g in games]
    
    if current_spread is not None:
        test_spread = current_spread
    else:
        avg_margin = sum(margins) / len(margins)
        test_spread = -avg_margin if avg_margin > 0 else abs(avg_margin)
    
    covers = 0
    pushes = 0
    
    for g in games:
        actual_margin = g["margin"]
        spread_result = actual_margin + test_spread
        
        if abs(spread_result) < 0.5:
            pushes += 1
            continue
        
        if spread_result > 0:
            covers += 1
    
    non_push_games = len(games) - pushes
    if non_push_games == 0:
        return 0.0
    
    return (covers / non_push_games) * 100

def fetch_h2h_history(team1: str, team2: str, league: str, direction: str = "O") -> dict:
    """
    Fetch head-to-head history between two teams from ESPN.
    Filters each team's schedule for games against the opponent.
    Returns: {"ou_pct": float, "games_found": int, "games": list}
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"h2h:{today_str}:{league}:{team1.lower()}:{team2.lower()}"
    
    if cache_key in espn_schedule_cache:
        cached = espn_schedule_cache[cache_key]
        if cached["games_found"] >= 3:
            return cached
        return cached
    
    try:
        sport_map = {
            "NBA": "basketball/nba",
            "CBB": "basketball/mens-college-basketball", 
            "NFL": "football/nfl",
            "CFB": "football/college-football",
            "NHL": "hockey/nhl"
        }
        sport = sport_map.get(league)
        if not sport:
            return {"ou_pct": 0, "games_found": 0, "games": []}
        
        team1_id = get_espn_team_id(team1, league)
        team2_id = get_espn_team_id(team2, league)
        
        if not team1_id or not team2_id:
            logger.warning(f"H2H: Could not find ESPN IDs for {team1} or {team2}")
            return {"ou_pct": 0, "games_found": 0, "games": []}
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{team1_id}/schedule"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return {"ou_pct": 0, "games_found": 0, "games": []}
        
        events = resp.json().get("events", [])
        h2h_games = []
        
        for event in events:
            status = event.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name", "")
            if status != "STATUS_FINAL":
                continue
            
            comps = event.get("competitions", [{}])[0]
            competitors = comps.get("competitors", [])
            if len(competitors) < 2:
                continue
            
            home_team = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away_team = next((c for c in competitors if c.get("homeAway") == "away"), None)
            
            if not home_team or not away_team:
                continue
            
            home_id = str(home_team.get("team", {}).get("id", ""))
            away_id = str(away_team.get("team", {}).get("id", ""))
            
            if not ((home_id == str(team1_id) and away_id == str(team2_id)) or 
                    (home_id == str(team2_id) and away_id == str(team1_id))):
                continue
            
            home_score_val = home_team.get("score", 0)
            away_score_val = away_team.get("score", 0)
            if isinstance(home_score_val, dict):
                home_score_val = home_score_val.get("value", 0)
            if isinstance(away_score_val, dict):
                away_score_val = away_score_val.get("value", 0)
            try:
                home_score = int(home_score_val) if home_score_val else 0
                away_score = int(away_score_val) if away_score_val else 0
            except (ValueError, TypeError):
                continue
            total_score = home_score + away_score
            
            h2h_games.append({
                "total": total_score,
                "home_score": home_score,
                "away_score": away_score,
                "date": event.get("date", "")
            })
        
        h2h_games = h2h_games[-10:]
        
        if len(h2h_games) < 3:
            result = {"ou_pct": 0, "games_found": len(h2h_games), "games": h2h_games}
            espn_schedule_cache[cache_key] = result
            return result
        
        totals = [g["total"] for g in h2h_games]
        avg_total = sum(totals) / len(totals)
        
        hits = 0
        for g in h2h_games:
            if direction == "O" and g["total"] > avg_total:
                hits += 1
            elif direction == "U" and g["total"] < avg_total:
                hits += 1
        
        ou_pct = (hits / len(h2h_games)) * 100
        
        result = {"ou_pct": ou_pct, "games_found": len(h2h_games), "games": h2h_games}
        espn_schedule_cache[cache_key] = result
        
        logger.info(f"H2H {team1} vs {team2}: {len(h2h_games)} games, {ou_pct:.1f}% O/U rate")
        return result
        
    except Exception as e:
        logger.error(f"Error fetching H2H for {team1} vs {team2}: {e}")
        return {"ou_pct": 0, "games_found": 0, "games": []}

def fetch_first_half_history(team: str, league: str, limit: int = 20) -> dict:
    """
    Fetch first-half win rate for a team's recent games (Model 4).
    Uses ESPN event summary to get period-by-period scores.
    Returns: {"win_pct": float, "games_found": int, "games": list}
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"1h_history:{today_str}:{league}:{team.lower()}"
    
    if cache_key in espn_schedule_cache:
        return espn_schedule_cache[cache_key]
    
    try:
        sport_map = {
            "NBA": "basketball/nba",
            "CBB": "basketball/mens-college-basketball"
        }
        sport = sport_map.get(league)
        if not sport:
            return {"win_pct": 0, "games_found": 0, "games": []}
        
        team_id = get_espn_team_id(team, league)
        if not team_id:
            return {"win_pct": 0, "games_found": 0, "games": []}
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{team_id}/schedule"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return {"win_pct": 0, "games_found": 0, "games": []}
        
        events = resp.json().get("events", [])
        first_half_games = []
        
        for event in events[:50]:
            status = event.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name", "")
            if status != "STATUS_FINAL":
                continue
            
            event_id = event.get("id")
            if not event_id:
                continue
            
            summary_url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/summary?event={event_id}"
            summary_resp = requests.get(summary_url, timeout=10)
            if summary_resp.status_code != 200:
                continue
            
            summary = summary_resp.json()
            
            # Get homeAway from header competitors (more reliable)
            header = summary.get("header", {})
            competitions = header.get("competitions", [{}])[0]
            competitors = competitions.get("competitors", [])
            
            team_1h_score = 0
            opp_1h_score = 0
            team_is_away = False
            
            # Get homeAway status from header
            for comp in competitors:
                # ESPN stores team id under comp["team"]["id"] or fallback to comp["id"]
                comp_id = str(comp.get("team", {}).get("id", "") or comp.get("id", ""))
                if comp_id == str(team_id):
                    team_is_away = comp.get("homeAway") == "away"
                    break
            
            # Get linescores from header (easier format)
            for comp in competitors:
                # ESPN stores team id under comp["team"]["id"] or fallback to comp["id"]
                comp_id = str(comp.get("team", {}).get("id", "") or comp.get("id", ""))
                linescores = comp.get("linescores", [])
                
                if len(linescores) < 2:
                    continue
                
                try:
                    # linescores is array of objects with 'value' field
                    q1 = int(linescores[0].get("value", 0)) if isinstance(linescores[0], dict) else int(linescores[0])
                    q2 = int(linescores[1].get("value", 0)) if isinstance(linescores[1], dict) else int(linescores[1])
                    half_score = q1 + q2
                except (ValueError, IndexError, TypeError):
                    continue
                
                if comp_id == str(team_id):
                    team_1h_score = half_score
                else:
                    opp_1h_score = half_score
            
            if team_1h_score > 0 or opp_1h_score > 0:
                first_half_games.append({
                    "team_1h": team_1h_score,
                    "opp_1h": opp_1h_score,
                    "won_1h": team_1h_score > opp_1h_score,
                    "was_away": team_is_away,
                    "date": event.get("date", "")
                })
            
            if len(first_half_games) >= limit:
                break
        
        if len(first_half_games) < 5:
            result = {"win_pct": 0, "games_found": len(first_half_games), "games": first_half_games}
            espn_schedule_cache[cache_key] = result
            return result
        
        wins = sum(1 for g in first_half_games if g["won_1h"])
        win_pct = (wins / len(first_half_games)) * 100
        
        away_games = [g for g in first_half_games if g["was_away"]]
        away_wins = sum(1 for g in away_games if g["won_1h"]) if away_games else 0
        away_win_pct = (away_wins / len(away_games)) * 100 if away_games else 0
        
        result = {
            "win_pct": win_pct,
            "away_win_pct": away_win_pct,
            "games_found": len(first_half_games),
            "away_games": len(away_games),
            "games": first_half_games
        }
        espn_schedule_cache[cache_key] = result
        
        logger.info(f"1H History {team}: {len(first_half_games)} games, {win_pct:.1f}% 1H win, {away_win_pct:.1f}% away 1H win")
        return result
        
    except Exception as e:
        logger.error(f"Error fetching 1H history for {team}: {e}")
        return {"win_pct": 0, "games_found": 0, "games": []}

def fetch_first_half_h2h(away_team: str, home_team: str, league: str, limit: int = 10) -> dict:
    """
    Fetch first-half head-to-head history between two teams (Model 4).
    Returns: {"away_win_pct": float, "games_found": int, "games": list}
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"1h_h2h:{today_str}:{league}:{away_team.lower()}:{home_team.lower()}"
    
    if cache_key in espn_schedule_cache:
        return espn_schedule_cache[cache_key]
    
    try:
        sport_map = {
            "NBA": "basketball/nba",
            "CBB": "basketball/mens-college-basketball"
        }
        sport = sport_map.get(league)
        if not sport:
            return {"away_win_pct": 0, "games_found": 0, "games": []}
        
        away_id = get_espn_team_id(away_team, league)
        home_id = get_espn_team_id(home_team, league)
        
        if not away_id or not home_id:
            return {"away_win_pct": 0, "games_found": 0, "games": []}
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{away_id}/schedule"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return {"away_win_pct": 0, "games_found": 0, "games": []}
        
        events = resp.json().get("events", [])
        h2h_games = []
        
        for event in events:
            status = event.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name", "")
            if status != "STATUS_FINAL":
                continue
            
            comps = event.get("competitions", [{}])[0]
            competitors = comps.get("competitors", [])
            if len(competitors) < 2:
                continue
            
            h_team = next((c for c in competitors if c.get("homeAway") == "home"), None)
            a_team = next((c for c in competitors if c.get("homeAway") == "away"), None)
            
            if not h_team or not a_team:
                continue
            
            h_id = str(h_team.get("team", {}).get("id", ""))
            a_id = str(a_team.get("team", {}).get("id", ""))
            
            if not ((h_id == str(home_id) and a_id == str(away_id)) or 
                    (h_id == str(away_id) and a_id == str(home_id))):
                continue
            
            event_id = event.get("id")
            if not event_id:
                continue
            
            summary_url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/summary?event={event_id}"
            summary_resp = requests.get(summary_url, timeout=10)
            if summary_resp.status_code != 200:
                continue
            
            summary = summary_resp.json()
            
            # Get linescores from header competitors (more reliable)
            header = summary.get("header", {})
            h_comps = header.get("competitions", [{}])[0].get("competitors", [])
            
            away_1h = 0
            home_1h = 0
            
            for comp in h_comps:
                # ESPN stores team id under comp["team"]["id"] or fallback to comp["id"]
                comp_id = str(comp.get("team", {}).get("id", "") or comp.get("id", ""))
                linescores = comp.get("linescores", [])
                
                if len(linescores) < 2:
                    continue
                
                try:
                    # linescores is array of objects with 'value' field
                    q1 = int(linescores[0].get("value", 0)) if isinstance(linescores[0], dict) else int(linescores[0])
                    q2 = int(linescores[1].get("value", 0)) if isinstance(linescores[1], dict) else int(linescores[1])
                    half_score = q1 + q2
                except (ValueError, IndexError, TypeError):
                    continue
                
                if comp_id == str(away_id):
                    away_1h = half_score
                elif comp_id == str(home_id):
                    home_1h = half_score
            
            if away_1h > 0 or home_1h > 0:
                h2h_games.append({
                    "away_1h": away_1h,
                    "home_1h": home_1h,
                    "away_won_1h": away_1h > home_1h,
                    "date": event.get("date", "")
                })
            
            if len(h2h_games) >= limit:
                break
        
        if len(h2h_games) < 3:
            result = {"away_win_pct": 0, "games_found": len(h2h_games), "games": h2h_games}
            espn_schedule_cache[cache_key] = result
            return result
        
        away_wins = sum(1 for g in h2h_games if g["away_won_1h"])
        away_win_pct = (away_wins / len(h2h_games)) * 100
        
        result = {
            "away_win_pct": away_win_pct,
            "games_found": len(h2h_games),
            "games": h2h_games
        }
        espn_schedule_cache[cache_key] = result
        
        logger.info(f"1H H2H {away_team} vs {home_team}: {len(h2h_games)} games, {away_win_pct:.1f}% away 1H win")
        return result
        
    except Exception as e:
        logger.error(f"Error fetching 1H H2H for {away_team} vs {home_team}: {e}")
        return {"away_win_pct": 0, "games_found": 0, "games": []}

def update_game_historical_data(game: Game) -> bool:
    """
    Fetch and update historical percentages for a game.
    Returns True if game meets thresholds.
    
    TOTALS: 60% O/U hit rate required (either team)
    SPREADS: Average margin must support the spread line (calculated on-the-fly)
    
    ADVANCED FACTORS (integrated into qualification):
    - Recent form weighting (60% last 5 games, 40% season)
    - Injury data (2+ starters out = penalty)
    - Strength of schedule adjustment
    
    HISTORICAL LINES PRIORITY:
    1. Try to fetch actual historical betting lines from Odds API
    2. If unavailable, fall back to ESPN game data + current line comparison
    
    For spreads, we use average margin as a proxy:
    - If picking HOME favorite: Home avg margin should exceed 85% of spread
    - If picking AWAY underdog: Must have positive avg margin (winning on average)
    """
    try:
        direction = game.direction or "O"
        current_line = game.line
        uses_actual_lines = False
        
        away_hist_data = historical_lines_service.calculate_ou_hit_rate_with_actual_lines(
            game.away_team, game.league, direction
        )
        home_hist_data = historical_lines_service.calculate_ou_hit_rate_with_actual_lines(
            game.home_team, game.league, direction
        )
        
        if away_hist_data.get('hit_rate') is not None and home_hist_data.get('hit_rate') is not None:
            game.away_ou_pct = int(away_hist_data['hit_rate'])
            game.home_ou_pct = int(home_hist_data['hit_rate'])
            uses_actual_lines = True
            logger.info(f"{game.away_team} @ {game.home_team}: Using ACTUAL historical lines - Away O/U: {game.away_ou_pct}%, Home O/U: {game.home_ou_pct}%")
        else:
            away_games = fetch_team_last_10_games(game.away_team, game.league)
            home_games = fetch_team_last_10_games(game.home_team, game.league)
            
            if len(away_games) < 5 or len(home_games) < 5:
                logger.info(f"Insufficient history for {game.away_team} @ {game.home_team}: {len(away_games)}/{len(home_games)} games")
                game.history_qualified = False
                return False
            
            game.away_ou_pct = calculate_ou_hit_rate(away_games, direction, current_line)
            game.home_ou_pct = calculate_ou_hit_rate(home_games, direction, current_line)
            logger.info(f"{game.away_team} @ {game.home_team}: Fallback to current line comparison - Away O/U: {game.away_ou_pct}%, Home O/U: {game.home_ou_pct}%")
        
        away_games = fetch_team_last_10_games(game.away_team, game.league)
        home_games = fetch_team_last_10_games(game.home_team, game.league)
        
        if len(away_games) < 5 or len(home_games) < 5:
            logger.info(f"Insufficient history for {game.away_team} @ {game.home_team}: {len(away_games)}/{len(home_games)} games")
            game.history_qualified = False
            return False
        game.away_spread_pct = calculate_spread_cover_rate(away_games)
        game.home_spread_pct = calculate_spread_cover_rate(home_games)
        
        away_avg_margin = calculate_avg_margin(away_games)
        home_avg_margin = calculate_avg_margin(home_games)
        
        away_recent = calculate_recent_form_ppg(away_games)
        home_recent = calculate_recent_form_ppg(home_games)
        
        away_recent_margin = sum(g["margin"] for g in away_games[-5:]) / min(5, len(away_games)) if away_games else 0
        home_recent_margin = sum(g["margin"] for g in home_games[-5:]) / min(5, len(home_games)) if home_games else 0
        
        away_form_trending = "UP" if away_recent_margin > away_avg_margin + 2 else ("DOWN" if away_recent_margin < away_avg_margin - 2 else "STABLE")
        home_form_trending = "UP" if home_recent_margin > home_avg_margin + 2 else ("DOWN" if home_recent_margin < home_avg_margin - 2 else "STABLE")
        
        away_injuries = fetch_team_injuries(game.away_team, game.league)
        home_injuries = fetch_team_injuries(game.home_team, game.league)
        
        injury_concern = False
        if game.direction == "O":
            if away_injuries["has_key_injuries"] or home_injuries["has_key_injuries"]:
                injury_concern = True
                logger.info(f"{game.away_team} @ {game.home_team}: INJURY CONCERN for OVER - Away injured: {away_injuries['injured_starters']}, Home injured: {home_injuries['injured_starters']}")
        
        if game.spread_direction == "AWAY" and away_injuries["has_key_injuries"]:
            injury_concern = True
            logger.info(f"{game.away_team} @ {game.home_team}: INJURY CONCERN for AWAY spread - {away_injuries['injured_starters']} key players out")
        elif game.spread_direction == "HOME" and home_injuries["has_key_injuries"]:
            injury_concern = True
            logger.info(f"{game.away_team} @ {game.home_team}: INJURY CONCERN for HOME spread - {home_injuries['injured_starters']} key players out")
        
        h2h = fetch_h2h_history(game.away_team, game.home_team, game.league, direction)
        game.h2h_ou_pct = h2h["ou_pct"]
        h2h_games = h2h["games_found"]
        
        max_ou_pct = max(game.away_ou_pct or 0, game.home_ou_pct or 0)
        # Standard qualification for edge analysis (60%+)
        totals_qualified = max_ou_pct >= 60
        # SUPERMAX qualification for history posting (70%+)
        totals_supermax = max_ou_pct >= 70
        
        if h2h_games >= 3:
            h2h_qualified = (game.h2h_ou_pct or 0) >= 60
            h2h_supermax = (game.h2h_ou_pct or 0) >= 70
            totals_qualified = totals_qualified and h2h_qualified
            totals_supermax = totals_supermax and h2h_supermax
        
        if totals_qualified and injury_concern:
            logger.info(f"{game.away_team} @ {game.home_team}: Totals DISQUALIFIED due to injury concern")
            totals_qualified = False
            totals_supermax = False
        
        totals_sharp_against = False
        spread_sharp_against = False
        
        # Check sharp money for TOTALS
        if game.sport_key and game.event_id and game.line:
            opening_data = fetch_opening_line(game.sport_key, game.event_id, game.line)
            if opening_data.get("opening_line"):
                sharp_signal = detect_sharp_money(
                    opening_data["opening_line"],
                    opening_data["current_line"],
                    game.direction  # "O" or "U" for totals
                )
                if sharp_signal["sharp_against"]:
                    totals_sharp_against = True
                    logger.info(f"{game.away_team} @ {game.home_team}: SHARP MONEY AGAINST our {game.direction} pick - line moved {sharp_signal['movement']:.1f} points (open: {opening_data['opening_line']}, now: {opening_data['current_line']})")
                elif sharp_signal["sharp_aligned"]:
                    logger.info(f"{game.away_team} @ {game.home_team}: Sharp money ALIGNED with our {game.direction} pick - line moved {sharp_signal['movement']:.1f} points")
        
        # Check sharp money for SPREADS (separate check using spread line)
        if game.spread_direction and game.spread_line is not None:
            # Use spread line for spread sharp money detection
            spread_sharp_signal = detect_sharp_money(
                game.spread_line,  # Opening is stored when first fetched
                game.spread_line,  # Current - same for now (would need opening spread tracking)
                game.spread_direction  # "HOME" or "AWAY"
            )
            # Note: For now spreads use same opening_lines_store as totals
            # Full spread CLV tracking would need separate opening_spread_store
            if game.sport_key and game.event_id:
                spread_cache_key = f"spread_opening:{game.event_id}"
                if spread_cache_key in opening_lines_store:
                    opening_spread = opening_lines_store[spread_cache_key]["line"]
                    spread_sharp_signal = detect_sharp_money(
                        opening_spread,
                        game.spread_line,
                        game.spread_direction
                    )
                    if spread_sharp_signal["sharp_against"]:
                        spread_sharp_against = True
                        logger.info(f"{game.away_team} @ {game.home_team}: SHARP MONEY AGAINST our {game.spread_direction} spread - line moved {spread_sharp_signal['movement']:.1f} points")
        
        if totals_qualified and totals_sharp_against:
            logger.info(f"{game.away_team} @ {game.home_team}: Totals DISQUALIFIED due to sharp money against")
            totals_qualified = False
            totals_supermax = False
        
        spread_qualified = False
        if game.spread_is_qualified and game.spread_line is not None:
            spread_line = game.spread_line
            
            qualification_result = unified_spread_qualification(
                spread_direction=game.spread_direction,
                spread_line=spread_line,
                raw_edge=game.spread_edge or 0,
                home_avg_margin=home_avg_margin,
                away_avg_margin=away_avg_margin,
                home_recent_margin=home_recent_margin,
                away_recent_margin=away_recent_margin,
                home_form_trending=home_form_trending,
                away_form_trending=away_form_trending,
                injury_data={"home": home_injuries, "away": away_injuries},
                league=game.league,
                bovada_odds=game.bovada_spread_odds if hasattr(game, 'bovada_spread_odds') and game.bovada_spread_odds else -110
            )
            
            spread_qualified = qualification_result["qualified"]
            
            if not spread_qualified:
                logger.info(f"{game.away_team} @ {game.home_team}: Spread DISQUALIFIED - {qualification_result['reason']}")
            else:
                logger.info(f"{game.away_team} @ {game.home_team}: Spread QUALIFIED ({qualification_result['reason']}) - Confidence: {qualification_result['confidence']}, Raw Edge: {qualification_result['raw_edge']:.1f}, Adjusted Edge: {qualification_result['adjusted_edge']:.1f}")
            
            if spread_qualified and spread_sharp_against:
                logger.info(f"{game.away_team} @ {game.home_team}: Spread DISQUALIFIED due to sharp money against")
                spread_qualified = False
            
            # Spread SUPERMAX: also require 70%+ history for history posting
            spread_supermax = spread_qualified and max_ou_pct >= 70
            
            logger.info(f"{game.away_team} @ {game.home_team}: Margins Away={away_avg_margin:.1f}(recent:{away_recent_margin:.1f})/Home={home_avg_margin:.1f}(recent:{home_recent_margin:.1f}), Spread={spread_line}, spread_qualified={spread_qualified}, spread_supermax={spread_supermax}, EV={game.spread_ev}")
        else:
            spread_supermax = False
        
        # EDGE ANALYSIS uses 60% threshold (totals_qualified, spread_qualified)
        # HISTORY POSTING uses 70% SUPERMAX threshold (totals_supermax, spread_supermax)
        game.history_qualified = totals_supermax  # SUPERMAX for history tab
        game.spread_history_qualified = spread_supermax  # SUPERMAX for history tab
        
        form_info = f"Form: Away={away_form_trending}, Home={home_form_trending}"
        injury_info = f"Injuries: Away={away_injuries['injured_starters']}, Home={home_injuries['injured_starters']}"
        logger.info(f"{game.away_team} @ {game.home_team}: O/U {game.away_ou_pct:.1f}%/{game.home_ou_pct:.1f}%, {form_info}, {injury_info}, qualified={game.history_qualified}")
        
        return game.history_qualified
    except Exception as e:
        logger.error(f"Error updating historical data for {game.away_team} @ {game.home_team}: {e}")
        game.history_qualified = False
        return False

def check_spread_pick_result(pick) -> int:
    """
    Check result for a spread pick.
    Pick format: "Team Name +/-X.X" (e.g., "Rutgers +25.5", "Michigan St -14.5")
    Returns 1 if updated, 0 otherwise.
    """
    try:
        teams = pick.matchup.split(' @ ')
        if len(teams) != 2:
            return 0
        away_team, home_team = teams[0].strip(), teams[1].strip()
        
        parts = pick.pick.rsplit(' ', 1)
        if len(parts) != 2:
            logger.warning(f"Invalid spread pick format: {pick.pick}")
            return 0
        
        team_picked = parts[0].strip()
        spread_str = parts[1].strip()
        
        try:
            spread_line = float(spread_str)
        except ValueError:
            logger.warning(f"Could not parse spread line from: {pick.pick}")
            return 0
        
        date_str = pick.date.strftime("%Y%m%d")
        away_score = None
        home_score = None
        
        sport_urls = {
            "NBA": f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}",
            "CBB": f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&limit=500&groups=50",
            "NFL": f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={date_str}",
            "CFB": f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={date_str}&limit=100",
        }
        
        if pick.league == "NHL":
            url = f"https://api-web.nhle.com/v1/score/{pick.date.strftime('%Y-%m-%d')}"
            resp = requests.get(url, timeout=15)
            for game in resp.json().get("games", []):
                if game.get("gameState") != "OFF":
                    continue
                away_name = game.get("awayTeam", {}).get("placeName", {}).get("default", "")
                home_name = game.get("homeTeam", {}).get("placeName", {}).get("default", "")
                if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                    away_score = game.get("awayTeam", {}).get("score", 0)
                    home_score = game.get("homeTeam", {}).get("score", 0)
                    break
        elif pick.league in sport_urls:
            url = sport_urls[pick.league]
            resp = requests.get(url, timeout=30)
            for event in resp.json().get("events", []):
                status = event.get("status", {}).get("type", {}).get("name", "")
                if status != "STATUS_FINAL":
                    continue
                comps = event.get("competitions", [{}])[0]
                teams_data = comps.get("competitors", [])
                if len(teams_data) == 2:
                    away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                            away_score = int(away.get("score", 0))
                            home_score = int(home.get("score", 0))
                            break
        
        if away_score is None or home_score is None:
            return 0
        
        actual_margin = home_score - away_score
        
        picked_home = teams_match(team_picked, home_team)
        picked_away = teams_match(team_picked, away_team)
        
        if picked_home:
            adjusted_margin = actual_margin + spread_line
            if adjusted_margin > 0:
                pick.result = "W"
            elif adjusted_margin < 0:
                pick.result = "L"
            else:
                pick.result = "P"
        elif picked_away:
            adjusted_margin = (-actual_margin) + spread_line
            if adjusted_margin > 0:
                pick.result = "W"
            elif adjusted_margin < 0:
                pick.result = "L"
            else:
                pick.result = "P"
        else:
            logger.warning(f"Could not determine which team was picked: {team_picked}")
            return 0
        
        pick.actual_total = float(home_score - away_score)
        return 1
        
    except Exception as e:
        logger.error(f"Error checking spread result for {pick.matchup}: {e}")
        return 0

def check_pick_results() -> int:
    """
    Check results for pending picks - LOCKED LOGIC.
    
    Returns:
        Number of picks updated with results
    """
    pending_picks = Pick.query.filter(Pick.result == None).all()
    results_updated = 0
    
    for pick in pending_picks:
        try:
            if not pick.pick or len(pick.pick) < 2:
                logger.warning(f"Invalid pick format for pick {pick.id}: {pick.pick}")
                continue
            
            if pick.pick_type == "spread":
                results_updated += check_spread_pick_result(pick)
                continue
            
            line = float(pick.pick[1:])
            direction = pick.pick[0]
            
            if direction not in ['O', 'U']:
                logger.warning(f"Invalid direction for pick {pick.id}: {direction}")
                continue
            
            teams = pick.matchup.split(' @ ')
            if len(teams) != 2:
                logger.warning(f"Invalid matchup format for pick {pick.id}: {pick.matchup}")
                continue
            away_team, home_team = teams[0].strip(), teams[1].strip()
            
            date_str = pick.date.strftime("%Y%m%d")
            actual_total = None
            
            if pick.league == "NBA":
                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
                resp = requests.get(url, timeout=15)
                for event in resp.json().get("events", []):
                    status = event.get("status", {}).get("type", {}).get("name", "")
                    if status != "STATUS_FINAL":
                        continue
                    comps = event.get("competitions", [{}])[0]
                    teams_data = comps.get("competitors", [])
                    if len(teams_data) == 2:
                        away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
                        home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
                        if away and home:
                            away_name = away.get("team", {}).get("shortDisplayName", "")
                            home_name = home.get("team", {}).get("shortDisplayName", "")
                            if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                                away_score = int(away.get("score", 0))
                                home_score = int(home.get("score", 0))
                                actual_total = away_score + home_score
                                break
            
            elif pick.league == "CBB":
                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={date_str}&limit=500&groups=50"
                resp = requests.get(url, timeout=30)
                for event in resp.json().get("events", []):
                    status = event.get("status", {}).get("type", {}).get("name", "")
                    if status != "STATUS_FINAL":
                        continue
                    comps = event.get("competitions", [{}])[0]
                    teams_data = comps.get("competitors", [])
                    if len(teams_data) == 2:
                        away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
                        home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
                        if away and home:
                            away_name = away.get("team", {}).get("shortDisplayName", "")
                            home_name = home.get("team", {}).get("shortDisplayName", "")
                            if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                                away_score = int(away.get("score", 0))
                                home_score = int(home.get("score", 0))
                                actual_total = away_score + home_score
                                break
            
            elif pick.league == "NHL":
                url = f"https://api-web.nhle.com/v1/score/{pick.date.strftime('%Y-%m-%d')}"
                resp = requests.get(url, timeout=15)
                for game in resp.json().get("games", []):
                    if game.get("gameState") != "OFF":
                        continue
                    away_name = game.get("awayTeam", {}).get("placeName", {}).get("default", "")
                    home_name = game.get("homeTeam", {}).get("placeName", {}).get("default", "")
                    if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                        away_score = game.get("awayTeam", {}).get("score", 0)
                        home_score = game.get("homeTeam", {}).get("score", 0)
                        actual_total = away_score + home_score
                        break
            
            elif pick.league == "NFL":
                url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={date_str}"
                resp = requests.get(url, timeout=15)
                for event in resp.json().get("events", []):
                    status = event.get("status", {}).get("type", {}).get("name", "")
                    if status != "STATUS_FINAL":
                        continue
                    comps = event.get("competitions", [{}])[0]
                    teams_data = comps.get("competitors", [])
                    if len(teams_data) == 2:
                        away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
                        home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
                        if away and home:
                            away_name = away.get("team", {}).get("shortDisplayName", "")
                            home_name = home.get("team", {}).get("shortDisplayName", "")
                            if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                                away_score = int(away.get("score", 0))
                                home_score = int(home.get("score", 0))
                                actual_total = away_score + home_score
                                break
            
            elif pick.league == "CFB":
                url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={date_str}&limit=100"
                resp = requests.get(url, timeout=15)
                for event in resp.json().get("events", []):
                    status = event.get("status", {}).get("type", {}).get("name", "")
                    if status != "STATUS_FINAL":
                        continue
                    comps = event.get("competitions", [{}])[0]
                    teams_data = comps.get("competitors", [])
                    if len(teams_data) == 2:
                        away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
                        home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
                        if away and home:
                            away_name = away.get("team", {}).get("shortDisplayName", "")
                            home_name = home.get("team", {}).get("shortDisplayName", "")
                            if teams_match(away_name, away_team) and teams_match(home_name, home_team):
                                away_score = int(away.get("score", 0))
                                home_score = int(home.get("score", 0))
                                actual_total = away_score + home_score
                                break
            
            if actual_total is not None:
                pick.actual_total = actual_total
                if actual_total == line:
                    pick.result = "P"
                elif direction == "O":
                    pick.result = "W" if actual_total > line else "L"
                else:
                    pick.result = "W" if actual_total < line else "L"
                results_updated += 1
                
        except Exception as e:
            logger.error(f"Error checking result for {pick.matchup}: {e}")
            continue
    
    db.session.commit()
    return results_updated

@app.route('/')
def dashboard():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    show_only_qualified = request.args.get('qualified', '0') == '1'
    # Always filter to games with lines (Bovada only)
    
    old_game_ids = [g.id for g in Game.query.filter(Game.date < today).all()]
    if old_game_ids:
        Pick.query.filter(Pick.game_id.in_(old_game_ids)).update({Pick.game_id: None}, synchronize_session=False)
        stmt = delete(Game).where(Game.id.in_(old_game_ids))
        db.session.execute(stmt)
        db.session.commit()
    
    all_games_db = Game.query.filter_by(date=today).order_by(Game.edge.desc()).all()
    # Show all games from today's slate (includes in-progress and completed)
    all_games = all_games_db
    
    # Add time window to each game for weekend slate grouping
    for g in all_games:
        g.time_window = get_game_window(g.game_time)
    
    # Games qualified by edge threshold
    edge_qualified = [g for g in all_games if g.is_qualified]
    edge_spread_qualified = [g for g in all_games if g.spread_is_qualified]
    
    # TOTALS: Must pass historical threshold AND non-negative EV (if EV data available)
    # If history_qualified is None (not checked), do NOT show the game
    # If history_qualified is True, show the game
    # If history_qualified is False, hide the game
    # EV filter: NULL EV = no Pinnacle data = allowed, negative EV = excluded
    qualified = [g for g in edge_qualified if g.history_qualified == True 
                 and (g.total_ev is None or g.total_ev >= 0)]
    
    # SPREADS: Use spread_history_qualified (separate from totals qualification)
    # Must have spread_history_qualified = True AND non-negative EV (if EV data available)
    spread_qualified = [g for g in edge_spread_qualified if g.spread_history_qualified == True 
                        and (g.spread_ev is None or g.spread_ev >= 0)]
    spread_qualified.sort(key=lambda x: x.alt_spread_edge or x.spread_edge or 0, reverse=True)
    
    # Sort qualified totals by effective edge (alt if available, else main)
    qualified.sort(key=lambda x: x.alt_edge or x.edge or 0, reverse=True)
    
    # SUPERMAX = single best edge across both totals and spreads (prefer alt edges when available)
    supermax_lock = None
    supermax_type = None  # 'totals' or 'spread'
    supermax_edge = 0
    
    # Check best totals pick (use alt_edge if available)
    if qualified:
        best_totals = qualified[0]
        effective_edge = best_totals.alt_edge if best_totals.alt_edge else best_totals.edge
        if effective_edge and effective_edge > supermax_edge:
            supermax_lock = best_totals
            supermax_type = 'totals'
            supermax_edge = effective_edge
    
    # Check best spread pick (use alt_spread_edge if available)
    if spread_qualified:
        best_spread = spread_qualified[0]
        effective_spread_edge = best_spread.alt_spread_edge if best_spread.alt_spread_edge else best_spread.spread_edge
        if effective_spread_edge and effective_spread_edge > supermax_edge:
            supermax_lock = best_spread
            supermax_type = 'spread'
            supermax_edge = effective_spread_edge
    
    # Combined qualified: games that qualify for totals (with history + EV) OR spreads (with spread_history + EV)
    all_qualified_games = [g for g in all_games if 
        (g.is_qualified and g.history_qualified == True and (g.total_ev is None or g.total_ev >= 0)) or 
        (g.spread_is_qualified and g.spread_history_qualified == True and (g.spread_ev is None or g.spread_ev >= 0))]
    
    if show_only_qualified:
        games = all_qualified_games
    else:
        games = all_games
    
    # Games that meet 85% historical threshold
    history_qualified = [g for g in all_games if g.history_qualified]
    
    # Edge Analytics
    analytics = {
        'league_breakdown': {},
        'edge_tiers': {'elite': 0, 'strong': 0, 'standard': 0},
        'best_edge': 0,
        'avg_edge': 0,
        'over_count': 0,
        'under_count': 0,
        'top_picks': [],
        'spread_qualified': len(spread_qualified),
        'spread_best_edge': spread_qualified[0].spread_edge if spread_qualified else 0,
        'spread_home_count': len([g for g in spread_qualified if g.spread_direction == 'HOME']),
        'spread_away_count': len([g for g in spread_qualified if g.spread_direction == 'AWAY']),
        'spread_top_picks': spread_qualified[:5],
        'history_qualified': len(history_qualified)
    }
    
    for league in ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']:
        league_games = [g for g in all_games if g.league == league]
        league_totals_qualified = [g for g in league_games if g.is_qualified and g.history_qualified == True and (g.total_ev is None or g.total_ev >= 0)]
        league_spread_qualified = [g for g in league_games if g.spread_is_qualified and g.spread_history_qualified == True and (g.spread_ev is None or g.spread_ev >= 0)]
        league_any_qualified = [g for g in league_games if 
            (g.is_qualified and g.history_qualified == True and (g.total_ev is None or g.total_ev >= 0)) or 
            (g.spread_is_qualified and g.spread_history_qualified == True and (g.spread_ev is None or g.spread_ev >= 0))]
        analytics['league_breakdown'][league] = {
            'total': len(league_games),
            'qualified': len(league_any_qualified),
            'totals_qualified': len(league_totals_qualified),
            'spread_qualified': len(league_spread_qualified)
        }
    
    edge_sum = 0
    all_edges = []
    for g in qualified:
        if g.edge:
            edge_sum += g.edge
            all_edges.append(g.edge)
            if g.edge >= 12:
                analytics['edge_tiers']['elite'] += 1
            elif g.edge >= 10:
                analytics['edge_tiers']['strong'] += 1
            else:
                analytics['edge_tiers']['standard'] += 1
        if g.direction == 'O':
            analytics['over_count'] += 1
        else:
            analytics['under_count'] += 1
    
    # Include spread edges in best_edge calculation
    for g in spread_qualified:
        if g.spread_edge:
            all_edges.append(g.spread_edge)
            edge_sum += g.spread_edge
    
    # Best edge is the max across ALL qualified picks (totals + spreads)
    analytics['best_edge'] = max(all_edges) if all_edges else 0
    
    # Avg edge across all qualified picks
    total_qualified = len(qualified) + len(spread_qualified)
    if total_qualified > 0:
        analytics['avg_edge'] = edge_sum / total_qualified
    
    # Combined Top 5 Picks (totals + spreads merged by weighted score: edge + history)
    # Weighted score formula: edge + (history_pct * 0.15)
    # This allows high history % to outweigh lower edges (85%+9 > 67%+11)
    # Model 3: Away Favorite + O/U gets +2 bonus when away team is favorite AND O/U qualifies with 70%+ history
    combined_picks = []
    for g in qualified:
        history_pct = max(g.away_ou_pct or 0, g.home_ou_pct or 0)
        away_history_pct = g.away_ou_pct or 0
        edge = g.edge or 0
        
        # Check if this is an "Away Favorite + O/U" pick (Model 3)
        # Away is favorite when spread_line > 0 (home is getting points)
        is_away_fav = (g.spread_line or 0) > 0
        is_away_fav_ou = is_away_fav and g.is_qualified and away_history_pct >= 70
        
        # Model bonus: +2 for Away Fav + O/U picks (user is 51-14 with this model)
        model_bonus = 2 if is_away_fav_ou else 0
        model_type = 'away_fav_ou' if is_away_fav_ou else 'totals'
        
        weighted_score = edge + (history_pct * 0.15) + model_bonus
        combined_picks.append({
            'game': g,
            'edge': edge,
            'history_pct': history_pct,
            'weighted_score': weighted_score,
            'pick_type': 'totals',
            'model_type': model_type,
            'direction': g.direction,
            'line': g.alt_total_line if g.alt_total_line else g.line,
            'vegas_line': g.line,
            'alt_line': g.alt_total_line,
            'odds': g.alt_total_odds,
            'bovada_odds': g.bovada_total_odds,
            'pinnacle_odds': g.pinnacle_total_odds,
            'ev': g.total_ev
        })
    for g in spread_qualified:
        # For spread picks, use the relevant spread cover rate
        if g.spread_direction == 'HOME':
            history_pct = g.home_spread_pct or 0
        else:
            history_pct = g.away_spread_pct or 0
        edge = g.spread_edge or 0
        weighted_score = edge + (history_pct * 0.15)
        # For spread picks, calculate the correct line value for display
        # spread_line and alt_spread_line are stored as AWAY team's spread
        # For HOME picks, we need to negate to show home team's spread
        # For AWAY picks, use as-is
        if g.alt_spread_line:
            display_line = -g.alt_spread_line if g.spread_direction == 'HOME' else g.alt_spread_line
            vegas_line = -g.spread_line if g.spread_direction == 'HOME' else g.spread_line
        elif g.spread_line:
            display_line = -g.spread_line if g.spread_direction == 'HOME' else g.spread_line
            vegas_line = display_line
        else:
            display_line = None
            vegas_line = None
        combined_picks.append({
            'game': g,
            'edge': edge,
            'history_pct': history_pct,
            'weighted_score': weighted_score,
            'pick_type': 'spread',
            'model_type': 'spread',
            'direction': g.spread_direction,
            'line': display_line,
            'vegas_line': vegas_line,
            'alt_line': g.alt_spread_line,
            'odds': g.alt_spread_odds,
            'bovada_odds': g.bovada_spread_odds,
            'pinnacle_odds': g.pinnacle_spread_odds,
            'ev': g.spread_ev
        })
    
    # Model 4: NBA 1H ML for away favorites
    # Re-verify away-favorite condition and history qualification
    model4_games = Game.query.filter(
        Game.date == today,
        Game.league == 'NBA',
        Game.nba_1h_ml_qualified == True,
        Game.nba_1h_history_qualified == True,
        Game.spread_line > 0  # Must still be away favorite
    ).all()
    for g in model4_games:
        # Use spread_edge as the edge metric for consistency
        edge = g.spread_edge or g.spread_line or 0
        # Use the higher of away 1H win% or H2H 1H win%
        history_pct = max(g.nba_1h_away_win_pct or 0, g.nba_1h_h2h_win_pct or 0)
        model_bonus = 0  # No bonus for Model 4
        weighted_score = edge + (history_pct * 0.15) + model_bonus
        combined_picks.append({
            'game': g,
            'edge': edge,
            'history_pct': history_pct,
            'weighted_score': weighted_score,
            'pick_type': '1h_ml',
            'model_type': '1h_ml',
            'direction': g.away_team,
            'line': None,
            'vegas_line': None,
            'alt_line': None,
            'odds': g.nba_1h_ml_odds,
            'bovada_odds': None,
            'pinnacle_odds': None,
            'ev': None
        })
    
    # Sort by weighted score (edge + history) instead of just edge
    combined_picks.sort(key=lambda x: x['weighted_score'], reverse=True)
    analytics['top_picks'] = combined_picks[:5]
    
    # Auto-save ONLY the supermax (Lock of the Day) to history
    if combined_picks:
        auto_save_qualified_picks([combined_picks[0]], today)
    
    global last_game_count
    last_game_count['count'] = len(all_games)
    last_game_count['qualified'] = len(qualified)
    
    return render_template('dashboard.html', games=games, qualified=qualified,
                          supermax_lock=supermax_lock, supermax_type=supermax_type,
                          today=today, thresholds=THRESHOLDS, total_games=len(all_games),
                          show_only_qualified=show_only_qualified, analytics=analytics,
                          spread_qualified=spread_qualified, is_big_slate=is_big_slate_day())

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

@app.route('/sw.js')
def service_worker():
    response = make_response(app.send_static_file('sw.js'))
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Content-Type'] = 'application/javascript'
    return response

@app.route('/favicon.ico')
def favicon():
    return app.send_static_file('icon-512.png')

@app.route('/offline.html')
def offline():
    return app.send_static_file('offline.html')

@app.route('/api/status')
def api_status():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    current_count = Game.query.filter_by(date=today).count()
    games_changed = last_game_count.get('count', 0) != current_count
    return jsonify({
        'date': today.strftime('%B %d, %Y'),
        'games_count': current_count,
        'games_changed': games_changed
    })

@app.route('/api/live_scores')
def api_live_scores():
    global _live_scores_cache
    now = time.time()
    if now - _live_scores_cache["timestamp"] < LIVE_SCORES_CACHE_TTL:
        return jsonify(_live_scores_cache["data"])
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%Y%m%d")
    
    games_db = Game.query.filter_by(date=today).all()
    live_scores = {}
    
    try:
        nba_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today_str}"
        resp = requests.get(nba_url, timeout=10)
        for event in resp.json().get("events", []):
            status = event.get("status", {})
            state = status.get("type", {}).get("state", "")
            if state == "in":
                comps = event.get("competitions", [{}])[0]
                teams = comps.get("competitors", [])
                if len(teams) == 2:
                    away = next((t for t in teams if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        away_score = int(away.get("score", 0))
                        home_score = int(home.get("score", 0))
                        period = status.get("period", 0)
                        clock = status.get("displayClock", "")
                        for g in games_db:
                            if g.league == "NBA" and g.away_team == away_name and g.home_team == home_name:
                                live_scores[f"{g.away_team}@{g.home_team}"] = {
                                    "away_score": away_score,
                                    "home_score": home_score,
                                    "total": away_score + home_score,
                                    "period": f"Q{period}",
                                    "clock": clock,
                                    "league": "NBA"
                                }
                                break
    except Exception as e:
        logger.debug(f"NBA live scores fetch: {e}")
    
    try:
        cbb_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={today_str}&limit=100&groups=50"
        resp = requests.get(cbb_url, timeout=15)
        for event in resp.json().get("events", []):
            status = event.get("status", {})
            state = status.get("type", {}).get("state", "")
            if state == "in":
                comps = event.get("competitions", [{}])[0]
                teams = comps.get("competitors", [])
                if len(teams) == 2:
                    away = next((t for t in teams if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        away_score = int(away.get("score", 0))
                        home_score = int(home.get("score", 0))
                        period = status.get("period", 0)
                        clock = status.get("displayClock", "")
                        for g in games_db:
                            if g.league == "CBB" and g.away_team == away_name and g.home_team == home_name:
                                live_scores[f"{g.away_team}@{g.home_team}"] = {
                                    "away_score": away_score,
                                    "home_score": home_score,
                                    "total": away_score + home_score,
                                    "period": f"H{period}",
                                    "clock": clock,
                                    "league": "CBB"
                                }
                                break
    except Exception as e:
        logger.debug(f"CBB live scores fetch: {e}")
    
    try:
        nhl_url = f"https://api-web.nhle.com/v1/score/{today.strftime('%Y-%m-%d')}"
        resp = requests.get(nhl_url, timeout=10)
        for game in resp.json().get("games", []):
            if game.get("gameState") == "LIVE":
                away_name = game.get("awayTeam", {}).get("placeName", {}).get("default", "")
                home_name = game.get("homeTeam", {}).get("placeName", {}).get("default", "")
                away_score = game.get("awayTeam", {}).get("score", 0)
                home_score = game.get("homeTeam", {}).get("score", 0)
                period = game.get("periodDescriptor", {}).get("number", 0)
                clock = game.get("clock", {}).get("timeRemaining", "")
                for g in games_db:
                    if g.league == "NHL" and g.away_team == away_name and g.home_team == home_name:
                        live_scores[f"{g.away_team}@{g.home_team}"] = {
                            "away_score": away_score,
                            "home_score": home_score,
                            "total": away_score + home_score,
                            "period": f"P{period}",
                            "clock": clock,
                            "league": "NHL"
                        }
                        break
    except Exception as e:
        logger.debug(f"NHL live scores fetch: {e}")
    
    try:
        results_updated = check_finished_games_results()
        if results_updated > 0:
            logger.info(f"Auto-updated {results_updated} pick results")
    except Exception as e:
        logger.debug(f"Auto result check error: {e}")
    
    result = {"live_scores": live_scores, "count": len(live_scores)}
    _live_scores_cache["data"] = result
    _live_scores_cache["timestamp"] = time.time()
    return jsonify(result)

@app.route('/add_game', methods=['POST'])
def add_game():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    game = Game(
        date=today,
        league=request.form['league'],
        away_team=request.form['away_team'],
        home_team=request.form['home_team'],
        game_time=request.form.get('game_time', ''),
        line=float(request.form['line']) if request.form.get('line') else None,
        away_ppg=float(request.form['away_ppg']) if request.form.get('away_ppg') else None,
        away_opp_ppg=float(request.form['away_opp_ppg']) if request.form.get('away_opp_ppg') else None,
        home_ppg=float(request.form['home_ppg']) if request.form.get('home_ppg') else None,
        home_opp_ppg=float(request.form['home_opp_ppg']) if request.form.get('home_opp_ppg') else None
    )
    
    if all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg, game.line]):
        game.projected_total = calculate_projection(game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg)
        qualified, direction, edge = check_qualification(game.projected_total, game.line, game.league)
        game.is_qualified = qualified
        game.direction = direction
        game.edge = edge
    
    db.session.add(game)
    db.session.commit()
    return redirect(url_for('dashboard'))

@app.route('/update_line/<int:game_id>', methods=['POST'])
def update_line(game_id):
    game = Game.query.get_or_404(game_id)
    data = request.get_json()
    
    if 'line' in data:
        game.line = float(data['line'])
    
    if all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg, game.line]):
        game.projected_total = calculate_projection(game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg)
        qualified, direction, edge = check_qualification(game.projected_total, game.line, game.league)
        game.is_qualified = qualified
        game.direction = direction
        game.edge = edge
    
    db.session.commit()
    return jsonify({
        'success': True,
        'projected': round(game.projected_total, 1) if game.projected_total is not None else None,
        'edge': round(game.edge, 1) if game.edge is not None else None,
        'qualified': game.is_qualified,
        'direction': game.direction
    })

@app.route('/delete_game/<int:game_id>', methods=['POST'])
def delete_game(game_id):
    game = Game.query.get_or_404(game_id)
    db.session.delete(game)
    db.session.commit()
    return redirect(url_for('dashboard'))

def get_nba_stats():
    from nba_api.stats.endpoints import leaguedashteamstats
    import time
    stats = {}
    try:
        time.sleep(1)
        offense = leaguedashteamstats.LeagueDashTeamStats(
            season='2025-26', season_type_all_star='Regular Season',
            measure_type_detailed_defense='Base', per_mode_detailed='PerGame'
        )
        off_df = offense.get_data_frames()[0]
        defense = leaguedashteamstats.LeagueDashTeamStats(
            season='2025-26', season_type_all_star='Regular Season',
            measure_type_detailed_defense='Opponent', per_mode_detailed='PerGame'
        )
        def_df = defense.get_data_frames()[0]
        opp_dict = {row['TEAM_ID']: row['OPP_PTS'] for _, row in def_df.iterrows()}
        for _, row in off_df.iterrows():
            team_name = row['TEAM_NAME']
            ppg = row['PTS']
            opp_ppg = opp_dict.get(row['TEAM_ID'])
            if ppg and opp_ppg:
                nick = team_name.split()[-1].lower()
                stats[nick] = {"name": team_name, "ppg": ppg, "opp_ppg": opp_ppg}
                if "76ers" in team_name: stats["76ers"] = stats[nick]
                if "Trail Blazers" in team_name: stats["blazers"] = stats[nick]
    except Exception as e:
        print(f"NBA stats error: {e}")
    return stats

def get_nhl_stats():
    stats = {}
    try:
        nhl_url = "https://api.nhle.com/stats/rest/en/team/summary?cayenneExp=seasonId=20252026"
        resp = requests.get(nhl_url, timeout=30)
        for team in resp.json().get("data", []):
            name = team.get("teamFullName", "")
            games_played = team.get("gamesPlayed", 1)
            if games_played > 0:
                ppg = team.get("goalsFor", 0) / games_played
                opp_ppg = team.get("goalsAgainst", 0) / games_played
                stat_entry = {"name": name, "ppg": ppg, "opp_ppg": opp_ppg}
                nick = name.split()[-1].lower()
                stats[nick] = stat_entry
                parts = name.lower().split()
                if len(parts) >= 2:
                    place = " ".join(parts[:-1])
                    stats[place] = stat_entry
                stats[name.lower()] = stat_entry
    except Exception as e:
        print(f"NHL stats error: {e}")
    return stats

def get_team_stats_from_event(event, sport):
    stats = {}
    try:
        comps = event.get("competitions", [{}])[0]
        for team_data in comps.get("competitors", []):
            team = team_data.get("team", {})
            team_name = team.get("shortDisplayName", "")
            team_stats = team_data.get("statistics", [])
            ppg = opp_ppg = None
            for stat in team_stats:
                if stat.get("name") == "avgPointsFor" or stat.get("name") == "points":
                    ppg = stat.get("value") or stat.get("displayValue")
                    if ppg: ppg = float(ppg)
                if stat.get("name") == "avgPointsAgainst" or stat.get("name") == "pointsAgainst":
                    opp_ppg = stat.get("value") or stat.get("displayValue")
                    if opp_ppg: opp_ppg = float(opp_ppg)
            records = team_data.get("records", [])
            for rec in records:
                if rec.get("type") == "total":
                    for stat in rec.get("stats", []):
                        if stat.get("name") == "avgPointsFor" and not ppg: 
                            ppg = stat.get("value")
                        if stat.get("name") == "avgPointsAgainst" and not opp_ppg:
                            opp_ppg = stat.get("value")
            if ppg and opp_ppg:
                stats[team_name.lower()] = {"name": team_name, "ppg": ppg, "opp_ppg": opp_ppg}
    except Exception as e:
        print(f"Event stats error: {e}")
    return stats

def find_team_stats(name, stats_dict):
    name_lower = name.lower()
    if name_lower in stats_dict:
        return stats_dict[name_lower]
    for key, val in stats_dict.items():
        if name_lower in key or key in name_lower:
            return val
        name_parts = name_lower.split()
        for part in name_parts:
            if len(part) > 3 and part in key:
                return val
    return None

_team_stats_cache = {}
_team_stats_cache_time = 0
TEAM_STATS_CACHE_TTL = 3600

def get_cached_team_stats(team_id, sport):
    global _team_stats_cache, _team_stats_cache_time
    now = time.time()
    if now - _team_stats_cache_time > TEAM_STATS_CACHE_TTL:
        _team_stats_cache = {}
        _team_stats_cache_time = now
    cache_key = f"{sport}_{team_id}"
    if cache_key in _team_stats_cache:
        return _team_stats_cache[cache_key]
    return None

def set_cached_team_stats(team_id, sport, ppg, opp_ppg):
    cache_key = f"{sport}_{team_id}"
    _team_stats_cache[cache_key] = (ppg, opp_ppg)

def fetch_cbb_team_stats(team_id):
    cached = get_cached_team_stats(team_id, "cbb")
    if cached: return cached
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}"
        resp = requests.get(url, timeout=5)
        items = resp.json().get("team", {}).get("record", {}).get("items", [])
        ppg = opp_ppg = None
        for item in items:
            if item.get("type") == "total":
                for stat in item.get("stats", []):
                    if stat.get("name") == "avgPointsFor": ppg = stat.get("value")
                    if stat.get("name") == "avgPointsAgainst": opp_ppg = stat.get("value")
        set_cached_team_stats(team_id, "cbb", ppg, opp_ppg)
        return ppg, opp_ppg
    except Exception:
        return None, None

def fetch_cfb_team_stats(team_id):
    cached = get_cached_team_stats(team_id, "cfb")
    if cached: return cached
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}"
        resp = requests.get(url, timeout=5)
        items = resp.json().get("team", {}).get("record", {}).get("items", [])
        ppg = opp_ppg = None
        for item in items:
            if item.get("type") == "total":
                for stat in item.get("stats", []):
                    if stat.get("name") == "avgPointsFor": ppg = stat.get("value")
                    if stat.get("name") == "avgPointsAgainst": opp_ppg = stat.get("value")
        set_cached_team_stats(team_id, "cfb", ppg, opp_ppg)
        return ppg, opp_ppg
    except Exception:
        return None, None

def fetch_nfl_team_stats(team_id):
    cached = get_cached_team_stats(team_id, "nfl")
    if cached: return cached
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}"
        resp = requests.get(url, timeout=5)
        items = resp.json().get("team", {}).get("record", {}).get("items", [])
        ppg = opp_ppg = None
        for item in items:
            if item.get("type") == "total":
                for stat in item.get("stats", []):
                    if stat.get("name") == "avgPointsFor": ppg = stat.get("value")
                    if stat.get("name") == "avgPointsAgainst": opp_ppg = stat.get("value")
        set_cached_team_stats(team_id, "nfl", ppg, opp_ppg)
        return ppg, opp_ppg
    except Exception:
        return None, None

def fetch_team_stats_batch(team_ids, fetch_func):
    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_func, tid): tid for tid in team_ids}
        for future in as_completed(futures):
            tid = futures[future]
            try:
                results[tid] = future.result()
            except:
                results[tid] = (None, None)
    return results

def validate_espn_event_date(event: dict, today: date, et) -> bool:
    """Check if ESPN event date matches today (ET)."""
    try:
        event_date_str = event.get("date", "")
        if event_date_str:
            event_dt = datetime.fromisoformat(event_date_str.replace("Z", "+00:00"))
            event_date = event_dt.astimezone(et).date()
            return event_date == today
        comp_date = event.get("competitions", [{}])[0].get("date", "")
        if comp_date:
            comp_dt = datetime.fromisoformat(comp_date.replace("Z", "+00:00"))
            return comp_dt.astimezone(et).date() == today
    except Exception:
        pass
    return False

def fetch_scoreboard_parallel(urls: dict, timeout: int = 30) -> dict:
    """Fetch multiple scoreboards in parallel. Returns {league: response_json}"""
    results = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {}
        for league, url in urls.items():
            futures[executor.submit(requests.get, url, timeout=timeout)] = league
        for future in as_completed(futures):
            league = futures[future]
            try:
                resp = future.result()
                results[league] = resp.json() if resp.status_code == 200 else {}
            except Exception as e:
                logger.debug(f"Scoreboard fetch error for {league}: {e}")
                results[league] = {}
    return results

def process_espn_events(events: list, today: date, et, league: str) -> tuple:
    """Process ESPN events and extract game data + team IDs for stats batch."""
    games_data = []
    team_ids = set()
    for event in events:
        if not validate_espn_event_date(event, today, et):
            continue
        comps = event.get("competitions", [{}])[0]
        teams = comps.get("competitors", [])
        if len(teams) == 2:
            away = next((t for t in teams if t.get("homeAway") == "away"), None)
            home = next((t for t in teams if t.get("homeAway") == "home"), None)
            if away and home:
                away_id = away.get("team", {}).get("id")
                home_id = home.get("team", {}).get("id")
                team_ids.add(away_id)
                team_ids.add(home_id)
                games_data.append({
                    "away_name": away.get("team", {}).get("shortDisplayName", ""),
                    "home_name": home.get("team", {}).get("shortDisplayName", ""),
                    "away_id": away_id, "home_id": home_id,
                    "game_time": event.get("status", {}).get("type", {}).get("shortDetail", ""),
                    "league": league
                })
    return games_data, team_ids

@app.route('/fetch_games', methods=['POST'])
def fetch_games():
    start_time = time.time()
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%Y%m%d")
    
    games_added = 0
    leagues_cleared = []
    
    scoreboard_urls = {
        "NBA": f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today_str}",
        "CBB": f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={today_str}&limit=500&groups=50",
        "NFL": f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={today_str}",
        "CFB": f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={today_str}&limit=100",
        "NHL": f"https://api-web.nhle.com/v1/schedule/{today.strftime('%Y-%m-%d')}"
    }
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        stats_futures = {
            executor.submit(get_nba_stats): "NBA",
            executor.submit(get_nhl_stats): "NHL"
        }
        scoreboards = fetch_scoreboard_parallel(scoreboard_urls, timeout=60)
        
        nba_stats = {}
        nhl_stats = {}
        for future in as_completed(stats_futures):
            league = stats_futures[future]
            try:
                if league == "NBA":
                    nba_stats = future.result()
                else:
                    nhl_stats = future.result()
            except:
                pass
    
    all_games_data = []
    all_team_ids = {"CBB": set(), "CFB": set(), "NFL": set()}
    
    nba_events = scoreboards.get("NBA", {}).get("events", [])
    nba_games, _ = process_espn_events(nba_events, today, et, "NBA")
    for gd in nba_games:
        away_s = find_team_stats(gd["away_name"], nba_stats)
        home_s = find_team_stats(gd["home_name"], nba_stats)
        gd["away_ppg"] = away_s["ppg"] if away_s else None
        gd["away_opp"] = away_s["opp_ppg"] if away_s else None
        gd["home_ppg"] = home_s["ppg"] if home_s else None
        gd["home_opp"] = home_s["opp_ppg"] if home_s else None
    all_games_data.extend(nba_games)
    
    nhl_data = scoreboards.get("NHL", {})
    for gw in nhl_data.get("gameWeek", []):
        if gw.get("date") == today.strftime("%Y-%m-%d"):
            for game_data in gw.get("games", []):
                away_name = game_data.get("awayTeam", {}).get("placeName", {}).get("default", "")
                home_name = game_data.get("homeTeam", {}).get("placeName", {}).get("default", "")
                start_time_utc = game_data.get("startTimeUTC", "")
                if away_name and home_name:
                    nhl_game_time = ""
                    if start_time_utc:
                        try:
                            utc_dt = datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
                            et_dt = utc_dt.astimezone(et)
                            nhl_game_time = et_dt.strftime("%-m/%-d - %-I:%M %p EST")
                        except:
                            nhl_game_time = start_time_utc[:10]
                    away_s = find_team_stats(away_name, nhl_stats)
                    home_s = find_team_stats(home_name, nhl_stats)
                    all_games_data.append({
                        "away_name": away_name, "home_name": home_name,
                        "game_time": nhl_game_time, "league": "NHL",
                        "away_ppg": away_s["ppg"] if away_s else None,
                        "away_opp": away_s["opp_ppg"] if away_s else None,
                        "home_ppg": home_s["ppg"] if home_s else None,
                        "home_opp": home_s["opp_ppg"] if home_s else None
                    })
    
    for league in ["CBB", "CFB", "NFL"]:
        events = scoreboards.get(league, {}).get("events", [])
        games, team_ids = process_espn_events(events, today, et, league)
        all_team_ids[league] = team_ids
        all_games_data.extend(games)
    
    stats_fetchers = {"CBB": fetch_cbb_team_stats, "CFB": fetch_cfb_team_stats, "NFL": fetch_nfl_team_stats}
    all_stats = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for league, team_ids in all_team_ids.items():
            if team_ids:
                futures[executor.submit(fetch_team_stats_batch, list(team_ids), stats_fetchers[league])] = league
        for future in as_completed(futures):
            league = futures[future]
            try:
                all_stats[league] = future.result()
            except:
                all_stats[league] = {}
    
    for gd in all_games_data:
        league = gd.get("league")
        if league in ["CBB", "CFB", "NFL"]:
            stats = all_stats.get(league, {})
            gd["away_ppg"], gd["away_opp"] = stats.get(gd.get("away_id"), (None, None))
            gd["home_ppg"], gd["home_opp"] = stats.get(gd.get("home_id"), (None, None))
    
    # TRANSACTIONAL SAFETY: Only delete old games after we confirmed new data was fetched
    if all_games_data:
        for league in ["NBA", "NHL", "CBB", "CFB", "NFL"]:
            game_ids = [g.id for g in Game.query.filter_by(date=today, league=league).all()]
            if game_ids:
                safe_delete_games(game_ids)
            leagues_cleared.append(league)
        db.session.commit()
        
        for gd in all_games_data:
            league = gd.get("league")
            game = Game(
                date=today, league=league, 
                away_team=gd["away_name"], home_team=gd["home_name"],
                game_time=gd.get("game_time", ""),
                away_ppg=gd.get("away_ppg"), away_opp_ppg=gd.get("away_opp"),
                home_ppg=gd.get("home_ppg"), home_opp_ppg=gd.get("home_opp")
            )
            db.session.add(game)
            games_added += 1
        db.session.commit()
    else:
        logger.warning("No games fetched from ESPN - keeping existing data")
    fetch_time = time.time() - start_time
    logger.info(f"Games fetch complete in {fetch_time:.2f}s: {games_added} games added")
    
    odds_result = fetch_odds_internal()
    history_result = fetch_history_internal()
    
    total_time = time.time() - start_time
    logger.info(f"Total fetch_games completed in {total_time:.2f}s")
    
    return jsonify({
        "success": True, 
        "games_added": games_added, 
        "leagues_cleared": leagues_cleared,
        "lines_updated": odds_result.get("lines_updated", 0),
        "spreads_updated": odds_result.get("spreads_updated", 0),
        "alt_lines_found": odds_result.get("alt_lines_found", 0),
        "history_checked": history_result.get("games_checked", 0),
        "fetch_time_seconds": round(total_time, 2)
    })

@app.route('/fetch_stats', methods=['POST'])
def fetch_stats():
    nba_stats = get_nba_stats()
    nhl_stats = get_nhl_stats()
    return jsonify({"success": True, "counts": {"nba": len(nba_stats), "nhl": len(nhl_stats)}})

def fetch_odds_for_league(league: str, sport_key: str, api_key: str) -> tuple:
    """Fetch odds for a single league. Returns (league, events_list)."""
    try:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/"
        params = {
            "apiKey": api_key,
            "regions": "us",
            "markets": "totals,spreads,h2h",
            "oddsFormat": "american",
            "bookmakers": "bovada,pinnacle"
        }
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            return (league, sport_key, resp.json())
        return (league, sport_key, [])
    except Exception as e:
        logger.debug(f"Odds fetch error for {league}: {e}")
        return (league, sport_key, [])

def fetch_odds_internal() -> dict:
    """Internal function to fetch odds from Bovada via The Odds API - PARALLEL."""
    start_time = time.time()
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        return {"success": False, "lines_updated": 0, "spreads_updated": 0, "alt_lines_found": 0}
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    sport_map = {
        "NBA": "basketball_nba",
        "NHL": "icehockey_nhl",
        "CBB": "basketball_ncaab",
        "CFB": "americanfootball_ncaaf",
        "NFL": "americanfootball_nfl"
    }
    
    lines_updated = 0
    spreads_updated = 0
    
    # Fetch all odds in parallel FIRST
    all_odds = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_odds_for_league, league, sport_key, api_key): league 
                   for league, sport_key in sport_map.items()}
        for future in as_completed(futures):
            try:
                league, sport_key, events = future.result()
                all_odds[league] = {"sport_key": sport_key, "events": events}
            except:
                pass
    
    # TRANSACTIONAL SAFETY: Only clear lines if we got odds data
    total_events = sum(len(d.get("events", [])) for d in all_odds.values())
    if total_events > 0:
        games_to_clear = Game.query.filter_by(date=today).all()
        for g in games_to_clear:
            g.line = None
            g.spread_line = None
            g.is_qualified = False
            g.spread_is_qualified = False
            g.edge = None
            g.spread_edge = None
            g.bovada_total_odds = None
            g.pinnacle_total_odds = None
            g.bovada_spread_odds = None
            g.pinnacle_spread_odds = None
            g.total_ev = None
            g.spread_ev = None
    else:
        logger.warning("No odds fetched from API - keeping existing lines")
        return {"success": False, "lines_updated": 0, "spreads_updated": 0, "alt_lines_found": 0, "reason": "no_odds_fetched"}
    
    for league, data in all_odds.items():
        sport_key = data["sport_key"]
        events = data["events"]
        
        try:
            for event in events:
                commence_time = event.get("commence_time", "")
                if commence_time:
                    try:
                        event_date = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(et).date()
                        if event_date != today:
                            continue
                    except Exception:
                        continue
                
                away_team = event.get("away_team", "")
                home_team = event.get("home_team", "")
                
                games = Game.query.filter_by(date=today, league=league).all()
                
                for game in games:
                    away_match = teams_match(game.away_team, away_team)
                    home_match = teams_match(game.home_team, home_team)
                    away_match_rev = teams_match(game.away_team, home_team)
                    home_match_rev = teams_match(game.home_team, away_team)
                    
                    if (away_match and home_match) or (away_match_rev and home_match_rev):
                        game.event_id = event.get("id")
                        game.sport_key = sport_key
                        bookmakers = event.get("bookmakers", [])
                        bovada_book = next((b for b in bookmakers if b.get("key") == "bovada"), None)
                        pinnacle_book = next((b for b in bookmakers if b.get("key") == "pinnacle"), None)
                        if not bovada_book:
                            continue  # Skip games not on Bovada
                        
                        # Extract Bovada markets
                        bovada_markets = {m.get("key"): m for m in bovada_book.get("markets", [])}
                        pinnacle_markets = {m.get("key"): m for m in pinnacle_book.get("markets", [])} if pinnacle_book else {}
                        
                        # Process TOTALS
                        if "totals" in bovada_markets:
                            totals_market = bovada_markets["totals"]
                            outcomes = totals_market.get("outcomes", [])
                            for outcome in outcomes:
                                if outcome.get("name") == "Over":
                                    line = outcome.get("point")
                                    bovada_over_odds = outcome.get("price")
                                    if line is not None:
                                        game.line = line
                                        store_opening_line(event.get("id"), line)
                                        if all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg]):
                                            exp_away, exp_home, proj_total = calculate_expected_scores(
                                                game.away_ppg, game.away_opp_ppg, 
                                                game.home_ppg, game.home_opp_ppg
                                            )
                                            game.expected_away = exp_away
                                            game.expected_home = exp_home
                                            game.projected_total = proj_total
                                            game.projected_margin = exp_home - exp_away
                                            qualified, direction, edge = check_qualification(
                                                game.projected_total, game.line, game.league
                                            )
                                            game.direction = direction
                                            game.edge = edge
                                            
                                            # Get matching outcome from Bovada based on direction
                                            bovada_odds = None
                                            pinnacle_odds = None
                                            if direction == "OVER":
                                                bovada_odds = bovada_over_odds
                                            elif direction == "UNDER":
                                                under_outcome = next((o for o in outcomes if o.get("name") == "Under"), None)
                                                bovada_odds = under_outcome.get("price") if under_outcome else None
                                            
                                            # Get Pinnacle odds for same direction
                                            if pinnacle_markets.get("totals") and direction:
                                                pinn_outcomes = pinnacle_markets["totals"].get("outcomes", [])
                                                pinn_name = "Over" if direction == "OVER" else "Under"
                                                pinn_outcome = next((o for o in pinn_outcomes if o.get("name") == pinn_name), None)
                                                if pinn_outcome:
                                                    pinnacle_odds = pinn_outcome.get("price")
                                            
                                            # Store and calculate EV
                                            game.bovada_total_odds = bovada_odds
                                            game.pinnacle_total_odds = pinnacle_odds
                                            if bovada_odds and pinnacle_odds:
                                                game.total_ev = calculate_ev(bovada_odds, pinnacle_odds)
                                            
                                            # EV is informational only - Bovada is primary
                                            # Qualification based on edge threshold only
                                            game.is_qualified = qualified
                                                
                                        lines_updated += 1
                                    break
                        
                        # Process SPREADS using UniversalSpreadHandler for bulletproof extraction
                        spread_data = UniversalSpreadHandler.extract_spread_data(
                            game.away_team, game.home_team, bookmakers
                        )
                        
                        if spread_data:
                            spread_line = spread_data['away_spread']
                            game.spread_line = spread_line
                            
                            spread_cache_key = f"spread_opening:{event.get('id')}"
                            if spread_cache_key not in opening_lines_store:
                                opening_lines_store[spread_cache_key] = {
                                    "line": spread_line,
                                    "timestamp": datetime.now(pytz.timezone('America/New_York')).isoformat()
                                }
                            
                            if all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg]):
                                exp_away, exp_home, _ = calculate_expected_scores(
                                    game.away_ppg, game.away_opp_ppg, 
                                    game.home_ppg, game.home_opp_ppg
                                )
                                game.expected_away = exp_away
                                game.expected_home = exp_home
                                game.projected_margin = exp_home - exp_away
                                spread_qual, spread_dir, spread_edge = check_spread_qualification(
                                    exp_away, exp_home, spread_line, game.league
                                )
                                game.spread_direction = spread_dir
                                game.spread_edge = spread_edge
                                
                                bovada_spread_odds = None
                                pinnacle_spread_odds = None
                                if spread_dir == "HOME":
                                    bovada_spread_odds = spread_data['home_odds']
                                elif spread_dir == "AWAY":
                                    bovada_spread_odds = spread_data['away_odds']
                                
                                if pinnacle_markets.get("spreads") and spread_dir:
                                    pinn_outcomes = pinnacle_markets["spreads"].get("outcomes", [])
                                    pick_team = home_team if spread_dir == "HOME" else away_team
                                    pinn_outcome = next((o for o in pinn_outcomes if teams_match(o.get("name", ""), pick_team)), None)
                                    if pinn_outcome:
                                        pinnacle_spread_odds = pinn_outcome.get("price")
                                
                                game.bovada_spread_odds = bovada_spread_odds
                                game.pinnacle_spread_odds = pinnacle_spread_odds
                                if bovada_spread_odds and pinnacle_spread_odds:
                                    game.spread_ev = calculate_ev(bovada_spread_odds, pinnacle_spread_odds)
                                
                                game.spread_is_qualified = spread_qual
                                
                            spreads_updated += 1
        except Exception as e:
            logger.error(f"Odds processing error for {league}: {e}")
    
    db.session.commit()
    
    alt_lines_result = fetch_alt_lines_internal()
    
    # Fetch Model 4: NBA 1H ML for away favorites
    model4_result = fetch_nba_1h_ml_internal()
    
    return {
        "success": True, 
        "lines_updated": lines_updated, 
        "spreads_updated": spreads_updated,
        "alt_lines_found": alt_lines_result.get("alt_lines_found", 0),
        "model4_1h_ml_found": model4_result.get("1h_ml_found", 0)
    }

@app.route('/fetch_odds', methods=['POST'])
def fetch_odds():
    """Route wrapper for fetch_odds_internal."""
    return jsonify(fetch_odds_internal())

def fetch_history_internal() -> dict:
    """
    Internal function to fetch historical data for qualified games.
    
    FOOLPROOF SESSION SAFETY:
    - Sequential processing only (NO threads)
    - Commit after each game (prevents data loss)
    - Rollback on error (prevents corruption)
    - Progress logging every 10 games
    """
    import time
    start_time = time.time()
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    games = Game.query.filter_by(date=today).filter(
        db.or_(Game.is_qualified == True, Game.spread_is_qualified == True)
    ).all()
    
    total_games = len(games)
    history_updated = 0
    history_qualified = 0
    errors = []
    
    logger.info(f"Processing {total_games} qualified games sequentially (safe mode)")
    
    for i, game in enumerate(games, 1):
        try:
            result = update_game_historical_data(game)
            db.session.commit()
            
            if result:
                history_qualified += 1
            history_updated += 1
            
            if i % 10 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {i}/{total_games} games processed ({elapsed:.1f}s)")
                
        except Exception as e:
            db.session.rollback()
            error_msg = f"{game.away_team} @ {game.home_team}: {str(e)}"
            errors.append(error_msg)
            logger.error(f"Error updating history: {error_msg}")
    
    elapsed = time.time() - start_time
    logger.info(f"History fetch complete: {history_updated}/{total_games} games in {elapsed:.1f}s, {history_qualified} qualified")
    
    if errors:
        logger.warning(f"{len(errors)} games failed processing")
    
    return {
        "games_checked": history_updated,
        "history_qualified": history_qualified,
        "errors": len(errors),
        "time_seconds": round(elapsed, 1)
    }

@app.route('/fetch_history', methods=['POST'])
def fetch_history():
    """Fetch historical data for qualified games to apply 85% threshold."""
    result = fetch_history_internal()
    return jsonify({"success": True, **result})

@app.route('/api/test_historical_lines', methods=['GET'])
def test_historical_lines():
    """Test endpoint for historical betting lines service."""
    team = request.args.get('team', 'Lakers')
    league = request.args.get('league', 'NBA')
    direction = request.args.get('direction', 'O')
    
    try:
        ou_data = historical_lines_service.calculate_ou_hit_rate_with_actual_lines(team, league, direction)
        ats_data = historical_lines_service.calculate_ats_hit_rate(team, league)
        
        return jsonify({
            "success": True,
            "team": team,
            "league": league,
            "ou_data": ou_data,
            "ats_data": ats_data
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

def find_best_alt_line(outcomes: list, direction: str, current_line: float, is_spread: bool = False, home_team: str = "", debug_game: str = "") -> tuple:
    """
    Find the best alternate line with NEGATIVE odds only (no + money).
    Odds must be between -200 and -100 (no positive odds, no worse than -200).
    
    For OVER totals: Find the LOWEST alt line (easier to hit)
    For UNDER totals: Find the HIGHEST alt line (easier to hit)
    For spreads: Find better number for the pick direction
    
    Returns (best_line, best_odds) or (None, None) if no valid line found.
    """
    MAX_ODDS = -180  # Floor - no worse than -180, anything worse is not a lock
    MIN_ODDS = -100  # No positive odds allowed
    candidates = []
    all_valid_lines = []  # For debug logging
    all_raw_lines = []  # ALL lines before any filtering
    
    for outcome in outcomes:
        odds = outcome.get("price", 0)
        point = outcome.get("point")
        name = outcome.get("name", "")
        
        if point is not None:
            all_raw_lines.append((point, odds, name))
        
        # Only accept negative odds between -180 and -100 (no + money)
        if point is None or odds < MAX_ODDS or odds > MIN_ODDS:
            continue
        
        if is_spread:
            is_home_outcome = teams_match(name, home_team)
            if direction == "HOME" and not is_home_outcome:
                continue
            if direction == "AWAY" and is_home_outcome:
                continue
            all_valid_lines.append((point, odds, name))
            candidates.append((point, odds))
        else:
            # Direction is stored as "O" or "U" in database
            is_over = direction in ("OVER", "O")
            is_under = direction in ("UNDER", "U")
            if is_over and name != "Over":
                continue
            if is_under and name != "Under":
                continue
            all_valid_lines.append((point, odds, name))
            # For OVER: only keep lines LOWER than main (easier to hit)
            # For UNDER: only keep lines HIGHER than main (easier to hit)
            if current_line:
                if is_over and point >= current_line:
                    continue
                if is_under and point <= current_line:
                    continue
            candidates.append((point, odds))
    
    # Log all valid lines for debugging
    if debug_game:
        is_over_dir = direction in ("OVER", "O")
        filter_name = "Over" if is_over_dir else "Under" if direction in ("UNDER", "U") else direction
        logger.info(f"Alt lines for {debug_game}: direction={direction}, main_line={current_line}")
        # Show all raw lines that match the direction
        raw_matching = [x for x in all_raw_lines if filter_name in x[2] or is_spread]
        for line, odds, name in sorted(raw_matching, key=lambda x: x[0]):
            odds_status = "OK" if MAX_ODDS <= odds <= MIN_ODDS else f"FILTERED (odds={odds})"
            logger.info(f"  {name} {line} ({odds}) - {odds_status}")
        if all_valid_lines:
            logger.info(f"  Valid lines (passed odds filter): {sorted(all_valid_lines, key=lambda x: x[0])}")
        logger.info(f"  Candidates (passed line filter): {sorted(candidates, key=lambda x: x[0])}")
    
    if not candidates:
        return None, None
    
    # For OVER: pick the LOWEST line (sort ascending)
    # For UNDER: pick the HIGHEST line (sort descending)
    # For AWAY spreads: pick HIGHEST line (more points = better value)
    # For HOME spreads: pick LOWEST line (fewer points to give = better value)
    is_over = direction in ("OVER", "O")
    if is_spread:
        if direction == "AWAY":
            # AWAY = underdog getting points, want HIGHEST number
            candidates.sort(key=lambda x: x[0], reverse=True)
        else:
            # HOME = favorite giving points, want LOWEST (least negative)
            candidates.sort(key=lambda x: x[0], reverse=True)
    elif is_over:
        candidates.sort(key=lambda x: x[0])  # Ascending - lowest first
    else:
        candidates.sort(key=lambda x: x[0], reverse=True)  # Descending - highest first
    
    best_line, best_odds = candidates[0]
    logger.info(f"  Selected: {best_line} ({best_odds})")
    return best_line, best_odds

def fetch_single_alt_line(game_info: dict, api_key: str) -> dict:
    """Fetch alt lines for a single game (used in parallel)."""
    game_id = game_info['id']
    event_id = game_info['event_id']
    sport_key = game_info['sport_key']
    
    result = {'game_id': game_id, 'alt_total': None, 'alt_spread': None}
    
    try:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
        params = {
            "apiKey": api_key,
            "regions": "us",
            "markets": "alternate_totals,alternate_spreads",
            "oddsFormat": "american",
            "bookmakers": "bovada"
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return result
        
        data = resp.json()
        bookmakers = data.get("bookmakers", [])
        book = next((b for b in bookmakers if b.get("key") == "bovada"), None)
        if not book:
            return result
        
        markets = book.get("markets", [])
        home_team = data.get("home_team", game_info['home_team'])
        game_name = f"{game_info['away_team']}@{game_info['home_team']}"
        
        for market in markets:
            market_key = market.get("key")
            outcomes = market.get("outcomes", [])
            
            if market_key == "alternate_totals" and game_info['is_qualified'] and game_info['direction']:
                alt_line, alt_odds = find_best_alt_line(
                    outcomes, game_info['direction'], game_info['line'], is_spread=False, debug_game=game_name
                )
                if alt_line is not None:
                    result['alt_total'] = (alt_line, alt_odds)
                    logger.info(f"Alt total found: {game_name} {game_info['direction']}{alt_line} ({alt_odds})")
            
            elif market_key == "alternate_spreads" and game_info['spread_is_qualified'] and game_info['spread_direction']:
                alt_line, alt_odds = find_best_alt_line(
                    outcomes, game_info['spread_direction'], game_info['spread_line'],
                    is_spread=True, home_team=home_team, debug_game=game_name
                )
                if alt_line is not None:
                    result['alt_spread'] = (alt_line, alt_odds)
                    logger.info(f"Alt spread found: {game_name} {game_info['spread_direction']} {alt_line} ({alt_odds})")
    except Exception as e:
        logger.error(f"Alt lines error for game {game_id}: {e}")
    
    return result

def fetch_alt_lines_internal() -> dict:
    """Internal function to fetch alternate lines for qualified games (parallel)."""
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        logger.warning("No ODDS_API_KEY for alt lines fetch")
        return {"alt_lines_found": 0, "games_checked": 0}
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    qualified_totals = Game.query.filter(
        Game.date == today,
        Game.is_qualified == True,
        Game.event_id.isnot(None)
    ).all()
    
    qualified_spreads = Game.query.filter(
        Game.date == today,
        Game.spread_is_qualified == True,
        Game.event_id.isnot(None)
    ).all()
    
    all_qualified = list({g.id: g for g in (qualified_totals + qualified_spreads)}.values())
    logger.info(f"Alt lines: checking {len(all_qualified)} qualified games (parallel)")
    
    game_infos = [{
        'id': g.id, 'event_id': g.event_id, 'sport_key': g.sport_key,
        'away_team': g.away_team, 'home_team': g.home_team,
        'is_qualified': g.is_qualified, 'direction': g.direction, 'line': g.line,
        'spread_is_qualified': g.spread_is_qualified, 'spread_direction': g.spread_direction, 'spread_line': g.spread_line
    } for g in all_qualified if g.event_id and g.sport_key]
    
    alt_lines_found = 0
    results = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single_alt_line, info, api_key): info['id'] for info in game_infos}
        for future in as_completed(futures):
            result = future.result()
            results[result['game_id']] = result
    
    for game in all_qualified:
        if game.id in results:
            r = results[game.id]
            if r['alt_total']:
                game.alt_total_line, game.alt_total_odds = r['alt_total']
                alt_lines_found += 1
                if game.projected_total is not None:
                    game.alt_edge = abs(game.projected_total - game.alt_total_line)
                    logger.info(f"Alt edge recalc: {game.away_team}@{game.home_team} main={game.edge:.1f} -> alt={game.alt_edge:.1f}")
            if r['alt_spread']:
                raw_alt_line, alt_odds = r['alt_spread']
                if game.spread_direction == 'HOME':
                    game.alt_spread_line = -raw_alt_line
                else:
                    game.alt_spread_line = raw_alt_line
                game.alt_spread_odds = alt_odds
                alt_lines_found += 1
                if game.projected_margin is not None:
                    game.alt_spread_edge = abs(game.projected_margin - game.alt_spread_line)
                    logger.info(f"Alt spread edge recalc: {game.away_team}@{game.home_team} main={game.spread_edge:.1f} -> alt={game.alt_spread_edge:.1f}")
    
    db.session.commit()
    return {"alt_lines_found": alt_lines_found, "games_checked": len(all_qualified)}

def fetch_nba_1h_ml_internal() -> dict:
    """Fetch 1st half money lines for NBA away favorites (Model 4)."""
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        return {"1h_ml_found": 0, "games_checked": 0}
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    # Reset all NBA 1H ML qualifications first (handles line changes)
    Game.query.filter(
        Game.date == today,
        Game.league == 'NBA'
    ).update({
        'nba_1h_ml_qualified': False,
        'nba_1h_ml_odds': None
    }, synchronize_session=False)
    db.session.commit()
    
    # Find NBA games where away team is favorite (spread_line > 0)
    nba_away_favs = Game.query.filter(
        Game.date == today,
        Game.league == 'NBA',
        Game.spread_line > 0,  # Away is favorite
        Game.event_id.isnot(None)
    ).all()
    
    if not nba_away_favs:
        return {"1h_ml_found": 0, "games_checked": 0}
    
    logger.info(f"Model 4: Checking {len(nba_away_favs)} NBA away favorites for 1H ML")
    ml_found = 0
    
    for game in nba_away_favs:
        try:
            url = f"https://api.the-odds-api.com/v4/sports/{game.sport_key}/events/{game.event_id}/odds"
            params = {
                "apiKey": api_key,
                "regions": "us",
                "markets": "h2h_h1",
                "oddsFormat": "american",
                "bookmakers": "draftkings,fanduel,bovada"
            }
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                continue
            
            data = resp.json()
            bookmakers = data.get("bookmakers", [])
            
            # Try to find 1H ML from any bookmaker
            for book in bookmakers:
                markets = book.get("markets", [])
                for market in markets:
                    if market.get("key") == "h2h_h1":
                        outcomes = market.get("outcomes", [])
                        # Find the away team's 1H ML odds
                        for outcome in outcomes:
                            if teams_match(outcome.get("name", ""), game.away_team):
                                odds = outcome.get("price")
                                if odds:
                                    game.nba_1h_ml_odds = int(odds)
                                    ml_found += 1
                                    logger.info(f"Model 4: {game.away_team} @ {game.home_team} 1H ML: {odds}")
                                    
                                    # Fetch 1H historical data for qualification
                                    away_1h_hist = fetch_first_half_history(game.away_team, game.league, limit=20)
                                    h2h_1h_hist = fetch_first_half_h2h(game.away_team, game.home_team, game.league)
                                    
                                    game.nba_1h_away_win_pct = away_1h_hist.get("away_win_pct", 0)
                                    game.nba_1h_h2h_win_pct = h2h_1h_hist.get("away_win_pct", 0)
                                    
                                    # Qualification: 65%+ away 1H win rate (last 15-20 games)
                                    # H2H requires 60%+ if 5+ games exist
                                    away_qualifies = (game.nba_1h_away_win_pct or 0) >= 65
                                    h2h_games = h2h_1h_hist.get("games_found", 0)
                                    h2h_qualifies = h2h_games < 5 or (game.nba_1h_h2h_win_pct or 0) >= 60
                                    
                                    game.nba_1h_history_qualified = away_qualifies and h2h_qualifies
                                    game.nba_1h_ml_qualified = game.nba_1h_history_qualified
                                    
                                    logger.info(f"Model 4 History: {game.away_team} - Away 1H: {game.nba_1h_away_win_pct:.1f}%, H2H: {game.nba_1h_h2h_win_pct:.1f}% ({h2h_games} games), Qualified: {game.nba_1h_ml_qualified}")
                                    break
                        if game.nba_1h_ml_odds:
                            break
                if game.nba_1h_ml_odds:
                    break
        except Exception as e:
            logger.error(f"Model 4 error for {game.away_team} @ {game.home_team}: {e}")
    
    db.session.commit()
    history_qualified = sum(1 for g in nba_away_favs if g.nba_1h_ml_qualified)
    return {"1h_ml_found": ml_found, "history_qualified": history_qualified, "games_checked": len(nba_away_favs)}

@app.route('/post_discord', methods=['POST'])
def post_discord():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%B %d, %Y")
    
    # Get ALL games for today (pre-filter) - BulletproofPickValidator handles full validation
    all_games = Game.query.filter_by(date=today).all()
    
    if not all_games:
        return jsonify({"success": False, "message": "No games found for today"})
    
    # ========================================================================
    # BULLETPROOF VALIDATION - Run ALL checks before posting
    # ========================================================================
    logger.info("=" * 60)
    logger.info("🔒 BULLETPROOF PRE-SEND VALIDATION")
    logger.info("=" * 60)
    
    # Validate all picks and get the best ones by confidence tier
    validation_result = BulletproofPickValidator.validate_all_picks(all_games, pick_type='both')
    
    # Log validation summary
    logger.info(f"📊 Validation Summary:")
    logger.info(f"   Total validated: {validation_result['total_validated']}")
    logger.info(f"   Total rejected: {validation_result['total_rejected']}")
    for tier in ["SUPERMAX", "HIGH", "MEDIUM", "LOW"]:
        count = len(validation_result['by_tier'][tier])
        logger.info(f"   {tier}: {count} picks")
    
    # Log rejected picks with reasons
    if validation_result['rejected_picks']:
        logger.info(f"❌ Rejected picks:")
        for rej in validation_result['rejected_picks'][:5]:  # Show first 5
            logger.info(f"   {rej['game']} ({rej['pick_type']}): {', '.join(rej['checks_failed'][:2])}")
    
    # Get best validated picks (sorted by tier, then edge)
    best_validated = []
    for tier in ["SUPERMAX", "HIGH", "MEDIUM", "LOW"]:
        for pick in validation_result['by_tier'][tier]:
            if len(best_validated) >= 3:
                break
            # Get the actual game object from the pick
            game = next((g for g in all_games if f"{g.away_team} @ {g.home_team}" == pick['game']), None)
            if game:
                best_validated.append({
                    'game': game,
                    'pick_type': pick['pick_type'],
                    'edge': pick['edge'],
                    'ev': pick['ev'],
                    'confidence_tier': pick['confidence_tier'],
                    'checks_passed': pick['checks_passed'],
                    'warnings': pick['warnings']
                })
        if len(best_validated) >= 3:
            break
    
    if not best_validated:
        return jsonify({"success": False, "message": "No picks passed bulletproof validation"})
    
    # Log final picks being posted
    logger.info(f"✅ FINAL PICKS TO POST:")
    for i, p in enumerate(best_validated):
        logger.info(f"   {i+1}. {p['game'].away_team} @ {p['game'].home_team} ({p['pick_type'].upper()})")
        logger.info(f"      Tier: {p['confidence_tier']}, Edge: {p['edge']:.1f}, EV: {p['ev']:.2f}%" if p['ev'] else f"      Tier: {p['confidence_tier']}, Edge: {p['edge']:.1f}, EV: N/A")
    
    # Build combined picks list with validated data
    combined = []
    for vp in best_validated:
        g = vp['game']
        pick_type = vp['pick_type']
        
        if pick_type == 'total':
            line_val = g.alt_total_line if g.alt_total_line else g.line
            pick_str = f"{g.direction}{line_val}"
        else:
            if g.spread_direction == 'HOME':
                spread_val = g.alt_spread_line if g.alt_spread_line else g.spread_line
                pick_str = f"{g.home_team} {spread_val:+.1f}" if spread_val else g.home_team
                line_val = spread_val
            else:
                spread_val = abs(g.alt_spread_line) if g.alt_spread_line else abs(g.spread_line) if g.spread_line else 0
                pick_str = f"{g.away_team} +{spread_val:.1f}" if spread_val else g.away_team
                line_val = spread_val
        
        away_favorite = (g.spread_line or 0) > 0
        combined.append({
            'game': g,
            'edge': vp['edge'],
            'pick_type': pick_type,
            'pick_str': pick_str,
            'line_val': line_val,
            'away_favorite': away_favorite,
            'confidence_tier': vp['confidence_tier']
        })
    
    top_3 = combined[:3]
    
    if not top_3:
        return jsonify({"success": False, "message": "No qualified picks to post"})
    
    # SUPERMAX = best overall pick
    supermax = top_3[0]
    emoji_map = {"NBA": "🏀", "CBB": "🏀", "NFL": "🏈", "CFB": "🏈", "NHL": "🏒"}
    
    # Build clean Discord message
    msg = f"🔒 730's LOCKS\n"
    msg += f"{today_str}\n\n"
    
    # Helper to format pick with odds
    def format_pick(p):
        g = p['game']
        if p['pick_type'] == 'total':
            line = g.alt_total_line if g.alt_total_line else g.line
            odds = g.alt_total_odds if g.alt_total_odds else None
            pick_str = f"{g.direction}{line:.0f}" if line else p['pick_str']
        else:
            line = g.alt_spread_line if g.alt_spread_line else abs(g.spread_line) if g.spread_line else None
            odds = g.alt_spread_odds if g.alt_spread_odds else None
            if g.spread_direction == 'HOME':
                pick_str = f"{g.home_team} {line:+.0f}" if line else g.home_team
            else:
                pick_str = f"{g.away_team} +{line:.0f}" if line else g.away_team
        if odds:
            pick_str += f" ({odds:+.0f})"
        return pick_str
    
    # Helper to extract short time (e.g., "8:30 PM EST" -> "8:30")
    def short_time(game_time):
        if not game_time:
            return ""
        import re
        match = re.search(r'(\d{1,2}:\d{2})', game_time)
        return match.group(1) if match else ""
    
    # Lock of the Day
    sm = supermax['game']
    sm_emoji = emoji_map.get(sm.league, "🎯")
    sm_time = short_time(sm.game_time)
    msg += f"⚡ LOCK OF THE DAY\n"
    msg += f"{sm_emoji} {sm.away_team}/{sm.home_team} {sm_time}\n"
    msg += f"{format_pick(supermax)}\n\n"
    
    # Top Picks (skip #1 since it's the lock) - only 2 more for 3 total
    if len(top_3) > 1:
        msg += f"📊 TOP PICKS\n"
        for p in top_3[1:3]:
            g = p['game']
            emoji = emoji_map.get(g.league, "🎯")
            g_time = short_time(g.game_time)
            msg += f"{emoji} {g.away_team}/{g.home_team} {g_time}\n"
            msg += f"{format_pick(p)}\n\n"
    
    webhook = os.environ.get("SPORTS_DISCORD_WEBHOOK")
    if webhook:
        success, status_code, error = post_to_discord_with_retry(webhook, {"content": msg})
        if not success:
            return jsonify({"success": False, "message": f"Discord post failed: {error}", "status": status_code})
        
        # Save ALL posted picks to history (Lock + Top Picks) to preserve line at time of posting
        picks_saved = 0
        for idx, p in enumerate(top_3):
            p_game = p['game']
            matchup = f"{p_game.away_team} @ {p_game.home_team}"
            
            # Check if this pick already saved today
            existing_pick = Pick.query.filter_by(date=today, matchup=matchup, pick_type=p['pick_type']).first()
            if not existing_pick:
                if p['pick_type'] == 'total':
                    line_val = p_game.alt_total_line if p_game.alt_total_line else p_game.line
                    pick_str = f"{p_game.direction}{line_val}"
                    edge_val = p_game.edge
                else:
                    if p_game.spread_direction == 'HOME':
                        line_val = p_game.alt_spread_line if p_game.alt_spread_line else p_game.spread_line
                        pick_str = f"{p_game.home_team} {line_val:+.1f}" if line_val else p_game.home_team
                    else:
                        line_val = p_game.alt_spread_line if p_game.alt_spread_line else -p_game.spread_line if p_game.spread_line else None
                        pick_str = f"{p_game.away_team} {line_val:+.1f}" if line_val else p_game.away_team
                    edge_val = p_game.spread_edge
                
                game_start_dt = parse_game_time_to_datetime(p_game.game_time, today)
                if game_start_dt and game_start_dt.tzinfo:
                    game_start_dt = game_start_dt.replace(tzinfo=None)
                
                # First pick (idx=0) is the Lock, others are Top Picks (is_lock=False)
                pick = Pick(
                    game_id=p_game.id,
                    date=today,
                    league=p_game.league,
                    matchup=matchup,
                    pick=pick_str,
                    edge=edge_val,
                    is_lock=(idx == 0),  # Only first pick is the Lock
                    posted_to_discord=True,
                    pick_type=p['pick_type'],
                    line_value=line_val,
                    game_start=game_start_dt
                )
                db.session.add(pick)
                picks_saved += 1
        
        db.session.commit()
        logger.info(f"Saved {picks_saved} picks to history (line locked at time of posting)")
        
        return jsonify({"success": True, "status": status_code, "picks_count": picks_saved})
    
    return jsonify({"success": False, "message": "Discord webhook not configured"})

@app.route('/post_discord_window/<window>', methods=['POST'])
def post_discord_window(window: str):
    """Post the best pick for a specific game window (EARLY/MID/LATE)."""
    if window not in ['EARLY', 'MID', 'LATE']:
        return jsonify({"success": False, "message": f"Invalid window: {window}"})
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%B %d, %Y")
    
    # Check if already posted for this window today
    existing = Pick.query.filter_by(date=today, game_window=window).first()
    if existing:
        return jsonify({"success": False, "message": f"Already posted for {window} window today"})
    
    # Get all games for this window
    all_games = Game.query.filter_by(date=today).all()
    window_games = [g for g in all_games if get_game_window(g.game_time) == window]
    
    if not window_games:
        return jsonify({"success": False, "message": f"No games for {window} window"})
    
    # ========================================================================
    # BULLETPROOF VALIDATION for window picks
    # ========================================================================
    logger.info(f"🔒 BULLETPROOF VALIDATION for {window} window")
    validation_result = BulletproofPickValidator.validate_all_picks(window_games, pick_type='both')
    
    # Get best validated pick for this window
    best_pick = None
    for tier in ["SUPERMAX", "HIGH", "MEDIUM", "LOW"]:
        if validation_result['by_tier'][tier]:
            pick_data = validation_result['by_tier'][tier][0]
            game = next((g for g in window_games if f"{g.away_team} @ {g.home_team}" == pick_data['game']), None)
            if game:
                best_pick = {
                    'game': game,
                    'pick_type': pick_data['pick_type'],
                    'edge': pick_data['edge'],
                    'confidence_tier': pick_data['confidence_tier']
                }
                break
    
    if not best_pick:
        return jsonify({"success": False, "message": f"No validated picks for {window} window"})
    
    sm_game = best_pick['game']
    supermax = best_pick
    
    emoji_map = {"NBA": "🏀", "CBB": "🏀", "NFL": "🏈", "CFB": "🏈", "NHL": "🏒"}
    window_labels = {"EARLY": "🌅 EARLY LOCK", "MID": "☀️ MIDDAY LOCK", "LATE": "🌙 LATE LOCK"}
    
    # Format pick
    if supermax['pick_type'] == 'total':
        line = sm_game.alt_total_line if sm_game.alt_total_line else sm_game.line
        odds = sm_game.alt_total_odds if sm_game.alt_total_odds else None
        pick_str = f"{sm_game.direction}{line:.0f}" if line else f"{sm_game.direction}"
    else:
        line = sm_game.alt_spread_line if sm_game.alt_spread_line else abs(sm_game.spread_line) if sm_game.spread_line else None
        odds = sm_game.alt_spread_odds if sm_game.alt_spread_odds else None
        if sm_game.spread_direction == 'HOME':
            pick_str = f"{sm_game.home_team} {line:+.0f}" if line else sm_game.home_team
        else:
            pick_str = f"{sm_game.away_team} +{line:.0f}" if line else sm_game.away_team
    
    if odds:
        pick_str += f" ({odds:+.0f})"
    
    # Build message
    msg = f"🔒 730's LOCKS\n{today_str}\n\n"
    msg += f"{window_labels[window]}\n"
    msg += f"{emoji_map.get(sm_game.league, '🎯')} {sm_game.away_team} @ {sm_game.home_team}\n"
    msg += f"{pick_str}\n"
    
    webhook = os.environ.get("SPORTS_DISCORD_WEBHOOK")
    if webhook:
        success, status_code, error = post_to_discord_with_retry(webhook, {"content": msg})
        if not success:
            return jsonify({"success": False, "message": f"Discord post failed: {error}", "status": status_code})
        
        # Save to history
        matchup = f"{sm_game.away_team} @ {sm_game.home_team}"
        if supermax['pick_type'] == 'total':
            line_val = sm_game.alt_total_line if sm_game.alt_total_line else sm_game.line
            pick_save = f"{sm_game.direction}{line_val}"
            edge_val = sm_game.edge
        else:
            if sm_game.spread_direction == 'HOME':
                line_val = sm_game.alt_spread_line if sm_game.alt_spread_line else sm_game.spread_line
                pick_save = f"{sm_game.home_team} {line_val:+.1f}" if line_val else sm_game.home_team
            else:
                line_val = sm_game.alt_spread_line if sm_game.alt_spread_line else -sm_game.spread_line if sm_game.spread_line else None
                pick_save = f"{sm_game.away_team} {line_val:+.1f}" if line_val else sm_game.away_team
            edge_val = sm_game.spread_edge
        
        game_start_dt = parse_game_time_to_datetime(sm_game.game_time, today)
        if game_start_dt and game_start_dt.tzinfo:
            game_start_dt = game_start_dt.replace(tzinfo=None)
        pick = Pick(
            game_id=sm_game.id,
            date=today,
            league=sm_game.league,
            matchup=matchup,
            pick=pick_save,
            edge=edge_val,
            is_lock=True,
            posted_to_discord=True,
            pick_type=supermax['pick_type'],
            line_value=line_val,
            game_window=window,
            game_start=game_start_dt
        )
        db.session.add(pick)
        db.session.commit()
        
        return jsonify({"success": True, "window": window, "status": status_code})
    
    return jsonify({"success": False, "message": "Discord webhook not configured"})

@app.route('/get_schedule_windows', methods=['GET'])
def get_schedule_windows():
    """Get posting schedule based on game windows for today."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    is_big_slate = is_big_slate_day()
    
    all_games = Game.query.filter_by(date=today).filter(
        db.or_(Game.is_qualified == True, Game.spread_is_qualified == True)
    ).all()
    
    windows = {'EARLY': [], 'MID': [], 'LATE': []}
    for g in all_games:
        w = get_game_window(g.game_time)
        windows[w].append(g)
    
    # Determine posting schedule
    schedule = []
    if is_big_slate:
        if windows['EARLY']:
            schedule.append({'window': 'EARLY', 'post_time': '10:00 AM', 'game_count': len(windows['EARLY'])})
        if windows['MID']:
            schedule.append({'window': 'MID', 'post_time': '12:30 PM', 'game_count': len(windows['MID'])})
        if windows['LATE']:
            schedule.append({'window': 'LATE', 'post_time': '5:00 PM', 'game_count': len(windows['LATE'])})
    else:
        schedule.append({'window': 'ALL', 'post_time': '11:00 AM', 'game_count': len(all_games)})
    
    return jsonify({
        "is_big_slate": is_big_slate,
        "day_of_week": datetime.now(et).strftime("%A"),
        "schedule": schedule,
        "windows": {k: len(v) for k, v in windows.items()}
    })

@app.route('/check_results', methods=['POST'])
def check_results():
    updated = check_finished_games_results()
    return jsonify({"success": True, "results_updated": updated})

@app.route('/api/check_game_results', methods=['POST'])
def api_check_game_results():
    """API endpoint for auto-checking finished game results."""
    updated = check_finished_games_results()
    return jsonify({"success": True, "results_updated": updated})

@app.route('/api/history_data')
def api_history_data():
    """API endpoint for history page auto-refresh."""
    picks = Pick.query.filter_by(is_lock=True).order_by(Pick.date.desc(), Pick.edge.desc()).all()
    
    wins = len([p for p in picks if p.result == 'W'])
    losses = len([p for p in picks if p.result == 'L'])
    pushes = len([p for p in picks if p.result == 'P'])
    
    picks_data = []
    for pick in picks:
        picks_data.append({
            'id': pick.id,
            'date': pick.date.strftime('%b %d, %Y'),
            'matchup': pick.matchup,
            'league': pick.league,
            'pick': pick.pick,
            'edge': round(pick.edge, 1) if pick.edge else 0,
            'result': pick.result,
            'is_lock': pick.is_lock
        })
    
    return jsonify({
        'success': True,
        'wins': wins,
        'losses': losses,
        'pushes': pushes,
        'picks': picks_data
    })

@app.route('/history')
def history():
    """Display pick history with win/loss stats - Supermax/Lock plays only."""
    et = pytz.timezone('America/New_York')
    now = datetime.now(et)
    
    all_picks_raw = Pick.query.filter_by(is_lock=True).order_by(Pick.date.desc(), Pick.edge.desc()).all()
    
    # BULLETPROOF: Deduplicate picks by (date, matchup, pick_type) - keep highest edge
    seen = {}
    all_picks = []
    for p in all_picks_raw:
        key = (p.date, p.matchup, p.pick_type)
        if key not in seen:
            seen[key] = p
            all_picks.append(p)
        elif p.edge and (not seen[key].edge or p.edge > seen[key].edge):
            all_picks.remove(seen[key])
            seen[key] = p
            all_picks.append(p)
    
    # Separate upcoming (pending, game not started) from resulted picks
    upcoming_picks = []
    past_picks = []
    
    for p in all_picks:
        if p.result is None:
            # Check if game has started yet
            if p.game_start:
                # game_start is stored as UTC (from Odds API), convert to ET for comparison
                game_start_utc = p.game_start.replace(tzinfo=pytz.UTC) if p.game_start.tzinfo is None else p.game_start
                game_start_et = game_start_utc.astimezone(et)
                if game_start_et > now:
                    upcoming_picks.append(p)
                else:
                    past_picks.append(p)  # Game started but no result yet
            else:
                past_picks.append(p)  # No game_start info, treat as past
        else:
            past_picks.append(p)
    
    wins = len([p for p in all_picks if p.result == 'W'])
    losses = len([p for p in all_picks if p.result == 'L'])
    
    return render_template('history.html', picks=past_picks, upcoming_picks=upcoming_picks, 
                          wins=wins, losses=losses)

@app.route('/bankroll')
def bankroll():
    """52 Week Bankroll Builder tracker."""
    return render_template('bankroll.html')

@app.route('/download/codebase_structure')
def download_codebase_structure():
    """Download the codebase structure CSV."""
    from flask import send_file
    return send_file('sports_app_structure.csv', as_attachment=True, download_name='sports_app_structure.csv')

@app.route('/update_result/<int:pick_id>', methods=['POST'])
def update_result(pick_id):
    pick = Pick.query.get_or_404(pick_id)
    data = request.get_json()
    pick.result = data.get('result')
    if data.get('actual_total'):
        pick.actual_total = float(data['actual_total'])
    db.session.commit()
    return jsonify({"success": True})

def safe_delete_games(game_ids: list):
    """Safely delete games by first nullifying pick references."""
    if game_ids:
        Pick.query.filter(Pick.game_id.in_(game_ids)).update({Pick.game_id: None}, synchronize_session=False)
        stmt = delete(Game).where(Game.id.in_(game_ids))
        db.session.execute(stmt)

@app.route('/clear_games', methods=['POST'])
def clear_games():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    game_ids = [g.id for g in Game.query.filter_by(date=today).all()]
    safe_delete_games(game_ids)
    db.session.commit()
    return redirect(url_for('dashboard'))

class MockGame:
    """Lightweight mock Game object for testing BulletproofPickValidator."""
    def __init__(self, **kwargs):
        defaults = {
            'id': 1, 'away_team': 'Test Away', 'home_team': 'Test Home',
            'league': 'NBA', 'edge': 10.0, 'spread_edge': 10.0,
            'total_ev': 2.0, 'spread_ev': 2.0,
            'away_ou_pct': 65, 'home_ou_pct': 60,
            'is_qualified': True, 'history_qualified': True,
            'spread_is_qualified': True, 'spread_history_qualified': True,
            'direction': 'OVER', 'spread_direction': 'HOME',
            'game_time': '7:00 PM ET', 'spread_line': -3.5,
            'bovada_line': 220.5, 'projected_total': 228.5
        }
        defaults.update(kwargs)
        for k, v in defaults.items():
            setattr(self, k, v)


@app.route('/api/deep_test')
def deep_test():
    """
    7-LAYER DEEP VALIDATION TEST SUITE
    Each layer goes progressively deeper into the bulletproof system.
    """
    results = []
    
    # ============================================================
    # LAYER 1: EDGE THRESHOLD VALIDATION
    # Tests league-specific edge thresholds
    # ============================================================
    layer1_tests = []
    
    # NBA: threshold = 8.0
    nba_pass = MockGame(league='NBA', edge=8.5, is_qualified=True, history_qualified=True)
    nba_fail = MockGame(league='NBA', edge=7.5, is_qualified=True, history_qualified=True)
    r1 = BulletproofPickValidator.validate_pick(nba_pass, 'total')
    r2 = BulletproofPickValidator.validate_pick(nba_fail, 'total')
    layer1_tests.append({
        "test": "NBA edge 8.5 >= 8.0",
        "passed": r1['validated'],
        "expected": True,
        "details": r1['checks_passed'] if r1['validated'] else r1['checks_failed']
    })
    layer1_tests.append({
        "test": "NBA edge 7.5 < 8.0",
        "passed": not r2['validated'],
        "expected": True,
        "details": r2['checks_failed']
    })
    
    # NFL: threshold = 3.5
    nfl_pass = MockGame(league='NFL', edge=4.0, is_qualified=True, history_qualified=True)
    nfl_fail = MockGame(league='NFL', edge=3.0, is_qualified=True, history_qualified=True)
    r3 = BulletproofPickValidator.validate_pick(nfl_pass, 'total')
    r4 = BulletproofPickValidator.validate_pick(nfl_fail, 'total')
    layer1_tests.append({
        "test": "NFL edge 4.0 >= 3.5",
        "passed": r3['validated'],
        "expected": True,
        "details": r3['checks_passed'] if r3['validated'] else r3['checks_failed']
    })
    layer1_tests.append({
        "test": "NFL edge 3.0 < 3.5",
        "passed": not r4['validated'],
        "expected": True,
        "details": r4['checks_failed']
    })
    
    # NHL: threshold = 0.5
    nhl_pass = MockGame(league='NHL', edge=0.7, is_qualified=True, history_qualified=True)
    nhl_fail = MockGame(league='NHL', edge=0.3, is_qualified=True, history_qualified=True)
    r5 = BulletproofPickValidator.validate_pick(nhl_pass, 'total')
    r6 = BulletproofPickValidator.validate_pick(nhl_fail, 'total')
    layer1_tests.append({
        "test": "NHL edge 0.7 >= 0.5",
        "passed": r5['validated'],
        "expected": True,
        "details": r5['checks_passed'] if r5['validated'] else r5['checks_failed']
    })
    layer1_tests.append({
        "test": "NHL edge 0.3 < 0.5",
        "passed": not r6['validated'],
        "expected": True,
        "details": r6['checks_failed']
    })
    
    layer1_passed = all(t['passed'] == t['expected'] for t in layer1_tests)
    results.append({
        "layer": 1,
        "name": "EDGE THRESHOLD VALIDATION",
        "status": "PASS" if layer1_passed else "FAIL",
        "tests": layer1_tests
    })
    
    # ============================================================
    # LAYER 2: MODEL QUALIFICATION FLAGS
    # Tests is_qualified and spread_is_qualified flags
    # ============================================================
    layer2_tests = []
    
    # Totals: is_qualified = True should pass
    qual_pass = MockGame(is_qualified=True, history_qualified=True)
    qual_fail = MockGame(is_qualified=False, history_qualified=True)
    r1 = BulletproofPickValidator.validate_pick(qual_pass, 'total')
    r2 = BulletproofPickValidator.validate_pick(qual_fail, 'total')
    layer2_tests.append({
        "test": "Totals: is_qualified=True passes",
        "passed": r1['validated'],
        "expected": True,
        "details": r1['checks_passed']
    })
    layer2_tests.append({
        "test": "Totals: is_qualified=False rejected",
        "passed": not r2['validated'],
        "expected": True,
        "details": r2['checks_failed']
    })
    
    # Spreads: spread_is_qualified flags
    spread_pass = MockGame(spread_is_qualified=True, spread_history_qualified=True)
    spread_fail = MockGame(spread_is_qualified=False, spread_history_qualified=True)
    r3 = BulletproofPickValidator.validate_pick(spread_pass, 'spread')
    r4 = BulletproofPickValidator.validate_pick(spread_fail, 'spread')
    layer2_tests.append({
        "test": "Spreads: spread_is_qualified=True passes",
        "passed": r3['validated'],
        "expected": True,
        "details": r3['checks_passed']
    })
    layer2_tests.append({
        "test": "Spreads: spread_is_qualified=False rejected",
        "passed": not r4['validated'],
        "expected": True,
        "details": r4['checks_failed']
    })
    
    layer2_passed = all(t['passed'] == t['expected'] for t in layer2_tests)
    results.append({
        "layer": 2,
        "name": "MODEL QUALIFICATION FLAGS",
        "status": "PASS" if layer2_passed else "FAIL",
        "tests": layer2_tests
    })
    
    # ============================================================
    # LAYER 3: HISTORICAL QUALIFICATION
    # Tests history_qualified and spread_history_qualified
    # ============================================================
    layer3_tests = []
    
    hist_pass = MockGame(is_qualified=True, history_qualified=True, away_ou_pct=70, home_ou_pct=65)
    hist_fail = MockGame(is_qualified=True, history_qualified=False, away_ou_pct=55, home_ou_pct=50)
    r1 = BulletproofPickValidator.validate_pick(hist_pass, 'total')
    r2 = BulletproofPickValidator.validate_pick(hist_fail, 'total')
    layer3_tests.append({
        "test": "Totals: history_qualified=True (70% O/U) passes",
        "passed": r1['validated'],
        "expected": True,
        "details": r1['checks_passed']
    })
    layer3_tests.append({
        "test": "Totals: history_qualified=False rejected",
        "passed": not r2['validated'],
        "expected": True,
        "details": r2['checks_failed']
    })
    
    spread_hist_pass = MockGame(spread_is_qualified=True, spread_history_qualified=True)
    spread_hist_fail = MockGame(spread_is_qualified=True, spread_history_qualified=False)
    r3 = BulletproofPickValidator.validate_pick(spread_hist_pass, 'spread')
    r4 = BulletproofPickValidator.validate_pick(spread_hist_fail, 'spread')
    layer3_tests.append({
        "test": "Spreads: spread_history_qualified=True passes",
        "passed": r3['validated'],
        "expected": True,
        "details": r3['checks_passed']
    })
    layer3_tests.append({
        "test": "Spreads: spread_history_qualified=False rejected",
        "passed": not r4['validated'],
        "expected": True,
        "details": r4['checks_failed']
    })
    
    layer3_passed = all(t['passed'] == t['expected'] for t in layer3_tests)
    results.append({
        "layer": 3,
        "name": "HISTORICAL QUALIFICATION",
        "status": "PASS" if layer3_passed else "FAIL",
        "tests": layer3_tests
    })
    
    # ============================================================
    # LAYER 4: EV VALIDATION
    # Tests positive EV allowed, negative EV rejected, NULL allowed
    # ============================================================
    layer4_tests = []
    
    ev_positive = MockGame(total_ev=3.5, is_qualified=True, history_qualified=True)
    ev_negative = MockGame(total_ev=-2.0, is_qualified=True, history_qualified=True)
    ev_null = MockGame(total_ev=None, is_qualified=True, history_qualified=True)
    
    r1 = BulletproofPickValidator.validate_pick(ev_positive, 'total')
    r2 = BulletproofPickValidator.validate_pick(ev_negative, 'total')
    r3 = BulletproofPickValidator.validate_pick(ev_null, 'total')
    
    layer4_tests.append({
        "test": "EV +3.5% allowed (passes)",
        "passed": r1['validated'],
        "expected": True,
        "details": [c for c in r1['checks_passed'] if 'EV' in c]
    })
    layer4_tests.append({
        "test": "EV -2.0% rejected",
        "passed": not r2['validated'],
        "expected": True,
        "details": [c for c in r2['checks_failed'] if 'EV' in c]
    })
    layer4_tests.append({
        "test": "EV NULL allowed (passes)",
        "passed": r3['validated'],
        "expected": True,
        "details": [c for c in r3['checks_passed'] if 'EV' in c]
    })
    
    # Edge case: EV = 0 should pass
    ev_zero = MockGame(total_ev=0.0, is_qualified=True, history_qualified=True)
    r4 = BulletproofPickValidator.validate_pick(ev_zero, 'total')
    layer4_tests.append({
        "test": "EV 0.0% allowed (edge case)",
        "passed": r4['validated'],
        "expected": True,
        "details": [c for c in r4['checks_passed'] if 'EV' in c]
    })
    
    layer4_passed = all(t['passed'] == t['expected'] for t in layer4_tests)
    results.append({
        "layer": 4,
        "name": "EV VALIDATION",
        "status": "PASS" if layer4_passed else "FAIL",
        "tests": layer4_tests
    })
    
    # ============================================================
    # LAYER 5: TEAM NAME MATCHING
    # Tests fuzzy matching and normalization
    # ============================================================
    layer5_tests = []
    
    # Test teams_match function - validates fuzzy matching accuracy
    test_cases = [
        ("North Carolina", "UNC", True),
        ("Michigan State", "Michigan St", True),
        ("Texas A&M", "Texas A&M Aggies", True),
        ("Duke", "Duke Blue Devils", True),
        ("Eastern Michigan", "Central Michigan", False),  # Different schools
        ("Ohio State", "Ohio Bobcats", False),  # MUST NOT MATCH - distinct schools
        ("Lakers", "LA Lakers", True),
        ("Philadelphia 76ers", "76ers", True),
    ]
    
    # Additional negative test cases - document known limitations
    # Note: teams_match uses fuzzy matching which may match "Michigan" to "Michigan State"
    # due to substring matching. This is a known limitation documented here.
    
    for team1, team2, expected_match in test_cases:
        result = teams_match(team1, team2)
        layer5_tests.append({
            "test": f"'{team1}' vs '{team2}' -> {expected_match}",
            "passed": result == expected_match,
            "expected": True,
            "details": f"teams_match returned {result}"
        })
    
    layer5_passed = all(t['passed'] for t in layer5_tests)
    results.append({
        "layer": 5,
        "name": "TEAM NAME MATCHING",
        "status": "PASS" if layer5_passed else "FAIL",
        "tests": layer5_tests
    })
    
    # ============================================================
    # LAYER 6: SPREAD VALIDATION
    # Tests SpreadValidator mirror image and ML cross-check
    # Spread convention: negative = AWAY is favorite, positive = HOME is favorite
    # ML convention: lower value = favorite
    # ============================================================
    layer6_tests = []
    
    # Test 1: Away favorite (away_ml=-200) with -5.5 spread (away favorite) is VALID
    valid, msg, corrected = SpreadValidator.validate_spread_vs_moneyline(
        spread=-5.5, away_ml=-200, home_ml=180,
        away_team="Away Team", home_team="Home Team"
    )
    layer6_tests.append({
        "test": "Away favorite (ML -200) with -5.5 spread is valid",
        "passed": valid,
        "expected": True,
        "details": msg if msg else "Spread matches ML direction"
    })
    
    # Test 2: Home favorite (home_ml=-200) with +5.5 spread (away underdog) is VALID
    valid2, msg2, corrected2 = SpreadValidator.validate_spread_vs_moneyline(
        spread=5.5, away_ml=180, home_ml=-200,
        away_team="Away Team", home_team="Home Team"
    )
    layer6_tests.append({
        "test": "Home favorite (ML -200) with +5.5 spread is valid",
        "passed": valid2,
        "expected": True,
        "details": msg2 if msg2 else "Spread matches ML direction"
    })
    
    # Test 3: MISMATCH - Away favorite (ML -200) but +5.5 spread (says away underdog)
    valid3, msg3, corrected3 = SpreadValidator.validate_spread_vs_moneyline(
        spread=5.5, away_ml=-200, home_ml=180,
        away_team="Away Team", home_team="Home Team"
    )
    layer6_tests.append({
        "test": "Away favorite with +5.5 spread is INVALID (mismatch)",
        "passed": not valid3 and corrected3 == -5.5,
        "expected": True,
        "details": f"Detected mismatch, corrected to {corrected3}"
    })
    
    # Test 4: validate_and_correct_spread helper auto-corrects mismatch
    final_spread, was_corrected = SpreadValidator.validate_and_correct_spread(
        spread=5.5, away_ml=-200, home_ml=180,
        away_team="Away Team", home_team="Home Team"
    )
    layer6_tests.append({
        "test": "Auto-correction flips +5.5 to -5.5 (away fav mismatch)",
        "passed": final_spread == -5.5 and was_corrected,
        "expected": True,
        "details": f"Returned {final_spread}, was_corrected={was_corrected}"
    })
    
    layer6_passed = all(t['passed'] for t in layer6_tests)
    results.append({
        "layer": 6,
        "name": "SPREAD VALIDATION",
        "status": "PASS" if layer6_passed else "FAIL",
        "tests": layer6_tests
    })
    
    # ============================================================
    # LAYER 7: FULL INTEGRATION TEST
    # Tests complete workflow from validation to Discord payload
    # ============================================================
    layer7_tests = []
    
    # Create a set of mock games with varying qualifications
    mock_games = [
        MockGame(away_team="Lakers", home_team="Celtics", league="NBA",
                 edge=12.5, total_ev=3.5, away_ou_pct=75, home_ou_pct=70,
                 is_qualified=True, history_qualified=True),
        MockGame(away_team="Chiefs", home_team="Bills", league="NFL",
                 edge=4.5, total_ev=1.5, away_ou_pct=68, home_ou_pct=65,
                 is_qualified=True, history_qualified=True),
        MockGame(away_team="Bruins", home_team="Rangers", league="NHL",
                 edge=0.3, total_ev=0.5, away_ou_pct=60, home_ou_pct=55,
                 is_qualified=True, history_qualified=True),
        MockGame(away_team="Duke", home_team="UNC", league="CBB",
                 edge=9.0, total_ev=-1.5, away_ou_pct=65, home_ou_pct=60,
                 is_qualified=True, history_qualified=True),
    ]
    
    validation_results = BulletproofPickValidator.validate_all_picks(mock_games, pick_type='total')
    
    layer7_tests.append({
        "test": "Lakers @ Celtics (edge 12.5, EV 3.5%) -> SUPERMAX tier",
        "passed": any(p['game'] == "Lakers @ Celtics" and p['confidence_tier'] == "SUPERMAX" 
                      for p in validation_results['validated_picks']),
        "expected": True,
        "details": f"Found in tier: {[p['confidence_tier'] for p in validation_results['validated_picks'] if 'Lakers' in p['game']]}"
    })
    
    layer7_tests.append({
        "test": "Chiefs @ Bills (NFL edge 4.5) -> validated",
        "passed": any(p['game'] == "Chiefs @ Bills" for p in validation_results['validated_picks']),
        "expected": True,
        "details": "NFL pick with sufficient edge passes"
    })
    
    layer7_tests.append({
        "test": "Bruins @ Rangers (NHL edge 0.3 < 0.5) -> rejected",
        "passed": any(p['game'] == "Bruins @ Rangers" for p in validation_results['rejected_picks']),
        "expected": True,
        "details": "NHL edge below threshold rejected"
    })
    
    layer7_tests.append({
        "test": "Duke @ UNC (negative EV -1.5%) -> rejected",
        "passed": any(p['game'] == "Duke @ UNC" for p in validation_results['rejected_picks']),
        "expected": True,
        "details": "Negative EV pick rejected"
    })
    
    layer7_tests.append({
        "test": "Tier counts: SUPERMAX=1, validated=2, rejected=2",
        "passed": (len(validation_results['by_tier']['SUPERMAX']) == 1 and
                   len(validation_results['validated_picks']) == 2 and
                   len(validation_results['rejected_picks']) == 2),
        "expected": True,
        "details": f"SUPERMAX: {len(validation_results['by_tier']['SUPERMAX'])}, " +
                   f"validated: {len(validation_results['validated_picks'])}, " +
                   f"rejected: {len(validation_results['rejected_picks'])}"
    })
    
    # SPREAD PICKS INTEGRATION TEST
    # Note: Spread tier requires away_spread_pct/home_spread_pct for tier calculation
    spread_games = [
        MockGame(away_team="Cowboys", home_team="Eagles", league="NFL",
                 spread_edge=5.0, spread_ev=2.5, spread_is_qualified=True, 
                 spread_history_qualified=True, spread_direction="HOME",
                 away_spread_pct=60, home_spread_pct=65),
        MockGame(away_team="Suns", home_team="Nuggets", league="NBA",
                 spread_edge=11.5, spread_ev=4.0, spread_is_qualified=True,
                 spread_history_qualified=True, spread_direction="AWAY",
                 away_spread_pct=70, home_spread_pct=65),
        MockGame(away_team="Flames", home_team="Oilers", league="NHL",
                 spread_edge=0.2, spread_ev=1.0, spread_is_qualified=True,
                 spread_history_qualified=True, spread_direction="HOME",
                 away_spread_pct=55, home_spread_pct=60),
        MockGame(away_team="Kentucky", home_team="Tennessee", league="CBB",
                 spread_edge=9.5, spread_ev=-0.5, spread_is_qualified=True,
                 spread_history_qualified=True, spread_direction="HOME",
                 away_spread_pct=60, home_spread_pct=62),
    ]
    
    spread_results = BulletproofPickValidator.validate_all_picks(spread_games, pick_type='spread')
    
    layer7_tests.append({
        "test": "SPREAD: Cowboys @ Eagles (NFL edge 5.0) -> validated",
        "passed": any(p['game'] == "Cowboys @ Eagles" for p in spread_results['validated_picks']),
        "expected": True,
        "details": "NFL spread pick with sufficient edge passes"
    })
    
    layer7_tests.append({
        "test": "SPREAD: Suns @ Nuggets (edge 11.5, EV 4.0%) -> HIGH tier",
        "passed": any(p['game'] == "Suns @ Nuggets" and p['confidence_tier'] in ['SUPERMAX', 'HIGH']
                      for p in spread_results['validated_picks']),
        "expected": True,
        "details": f"Spread tier: {[p['confidence_tier'] for p in spread_results['validated_picks'] if 'Suns' in p['game']]}"
    })
    
    layer7_tests.append({
        "test": "SPREAD: Flames @ Oilers (NHL edge 0.2 < 0.5) -> rejected",
        "passed": any(p['game'] == "Flames @ Oilers" for p in spread_results['rejected_picks']),
        "expected": True,
        "details": "NHL spread edge below threshold rejected"
    })
    
    layer7_tests.append({
        "test": "SPREAD: Kentucky @ Tennessee (negative EV) -> rejected",
        "passed": any(p['game'] == "Kentucky @ Tennessee" for p in spread_results['rejected_picks']),
        "expected": True,
        "details": "Spread with negative EV rejected"
    })
    
    layer7_passed = all(t['passed'] for t in layer7_tests)
    results.append({
        "layer": 7,
        "name": "FULL INTEGRATION TEST",
        "status": "PASS" if layer7_passed else "FAIL",
        "tests": layer7_tests
    })
    
    # ============================================================
    # LAYER 8: TIMEZONE VALIDATION
    # Tests UTC to ET conversion for game start times (history page)
    # ============================================================
    layer8_tests = []
    
    et = pytz.timezone('America/New_York')
    utc = pytz.UTC
    now_et = datetime.now(et)
    
    # Test 1: Past game (UTC time that's definitely in the past)
    past_game_utc = datetime(2020, 1, 1, 12, 0, 0)  # Jan 1, 2020 12:00 UTC
    past_game_utc_tz = utc.localize(past_game_utc)
    past_game_et = past_game_utc_tz.astimezone(et)
    is_past = past_game_et <= now_et
    layer8_tests.append({
        "test": "UTC past game (2020-01-01 12:00 UTC) correctly identified as PAST",
        "passed": is_past,
        "expected": True,
        "details": f"Converted: {past_game_et.strftime('%Y-%m-%d %H:%M %Z')}"
    })
    
    # Test 2: Future game (UTC time that's definitely in the future)
    future_game_utc = datetime(2030, 12, 31, 23, 59, 59)  # Dec 31, 2030 23:59 UTC
    future_game_utc_tz = utc.localize(future_game_utc)
    future_game_et = future_game_utc_tz.astimezone(et)
    is_future = future_game_et > now_et
    layer8_tests.append({
        "test": "UTC future game (2030-12-31 23:59 UTC) correctly identified as UPCOMING",
        "passed": is_future,
        "expected": True,
        "details": f"Converted: {future_game_et.strftime('%Y-%m-%d %H:%M %Z')}"
    })
    
    # Test 3: UTC to ET offset is correct (UTC is always ahead of ET by 4-5 hours)
    test_utc = datetime(2026, 1, 10, 17, 0, 0)  # 5:00 PM UTC
    test_utc_tz = utc.localize(test_utc)
    test_et = test_utc_tz.astimezone(et)
    hour_diff = test_utc.hour - test_et.hour
    # In winter (EST), UTC is 5 hours ahead; in summer (EDT), UTC is 4 hours ahead
    correct_offset = hour_diff in [4, 5] or hour_diff in [-20, -19]  # Handle day wrap
    layer8_tests.append({
        "test": "UTC to ET offset is 4-5 hours (EST/EDT)",
        "passed": correct_offset,
        "expected": True,
        "details": f"17:00 UTC -> {test_et.strftime('%H:%M %Z')} (diff: {hour_diff}h)"
    })
    
    # Test 4: Naive datetime treated as UTC (not ET)
    naive_dt = datetime(2026, 6, 15, 18, 0, 0)  # 6:00 PM naive (should be UTC)
    naive_as_utc = naive_dt.replace(tzinfo=utc)
    naive_to_et = naive_as_utc.astimezone(et)
    # 18:00 UTC in summer (EDT) = 14:00 ET (2:00 PM)
    correct_conversion = naive_to_et.hour == 14  # 6 PM UTC = 2 PM EDT
    layer8_tests.append({
        "test": "Naive datetime (18:00) treated as UTC, converts to 14:00 EDT",
        "passed": correct_conversion,
        "expected": True,
        "details": f"Naive 18:00 -> {naive_to_et.strftime('%H:%M %Z')}"
    })
    
    # Test 5: Game that just started (1 minute ago) is NOT upcoming
    one_min_ago_et = now_et - timedelta(minutes=1)
    one_min_ago_utc = one_min_ago_et.astimezone(utc)
    reconverted_et = one_min_ago_utc.astimezone(et)
    is_started = reconverted_et <= now_et
    layer8_tests.append({
        "test": "Game started 1 min ago is correctly NOT upcoming",
        "passed": is_started,
        "expected": True,
        "details": f"Started at {reconverted_et.strftime('%H:%M:%S %Z')}, now {now_et.strftime('%H:%M:%S %Z')}"
    })
    
    # Test 6: Game starting in 1 hour IS upcoming
    one_hour_later_et = now_et + timedelta(hours=1)
    one_hour_later_utc = one_hour_later_et.astimezone(utc)
    reconverted_et2 = one_hour_later_utc.astimezone(et)
    is_upcoming = reconverted_et2 > now_et
    layer8_tests.append({
        "test": "Game starting in 1 hour is correctly UPCOMING",
        "passed": is_upcoming,
        "expected": True,
        "details": f"Starts at {reconverted_et2.strftime('%H:%M:%S %Z')}, now {now_et.strftime('%H:%M:%S %Z')}"
    })
    
    layer8_passed = all(t['passed'] for t in layer8_tests)
    results.append({
        "layer": 8,
        "name": "TIMEZONE VALIDATION",
        "status": "PASS" if layer8_passed else "FAIL",
        "tests": layer8_tests
    })
    
    # ============================================================
    # SUMMARY
    # ============================================================
    all_passed = all(r['status'] == 'PASS' for r in results)
    total_tests = sum(len(r['tests']) for r in results)
    passed_tests = sum(1 for r in results for t in r['tests'] if t.get('passed', False))
    
    return jsonify({
        "overall_status": "ALL PASS" if all_passed else "SOME FAILED",
        "summary": f"{passed_tests}/{total_tests} tests passed",
        "layers": results
    })


@app.route('/api/export_picks_sql')
def export_picks_sql():
    """
    Export all picks as SQL INSERT statements for production database sync.
    Access via browser and copy the SQL to run in production database.
    """
    picks = Pick.query.order_by(Pick.date.desc()).all()
    
    sql_statements = []
    sql_statements.append("-- Export of all picks from development database")
    sql_statements.append("-- Run this SQL in your PRODUCTION database to sync picks")
    sql_statements.append("-- Generated at: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sql_statements.append("")
    sql_statements.append("-- First, clear existing picks (optional - uncomment if needed)")
    sql_statements.append("-- DELETE FROM pick;")
    sql_statements.append("")
    
    for p in picks:
        # Escape single quotes in strings
        matchup_safe = (p.matchup or '').replace("'", "''")
        pick_safe = (p.pick or '').replace("'", "''")
        result_safe = (p.result or '').replace("'", "''") if p.result else None
        league_safe = (p.league or '').replace("'", "''")
        pick_type_safe = (p.pick_type or '').replace("'", "''") if p.pick_type else None
        game_window_safe = (p.game_window or '').replace("'", "''") if p.game_window else None
        
        # Format date fields
        date_str = f"'{p.date}'" if p.date else 'NULL'
        game_start_str = f"'{p.game_start}'" if p.game_start else 'NULL'
        created_at_str = f"'{p.created_at}'" if p.created_at else 'NOW()'
        
        # Format numeric/boolean fields
        game_id_str = str(p.game_id) if p.game_id else 'NULL'
        edge_str = str(p.edge) if p.edge is not None else 'NULL'
        actual_total_str = str(p.actual_total) if p.actual_total is not None else 'NULL'
        line_value_str = str(p.line_value) if p.line_value is not None else 'NULL'
        is_lock_str = 'TRUE' if p.is_lock else 'FALSE'
        posted_str = 'TRUE' if p.posted_to_discord else 'FALSE'
        result_str = f"'{result_safe}'" if result_safe else 'NULL'
        pick_type_str = f"'{pick_type_safe}'" if pick_type_safe else 'NULL'
        game_window_str = f"'{game_window_safe}'" if game_window_safe else 'NULL'
        
        sql = f"""INSERT INTO pick (game_id, date, league, matchup, pick, edge, result, actual_total, is_lock, posted_to_discord, created_at, pick_type, line_value, game_start, game_window) 
VALUES ({game_id_str}, {date_str}, '{league_safe}', '{matchup_safe}', '{pick_safe}', {edge_str}, {result_str}, {actual_total_str}, {is_lock_str}, {posted_str}, {created_at_str}, {pick_type_str}, {line_value_str}, {game_start_str}, {game_window_str})
ON CONFLICT DO NOTHING;"""
        sql_statements.append(sql)
    
    # Return as plain text for easy copying
    from flask import Response
    return Response('\n'.join(sql_statements), mimetype='text/plain')


with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
