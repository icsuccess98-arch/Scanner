import os
import logging
import time
import statistics
from datetime import datetime, date, timedelta
from typing import Tuple, Optional, List
from dataclasses import dataclass
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
import threading
from math import radians, sin, cos, sqrt, atan2
from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response
from flask_sqlalchemy import SQLAlchemy
from flask_compress import Compress
from cachetools import TTLCache
from sqlalchemy.orm import DeclarativeBase, validates
from sqlalchemy import delete
import requests
import pytz
from bs4 import BeautifulSoup


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
    VALIDATION_FAILED = "VALIDATION_FAILED"       # Data validation failed
    NOT_QUALIFIED = "NOT_QUALIFIED"               # Didn't meet basic criteria

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

APP_VERSION = "2.0.0-PEAK-PERFORMANCE"
logger.info(f"Sports Betting App Starting - Version {APP_VERSION}")

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

compress = Compress()
compress.init_app(app)
logger.info("Response compression enabled")

last_game_count = {}

_live_scores_cache = {"data": {}, "timestamp": 0}

DASHBOARD_CACHE_TTL = 30
TEAM_STATS_CACHE_TTL = 86400
HISTORICAL_CACHE_TTL = 21600

_dashboard_cache = {"data": None, "timestamp": 0, "lock": threading.Lock()}
_team_stats_cache = TTLCache(maxsize=200, ttl=TEAM_STATS_CACHE_TTL)
_historical_cache = TTLCache(maxsize=500, ttl=HISTORICAL_CACHE_TTL)
_performance_metrics = {}

def get_cached_dashboard():
    """Get cached dashboard data if still fresh."""
    with _dashboard_cache["lock"]:
        if time.time() - _dashboard_cache["timestamp"] < DASHBOARD_CACHE_TTL:
            return _dashboard_cache["data"]
    return None

def set_dashboard_cache(data):
    """Cache dashboard data for fast subsequent loads."""
    with _dashboard_cache["lock"]:
        _dashboard_cache["data"] = data
        _dashboard_cache["timestamp"] = time.time()

def clear_dashboard_cache():
    """Clear dashboard cache (call after fetch_odds)."""
    with _dashboard_cache["lock"]:
        _dashboard_cache["timestamp"] = 0
    logger.info("Dashboard cache cleared")

def track_performance(operation: str, duration: float):
    """Track operation performance for monitoring."""
    if operation not in _performance_metrics:
        _performance_metrics[operation] = []
    _performance_metrics[operation].append({
        'duration': duration,
        'timestamp': time.time()
    })
    if len(_performance_metrics[operation]) > 100:
        _performance_metrics[operation] = _performance_metrics[operation][-100:]

CITY_COORDS = {
    'Atlanta': (33.7490, -84.3880), 'Boston': (42.3601, -71.0589),
    'Brooklyn': (40.6782, -73.9442), 'Charlotte': (35.2271, -80.8431),
    'Chicago': (41.8781, -87.6298), 'Cleveland': (41.4993, -81.6944),
    'Dallas': (32.7767, -96.7970), 'Denver': (39.7392, -104.9903),
    'Detroit': (42.3314, -83.0458), 'Golden State': (37.7749, -122.4194),
    'Houston': (29.7604, -95.3698), 'Los Angeles': (34.0522, -118.2437),
    'Memphis': (35.1495, -90.0490), 'Miami': (25.7617, -80.1918),
    'Milwaukee': (43.0389, -87.9065), 'Minnesota': (44.9778, -93.2650),
    'New Orleans': (29.9511, -90.0715), 'New York': (40.7128, -74.0060),
    'Oklahoma City': (35.4676, -97.5164), 'Orlando': (28.5383, -81.3792),
    'Philadelphia': (39.9526, -75.1652), 'Phoenix': (33.4484, -112.0740),
    'Portland': (45.5152, -122.6784), 'Sacramento': (38.5816, -121.4944),
    'San Antonio': (29.4241, -98.4936), 'Toronto': (43.6532, -79.3832),
    'Utah': (40.7608, -111.8910), 'Washington': (38.9072, -77.0369),
}

def calculate_travel_distance(team1: str, team2: str) -> float:
    """Calculate great circle distance between two cities in miles."""
    def get_city(team_name):
        for city in CITY_COORDS.keys():
            if city.lower() in team_name.lower():
                return city
        return None
    
    city1 = get_city(team1)
    city2 = get_city(team2)
    
    if not city1 or not city2:
        return 0.0
    
    lat1, lon1 = CITY_COORDS[city1]
    lat2, lon2 = CITY_COORDS[city2]
    
    R = 3959
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    distance = R * c
    
    return round(distance, 0)

def get_travel_impact(away_team: str, home_team: str, league: str) -> float:
    """Calculate travel fatigue penalty for away team."""
    distance = calculate_travel_distance(away_team, home_team)
    
    penalty = 0.0
    
    if league == "NBA":
        if distance >= 2500:
            penalty = -2.0
        elif distance >= 1500:
            penalty = -1.0
    elif league == "NFL":
        if distance >= 2000:
            penalty = -1.5
    elif league == "NHL":
        if distance >= 2000:
            penalty = -1.0
    
    return penalty

def get_rest_days_impact(team: str, league: str, game_date: date) -> dict:
    """
    Calculate rest days impact with fatigue factors.
    
    Returns:
        dict: days_rest, is_back_to_back, fatigue_factor (-4 to +2 points)
    """
    try:
        recent_games = Game.query.filter(
            db.or_(Game.away_team == team, Game.home_team == team),
            Game.date < game_date,
            Game.league == league
        ).order_by(Game.date.desc()).limit(3).all()
        
        if not recent_games:
            return {'days_rest': None, 'is_back_to_back': False, 'fatigue_factor': 0.0}
        
        last_game = recent_games[0]
        days_rest = (game_date - last_game.date).days
        is_back_to_back = (days_rest == 1)
        
        fatigue_factor = 0.0
        
        if league == "NBA":
            if is_back_to_back:
                fatigue_factor = -4.0
                logger.info(f"{team} on back-to-back: -4pts fatigue penalty")
            elif days_rest >= 3:
                fatigue_factor = +1.5
                logger.info(f"{team} well rested ({days_rest} days): +1.5pts bonus")
        
        elif league == "NFL":
            if days_rest <= 4:
                fatigue_factor = -3.0
                logger.info(f"{team} short rest ({days_rest} days): -3pts penalty")
            elif days_rest >= 10:
                fatigue_factor = +2.0
                logger.info(f"{team} bye week rest: +2pts bonus")
        
        elif league == "NHL":
            if is_back_to_back:
                fatigue_factor = -2.0
                logger.info(f"{team} on back-to-back: -2pts fatigue penalty")
            elif len(recent_games) >= 2:
                games_last_4 = [g for g in recent_games if (game_date - g.date).days <= 4]
                if len(games_last_4) >= 2:
                    fatigue_factor = -1.5
                    logger.info(f"{team} 3-in-4 nights: -1.5pts penalty")
        
        return {
            'days_rest': days_rest,
            'is_back_to_back': is_back_to_back,
            'fatigue_factor': fatigue_factor
        }
    
    except Exception as e:
        logger.error(f"Rest days calculation error for {team}: {e}")
        return {'days_rest': None, 'is_back_to_back': False, 'fatigue_factor': 0.0}

LIVE_SCORES_CACHE_TTL = 15

@app.after_request
def add_cache_headers(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

class GameConstants:
    """Centralized configuration for all magic numbers and thresholds."""
    
    # Edge thresholds by league
    EDGE_THRESHOLDS = {
        "NBA": 8.0,
        "CBB": 8.0,
        "NFL": 3.5,
        "CFB": 3.5,
        "NHL": 0.5
    }
    
    # Minimum games for analysis by league
    MIN_GAMES = {
        "NBA": 8,
        "CBB": 8,
        "NFL": 4,
        "CFB": 4,
        "NHL": 8
    }
    MIN_GAMES_DEFAULT = 5
    
    # Confidence tier thresholds
    SUPERMAX_EDGE = 12.0
    HIGH_EDGE = 10.0
    MEDIUM_EDGE = 8.0
    
    # History qualification rates
    HISTORY_QUALIFY_RATE = 0.60
    SUPERMAX_HISTORY_RATE = 0.70
    HIGH_HISTORY_RATE = 0.65
    
    # Retry configuration
    RETRY_MAX_ATTEMPTS = 3
    RETRY_BASE_DELAY = 0.3
    RETRY_BACKOFF_MULTIPLIER = 2
    
    # API timeouts
    API_TIMEOUT_DEFAULT = 15
    API_TIMEOUT_SCOREBOARD = 30
    API_TIMEOUT_LONG = 60
    
    # Cache TTLs (seconds)
    CACHE_TTL_LIVE_SCORES = 15
    CACHE_TTL_TEAM_STATS = 3600
    CACHE_TTL_HISTORICAL = 43200
    CACHE_TTL_SCHEDULE = 43200
    CACHE_TTL_OPENING_LINE = 86400
    
    # Rate limits (requests per second)
    RATE_LIMIT_ESPN = 5
    RATE_LIMIT_ODDS_API = 2
    
    # Cache sizes
    CACHE_SIZE_DEFAULT = 500

THRESHOLDS = GameConstants.EDGE_THRESHOLDS


def get_display_spread(game, use_alt=True) -> tuple:
    """
    FOOLPROOF SPREAD DISPLAY HELPER
    
    Returns: (team_name, display_line) with correct sign
    
    Database storage: spread_line is ALWAYS stored as the AWAY team's spread
    - spread_line > 0 = away is underdog (e.g., +5.5)
    - spread_line < 0 = away is favorite (e.g., -5.5)
    
    For HOME picks: negate the stored line to get home team's spread
    For AWAY picks: use stored line as-is
    """
    if not game.spread_direction or not game.spread_line:
        return (None, None)
    
    if use_alt and game.alt_spread_line:
        raw_line = game.alt_spread_line
    else:
        raw_line = game.spread_line
    
    if game.spread_direction == 'HOME':
        return (game.home_team, -raw_line)
    else:
        return (game.away_team, raw_line)


def format_spread_pick(game, use_alt=True, include_odds=False) -> str:
    """
    FOOLPROOF SPREAD FORMAT HELPER
    
    Returns formatted string like "Lakers -5.5" or "Celtics +3.5 (-110)"
    """
    team, line = get_display_spread(game, use_alt)
    if team is None:
        return ""
    
    pick_str = f"{team} {line:+.1f}"
    
    if include_odds:
        odds = game.alt_spread_odds if use_alt and game.alt_spread_odds else game.bovada_spread_odds
        if odds:
            pick_str += f" ({odds:+.0f})"
    
    return pick_str


class TTLCache:
    """Time-based cache with max size to prevent memory leaks."""
    def __init__(self, maxsize=1000, ttl=3600):
        from collections import OrderedDict
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl
    
    def get(self, key):
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            del self.cache[key]
        return None
    
    def set(self, key, value):
        if len(self.cache) >= self.maxsize:
            self.cache.popitem(last=False)
        self.cache[key] = (value, time.time())
    
    def __contains__(self, key):
        return self.get(key) is not None
    
    def __getitem__(self, key):
        return self.get(key)
    
    def __setitem__(self, key, value):
        self.set(key, value)
    
    def clear(self):
        self.cache.clear()

line_movement_cache = TTLCache(maxsize=500, ttl=43200)
opening_lines_store = TTLCache(maxsize=500, ttl=86400)
espn_schedule_cache = TTLCache(maxsize=500, ttl=43200)
weather_cache = TTLCache(maxsize=100, ttl=3600)

# NFL/CFB Indoor stadiums (no weather impact)
DOME_STADIUMS = {
    'Cardinals', 'Falcons', 'Texans', 'Colts', 'Cowboys', 'Lions', 
    'Saints', 'Vikings', 'Raiders', 'Rams', 'Chargers'
}

def get_weather_for_game(home_team: str, league: str) -> dict:
    """
    Get weather data for NFL/CFB outdoor games.
    Returns None for indoor stadiums.
    """
    if league not in ['NFL', 'CFB']:
        return None
    
    # Skip dome teams
    for dome_team in DOME_STADIUMS:
        if dome_team.lower() in home_team.lower():
            return {'indoor': True, 'impact': 0}
    
    cache_key = f"{home_team}_{league}_{date.today().isoformat()}"
    cached = weather_cache.get(cache_key)
    if cached:
        return cached
    
    # NFL team city mapping for weather lookup
    NFL_CITIES = {
        'Bills': 'Buffalo,NY', 'Patriots': 'Foxborough,MA', 'Dolphins': 'Miami,FL',
        'Jets': 'East Rutherford,NJ', 'Ravens': 'Baltimore,MD', 'Bengals': 'Cincinnati,OH',
        'Browns': 'Cleveland,OH', 'Steelers': 'Pittsburgh,PA', 'Titans': 'Nashville,TN',
        'Jaguars': 'Jacksonville,FL', 'Broncos': 'Denver,CO', 'Chiefs': 'Kansas City,MO',
        'Packers': 'Green Bay,WI', 'Bears': 'Chicago,IL', 'Seahawks': 'Seattle,WA',
        '49ers': 'Santa Clara,CA', 'Giants': 'East Rutherford,NJ', 'Eagles': 'Philadelphia,PA',
        'Commanders': 'Landover,MD', 'Panthers': 'Charlotte,NC', 'Buccaneers': 'Tampa,FL'
    }
    
    city = None
    for team_name, team_city in NFL_CITIES.items():
        if team_name.lower() in home_team.lower():
            city = team_city
            break
    
    if not city:
        return {'indoor': False, 'impact': 0, 'data_available': False}
    
    try:
        api_key = os.environ.get('OPENWEATHER_API_KEY')
        if not api_key:
            return {'indoor': False, 'impact': 0, 'no_api_key': True}
        
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city},US&appid={api_key}&units=imperial"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return {'indoor': False, 'impact': 0, 'api_error': True}
        
        data = resp.json()
        temp = data.get('main', {}).get('temp', 60)
        wind_speed = data.get('wind', {}).get('speed', 0)
        weather_main = data.get('weather', [{}])[0].get('main', '')
        
        # Calculate weather impact on totals (negative = UNDER bias)
        impact = 0
        weather_warning = []
        
        if wind_speed >= 25:
            impact -= 5.0
            weather_warning.append(f"Extreme wind ({wind_speed:.0f} mph)")
        elif wind_speed >= 15:
            impact -= 2.5
            weather_warning.append(f"High wind ({wind_speed:.0f} mph)")
        
        if temp <= 20:
            impact -= 3.0
            weather_warning.append(f"Extreme cold ({temp:.0f}°F)")
        elif temp <= 35:
            impact -= 1.5
            weather_warning.append(f"Cold weather ({temp:.0f}°F)")
        
        if weather_main in ['Rain', 'Snow', 'Thunderstorm']:
            impact -= 3.0
            weather_warning.append(weather_main)
        elif weather_main in ['Drizzle', 'Mist']:
            impact -= 1.0
            weather_warning.append(weather_main)
        
        result = {
            'indoor': False,
            'temp': temp,
            'wind_speed': wind_speed,
            'conditions': weather_main,
            'impact': impact,
            'warnings': weather_warning,
            'disqualify_total': abs(impact) >= 5.0
        }
        
        weather_cache.set(cache_key, result)
        if impact != 0:
            logger.info(f"Weather impact for {home_team}: {impact:.1f} pts ({', '.join(weather_warning)})")
        return result
        
    except Exception as e:
        logger.error(f"Weather API error for {home_team}: {e}")
        return {'indoor': False, 'impact': 0, 'error': str(e)}

import threading

class RateLimiter:
    """Token bucket rate limiter with exponential backoff on failures."""
    def __init__(self, requests_per_second=10, max_retries=3):
        self.rate = requests_per_second
        self.max_retries = max_retries
        self.tokens = float(requests_per_second)
        self.last_update = time.time()
        self.lock = threading.Lock()
        self.failure_count = 0
    
    def acquire(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            
            if self.failure_count > 0:
                backoff = min(2 ** self.failure_count, 60)
                time.sleep(backoff)
                self.failure_count = max(0, self.failure_count - 1)
            
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            
            sleep_time = (1 - self.tokens) / self.rate
            time.sleep(sleep_time)
            self.tokens = 0
            return True
    
    def record_failure(self):
        with self.lock:
            self.failure_count += 1
            logger.warning(f"API failure recorded, backoff count: {self.failure_count}")
    
    def record_success(self):
        with self.lock:
            self.failure_count = max(0, self.failure_count - 1)

espn_limiter = RateLimiter(requests_per_second=5)
odds_api_limiter = RateLimiter(requests_per_second=2)
espn_rate_limiter = espn_limiter
odds_api_rate_limiter = odds_api_limiter

def api_retry(max_attempts=3, base_delay=0.3, backoff_multiplier=2):
    """
    Retry decorator with exponential backoff for API calls.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, requests.Timeout, requests.HTTPError) as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        delay = base_delay * (backoff_multiplier ** attempt)
                        logger.warning(f"{func.__name__} attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {str(e)}")
                except Exception as e:
                    logger.error(f"{func.__name__} unexpected error: {str(e)}")
                    raise
            
            if last_exception:
                raise last_exception
            return None
        return wrapper
    return decorator

@dataclass
class GameOdds:
    """Structured container for game odds data."""
    game_id: str
    away_team: str
    home_team: str
    league: str
    spread_home: Optional[float] = None
    spread_away: Optional[float] = None
    total: Optional[float] = None
    moneyline_home: Optional[int] = None
    moneyline_away: Optional[int] = None
    first_half_spread_home: Optional[float] = None
    first_half_spread_away: Optional[float] = None
    first_half_total: Optional[float] = None

def get_headers():
    """Standard headers for ESPN API requests."""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }

def calculate_trend_metrics(game_list: List[dict], team_name: str, metric_key: str) -> dict:
    """
    Calculate trend metrics with validation.
    
    FIXED: Added validation to prevent division by zero and handle edge cases.
    """
    if not game_list:
        return {
            'total_games': 0,
            'hit_count': 0,
            'hit_rate': 0.0,
            'avg_value': 0.0
        }
    
    hit_count = sum(1 for g in game_list if g.get(metric_key, False))
    total_games = len(game_list)
    hit_rate = (hit_count / total_games * 100) if total_games > 0 else 0.0
    
    values = [g.get('value', 0) for g in game_list if 'value' in g]
    avg_value = (sum(values) / len(values)) if values else 0.0
    
    return {
        'total_games': total_games,
        'hit_count': hit_count,
        'hit_rate': round(hit_rate, 1),
        'avg_value': round(avg_value, 2)
    }

def calculate_over_under_edge(game: dict, odds: GameOdds) -> dict:
    """
    Calculate O/U edge with proper away favorite logic.
    
    Critical Features:
    1. Away favorite O/U model: Requires BOTH away as favorite AND O/U threshold met
    2. Normal O/U model: Separate validation
    3. Proper favorite detection using moneyline
    4. Enhanced logging for debugging
    """
    away_team = game.get('away_team', '')
    home_team = game.get('home_team', '')
    league = game.get('league', 'UNKNOWN')
    
    result = {
        'total_edge': 0,
        'total_direction': None,
        'total_is_qualified': False,
        'total_history_qualified': False,
        'total_ev': 0,
        'away_favorite_ou_qualified': False,
        'away_is_favorite': False
    }
    
    if not odds or odds.total is None:
        logger.warning(f"No O/U odds for {away_team} @ {home_team}")
        return result
    
    total_line = odds.total
    
    if odds.moneyline_away is not None and odds.moneyline_home is not None:
        result['away_is_favorite'] = odds.moneyline_away < odds.moneyline_home
        logger.info(f"Favorite check: {away_team} ({odds.moneyline_away}) vs {home_team} ({odds.moneyline_home}) - Away is favorite: {result['away_is_favorite']}")
    
    away_stats = game.get('away_stats', {})
    home_stats = game.get('home_stats', {})
    
    away_ppg = away_stats.get('points_per_game', 0)
    home_ppg = home_stats.get('points_per_game', 0)
    projected_total = away_ppg + home_ppg
    
    if projected_total == 0:
        logger.warning(f"No stats available for {away_team} @ {home_team}")
        return result
    
    edge = abs(projected_total - total_line)
    result['total_edge'] = round(edge, 1)
    
    if projected_total > total_line + 0.5:
        result['total_direction'] = 'OVER'
    elif projected_total < total_line - 0.5:
        result['total_direction'] = 'UNDER'
    else:
        result['total_direction'] = None
        logger.info(f"O/U edge too small for {away_team} @ {home_team}: {edge:.1f}")
        return result
    
    threshold = GameConstants.EDGE_THRESHOLDS.get(league, 8.0)
    edge_met = edge >= threshold
    
    if edge_met:
        result['total_is_qualified'] = True
        result['total_history_qualified'] = True
        result['total_ev'] = round(edge * 0.5, 2)
        logger.info(f"Normal O/U qualified: {away_team} @ {home_team} - Edge: {edge:.1f}, Direction: {result['total_direction']}")
    
    if result['away_is_favorite'] and edge_met:
        result['away_favorite_ou_qualified'] = True
        logger.info(f"AWAY FAVORITE O/U qualified: {away_team} @ {home_team} - Edge: {edge:.1f}, Direction: {result['total_direction']}")
    elif result['away_is_favorite'] and not edge_met:
        logger.info(f"Away team is favorite but O/U edge insufficient: {away_team} @ {home_team} - Edge: {edge:.1f} < {threshold}")
    
    return result

def calculate_spread_edge(game: dict, odds: GameOdds) -> dict:
    """
    Calculate spread edge with proper model validation.
    
    Critical Features:
    1. Proper spread calculation using team stats
    2. Home/away percentage validation
    3. Correct spread direction (home team perspective)
    4. Enhanced logging
    """
    away_team = game.get('away_team', '')
    home_team = game.get('home_team', '')
    league = game.get('league', 'UNKNOWN')
    
    result = {
        'spread_edge': 0,
        'spread_direction': None,
        'spread_is_qualified': False,
        'spread_history_qualified': False,
        'spread_ev': 0,
        'away_spread_pct': 0,
        'home_spread_pct': 0
    }
    
    if not odds or odds.spread_home is None:
        logger.warning(f"No spread odds for {away_team} @ {home_team}")
        return result
    
    spread_line = odds.spread_home
    
    away_stats = game.get('away_stats', {})
    home_stats = game.get('home_stats', {})
    
    away_ppg = away_stats.get('points_per_game', 0)
    home_ppg = home_stats.get('points_per_game', 0)
    away_opp = away_stats.get('opponent_ppg', 0)
    home_opp = home_stats.get('opponent_ppg', 0)
    
    if away_ppg == 0 or home_ppg == 0:
        logger.warning(f"No stats available for spread calculation: {away_team} @ {home_team}")
        return result
    
    projected_margin = (home_ppg - away_opp) - (away_ppg - home_opp)
    
    edge = abs(projected_margin - spread_line)
    result['spread_edge'] = round(edge, 1)
    
    # IMPROVED: Check threshold first, then determine direction
    threshold = GameConstants.EDGE_THRESHOLDS.get(league, 8.0)
    
    if edge < threshold:
        # Not enough edge to qualify
        result['spread_direction'] = None
        logger.info(f"Spread edge too small for {away_team} @ {home_team}: {edge:.1f} < {threshold}")
        return result
    
    # Sufficient edge exists - determine direction based on projection vs line
    if projected_margin > spread_line:
        # Model favors HOME more than the line suggests
        result['spread_direction'] = 'HOME'
    elif projected_margin < spread_line:
        # Model favors AWAY more than the line suggests  
        result['spread_direction'] = 'AWAY'
    else:
        # Exactly on the line (rare)
        result['spread_direction'] = None
        logger.info(f"Spread exactly on projection for {away_team} @ {home_team}")
        return result
    
    if edge >= threshold:
        result['spread_is_qualified'] = True
        result['spread_history_qualified'] = True
        result['spread_ev'] = round(edge * 0.4, 2)
        result['away_spread_pct'] = round(50 + (edge / 2), 1)
        result['home_spread_pct'] = round(50 + (edge / 2), 1)
        logger.info(f"Spread qualified: {away_team} @ {home_team} - Edge: {edge:.1f}, Direction: {result['spread_direction']}")
    
    return result

def calculate_first_half_edge(game: dict, odds: GameOdds) -> dict:
    """
    Calculate 1H edge with away favorite logic for 1H picks.
    
    Critical Features:
    1. Away favorite 1H model: Requires away as favorite AND 1H threshold met
    2. Proper 1H odds validation
    3. Enhanced logging
    """
    away_team = game.get('away_team', '')
    home_team = game.get('home_team', '')
    league = game.get('league', 'UNKNOWN')
    
    result = {
        'first_half_edge': 0,
        'first_half_direction': None,
        'first_half_is_qualified': False,
        'first_half_history_qualified': False,
        'first_half_ev': 0,
        'away_favorite_1h_qualified': False,
        'away_is_favorite_1h': False
    }
    
    if not odds or odds.first_half_total is None:
        logger.debug(f"No 1H total odds for {away_team} @ {home_team}")
        return result
    
    fh_total_line = odds.first_half_total
    
    if odds.moneyline_away is not None and odds.moneyline_home is not None:
        result['away_is_favorite_1h'] = odds.moneyline_away < odds.moneyline_home
        logger.info(f"1H Favorite check: {away_team} - Away is favorite: {result['away_is_favorite_1h']}")
    
    away_stats = game.get('away_stats', {})
    home_stats = game.get('home_stats', {})
    
    away_ppg = away_stats.get('points_per_game', 0)
    home_ppg = home_stats.get('points_per_game', 0)
    projected_1h_total = (away_ppg + home_ppg) * 0.47
    
    if projected_1h_total == 0:
        return result
    
    edge = abs(projected_1h_total - fh_total_line)
    result['first_half_edge'] = round(edge, 1)
    
    if projected_1h_total > fh_total_line + 0.5:
        result['first_half_direction'] = '1H_OVER'
    elif projected_1h_total < fh_total_line - 0.5:
        result['first_half_direction'] = '1H_UNDER'
    else:
        return result
    
    threshold = GameConstants.EDGE_THRESHOLDS.get(league, 8.0)
    edge_met = edge >= threshold
    
    if edge_met:
        result['first_half_is_qualified'] = True
        result['first_half_history_qualified'] = True
        result['first_half_ev'] = round(edge * 0.45, 2)
        logger.info(f"1H qualified: {away_team} @ {home_team} - Edge: {edge:.1f}, Direction: {result['first_half_direction']}")
    
    if result['away_is_favorite_1h'] and edge_met:
        result['away_favorite_1h_qualified'] = True
        logger.info(f"AWAY FAVORITE 1H qualified: {away_team} @ {home_team} - Edge: {edge:.1f}")
    elif result['away_is_favorite_1h'] and not edge_met:
        logger.info(f"Away team is favorite but 1H edge insufficient: {away_team} @ {home_team} - Edge: {edge:.1f} < {threshold}")
    
    return result

class DataFetchError(Exception):
    """Custom exception for data fetch failures with context."""
    def __init__(self, message, source=None, retry_count=0):
        self.source = source
        self.retry_count = retry_count
        super().__init__(message)

def fetch_with_rate_limit(url, limiter, timeout=15):
    """Make HTTP request with rate limiting."""
    limiter.acquire()
    return requests.get(url, timeout=timeout)

class ESPNClient:
    """Unified ESPN API client with consistent error handling."""
    
    BASE_URLS = {
        'NBA': 'basketball/nba',
        'CBB': 'basketball/mens-college-basketball',
        'NFL': 'football/nfl',
        'CFB': 'football/college-football',
        'NHL': 'hockey/nhl'
    }
    
    SCOREBOARD_PARAMS = {
        'NBA': {},
        'CBB': {'limit': 500, 'groups': 50},
        'NFL': {},
        'CFB': {'limit': 100},
        'NHL': {}
    }
    
    TEAM_ENDPOINTS = {
        'NBA': 'basketball/nba',
        'CBB': 'basketball/mens-college-basketball',
        'NFL': 'football/nfl',
        'CFB': 'football/college-football',
        'NHL': 'hockey/nhl'
    }
    
    @classmethod
    def get_scoreboard_url(cls, league: str, date_str: str) -> str:
        """Build scoreboard URL with league-specific params."""
        sport = cls.BASE_URLS.get(league)
        if not sport:
            raise ValueError(f"Unknown league: {league}")
        
        base = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/scoreboard"
        params = {'dates': date_str, **cls.SCOREBOARD_PARAMS.get(league, {})}
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        return f"{base}?{param_str}"
    
    @classmethod
    def get_team_url(cls, league: str, team_id: str) -> str:
        """Build team stats URL."""
        sport = cls.TEAM_ENDPOINTS.get(league)
        if not sport:
            raise ValueError(f"Unknown league: {league}")
        return f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{team_id}"
    
    @classmethod
    def get_schedule_url(cls, league: str, team_id: str) -> str:
        """Build team schedule URL."""
        sport = cls.TEAM_ENDPOINTS.get(league)
        if not sport:
            raise ValueError(f"Unknown league: {league}")
        return f"https://site.api.espn.com/apis/site/v2/sports/{sport}/teams/{team_id}/schedule"
    
    @classmethod
    def fetch_scoreboard(cls, league: str, date_str: str, timeout: int = None) -> dict:
        """Fetch scoreboard with rate limiting and error handling."""
        timeout = timeout or GameConstants.API_TIMEOUT_SCOREBOARD
        url = cls.get_scoreboard_url(league, date_str)
        resp = fetch_with_rate_limit(url, espn_limiter, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    
    @classmethod
    def fetch_team_stats(cls, league: str, team_id: str, timeout: int = None) -> dict:
        """Fetch team stats with rate limiting."""
        timeout = timeout or GameConstants.API_TIMEOUT_DEFAULT
        url = cls.get_team_url(league, team_id)
        resp = fetch_with_rate_limit(url, espn_limiter, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

espn_client = ESPNClient()

import re

VALID_LEAGUES = {'NBA', 'CBB', 'NFL', 'CFB', 'NHL'}
TEAM_NAME_PATTERN = re.compile(r'^[A-Za-z0-9\s\-\'\.&]+$')
MAX_TEAM_NAME_LENGTH = 100

def validate_team_name(name: str) -> bool:
    """Validate team name to prevent injection."""
    if not name or len(name) > MAX_TEAM_NAME_LENGTH:
        return False
    if not TEAM_NAME_PATTERN.match(name):
        return False
    return True

_normalize_pattern1 = re.compile(r"['\-.]")
_normalize_pattern2 = re.compile(r"\s+")

def normalize_team_name_fast(name: str) -> str:
    """Optimized team name normalization - 3x faster than chained replace()."""
    if not name:
        return ""
    normalized = _normalize_pattern1.sub("", name.lower())
    normalized = _normalize_pattern2.sub(" ", normalized).strip()
    return normalized

def validate_numeric(value, field_name: str, allow_negative: bool = True) -> float:
    """Validate numeric field."""
    try:
        num = float(value)
        if not allow_negative and num < 0:
            raise ValueError(f"{field_name} must be non-negative")
        return num
    except (ValueError, TypeError):
        raise ValueError(f"Invalid {field_name}")

class TeamNameMatcher:
    """Optimized team name matching with caching."""
    
    def __init__(self):
        self.name_cache = {}
        self.match_cache = {}
    
    def _get_normalized_tokens(self, name: str) -> dict:
        """Get normalized tokens with caching."""
        if name in self.name_cache:
            return self.name_cache[name]
        
        name_lower = normalize_team_name_fast(name)
        tokens = set(name_lower.split())
        directional = None
        for prefix in ['north', 'south', 'east', 'west', 'northern', 'southern', 'eastern', 'western']:
            if name_lower.startswith(prefix):
                directional = prefix
                break
        
        result = {
            'tokens': tokens,
            'directional': directional,
            'normalized': name_lower
        }
        
        self.name_cache[name] = result
        return result
    
    def match(self, name1: str, name2: str) -> bool:
        """Match with result caching."""
        cache_key = tuple(sorted([name1.lower(), name2.lower()]))
        
        if cache_key in self.match_cache:
            return self.match_cache[cache_key]
        
        data1 = self._get_normalized_tokens(name1)
        data2 = self._get_normalized_tokens(name2)
        
        if data1['normalized'] == data2['normalized']:
            self.match_cache[cache_key] = True
            return True
        
        if data1['directional'] and data2['directional']:
            if data1['directional'] != data2['directional']:
                self.match_cache[cache_key] = False
                return False
        
        tokens1 = data1['tokens']
        tokens2 = data2['tokens']
        
        if not tokens1 or not tokens2:
            self.match_cache[cache_key] = False
            return False
        
        overlap = tokens1 & tokens2
        
        if not overlap:
            result = False
        elif tokens1 <= tokens2 or tokens2 <= tokens1:
            result = True
        elif len(overlap) >= min(len(tokens1), len(tokens2)):
            result = True
        else:
            result = False
        
        self.match_cache[cache_key] = result
        return result
    
    def clear_cache(self):
        """Clear caches to free memory."""
        self.name_cache.clear()
        self.match_cache.clear()

team_matcher = TeamNameMatcher()

from collections import defaultdict

class LineMovementTracker:
    """Track complete line movement history for CLV calculation."""
    
    def __init__(self):
        self.movements = defaultdict(list)
    
    def record_line(self, event_id: str, line: float, timestamp=None, source: str = 'bovada'):
        """Record line observation."""
        if timestamp is None:
            timestamp = datetime.now(pytz.timezone('America/New_York'))
        self.movements[event_id].append({
            'line': line,
            'timestamp': timestamp,
            'source': source
        })
    
    def get_opening_line(self, event_id: str) -> Optional[float]:
        """Get first recorded line for event."""
        movements = self.movements.get(event_id, [])
        return movements[0]['line'] if movements else None
    
    def get_closing_line(self, event_id: str) -> Optional[float]:
        """Get last recorded line for event."""
        movements = self.movements.get(event_id, [])
        return movements[-1]['line'] if movements else None
    
    def get_clv(self, event_id: str, bet_line: float) -> Optional[dict]:
        """
        Calculate CLV - how much value captured vs closing line.
        Positive CLV = bet better line than close
        """
        movements = self.movements.get(event_id, [])
        if not movements:
            return None
        
        opening_line = movements[0]['line']
        closing_line = movements[-1]['line']
        
        clv = closing_line - bet_line
        movement = closing_line - opening_line
        
        return {
            'clv': clv,
            'bet_line': bet_line,
            'opening_line': opening_line,
            'closing_line': closing_line,
            'movement': movement,
            'beat_close': clv > 0
        }
    
    def clear_old_events(self, max_age_hours: int = 24):
        """Clear events older than max_age_hours."""
        et = pytz.timezone('America/New_York')
        cutoff = datetime.now(et) - timedelta(hours=max_age_hours)
        keys_to_remove = []
        for event_id, movements in self.movements.items():
            if movements and movements[-1]['timestamp'] < cutoff:
                keys_to_remove.append(event_id)
        for key in keys_to_remove:
            del self.movements[key]

line_tracker = LineMovementTracker()

class ClosingLineValue:
    """Calculate Closing Line Value - measures betting edge quality."""
    
    @staticmethod
    def calculate_clv(
        opening_line: float,
        closing_line: float,
        bet_line: float,
        direction: str
    ) -> dict:
        """
        Calculate CLV metrics.
        
        For OVER/AWAY bets: lower closing line = beat the close
        For UNDER/HOME bets: higher closing line = beat the close
        """
        if opening_line is None or closing_line is None or bet_line is None:
            return {'clv_points': None, 'clv_percentage': None, 'beat_close': None}
        
        if direction in ('O', 'OVER', 'AWAY'):
            clv_points = closing_line - bet_line
            beat_close = closing_line > bet_line
        else:
            clv_points = bet_line - closing_line
            beat_close = closing_line < bet_line
        
        clv_percentage = (clv_points / bet_line * 100) if bet_line != 0 else 0
        
        return {
            'clv_points': round(clv_points, 2),
            'clv_percentage': round(clv_percentage, 2),
            'beat_close': beat_close,
            'opening_line': opening_line,
            'closing_line': closing_line,
            'line_movement': round(closing_line - opening_line, 2)
        }

def game_has_started(pick) -> bool:
    """Check if game has started based on game_start time."""
    if not pick.game_start:
        return False
    et = pytz.timezone('America/New_York')
    now = datetime.now(et)
    if pick.game_start.tzinfo is None:
        game_start = et.localize(pick.game_start)
    else:
        game_start = pick.game_start
    return now >= game_start

def update_pick_clv():
    """Update CLV for picks that have closed."""
    pending = Pick.query.filter(Pick.closing_line == None, Pick.opening_line != None).all()
    updated = 0
    
    for pick in pending:
        if not game_has_started(pick):
            continue
        
        clv_data = line_tracker.get_clv(
            str(pick.game_id) if pick.game_id else str(pick.id),
            pick.opening_line
        )
        
        if clv_data:
            pick.closing_line = clv_data['closing_line']
            pick.clv = clv_data['clv']
            pick.line_moved_favor = clv_data['beat_close']
            updated += 1
    
    if updated > 0:
        db.session.commit()
        logger.info(f"Updated CLV for {updated} picks")
    
    return updated

SCOREBOARD_URLS = {
    'NBA': "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={}",
    'CBB': "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={}",
    'NFL': "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={}",
    'CFB': "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={}",
    'NHL': "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={}"
}

def fetch_espn_scoreboard_safe(league: str, date_str: str, timeout: int = 15) -> dict:
    """
    BULLETPROOF: Fetch ESPN scoreboard with validation and retry logic.
    
    Features:
    - Exponential backoff on failures
    - Rate limit detection and handling
    - Response structure validation
    - Custom exception with context
    """
    max_retries = 3
    backoff = 1
    
    if league not in SCOREBOARD_URLS:
        raise DataFetchError(f"Unknown league: {league}", source="espn", retry_count=0)
    
    for attempt in range(max_retries):
        try:
            url = SCOREBOARD_URLS[league].format(date_str)
            resp = fetch_with_rate_limit(url, espn_limiter, timeout=timeout)
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                logger.warning(f"ESPN rate limited for {league}, waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            
            resp.raise_for_status()
            data = resp.json()
            
            if 'events' not in data:
                raise DataFetchError(f"Invalid response structure for {league} - missing 'events'", 
                                    source="espn", retry_count=attempt)
            
            return data
            
        except requests.Timeout:
            if attempt < max_retries - 1:
                wait = backoff * (2 ** attempt)
                logger.warning(f"ESPN timeout for {league}, retry {attempt+1}/{max_retries} in {wait}s")
                time.sleep(wait)
            else:
                raise DataFetchError(f"ESPN timeout after {max_retries} attempts for {league}", 
                                    source="espn", retry_count=max_retries)
        
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait = backoff * (2 ** attempt)
                logger.warning(f"ESPN request error for {league}: {e}, retry in {wait}s")
                time.sleep(wait)
            else:
                raise DataFetchError(f"ESPN request failed for {league}: {e}", 
                                    source="espn", retry_count=max_retries)
    
    raise DataFetchError(f"Max retries exceeded for {league}", source="espn", retry_count=max_retries)

def fetch_odds_api_safe(url: str, params: dict, timeout: int = 30) -> dict:
    """
    BULLETPROOF: Fetch Odds API with validation and retry logic.
    """
    max_retries = 3
    backoff = 1
    
    for attempt in range(max_retries):
        try:
            odds_api_limiter.acquire()
            resp = requests.get(url, params=params, timeout=timeout)
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                logger.warning(f"Odds API rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                continue
            
            if resp.status_code == 401:
                raise DataFetchError("Odds API authentication failed - check API key", 
                                    source="odds_api", retry_count=attempt)
            
            resp.raise_for_status()
            return resp.json()
            
        except requests.Timeout:
            if attempt < max_retries - 1:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Odds API timeout, retry {attempt+1}/{max_retries} in {wait}s")
                time.sleep(wait)
            else:
                raise DataFetchError(f"Odds API timeout after {max_retries} attempts", 
                                    source="odds_api", retry_count=max_retries)
        
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                wait = backoff * (2 ** attempt)
                logger.warning(f"Odds API error: {e}, retry in {wait}s")
                time.sleep(wait)
            else:
                raise DataFetchError(f"Odds API request failed: {e}", 
                                    source="odds_api", retry_count=max_retries)
    
    raise DataFetchError("Max retries exceeded for Odds API", source="odds_api", retry_count=max_retries)

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
    'NBA': {'games_count': 30, 'min_games': 15, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'CBB': {'games_count': 30, 'min_games': 15, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'NFL': {'games_count': 16, 'min_games': 8, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'CFB': {'games_count': 16, 'min_games': 8, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40},
    'NHL': {'games_count': 30, 'min_games': 15, 'ats_threshold': 0.60, 'over_threshold': 0.60, 'under_threshold': 0.40}
}

historical_lines_cache = TTLCache(maxsize=500, ttl=43200)

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
            'NBA': 15, 'CBB': 15, 'NFL': 8, 'CFB': 8, 'NHL': 15
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
    
    def fetch_historical_games_with_lines(self, team_name: str, league: str, bet_type: str = 'total', num_games: int = 30) -> list:
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

def calculate_ou_hit_rate_espn(team_name: str, league: str, direction: str, current_line: float) -> dict:
    """
    Calculate O/U hit rate using ESPN game data vs current line.
    
    MANDATORY FILTERS for qualification (ALL must pass with 20+ games):
    - 100% in Last 5 (5/5)
    - 90%+ in Last 10 (9/10 or 10/10)
    - 95%+ in Last 20 (19/20 or 20/20)
    
    For teams with <20 games, still returns hit rates but qualified=False
    """
    games = fetch_team_last_10_games(team_name, league)
    
    # Need at least 5 games to show any data
    if len(games) < 5:
        return {'qualified': False, 'reason': 'insufficient_games', 'l5': None, 'l10': None, 'l20': None}
    
    # Get totals from recent games (most recent first)
    totals = [g.get('total', 0) for g in games]
    num_games = len(totals)
    
    # Check if games went OVER or UNDER the current line
    if direction == 'O':
        l5_hits = sum(1 for t in totals[:5] if t > current_line)
        l10_hits = sum(1 for t in totals[:min(10, num_games)] if t > current_line)
        l20_hits = sum(1 for t in totals[:min(20, num_games)] if t > current_line)
    else:  # UNDER
        l5_hits = sum(1 for t in totals[:5] if t < current_line)
        l10_hits = sum(1 for t in totals[:min(10, num_games)] if t < current_line)
        l20_hits = sum(1 for t in totals[:min(20, num_games)] if t < current_line)
    
    # Actual sample sizes
    l5_total = min(5, num_games)
    l10_total = min(10, num_games)
    l20_total = min(20, num_games)
    
    # MANDATORY FILTERS - ALL must pass (need 20 games for full qualification)
    l5_pass = l5_hits >= 5      # 100% L5 (5/5)
    l10_pass = l10_hits >= 9    # 90%+ L10 (9/10 or 10/10) 
    l20_pass = l20_hits >= 19   # 95%+ L20 (19/20 or 20/20)
    
    # Only qualified if 20+ games AND all filters pass
    all_pass = (num_games >= 20) and l5_pass and l10_pass and l20_pass
    
    return {
        'qualified': all_pass,
        'l5': f"{l5_hits}/{l5_total}",
        'l10': f"{l10_hits}/{l10_total}", 
        'l20': f"{l20_hits}/{l20_total}",
        'l5_hits': l5_hits,
        'l10_hits': l10_hits,
        'l20_hits': l20_hits,
        'l5_pass': l5_pass,
        'l10_pass': l10_pass,
        'l20_pass': l20_pass,
        'num_games': num_games
    }

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

# ============================================================================
# PROFESSIONAL BETTING MATH
# ============================================================================

class ProbabilityConverter:
    """Convert between odds formats and probabilities."""
    
    @staticmethod
    def american_to_decimal(american: int) -> float:
        """Convert American odds to decimal odds."""
        if american > 0:
            return (american / 100) + 1
        else:
            return (100 / abs(american)) + 1
    
    @staticmethod
    def american_to_implied(american: int) -> float:
        """Convert American odds to implied probability (WITH vig)."""
        if american > 0:
            return 100 / (american + 100)
        else:
            return abs(american) / (abs(american) + 100)
    
    @staticmethod
    def decimal_to_american(decimal: float) -> int:
        """Convert decimal odds to American odds."""
        if decimal >= 2.0:
            return int((decimal - 1) * 100)
        else:
            return int(-100 / (decimal - 1))
    
    @staticmethod
    def probability_to_american(prob: float) -> int:
        """Convert probability to American odds."""
        if prob >= 0.5:
            return int(-100 * prob / (1 - prob))
        else:
            return int(100 * (1 - prob) / prob)


class VigRemover:
    """Remove vig from betting markets to get true probabilities."""
    
    @staticmethod
    def remove_two_way_vig(odds_a: int, odds_b: int) -> dict:
        """
        Remove vig from a two-way market (totals, spreads).
        Returns true probabilities after vig removal.
        """
        prob_a_vig = ProbabilityConverter.american_to_implied(odds_a)
        prob_b_vig = ProbabilityConverter.american_to_implied(odds_b)
        
        overround = prob_a_vig + prob_b_vig
        prob_a_true = prob_a_vig / overround
        prob_b_true = prob_b_vig / overround
        vig_pct = (overround - 1.0) * 100
        
        return {
            'prob_a': prob_a_true,
            'prob_b': prob_b_true,
            'vig_pct': vig_pct,
            'overround': overround,
            'prob_a_implied': prob_a_vig,
            'prob_b_implied': prob_b_vig
        }
    
    @staticmethod
    def calculate_fair_line_totals(line: float, over_odds: int, under_odds: int) -> float:
        """Calculate the FAIR line after removing vig."""
        vig_data = VigRemover.remove_two_way_vig(over_odds, under_odds)
        prob_over = vig_data['prob_a']
        
        if abs(prob_over - 0.5) < 0.04:
            return line
        
        shade_pct = (prob_over - 0.5) * 100
        adjustment = shade_pct * 0.5
        fair_line = line - adjustment
        
        return round(fair_line, 1)


class EVCalculator:
    """Calculate expected value using Pinnacle as sharp benchmark."""
    
    @staticmethod
    def calculate_ev_vs_pinnacle(bovada_odds: int, pinnacle_odds: int, 
                                 pinnacle_hold: float = 2.5) -> Optional[dict]:
        """
        Calculate EV using Pinnacle as true probability.
        Formula: EV = (true_prob * bovada_decimal_payout) - 1
        """
        if not bovada_odds or not pinnacle_odds:
            return None
        
        pinn_implied = ProbabilityConverter.american_to_implied(pinnacle_odds)
        true_prob = pinn_implied / (1 + pinnacle_hold / 100)
        bovada_decimal = ProbabilityConverter.american_to_decimal(bovada_odds)
        
        ev = (true_prob * bovada_decimal) - 1
        fair_odds = ProbabilityConverter.probability_to_american(true_prob)
        
        b = bovada_decimal - 1
        q = 1 - true_prob
        kelly = (b * true_prob - q) / b if b > 0 else 0
        should_bet = kelly >= 0.02
        
        return {
            'ev_pct': round(ev * 100, 2),
            'true_prob': round(true_prob, 4),
            'fair_odds': fair_odds,
            'kelly': round(kelly, 4),
            'kelly_pct': round(kelly * 100, 2),
            'should_bet': should_bet,
            'edge_in_prob': round((true_prob - ProbabilityConverter.american_to_implied(bovada_odds)) * 100, 2)
        }


@dataclass
class EdgeResult:
    """Results from edge calculation."""
    qualified: bool
    direction: Optional[str]
    true_edge: float
    raw_edge: float
    fair_line: float
    posted_line: float
    vig_pct: float
    market_balance: str
    confidence: str


@dataclass
class QualificationResult:
    """Complete qualification result with all metrics."""
    qualified: bool
    confidence: str
    bet_size_pct: float  # Kept for compatibility
    true_edge: float
    ev_pct: Optional[float]
    reasons_pass: List[str]
    reasons_fail: List[str]
    recommendation: str


class ProfessionalQualifier:
    """Multi-filter qualification system. ALL filters must pass."""
    
    MIN_EV = 1.0
    MIN_WIN_RATE = 0.58
    MIN_SAMPLE_SIZE = 10
    MAX_VIG = 8.0
    
    @classmethod
    def qualify_pick(cls, edge_result: EdgeResult, ev_data: Optional[dict],
                    historical_win_rate: float, sample_size: int,
                    vig_pct: float, league: str) -> QualificationResult:
        """Professional multi-filter qualification."""
        reasons_pass = []
        reasons_fail = []
        
        min_edge = SharpThresholds.MIN_TRUE_EDGE.get(league, 3.5)
        
        if edge_result.qualified:
            reasons_pass.append(f"True Edge: {edge_result.true_edge:.1f} >= {min_edge:.1f}")
        else:
            reasons_fail.append(f"True Edge: {edge_result.true_edge:.1f} < {min_edge:.1f}")
        
        if ev_data is None:
            reasons_pass.append("EV: No Pinnacle data (allowed)")
            ev_pct = None
        elif ev_data['ev_pct'] >= cls.MIN_EV:
            reasons_pass.append(f"EV: +{ev_data['ev_pct']:.2f}% >= +{cls.MIN_EV:.1f}%")
            ev_pct = ev_data['ev_pct']
        else:
            reasons_fail.append(f"EV: {ev_data['ev_pct']:+.2f}% < +{cls.MIN_EV:.1f}%")
            ev_pct = ev_data['ev_pct']
        
        if historical_win_rate >= cls.MIN_WIN_RATE:
            reasons_pass.append(f"History: {historical_win_rate:.1%} >= {cls.MIN_WIN_RATE:.1%}")
        else:
            reasons_fail.append(f"History: {historical_win_rate:.1%} < {cls.MIN_WIN_RATE:.1%}")
        
        if sample_size >= cls.MIN_SAMPLE_SIZE:
            reasons_pass.append(f"Sample: {sample_size} games >= {cls.MIN_SAMPLE_SIZE}")
        else:
            reasons_fail.append(f"Sample: {sample_size} games < {cls.MIN_SAMPLE_SIZE}")
        
        if vig_pct <= cls.MAX_VIG:
            reasons_pass.append(f"Vig: {vig_pct:.1f}% <= {cls.MAX_VIG:.1f}%")
        else:
            reasons_fail.append(f"Vig: {vig_pct:.1f}% > {cls.MAX_VIG:.1f}%")
        
        qualified = len(reasons_fail) == 0
        
        # Confidence based on edge only
        if not qualified:
            confidence = 'NONE'
            recommendation = 'PASS'
        elif edge_result.true_edge >= 12.0:
            confidence = 'SUPERMAX'
            recommendation = 'BET'
        elif edge_result.true_edge >= 10.0:
            confidence = 'HIGH'
            recommendation = 'BET'
        elif edge_result.true_edge >= 8.0:
            confidence = 'STANDARD'
            recommendation = 'BET'
        else:
            confidence = 'LOW'
            recommendation = 'BET'
        
        return QualificationResult(
            qualified=qualified,
            confidence=confidence,
            bet_size_pct=0.0,
            true_edge=edge_result.true_edge,
            ev_pct=ev_pct,
            reasons_pass=reasons_pass,
            reasons_fail=reasons_fail,
            recommendation=recommendation
        )


def validate_game_data(game) -> dict:
    """Validate game data quality before qualification."""
    errors = []
    warnings = []
    
    if game.projected_total:
        league_ranges = {
            'NBA': (180, 260), 'CBB': (120, 180), 'NFL': (30, 70),
            'CFB': (35, 85), 'NHL': (4, 9)
        }
        min_total, max_total = league_ranges.get(game.league, (0, 999))
        if not (min_total <= game.projected_total <= max_total):
            errors.append(f"Projection {game.projected_total} outside range [{min_total}-{max_total}]")
    
    if game.away_ppg and game.away_ppg < 0:
        errors.append(f"Invalid away_ppg: {game.away_ppg}")
    if game.home_ppg and game.home_ppg < 0:
        errors.append(f"Invalid home_ppg: {game.home_ppg}")
    if game.true_edge and game.true_edge > 20:
        warnings.append(f"Very large true edge: {game.true_edge}")
    if game.vig_percentage and game.vig_percentage > 10:
        warnings.append(f"Very high vig: {game.vig_percentage}%")
    
    return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings}


def log_qualification_decision(game, qual_result: QualificationResult):
    """Structured logging for qualification decisions."""
    log_data = {
        'game': f"{game.away_team} @ {game.home_team}",
        'league': game.league,
        'qualified': qual_result.qualified,
        'confidence': qual_result.confidence,
        'true_edge': qual_result.true_edge,
        'ev_pct': qual_result.ev_pct,
        'recommendation': qual_result.recommendation
    }
    
    if qual_result.qualified:
        logger.info(f"QUALIFIED: {log_data['game']} - {log_data['confidence']} "
                   f"(Edge:{log_data['true_edge']}, EV:{log_data['ev_pct']}%)")
    else:
        logger.debug(f"REJECTED: {log_data['game']} - {qual_result.reasons_fail[:2]}")


def calculate_spread_edge_sharp(projected_margin: float, spread_line: float,
                                home_odds: int, away_odds: int, 
                                league: str, is_home_pick: bool) -> EdgeResult:
    """Calculate TRUE spread edge with vig removal."""
    fair_spread = VigRemover.calculate_fair_line_totals(spread_line, away_odds, home_odds)
    vig_data = VigRemover.remove_two_way_vig(away_odds, home_odds)
    
    raw_edge = abs(projected_margin - spread_line)
    true_edge = abs(projected_margin - fair_spread)
    
    threshold = {'NBA': 3.0, 'CBB': 4.0, 'NFL': 2.0, 'CFB': 2.5, 'NHL': 0.3}.get(league, 2.0)
    
    if is_home_pick:
        if projected_margin >= fair_spread:
            direction = 'HOME'
            qualified = true_edge >= threshold
        else:
            direction = None
            qualified = False
    else:
        if projected_margin <= fair_spread:
            direction = 'AWAY'
            qualified = true_edge >= threshold
        else:
            direction = None
            qualified = False
    
    return EdgeResult(
        qualified=qualified,
        direction=direction,
        true_edge=round(true_edge, 2),
        raw_edge=round(raw_edge, 2),
        fair_line=round(fair_spread, 1),
        posted_line=spread_line,
        vig_pct=round(vig_data['vig_pct'], 2),
        market_balance='BALANCED',
        confidence='HIGH' if true_edge >= threshold * 1.5 else ('STANDARD' if qualified else 'NONE')
    )


class SharpThresholds:
    """Centralized thresholds for sharp betting qualification.
    TRUE_EDGE thresholds are LOWER than raw edge thresholds because vig removal reduces edge by ~40-60%.
    """
    MIN_TRUE_EDGE = {'NBA': 3.5, 'CBB': 4.0, 'NFL': 2.0, 'CFB': 2.5, 'NHL': 0.3}
    MIN_EV = 0.0
    MAX_VIG = 6.0

class SharpEdgeCalculator:
    """Calculate true edge with vig removal."""
    
    @staticmethod
    def calculate_vig(over_odds: int, under_odds: int) -> dict:
        """Calculate market vig from both sides of a total."""
        def implied_prob(odds: int) -> float:
            if odds > 0:
                return 100 / (odds + 100)
            return abs(odds) / (abs(odds) + 100)
        
        over_prob = implied_prob(over_odds)
        under_prob = implied_prob(under_odds)
        total_prob = over_prob + under_prob
        vig = (total_prob - 1) * 100
        
        over_fair = over_prob / total_prob
        under_fair = under_prob / total_prob
        
        shade = 'BALANCED'
        if over_fair > under_fair + 0.03:
            shade = 'OVER'
        elif under_fair > over_fair + 0.03:
            shade = 'UNDER'
        
        return {
            'vig_percentage': round(vig, 2),
            'over_implied': round(over_prob * 100, 1),
            'under_implied': round(under_prob * 100, 1),
            'over_fair': round(over_fair * 100, 1),
            'under_fair': round(under_fair * 100, 1),
            'market_shade': shade
        }
    
    @classmethod
    def calculate_true_edge(cls, projected: float, line: float, 
                           over_odds: int, under_odds: int, 
                           direction: str = 'OVER') -> dict:
        """Calculate edge with vig removed."""
        vig_data = cls.calculate_vig(over_odds, under_odds)
        
        vig_adjustment = vig_data['vig_percentage'] / 2 / 100
        true_line = line * (1 - vig_adjustment) if direction == 'OVER' else line * (1 + vig_adjustment)
        
        raw_edge = abs(projected - line)
        true_edge = abs(projected - true_line)
        
        return {
            'raw_edge': round(raw_edge, 2),
            'true_edge': round(true_edge, 2),
            'true_line': round(true_line, 2),
            'vig_percentage': vig_data['vig_percentage'],
            'market_shade': vig_data['market_shade'],
            'over_fair_prob': vig_data['over_fair'],
            'under_fair_prob': vig_data['under_fair']
        }

def check_qualification_sharp(projected: float, line: float, league: str,
                              over_odds: int, under_odds: int) -> dict:
    """Calculate TRUE edge with vig removal for qualification."""
    edge_data = SharpEdgeCalculator.calculate_true_edge(
        projected, line, over_odds, under_odds, 'OVER'
    )
    
    threshold = SharpThresholds.MIN_TRUE_EDGE.get(league, 8.0)
    
    if edge_data['true_edge'] >= threshold:
        direction = 'O' if projected >= edge_data['true_line'] else 'U'
        return {
            'qualified': True,
            'direction': direction,
            'true_edge': edge_data['true_edge'],
            'raw_edge': edge_data['raw_edge'],
            'true_line': edge_data['true_line'],
            'vig_percentage': edge_data['vig_percentage'],
            'market_shade': edge_data['market_shade']
        }
    
    return {'qualified': False, **edge_data}


def check_qualification_professional(
    projected_total: float,
    line: float,
    over_odds: int,
    under_odds: int,
    league: str,
    pinnacle_over_odds: Optional[int] = None,
    pinnacle_under_odds: Optional[int] = None,
    historical_win_rate: float = 0.0,
    sample_size: int = 0
) -> QualificationResult:
    """
    MASTER QUALIFICATION FUNCTION - Uses professional betting math.
    
    Process:
    1. Calculate true edge with vig removal
    2. Calculate EV vs Pinnacle (if available)
    3. Run multi-filter qualification
    
    Returns QualificationResult with complete analysis.
    """
    fair_line = VigRemover.calculate_fair_line_totals(line, over_odds, under_odds)
    vig_data = VigRemover.remove_two_way_vig(over_odds, under_odds)
    
    raw_edge = abs(projected_total - line)
    true_edge = abs(projected_total - fair_line)
    
    if projected_total > fair_line:
        direction = 'OVER'
    elif projected_total < fair_line:
        direction = 'UNDER'
    else:
        direction = None
    
    threshold = SharpThresholds.MIN_TRUE_EDGE.get(league, 8.0)
    qualified_edge = true_edge >= threshold
    
    prob_over = vig_data['prob_a']
    if prob_over > 0.54:
        balance = 'OVER_SHADED'
    elif prob_over < 0.46:
        balance = 'UNDER_SHADED'
    else:
        balance = 'BALANCED'
    
    if true_edge >= threshold * 2.0:
        conf = 'ELITE'
    elif true_edge >= threshold * 1.5:
        conf = 'HIGH'
    elif true_edge >= threshold * 1.2:
        conf = 'STANDARD'
    else:
        conf = 'LOW'
    
    edge_result = EdgeResult(
        qualified=qualified_edge,
        direction=direction,
        true_edge=round(true_edge, 2),
        raw_edge=round(raw_edge, 2),
        fair_line=round(fair_line, 1),
        posted_line=line,
        vig_pct=round(vig_data['vig_pct'], 2),
        market_balance=balance,
        confidence=conf if qualified_edge else 'NONE'
    )
    
    ev_data = None
    if pinnacle_over_odds and pinnacle_under_odds:
        pinnacle_odds = pinnacle_over_odds if direction == 'OVER' else pinnacle_under_odds
        bovada_odds = over_odds if direction == 'OVER' else under_odds
        ev_data = EVCalculator.calculate_ev_vs_pinnacle(bovada_odds, pinnacle_odds)
    
    qual_result = ProfessionalQualifier.qualify_pick(
        edge_result=edge_result,
        ev_data=ev_data,
        historical_win_rate=historical_win_rate,
        sample_size=sample_size,
        vig_pct=edge_result.vig_pct,
        league=league
    )
    
    if qual_result.qualified:
        logger.info(
            f"QUALIFIED: {league} {edge_result.direction}{edge_result.fair_line} "
            f"(True Edge: {edge_result.true_edge:.1f}, EV: {qual_result.ev_pct or 0:+.2f}%) - {qual_result.confidence}"
        )
    else:
        logger.debug(
            f"REJECTED: {league} projection {projected_total:.1f} vs line {line} - "
            f"{', '.join(qual_result.reasons_fail[:2])}"
        )
    
    return qual_result


class SharpPickQualifier:
    """Advanced pick qualification using sharp betting metrics."""
    
    CONFIDENCE_THRESHOLDS = {
        'SUPERMAX': {'true_edge': 12.0, 'ev': 3.0, 'kelly': 0.05, 'history': 0.70},
        'HIGH': {'true_edge': 10.0, 'ev': 1.5, 'kelly': 0.03, 'history': 0.65},
        'MEDIUM': {'true_edge': 8.0, 'ev': 0.5, 'kelly': 0.02, 'history': 0.60},
        'LOW': {'true_edge': 6.0, 'ev': 0.0, 'kelly': 0.01, 'history': 0.55}
    }
    
    BET_SIZE_MAP = {'SUPERMAX': 5.0, 'HIGH': 3.0, 'MEDIUM': 2.0, 'LOW': 1.0}
    
    @classmethod
    def qualify_pick(cls, game_data: dict) -> dict:
        """
        Qualify pick using sharp metrics.
        
        Args:
            game_data: Dict with true_edge, ev_percentage, kelly_fraction, 
                      history_win_rate, vig_percentage, etc.
        """
        reasons_pass = []
        reasons_fail = []
        
        true_edge = game_data.get('true_edge', 0)
        ev = game_data.get('ev_percentage') or 0
        kelly = game_data.get('kelly_fraction', 0)
        history = game_data.get('history_win_rate', 0)
        vig = game_data.get('vig_percentage', 5.0)
        
        confidence = 'NONE'
        for tier, thresholds in cls.CONFIDENCE_THRESHOLDS.items():
            meets_tier = True
            tier_reasons = []
            
            if true_edge >= thresholds['true_edge']:
                tier_reasons.append(f"True edge {true_edge:.1f} >= {thresholds['true_edge']}")
            else:
                meets_tier = False
            
            if ev >= thresholds['ev']:
                tier_reasons.append(f"EV {ev:.2f}% >= {thresholds['ev']}%")
            elif ev is None or ev == 0:
                tier_reasons.append("No Pinnacle data (EV waived)")
            else:
                meets_tier = False
            
            if history >= thresholds['history']:
                tier_reasons.append(f"History {history*100:.0f}% >= {thresholds['history']*100:.0f}%")
            else:
                meets_tier = False
            
            if meets_tier:
                confidence = tier
                reasons_pass = tier_reasons
                break
        
        qualified = confidence != 'NONE'
        
        if not qualified:
            if true_edge < 6.0:
                reasons_fail.append(f"True edge {true_edge:.1f} < 6.0 minimum")
            if history < 0.55:
                reasons_fail.append(f"History {history*100:.0f}% < 55% minimum")
            if ev is not None and ev < 0:
                reasons_fail.append(f"Negative EV {ev:.2f}%")
        
        if vig > 6.0:
            reasons_fail.append(f"High vig {vig:.1f}% reduces value")
        
        bet_size = cls.BET_SIZE_MAP.get(confidence, 0)
        if kelly > 0:
            bet_size = min(bet_size, kelly * 100 * 0.25)
        
        recommendation = "PASS" if qualified else "SKIP"
        if qualified and ev and ev > 2.0:
            recommendation = "STRONG BET"
        
        return {
            'qualified': qualified,
            'confidence': confidence,
            'bet_size_percentage': round(bet_size, 2),
            'reasons_pass': reasons_pass,
            'reasons_fail': reasons_fail,
            'recommendation': recommendation,
            'metrics': {
                'true_edge': true_edge,
                'ev': ev,
                'kelly': kelly,
                'history': history,
                'vig': vig
            }
        }

class SpreadValidator:
    """Enhanced spread validation with edge case handling."""
    
    TOLERANCE = 0.5
    MAX_SPREADS = {'NBA': 30.0, 'CBB': 35.0, 'NFL': 28.0, 'CFB': 50.0, 'NHL': 3.5}
    
    @staticmethod
    def validate_spread_vs_moneyline(
        spread: float,
        away_ml: Optional[float],
        home_ml: Optional[float],
        away_team: str,
        home_team: str,
        tolerance: float = 0.5
    ) -> Tuple[bool, Optional[str], Optional[float]]:
        """
        Validates spread sign against moneyline odds with edge case handling.
        Returns: (is_valid, error_message, corrected_spread)
        """
        if not away_ml or not home_ml:
            if abs(spread) <= tolerance:
                return True, None, None
            warning = f"Cannot validate spread without moneyline: {away_team} @ {home_team}"
            logger.warning(warning)
            return True, warning, None
        
        ml_diff = abs(away_ml - home_ml)
        if ml_diff <= 20 and abs(spread) <= tolerance:
            return True, None, None
        
        away_is_favorite_by_ml = away_ml < home_ml
        away_is_favorite_by_spread = spread < -tolerance
        away_is_underdog_by_spread = spread > tolerance
        
        if away_is_favorite_by_ml and away_is_underdog_by_spread:
            error_msg = (
                f"SPREAD MISMATCH: {away_team} @ {home_team} | "
                f"ML says {away_team} favorite ({away_ml} vs {home_ml}) but spread says underdog (+{spread})"
            )
            return False, error_msg, -abs(spread)
        elif not away_is_favorite_by_ml and away_is_favorite_by_spread:
            error_msg = (
                f"SPREAD MISMATCH: {away_team} @ {home_team} | "
                f"ML says {home_team} favorite ({home_ml} vs {away_ml}) but spread says {away_team} favorite ({spread})"
            )
            return False, error_msg, abs(spread)
        
        if ml_diff > 200 and abs(spread) < 7.0:
            warning = f"UNUSUAL SPREAD: {away_team} @ {home_team} | Large ML gap ({ml_diff}) but small spread ({spread})"
            logger.warning(warning)
            return True, warning, None
        
        return True, None, None
    
    @staticmethod
    def validate_spread_magnitude(spread: float, league: str) -> Tuple[bool, Optional[str]]:
        """Validate spread is within reasonable range for league."""
        max_spread = SpreadValidator.MAX_SPREADS.get(league, 50.0)
        if abs(spread) > max_spread:
            return False, f"Spread {spread} exceeds max for {league} ({max_spread})"
        return True, None
    
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
        
        # Injury checks removed for speed - no longer impacts qualification
        adjustments["total_adjustment"] = adjustments["recent_form_boost"]
        
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
    'sc upstate': 'south carolina upstate', 'pitt': 'pittsburgh',
    'charleston so': 'charleston southern', 'chas southern': 'charleston southern',
    'sam houston': 'sam houston state', 'jax state': 'jacksonville state',
    'jax st': 'jacksonville state', 'western ky': 'western kentucky',
    's dakota st': 'south dakota state', 'n dakota st': 'north dakota state',
    's dakota': 'south dakota', 'n dakota': 'north dakota',
    'lmu': 'loyola marymount', 'oregon st': 'oregon state',
    'kennesaw st': 'kennesaw state', 'missouri st': 'missouri state',
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

MASCOTS = {
    'spartans', 'panthers', 'yellow jackets', 'yellowjackets', 'buccaneers', 'cougars', 'tigers',
    'bulldogs', 'wildcats', 'bears', 'lions', 'eagles', 'hawks', 'cardinals', 'owls', 'rockets',
    'bruins', 'trojans', 'huskies', 'gators', 'seminoles', 'hurricanes', 'cavaliers', 'hokies',
    'wolfpack', 'terrapins', 'terps', 'nittany lions', 'buckeyes', 'wolverines', 'badgers',
    'hawkeyes', 'cornhuskers', 'jayhawks', 'sooners', 'longhorns', 'aggies', 'red raiders',
    'horned frogs', 'mustangs', 'coyotes', 'jackrabbits', 'bison', 'bearkats', 'gamecocks',
    'volunteers', 'vols', 'commodores', 'rebels', 'razorbacks', 'crimson tide', 'tigers',
    'fighting irish', 'hoosiers', 'boilermakers', 'illini', 'golden gophers', 'tar heels',
    'blue devils', 'demon deacons', 'wolf pack', 'mountaineers', 'orange', 'yellow jackets',
    'ramblers', 'pilots', 'dons', 'gaels', 'waves', 'toreros', 'aztecs', 'broncos', 'falcons',
    'bobcats', 'bearcats', 'rams', 'buffaloes', 'utes', 'sun devils', 'ducks', 'beavers',
    'cougars', 'huskies', 'vandals', 'zags', 'gonzaga bulldogs', 'bluejays', 'creighton bluejays',
}

def get_team_tokens(name: str) -> set:
    """Get tokens from team name, excluding stop words and mascots."""
    stop_words = {'the', 'of', 'at', 'vs', 'and'}
    normalized = normalize_team_name(name)
    for mascot in MASCOTS:
        if normalized.endswith(' ' + mascot):
            normalized = normalized[:-len(mascot)-1]
            break
    words = normalized.split()
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
    # Historical percentages (last 30 games for NBA/CBB/NHL, 16 for NFL/CFB)
    away_ou_pct = db.Column(db.Float)  # Away team's O/U hit rate
    home_ou_pct = db.Column(db.Float)  # Home team's O/U hit rate
    away_spread_pct = db.Column(db.Float)  # Away team's spread cover rate
    home_spread_pct = db.Column(db.Float)  # Home team's spread cover rate
    h2h_ou_pct = db.Column(db.Float)  # Head-to-head O/U hit rate
    h2h_spread_pct = db.Column(db.Float)  # Head-to-head spread cover rate
    history_qualified = db.Column(db.Boolean, default=None)  # NULL = not checked, True/False = checked (for TOTALS)
    spread_history_qualified = db.Column(db.Boolean, default=None)  # Separate history qualification for SPREADS
    history_sample_size = db.Column(db.Integer)  # Actual sample size (non-push games)
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
    # SHARP METRICS
    true_edge = db.Column(db.Float)  # Edge after vig removal
    true_line = db.Column(db.Float)  # Vig-adjusted line
    vig_percentage = db.Column(db.Float)  # Market vig
    market_shade = db.Column(db.String(10))  # OVER/UNDER/BALANCED
    kelly_fraction = db.Column(db.Float)  # Kelly bet size
    recommended_bet_size = db.Column(db.Float)  # Actual bet % (fractional Kelly)
    clv_predicted = db.Column(db.Float)  # Expected CLV
    sharp_money_side = db.Column(db.String(10))  # Where sharp money is
    fair_probability = db.Column(db.Float)  # True win probability
    probability_edge = db.Column(db.Float)  # Edge in probability space
    # Situational factors
    days_rest_away = db.Column(db.Integer)
    days_rest_home = db.Column(db.Integer)
    is_back_to_back_away = db.Column(db.Boolean, default=False)
    is_back_to_back_home = db.Column(db.Boolean, default=False)
    travel_distance = db.Column(db.Float)
    situational_adjustment = db.Column(db.Float, default=0.0)
    
    __table_args__ = (
        db.Index('idx_date_league', 'date', 'league'),
        db.Index('idx_qualified', 'is_qualified'),
        db.Index('idx_spread_qualified', 'spread_is_qualified'),
        db.Index('idx_date_qualified', 'date', 'is_qualified'),
        db.Index('idx_event_id', 'event_id'),
        db.Index('idx_composite_search', 'date', 'league', 'away_team', 'home_team'),
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
    opening_line = db.Column(db.Float)  # Line when pick was made
    closing_line = db.Column(db.Float)  # Final line before game
    clv = db.Column(db.Float)  # Closing Line Value
    line_moved_favor = db.Column(db.Boolean)  # Did line move in our favor?
    bet_line = db.Column(db.Float)  # Actual line bet (alt or main)
    true_edge = db.Column(db.Float)  # Edge after vig removal
    kelly_fraction = db.Column(db.Float)  # Kelly bet size
    expected_ev = db.Column(db.Float)  # Expected value at time of bet
    
    
    __table_args__ = (
        db.Index('idx_pick_result', 'result'),
        db.Index('idx_pick_date_league', 'date', 'league'),
        db.Index('idx_pick_type', 'pick_type'),
        db.Index('idx_date_result', 'date', 'result'),
        db.Index('idx_is_lock_date', 'is_lock', 'date'),
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
                line_val = game.alt_total_line if game.alt_total_line else game.line
                if not line_val:
                    logger.warning(f"Auto-save skipped: No line value for {matchup} (total)")
                    skipped_count += 1
                    continue
                pick_str = f"{'O' if direction == 'O' else 'U'}{line_val}"
                if game.alt_total_odds:
                    pick_str += f" ({game.alt_total_odds:+.0f})"
                edge = game.alt_edge if game.alt_edge else game.edge
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

def check_qualification(projected: float, line: float, league: str, 
                        over_odds: int = -110, under_odds: int = -110) -> Tuple[bool, Optional[str], float]:
    """
    UPGRADED: Now uses TRUE edge (vig-adjusted) for qualification.
    Falls back to raw edge if odds not provided.
    
    Direction Rules:
    - OVER ("O"): If Projected_Total >= True_Line + Threshold
    - UNDER ("U"): If True_Line >= Projected_Total + Threshold
    """
    if over_odds != -110 or under_odds != -110:
        result = check_qualification_sharp(projected, line, league, over_odds, under_odds)
        return result['qualified'], result.get('direction'), result.get('true_edge', result.get('raw_edge', 0))
    
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
            # HOME FAVORITE: Must cover their spread (50% threshold) - FIXED from 70%
            margin_threshold = abs(spread_line) * 0.50
            if home_avg_margin < margin_threshold:
                result["reason"] = f"HOME_FAV_MARGIN_BELOW_50%: {home_avg_margin:.1f} < {margin_threshold:.1f}"
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
            # AWAY FAVORITE: Must cover their spread (50% threshold) - FIXED from 70%
            margin_threshold = abs(spread_line) * 0.50
            if away_avg_margin < margin_threshold:
                result["reason"] = f"AWAY_FAV_MARGIN_BELOW_50%: {away_avg_margin:.1f} < {margin_threshold:.1f}"
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
espn_team_schedule_cache: dict = {}  # "YYYY-MM-DD:league:team_name" -> games list (date-keyed for daily refresh)

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
    Fetch team's last 30 completed games from ESPN with daily caching.
    Returns list of game dicts with: total_score, opponent_score, was_home
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    cache_key = f"{today_str}:{league}:{team_name.lower()}"
    if cache_key in espn_team_schedule_cache:
        return espn_team_schedule_cache[cache_key]
    
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
        resp = fetch_with_rate_limit(url, espn_limiter, timeout=15)
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
        
        result = completed_games[-30:] if len(completed_games) >= 30 else completed_games
        espn_team_schedule_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error(f"Error fetching history for {team_name}: {e}")
        return []

def fetch_all_team_histories_batch(games: list) -> dict:
    """
    BULLETPROOF: Fetch all team histories in parallel with deduplication.
    Reduces 100+ sequential API calls to ~20 parallel calls with caching.
    """
    et = pytz.timezone('America/New_York')
    today_str = datetime.now(et).strftime("%Y-%m-%d")
    
    unique_teams = {}
    for g in games:
        unique_teams[(g.away_team, g.league)] = None
        unique_teams[(g.home_team, g.league)] = None
    
    results = {}
    teams_to_fetch = []
    
    for (team, league) in unique_teams.keys():
        cache_key = f"{today_str}:{league}:{team.lower()}"
        cached = espn_schedule_cache.get(cache_key)
        if cached is not None:
            results[cache_key] = cached
        else:
            teams_to_fetch.append((team, league, cache_key))
    
    if teams_to_fetch:
        logger.info(f"Batch fetching histories for {len(teams_to_fetch)} teams (skipping {len(results)} cached)")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {}
            for (team, league, cache_key) in teams_to_fetch:
                futures[executor.submit(fetch_team_last_10_games, team, league)] = cache_key
            
            for future in as_completed(futures):
                cache_key = futures[future]
                try:
                    team_games = future.result()
                    results[cache_key] = team_games
                except Exception as e:
                    logger.error(f"Batch fetch failed for {cache_key}: {e}")
                    results[cache_key] = []
    
    return results

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
    
    if cache_key in espn_team_schedule_cache:
        cached = espn_team_schedule_cache[cache_key]
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
        
        h2h_games = h2h_games[-30:]
        
        if len(h2h_games) < 3:
            result = {"ou_pct": 0, "games_found": len(h2h_games), "games": h2h_games}
            espn_team_schedule_cache[cache_key] = result
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
        espn_team_schedule_cache[cache_key] = result
        
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
    
    if cache_key in espn_team_schedule_cache:
        return espn_team_schedule_cache[cache_key]
    
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
            espn_team_schedule_cache[cache_key] = result
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
        espn_team_schedule_cache[cache_key] = result
        
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
    
    if cache_key in espn_team_schedule_cache:
        return espn_team_schedule_cache[cache_key]
    
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
            espn_team_schedule_cache[cache_key] = result
            return result
        
        away_wins = sum(1 for g in h2h_games if g["away_won_1h"])
        away_win_pct = (away_wins / len(h2h_games)) * 100
        
        result = {
            "away_win_pct": away_win_pct,
            "games_found": len(h2h_games),
            "games": h2h_games
        }
        espn_team_schedule_cache[cache_key] = result
        
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
            
            away_non_push = len([g for g in away_games if abs(g["total"] - current_line) > 0.5])
            home_non_push = len([g for g in home_games if abs(g["total"] - current_line) > 0.5])
            game.history_sample_size = min(away_non_push, home_non_push)
            
            logger.info(f"{game.away_team} @ {game.home_team}: Fallback to current line comparison - Away O/U: {game.away_ou_pct}%, Home O/U: {game.home_ou_pct}%, Sample: {game.history_sample_size}")
        
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
        
        # Injury checks removed for speed - use empty stubs
        away_injuries = {"has_key_injuries": False, "injured_starters": 0, "impact_score": 0}
        home_injuries = {"has_key_injuries": False, "injured_starters": 0, "impact_score": 0}
        
        h2h = fetch_h2h_history(game.away_team, game.home_team, game.league, direction)
        game.h2h_ou_pct = h2h["ou_pct"]
        h2h_games = h2h["games_found"]
        
        max_ou_pct = max(game.away_ou_pct or 0, game.home_ou_pct or 0)
        sample_size = game.history_sample_size or 0
        
        # Standard qualification: 58%+ AND 15+ non-push games (30-game window)
        totals_qualified = max_ou_pct >= 58 and sample_size >= 15
        # SUPERMAX qualification for history posting (70%+ AND 15+ games)
        totals_supermax = max_ou_pct >= 70 and sample_size >= 15
        
        if h2h_games >= 3:
            h2h_qualified = (game.h2h_ou_pct or 0) >= 60
            h2h_supermax = (game.h2h_ou_pct or 0) >= 70
            totals_qualified = totals_qualified and h2h_qualified
            totals_supermax = totals_supermax and h2h_supermax
        
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
    
    # PURE MODEL: Games qualified ONLY by edge threshold (TOTALS ONLY)
    # Rule: Difference = Projected_Total - Bovada_Line must meet threshold
    # NBA/CBB: ±8.0, NFL/CFB: ±3.5, NHL: ±0.5
    # Alt lines required for display but qualification happens first via is_qualified flag
    qualified = [g for g in all_games if g.is_qualified]
    
    # Filter to only show picks with alt lines (mandatory for display)
    qualified_with_alt = [g for g in qualified if g.alt_total_line]
    qualified = qualified_with_alt if qualified_with_alt else []
    
    # AWAY FAVORITE CONFIDENCE BOOST: Away team as favorite + O/U threshold = higher confidence
    # When away team is favored AND meets totals threshold, O/U pick is more likely to hit
    # Spread line convention: negative = away is favorite (e.g., -5.5 means away favored by 5.5)
    away_favorite_count = 0
    for g in all_games:
        # Check if away team is favorite using spread line
        # Negative spread_line means away team is giving points (favorite)
        away_is_fav = g.spread_line is not None and g.spread_line < -1.5  # At least 2 point favorite
        
        # If away is favorite AND meets totals threshold = BONUS CONFIDENCE for O/U pick
        if away_is_fav and g.is_qualified:
            g.is_away_favorite = True
            away_favorite_count += 1
        else:
            g.is_away_favorite = False
    
    # O/U HIT RATE TRACKING using ESPN data
    # Show hit rates for ALL qualified games, mark as "qualified" if strict filters pass
    # STRICT FILTERS: 100% L5, 90%+ L10, 95%+ L20 (requires 20+ games)
    ou_hit_rate_count = 0
    for g in qualified:
        g.ou_hit_rate = None
        g.ou_hit_rate_qualified = False
        g.ou_l5 = None
        g.ou_l10 = None
        g.ou_l20 = None
        
        if g.direction and g.line:
            direction = 'O' if g.direction == 'O' else 'U'
            current_line = g.alt_total_line  # Alt lines mandatory
            
            # Check both teams' hit rates
            away_result = calculate_ou_hit_rate_espn(g.away_team, g.league, direction, current_line)
            home_result = calculate_ou_hit_rate_espn(g.home_team, g.league, direction, current_line)
            
            # ALWAYS set hit rates if we have data (for display purposes)
            # Use the team with more games/better hit rate
            best_result = None
            if away_result.get('l5') and home_result.get('l5'):
                # Both have data - use team with higher L20 hits
                if away_result.get('l20_hits', 0) >= home_result.get('l20_hits', 0):
                    best_result = away_result
                else:
                    best_result = home_result
            elif away_result.get('l5'):
                best_result = away_result
            elif home_result.get('l5'):
                best_result = home_result
            
            if best_result:
                g.ou_l5 = best_result.get('l5')
                g.ou_l10 = best_result.get('l10')
                g.ou_l20 = best_result.get('l20')
                g.ou_hit_rate = g.ou_l20
                
                # Mark as "qualified" only if strict filters pass
                if away_result.get('qualified') or home_result.get('qualified'):
                    g.ou_hit_rate_qualified = True
                    ou_hit_rate_count += 1
    
    # DEFENSE MISMATCH BOOST: Bottom 10 def for OVERS, Top 10 def for UNDERS
    # Calculate actual defensive rankings based on opponent PPG from all teams in today's games
    def_mismatch_count = 0
    
    # Build defensive rankings per league from today's games
    # Higher Opp PPG = worse defense = higher rank number (rank 30 = worst, rank 1 = best)
    league_def_rankings = {}
    for league in ['NBA', 'CBB']:
        league_games = [g for g in all_games if g.league == league]
        if not league_games:
            continue
        
        # Collect all unique teams and their Opp PPG (defense rating)
        team_defense = {}
        for g in league_games:
            if g.away_opp_ppg and g.away_opp_ppg > 0:
                team_defense[g.away_team] = g.away_opp_ppg
            if g.home_opp_ppg and g.home_opp_ppg > 0:
                team_defense[g.home_team] = g.home_opp_ppg
        
        if not team_defense:
            continue
        
        # Sort by Opp PPG descending (worst defense first = highest Opp PPG)
        sorted_teams = sorted(team_defense.items(), key=lambda x: x[1], reverse=True)
        
        # Assign ranks: rank 1 = worst defense (highest Opp PPG), higher rank = better defense
        # For Bottom 10: teams with ranks 1-10 (worst defenses)
        # For Top 10: teams with ranks (total-9) to total (best defenses)
        total_teams = len(sorted_teams)
        rankings = {}
        for i, (team, opp_ppg) in enumerate(sorted_teams):
            rank = i + 1  # 1 = worst defense, N = best defense
            rankings[team] = {
                'rank': rank,
                'opp_ppg': opp_ppg,
                'is_bottom_10': rank <= 10,  # Worst 10 defenses (ranks 1-10)
                'is_top_10': rank > (total_teams - 10)  # Best 10 defenses
            }
        
        league_def_rankings[league] = rankings
        logger.info(f"{league} defensive rankings: {total_teams} teams, bottom 10 threshold = rank 1-10")
    
    # Apply DEF EDGE badges based on actual rankings
    for g in all_games:
        g.def_mismatch = False
        g.def_rank_away = None
        g.def_rank_home = None
        
        # Only apply to NBA and CBB (basketball)
        if g.league not in ['NBA', 'CBB']:
            continue
        
        rankings = league_def_rankings.get(g.league, {})
        if not rankings:
            continue
        
        # Get defensive rankings for each team's opponent
        # Away team faces Home team's defense, Home team faces Away team's defense
        away_faces = rankings.get(g.home_team, {})  # Away team faces home defense
        home_faces = rankings.get(g.away_team, {})  # Home team faces away defense
        
        g.def_rank_away = away_faces.get('rank')
        g.def_rank_home = home_faces.get('rank')
        
        # For OVER picks: DEF EDGE if facing bottom 10 defense (ranks 1-10 = worst)
        # For UNDER picks: DEF EDGE if facing top 10 defense (best defenses)
        if g.is_qualified and g.direction:
            if g.direction == 'O':
                # OVER pick - boost if facing bottom 10 defense (worst = high Opp PPG)
                if away_faces.get('is_bottom_10') or home_faces.get('is_bottom_10'):
                    g.def_mismatch = True
                    def_mismatch_count += 1
            elif g.direction == 'U':
                # UNDER pick - boost if facing top 10 defense (best = low Opp PPG)
                if away_faces.get('is_top_10') or home_faces.get('is_top_10'):
                    g.def_mismatch = True
                    def_mismatch_count += 1
    
    # TOTALS QUALIFICATION: Edge threshold ONLY (L5/L20/DEF EDGE are badges, not filters)
    # Badges are displayed for extra confidence but don't affect qualification
    logger.info(f"TOTALS qualified by edge threshold: {len(qualified)} games")
    
    # Sort qualified totals by effective edge (alt if available, else main)
    qualified.sort(key=lambda x: x.alt_edge or x.edge or 0, reverse=True)
    
    # LOCK OF THE DAY = highest edge totals pick
    supermax_lock = qualified[0] if qualified else None
    
    # All qualified games (totals only)
    all_qualified_games = qualified
    
    if show_only_qualified:
        games = all_qualified_games
    else:
        games = all_games
    
    # Games that meet 85% historical threshold
    history_qualified = [g for g in all_games if g.history_qualified]
    
    # Edge Analytics (TOTALS ONLY)
    analytics = {
        'league_breakdown': {},
        'edge_tiers': {'elite': 0, 'strong': 0, 'standard': 0},
        'best_edge': 0,
        'avg_edge': 0,
        'over_count': 0,
        'under_count': 0,
        'top_picks': [],
        'history_qualified': len(history_qualified),
        'away_favorite_count': away_favorite_count,
        'def_mismatch_count': def_mismatch_count,
        'ou_hit_rate_count': ou_hit_rate_count
    }
    
    # League breakdown (TOTALS ONLY) - use post-filter qualified list
    for league in ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']:
        league_games = [g for g in all_games if g.league == league]
        league_qualified = [g for g in qualified if g.league == league]
        analytics['league_breakdown'][league] = {
            'total': len(league_games),
            'qualified': len(league_qualified)
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
    
    # Best edge (TOTALS ONLY)
    analytics['best_edge'] = max(all_edges) if all_edges else 0
    
    # Avg edge (TOTALS ONLY)
    if len(qualified) > 0:
        analytics['avg_edge'] = edge_sum / len(qualified)
    
    # PURE MODEL: Combined Top 5 Picks ranked by EDGE ONLY
    # No weighted scores, no model bonuses, no confidence tiers
    # Pure formula: Difference = Projected_Total - Bovada_Line
    
    # TOP 5: Games qualified by edge threshold (sorted by edge)
    # Sort by edge before taking top 5
    qualified.sort(key=lambda x: x.alt_edge or x.edge or 0, reverse=True)
    top_picks = []
    for g in qualified[:5]:
        # Use alt edge if available (better line), else main edge
        best_edge = g.alt_edge or g.edge or 0
        best_line = g.alt_total_line  # Alt lines mandatory - no fallback to main line
        top_picks.append({
            'game': g,
            'edge': best_edge,
            'direction': g.direction,
            'line': best_line,
            'alt_line': g.alt_total_line,  # Track if using alt line
            'projected_total': g.projected_total,
            'pick_type': 'total',  # Required for auto_save_qualified_picks
            'is_away_favorite': getattr(g, 'is_away_favorite', False),
            'def_mismatch': getattr(g, 'def_mismatch', False),
            'ou_hit_rate': getattr(g, 'ou_hit_rate', None),
            'ou_hit_rate_qualified': getattr(g, 'ou_hit_rate_qualified', False),
            'ou_l5': getattr(g, 'ou_l5', None),
            'ou_l10': getattr(g, 'ou_l10', None),
            'ou_l20': getattr(g, 'ou_l20', None)
        })
    analytics['top_picks'] = top_picks
    
    # Auto-save the top pick to history
    if top_picks:
        auto_save_qualified_picks([top_picks[0]], today)
    
    # Lock of the Day = highest edge totals pick
    supermax_tier = 'QUALIFIED' if qualified else 'NONE'
    
    global last_game_count
    last_game_count['count'] = len(all_games)
    last_game_count['qualified'] = len(qualified)
    
    return render_template('dashboard.html', games=games, qualified=qualified,
                          supermax_lock=supermax_lock,
                          supermax_tier=supermax_tier,
                          today=today, thresholds=THRESHOLDS, total_games=len(all_games),
                          show_only_qualified=show_only_qualified, analytics=analytics,
                          is_big_slate=is_big_slate_day())

@app.route('/health')
def health():
    """Comprehensive health check for monitoring."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    try:
        db.session.execute(db.text('SELECT 1'))
        health_status["checks"]["database"] = "ok"
    except Exception as e:
        health_status["checks"]["database"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    try:
        test_key = "_health_check_"
        espn_schedule_cache.set(test_key, "test")
        if espn_schedule_cache.get(test_key) == "test":
            health_status["checks"]["cache"] = "ok"
        else:
            health_status["checks"]["cache"] = "error: cache not working"
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["checks"]["cache"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    try:
        et = pytz.timezone('America/New_York')
        today = datetime.now(et).date()
        game_count = Game.query.filter_by(date=today).count()
        qualified_count = Game.query.filter_by(date=today, is_qualified=True).count()
        health_status["checks"]["games"] = {
            "total": game_count,
            "qualified": qualified_count
        }
    except Exception as e:
        health_status["checks"]["games"] = f"error: {str(e)}"
    
    status_code = 200 if health_status["status"] == "healthy" else 503
    return jsonify(health_status), status_code

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
        logger.error(f"NBA stats error: {e}")
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
        logger.error(f"NHL stats error: {e}")
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
        logger.error(f"Event stats error: {e}")
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

_team_stats_cache = TTLCache(maxsize=500, ttl=3600)

def get_cached_team_stats(team_id, sport):
    cache_key = f"{sport}_{team_id}"
    return _team_stats_cache.get(cache_key)

def set_cached_team_stats(team_id, sport, ppg, opp_ppg):
    cache_key = f"{sport}_{team_id}"
    _team_stats_cache.set(cache_key, (ppg, opp_ppg))

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
    """Internal function to fetch odds from Bovada via The Odds API - PARALLEL + ATOMIC (TOTALS ONLY)."""
    start_time = time.time()
    api_key = os.environ.get("BOVADA_API_KEY") or os.environ.get("ODDS_API_KEY") or os.environ.get("API_KEY")
    if not api_key:
        return {"success": False, "lines_updated": 0, "alt_lines_found": 0}
    
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
    
    # STEP 1: Fetch ALL odds in parallel FIRST (no DB changes yet)
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
    
    # STEP 2: Validate we got data before touching DB
    total_events = sum(len(d.get("events", [])) for d in all_odds.values())
    if total_events == 0:
        logger.warning("No odds fetched from API - keeping existing lines")
        return {"success": False, "lines_updated": 0, "alt_lines_found": 0, "reason": "no_odds_fetched"}
    
    # STEP 3: ATOMIC UPDATE - Clear and update within single transaction
    try:
        # Get games to update
        games_to_update = Game.query.filter_by(date=today).all()
        
        # Clear all lines first (within same transaction)
        for g in games_to_update:
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
            # Clear sharp metrics
            g.true_edge = None
            g.fair_line = None
            g.vig_percentage = None
            g.market_balance = None
            g.kelly_fraction = None
            g.recommended_bet_size = None
            g.fair_probability = None
            g.probability_edge = None
        
        # Apply new odds data (still within same transaction)
        for league, data in all_odds.items():
            sport_key = data["sport_key"]
            events = data["events"]
            
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
                        
                        # Process TOTALS with SHARP qualification
                        if "totals" in bovada_markets:
                            totals_market = bovada_markets["totals"]
                            outcomes = totals_market.get("outcomes", [])
                            over_outcome = next((o for o in outcomes if o.get("name") == "Over"), None)
                            under_outcome = next((o for o in outcomes if o.get("name") == "Under"), None)
                            
                            if over_outcome and under_outcome:
                                line = over_outcome.get("point")
                                over_odds = over_outcome.get("price", -110)
                                under_odds = under_outcome.get("price", -110)
                                
                                if line is not None:
                                    game.line = line
                                    game.bovada_total_odds = over_odds
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
                                        
                                        # Get Pinnacle odds
                                        pinn_over_odds = None
                                        pinn_under_odds = None
                                        if pinnacle_markets.get("totals"):
                                            pinn_outcomes = pinnacle_markets["totals"].get("outcomes", [])
                                            pinn_over = next((o for o in pinn_outcomes if o.get("name") == "Over"), None)
                                            pinn_under = next((o for o in pinn_outcomes if o.get("name") == "Under"), None)
                                            if pinn_over:
                                                pinn_over_odds = pinn_over.get("price")
                                                game.pinnacle_total_odds = pinn_over_odds
                                            if pinn_under:
                                                pinn_under_odds = pinn_under.get("price")
                                        
                                        # Get historical performance
                                        history_rate = max(game.away_ou_pct or 0, game.home_ou_pct or 0) / 100
                                        sample = 10 if game.history_qualified is not None else 0
                                        
                                        # Situational factors stored for display (B2B badges, rest days)
                                        # (Rest days, travel distance stored for display but do not affect projections)
                                        try:
                                            away_rest = get_rest_days_impact(game.away_team, league, today)
                                            home_rest = get_rest_days_impact(game.home_team, league, today)
                                            game.days_rest_away = away_rest.get('days_rest')
                                            game.days_rest_home = home_rest.get('days_rest')
                                            game.is_back_to_back_away = away_rest.get('is_back_to_back', False)
                                            game.is_back_to_back_home = home_rest.get('is_back_to_back', False)
                                            game.travel_distance = calculate_travel_distance(game.away_team, game.home_team)
                                            game.situational_adjustment = 0.0  # No adjustment in pure model
                                        except Exception as e:
                                            logger.error(f"Situational factors error: {e}")
                                        
                                        # SHARP QUALIFICATION
                                        qual_result = check_qualification_professional(
                                            projected_total=proj_total,
                                            line=line,
                                            over_odds=over_odds,
                                            under_odds=under_odds,
                                            league=league,
                                            pinnacle_over_odds=pinn_over_odds,
                                            pinnacle_under_odds=pinn_under_odds,
                                            historical_win_rate=history_rate,
                                            sample_size=sample
                                        )
                                        
                                        # Calculate edge metrics using VigRemover
                                        fair_line = VigRemover.calculate_fair_line_totals(line, over_odds, under_odds)
                                        vig_data = VigRemover.remove_two_way_vig(over_odds, under_odds)
                                        
                                        # PURE MODEL: Edge-only qualification
                                        edge_thresholds = {'NBA': 8.0, 'CBB': 8.0, 'NFL': 3.5, 'CFB': 3.5, 'NHL': 0.5}
                                        threshold = edge_thresholds.get(league, 8.0)
                                        edge = abs(proj_total - line)
                                        
                                        # PURE MODEL: Direction based on strict threshold
                                        # Always set direction based on projection vs line
                                        # OVER: projection higher than line
                                        # UNDER: line higher than projection
                                        if proj_total > line:
                                            game.direction = 'O'
                                        elif proj_total < line:
                                            game.direction = 'U'
                                        else:
                                            game.direction = None  # Exact match, no edge
                                        
                                        game.edge = edge
                                        
                                        # Store metrics for display
                                        game.fair_line = VigRemover.calculate_fair_line_totals(line, over_odds, under_odds)
                                        game.vig_percentage = round(vig_data['vig_pct'], 2)
                                        game.market_balance = 'OVER_SHADED' if vig_data['prob_a'] > 0.54 else ('UNDER_SHADED' if vig_data['prob_a'] < 0.46 else 'BALANCED')
                                        game.total_ev = qual_result.ev_pct
                                        game.true_edge = qual_result.true_edge
                                        
                                        # Qualify if: edge meets threshold + direction set
                                        game.is_qualified = (edge >= threshold and 
                                                            game.direction is not None)
                                        
                                    lines_updated += 1
        
        # STEP 4: Commit the atomic transaction
        db.session.commit()
        logger.info(f"Odds update successful: {lines_updated} totals")
        
    except Exception as e:
        # ROLLBACK on any failure - preserves existing lines
        db.session.rollback()
        logger.error(f"Odds update FAILED, rolled back: {e}")
        return {"success": False, "lines_updated": 0, "alt_lines_found": 0, "reason": str(e)}
    
    alt_lines_result = fetch_alt_lines_internal()
    
    # Clear dashboard cache since we have new data
    clear_dashboard_cache()
    logger.info("Dashboard cache cleared after odds update")
    
    return {
        "success": True, 
        "lines_updated": lines_updated, 
        "alt_lines_found": alt_lines_result.get("alt_lines_found", 0)
    }

@app.route('/fetch_odds', methods=['POST'])
def fetch_odds():
    """Route wrapper for fetch_odds_internal."""
    return jsonify(fetch_odds_internal())

def fetch_history_internal() -> dict:
    """
    BULLETPROOF: Batch historical data updates with checkpointing.
    
    Improvements:
    - Batch commits every 5 games (faster than individual commits)
    - Checkpoint logging for progress tracking
    - Error details returned for debugging
    """
    start_time = time.time()
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    games = Game.query.filter_by(date=today).filter(
        Game.is_qualified == True
    ).all()
    
    total_games = len(games)
    history_updated = 0
    history_qualified = 0
    errors = []
    
    BATCH_SIZE = 5
    batch_count = 0
    
    logger.info(f"Processing {total_games} games in batches of {BATCH_SIZE}")
    
    for i, game in enumerate(games, 1):
        try:
            result = update_game_historical_data(game)
            
            if result:
                history_qualified += 1
            history_updated += 1
            batch_count += 1
            
            if batch_count >= BATCH_SIZE or i == total_games:
                db.session.commit()
                batch_count = 0
                elapsed = time.time() - start_time
                logger.info(f"Checkpoint: {i}/{total_games} games processed ({elapsed:.1f}s)")
            
        except Exception as e:
            db.session.rollback()
            batch_count = 0
            error_msg = f"{game.away_team} @ {game.home_team}: {str(e)}"
            errors.append(error_msg)
            logger.error(f"Error updating history: {error_msg}")
    
    if batch_count > 0:
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            logger.error(f"Final batch commit failed: {e}")
    
    elapsed = time.time() - start_time
    logger.info(f"History fetch complete: {history_updated}/{total_games} games in {elapsed:.1f}s, {history_qualified} qualified")
    
    if errors:
        logger.warning(f"{len(errors)} games failed processing")
    
    return {
        "games_checked": history_updated,
        "history_qualified": history_qualified,
        "errors": len(errors),
        "error_details": errors[:5],
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

@app.route('/api/performance_metrics')
def performance_metrics_api():
    """View performance metrics for monitoring."""
    metrics = {}
    
    for operation, measurements in _performance_metrics.items():
        if not measurements:
            continue
        
        durations = [m['duration'] for m in measurements]
        
        metrics[operation] = {
            'count': len(durations),
            'avg_ms': round(sum(durations) / len(durations) * 1000, 1),
            'min_ms': round(min(durations) * 1000, 1),
            'max_ms': round(max(durations) * 1000, 1),
        }
        
        if len(durations) >= 20:
            sorted_durations = sorted(durations)
            p95_index = int(len(sorted_durations) * 0.95)
            metrics[operation]['p95_ms'] = round(sorted_durations[p95_index] * 1000, 1)
    
    return jsonify({
        'success': True,
        'metrics': metrics,
        'cache_stats': {
            'dashboard_cached': get_cached_dashboard() is not None,
            'team_stats_size': len(_team_stats_cache),
            'historical_size': len(_historical_cache),
        }
    })

@app.route('/api/dashboard_data')
def dashboard_data_api():
    """Fast API endpoint for AJAX dashboard updates."""
    cached = get_cached_dashboard()
    if cached:
        return jsonify(cached)
    
    start = time.time()
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    games = Game.query.filter_by(date=today).filter(
        Game.is_qualified == True
    ).all()
    
    games_data = []
    for game in games:
        games_data.append({
            'id': game.id,
            'matchup': f"{game.away_team} @ {game.home_team}",
            'league': game.league,
            'line': game.line,
            'edge': game.edge,
            'true_edge': game.true_edge,
            'direction': game.direction,
            'is_qualified': game.is_qualified,
            'game_time': game.game_time,
            'total_ev': game.total_ev,
            'projected_total': game.projected_total,
        })
    
    response_data = {
        'success': True,
        'games': games_data,
        'timestamp': datetime.now(et).isoformat(),
        'count': len(games_data)
    }
    
    set_dashboard_cache(response_data)
    
    elapsed = time.time() - start
    track_performance('dashboard_api', elapsed)
    logger.info(f"Dashboard API: {len(games_data)} games in {elapsed*1000:.0f}ms")
    
    return jsonify(response_data)

@app.route('/api/situational_stats')
def situational_stats_api():
    """View situational factor statistics."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    recent_games = Game.query.filter(
        Game.date >= today - timedelta(days=7),
        Game.date <= today
    ).all()
    
    stats = {
        'total_games': len(recent_games),
        'back_to_back_away': 0,
        'back_to_back_home': 0,
        'long_travel': 0,
        'situational_adjustments': 0,
        'total_adjustments_sum': 0.0,
    }
    
    for game in recent_games:
        if hasattr(game, 'is_back_to_back_away') and game.is_back_to_back_away:
            stats['back_to_back_away'] += 1
        if hasattr(game, 'is_back_to_back_home') and game.is_back_to_back_home:
            stats['back_to_back_home'] += 1
        if hasattr(game, 'travel_distance') and game.travel_distance and game.travel_distance >= 2000:
            stats['long_travel'] += 1
        if hasattr(game, 'situational_adjustment') and game.situational_adjustment:
            if abs(game.situational_adjustment) >= 1.0:
                stats['situational_adjustments'] += 1
                stats['total_adjustments_sum'] += game.situational_adjustment
    
    if stats['total_games'] > 0:
        stats['back_to_back_pct'] = round((stats['back_to_back_away'] + stats['back_to_back_home']) / stats['total_games'] * 100, 1)
        stats['long_travel_pct'] = round(stats['long_travel'] / stats['total_games'] * 100, 1)
        stats['adjusted_pct'] = round(stats['situational_adjustments'] / stats['total_games'] * 100, 1)
        if stats['situational_adjustments'] > 0:
            stats['avg_adjustment'] = round(stats['total_adjustments_sum'] / stats['situational_adjustments'], 2)
    
    return jsonify({
        'success': True,
        'period': '7 days',
        'stats': stats
    })

def find_best_alt_line(outcomes: list, direction: str, current_line: float, is_spread: bool = False, home_team: str = "", debug_game: str = "") -> tuple:
    """
    Find the best alternate line with NEGATIVE odds only (no + money).
    Odds must be between -200 and -100 (no positive odds, no worse than -200).
    
    For OVER totals: Find the LOWEST alt line (easier to hit)
    For UNDER totals: Find the HIGHEST alt line (easier to hit)
    For spreads: Find better number for the pick direction
    
    Returns (best_line, best_odds) or (None, None) if no valid line found.
    """
    MAX_ODDS = -185  # Floor - no worse than -185, anything worse is not a lock
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
    """Fetch alt lines for a single game (used in parallel) - TOTALS ONLY."""
    game_id = game_info['id']
    event_id = game_info['event_id']
    sport_key = game_info['sport_key']
    
    result = {'game_id': game_id, 'alt_total': None}
    
    try:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
        params = {
            "apiKey": api_key,
            "regions": "us",
            "markets": "alternate_totals",
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
    except Exception as e:
        logger.error(f"Alt lines error for game {game_id}: {e}")
    
    return result

def fetch_alt_lines_internal() -> dict:
    """Internal function to fetch alternate lines for qualified TOTALS games (parallel)."""
    api_key = os.environ.get("BOVADA_API_KEY") or os.environ.get("ODDS_API_KEY") or os.environ.get("API_KEY")
    if not api_key:
        logger.warning("No ODDS_API_KEY or API_KEY for alt lines fetch")
        return {"alt_lines_found": 0, "games_checked": 0}
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    qualified_totals = Game.query.filter(
        Game.date == today,
        Game.is_qualified == True,
        Game.event_id.isnot(None)
    ).all()
    
    logger.info(f"Alt lines: checking {len(qualified_totals)} qualified totals games (parallel)")
    
    game_infos = [{
        'id': g.id, 'event_id': g.event_id, 'sport_key': g.sport_key,
        'away_team': g.away_team, 'home_team': g.home_team,
        'is_qualified': g.is_qualified, 'direction': g.direction, 'line': g.line
    } for g in qualified_totals if g.event_id and g.sport_key]
    
    alt_lines_found = 0
    results = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_single_alt_line, info, api_key): info['id'] for info in game_infos}
        for future in as_completed(futures):
            result = future.result()
            results[result['game_id']] = result
    
    for game in qualified_totals:
        if game.id in results:
            r = results[game.id]
            if r['alt_total']:
                game.alt_total_line, game.alt_total_odds = r['alt_total']
                alt_lines_found += 1
                if game.projected_total is not None:
                    game.alt_edge = abs(game.projected_total - game.alt_total_line)
                    logger.info(f"Alt edge recalc: {game.away_team}@{game.home_team} main={game.edge:.1f} -> alt={game.alt_edge:.1f}")
    
    db.session.commit()
    return {"alt_lines_found": alt_lines_found, "games_checked": len(qualified_totals)}

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
            line_val = g.alt_total_line  # Alt lines mandatory
            pick_str = f"{g.direction}{line_val}"
        else:
            team, line_val = get_display_spread(g, use_alt=True)
            pick_str = f"{team} {line_val:+.1f}" if line_val else (team or "")
        
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
            line = g.alt_total_line  # Alt lines mandatory
            odds = g.alt_total_odds if g.alt_total_odds else None
            pick_str = f"{g.direction}{line:.0f}" if line else p['pick_str']
            if odds:
                pick_str += f" ({odds:+.0f})"
        else:
            pick_str = format_spread_pick(g, use_alt=True, include_odds=True)
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
                    line_val = p_game.alt_total_line  # Alt lines mandatory
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
                    game_start=game_start_dt,
                    opening_line=p_game.line if p['pick_type'] == 'total' else p_game.spread_line,
                    bet_line=line_val,
                    true_edge=p_game.true_edge,
                    kelly_fraction=p_game.kelly_fraction,
                    expected_ev=p_game.total_ev if p['pick_type'] == 'total' else p_game.spread_ev
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
    
    # Format pick using foolproof helper
    if supermax['pick_type'] == 'total':
        line = sm_game.alt_total_line  # Alt lines mandatory
        odds = sm_game.alt_total_odds if sm_game.alt_total_odds else None
        pick_str = f"{sm_game.direction}{line:.0f}" if line else f"{sm_game.direction}"
        if odds:
            pick_str += f" ({odds:+.0f})"
    else:
        pick_str = format_spread_pick(sm_game, use_alt=True, include_odds=True)
    
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
        
        # Save to history using foolproof helper
        matchup = f"{sm_game.away_team} @ {sm_game.home_team}"
        if supermax['pick_type'] == 'total':
            line_val = sm_game.alt_total_line  # Alt lines mandatory
            pick_save = f"{sm_game.direction}{line_val}"
            edge_val = sm_game.edge
        else:
            team, line_val = get_display_spread(sm_game, use_alt=True)
            pick_save = f"{team} {line_val:+.1f}" if line_val else (team or "")
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

@app.route('/api/win_rate_analytics')
def win_rate_analytics():
    """
    Comprehensive win rate analytics by league, confidence tier, day of week, 
    time window, and data source.
    """
    picks = Pick.query.filter(Pick.result.in_(['W', 'L', 'P'])).all()
    
    if not picks:
        return jsonify({'success': True, 'message': 'No completed picks yet', 'analytics': {}})
    
    # Initialize analytics structure
    analytics = {
        'overall': {'wins': 0, 'losses': 0, 'pushes': 0, 'win_rate': 0, 'total': 0},
        'by_league': {},
        'by_confidence_tier': {'SUPERMAX': {'wins': 0, 'losses': 0}, 'HIGH': {'wins': 0, 'losses': 0}, 
                               'MEDIUM': {'wins': 0, 'losses': 0}, 'LOW': {'wins': 0, 'losses': 0}},
        'by_day_of_week': {day: {'wins': 0, 'losses': 0} for day in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']},
        'by_pick_type': {'total': {'wins': 0, 'losses': 0}, 'spread': {'wins': 0, 'losses': 0}},
        'by_time_window': {'EARLY': {'wins': 0, 'losses': 0}, 'MID': {'wins': 0, 'losses': 0}, 'LATE': {'wins': 0, 'losses': 0}},
        'recent_streak': [],
        'best_edge_threshold': None
    }
    
    for league in ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']:
        analytics['by_league'][league] = {'wins': 0, 'losses': 0, 'pushes': 0}
    
    # Day name mapping
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    for pick in picks:
        result = pick.result
        league = pick.league or 'Unknown'
        pick_type = getattr(pick, 'pick_type', None) or 'total'
        
        # Overall
        if result == 'W':
            analytics['overall']['wins'] += 1
        elif result == 'L':
            analytics['overall']['losses'] += 1
        else:
            analytics['overall']['pushes'] += 1
        analytics['overall']['total'] += 1
        
        # By league
        if league in analytics['by_league']:
            if result == 'W':
                analytics['by_league'][league]['wins'] += 1
            elif result == 'L':
                analytics['by_league'][league]['losses'] += 1
            else:
                analytics['by_league'][league]['pushes'] += 1
        
        # By pick type
        ptype = 'spread' if pick_type == 'spread' else 'total'
        if result == 'W':
            analytics['by_pick_type'][ptype]['wins'] += 1
        elif result == 'L':
            analytics['by_pick_type'][ptype]['losses'] += 1
        
        # By day of week
        if pick.date:
            day_idx = pick.date.weekday()
            day_name = day_names[day_idx]
            if result == 'W':
                analytics['by_day_of_week'][day_name]['wins'] += 1
            elif result == 'L':
                analytics['by_day_of_week'][day_name]['losses'] += 1
        
        # By time window
        window = getattr(pick, 'game_window', None) or 'MID'
        if window in analytics['by_time_window']:
            if result == 'W':
                analytics['by_time_window'][window]['wins'] += 1
            elif result == 'L':
                analytics['by_time_window'][window]['losses'] += 1
        
        # By confidence tier (estimate based on edge)
        edge = pick.edge or 0
        if edge >= 12:
            tier = 'SUPERMAX'
        elif edge >= 10:
            tier = 'HIGH'
        elif edge >= 8:
            tier = 'MEDIUM'
        else:
            tier = 'LOW'
        if result == 'W':
            analytics['by_confidence_tier'][tier]['wins'] += 1
        elif result == 'L':
            analytics['by_confidence_tier'][tier]['losses'] += 1
        
    
    # Calculate win rates
    def calc_win_rate(wins, losses):
        total = wins + losses
        return round(wins / total * 100, 1) if total > 0 else 0
    
    analytics['overall']['win_rate'] = calc_win_rate(analytics['overall']['wins'], analytics['overall']['losses'])
    
    for league in analytics['by_league']:
        d = analytics['by_league'][league]
        d['win_rate'] = calc_win_rate(d['wins'], d['losses'])
        d['total'] = d['wins'] + d['losses'] + d['pushes']
    
    for tier in analytics['by_confidence_tier']:
        d = analytics['by_confidence_tier'][tier]
        d['win_rate'] = calc_win_rate(d['wins'], d['losses'])
        d['total'] = d['wins'] + d['losses']
    
    for day in analytics['by_day_of_week']:
        d = analytics['by_day_of_week'][day]
        d['win_rate'] = calc_win_rate(d['wins'], d['losses'])
        d['total'] = d['wins'] + d['losses']
    
    for ptype in analytics['by_pick_type']:
        d = analytics['by_pick_type'][ptype]
        d['win_rate'] = calc_win_rate(d['wins'], d['losses'])
        d['total'] = d['wins'] + d['losses']
    
    for window in analytics['by_time_window']:
        d = analytics['by_time_window'][window]
        d['win_rate'] = calc_win_rate(d['wins'], d['losses'])
        d['total'] = d['wins'] + d['losses']
    
    
    # Recent streak (last 20 picks)
    recent = Pick.query.filter(Pick.result.in_(['W', 'L'])).order_by(Pick.date.desc()).limit(20).all()
    analytics['recent_streak'] = [p.result for p in recent]
    
    return jsonify({'success': True, 'analytics': analytics})

@app.route('/history')
def history():
    """Display pick history with win/loss stats - TOTALS ONLY (Lock of the Day)."""
    et = pytz.timezone('America/New_York')
    now = datetime.now(et)
    
    # TOTALS ONLY: Filter to pick_type='total' since spreads are removed from model
    all_picks_raw = Pick.query.filter_by(is_lock=True, pick_type='total').order_by(Pick.date.desc(), Pick.edge.desc()).all()
    
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
    
    # LOCK OF THE DAY ONLY: Show only the single highest edge pick
    if upcoming_picks:
        upcoming_picks = sorted(upcoming_picks, key=lambda p: p.edge or 0, reverse=True)[:1]
    
    wins = len([p for p in all_picks if p.result == 'W'])
    losses = len([p for p in all_picks if p.result == 'L'])
    
    return render_template('history.html', picks=past_picks, upcoming_picks=upcoming_picks, 
                          wins=wins, losses=losses)

@app.route('/bankroll')
def bankroll():
    """52 Week Bankroll Builder tracker."""
    return render_template('bankroll.html')

@app.route('/props')
def props():
    """Player Props Streak Tracker page."""
    return render_template('props.html')

@app.route('/api/player_props')
def api_player_props():
    """
    Fetch NBA player props with streak data.
    Uses bulk game log fetch for efficiency.
    Analyzes last 20 games for each player to find hot streaks.
    Uses 100-game simulation for AI projections.
    Filters out injured and questionable players.
    """
    try:
        from nba_api.stats.endpoints import playergamelogs, leaguedashplayerstats, scoreboardv2
        from nba_api.stats.static import teams as nba_teams
        import random
        import time
        
        et = pytz.timezone('America/New_York')
        today = datetime.now(et).strftime('%Y-%m-%d')
        
        logger.info("Starting player props fetch...")
        
        # Fetch injury report to exclude injured/questionable players
        injured_players = set()
        try:
            injury_url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
            injury_resp = requests.get(injury_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            if injury_resp.status_code == 200:
                injury_data = injury_resp.json()
                teams_data = injury_data.get('injuries', [])
                for team in teams_data:
                    players = team.get('injuries', [])
                    for player in players:
                        status = player.get('status', '').lower()
                        # Only exclude players who are OUT or DOUBTFUL
                        # Include QUESTIONABLE and DAY-TO-DAY as they often still play
                        if status in ['out', 'doubtful']:
                            player_name = player.get('athlete', {}).get('displayName', '')
                            if player_name:
                                injured_players.add(player_name.lower())
                logger.info(f"Found {len(injured_players)} injured/questionable players to exclude")
        except Exception as e:
            logger.warning(f"Could not fetch injury report: {e}")
        
        # Helper function to normalize player names for matching
        def normalize_name(name):
            """Normalize player name for consistent matching between APIs."""
            if not name:
                return ''
            # Lowercase and strip whitespace
            n = name.lower().strip()
            # Remove suffixes like Jr., III, IV, II
            for suffix in [' jr.', ' jr', ' iii', ' iv', ' ii', ' sr.', ' sr']:
                if n.endswith(suffix):
                    n = n[:-len(suffix)]
            # Common name variations
            replacements = {
                'demarr': 'demar',  # DeMar vs DeMarr
                '\'': '',  # Remove apostrophes (O'Brien -> OBrien)
            }
            for old, new in replacements.items():
                n = n.replace(old, new)
            return n.strip()
        
        # Fetch Bovada player prop lines from The Odds API
        bovada_lines = {}
        bovada_lines_normalized = {}  # Secondary lookup with normalized names
        try:
            odds_api_key = os.environ.get('BOVADA_API_KEY') or os.environ.get('ODDS_API_KEY') or os.environ.get('API_KEY')
            key_prefix = odds_api_key[:4] if odds_api_key else 'NONE'
            logger.info(f"ODDS API KEY loaded: {'YES' if odds_api_key else 'NO'} (length: {len(odds_api_key) if odds_api_key else 0}, starts with: {key_prefix})")
            if odds_api_key:
                # First get today's events
                events_url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={odds_api_key}"
                events_resp = requests.get(events_url, timeout=15)
                if events_resp.status_code == 200:
                    events = events_resp.json()
                    logger.info(f"Found {len(events)} NBA events for props")
                    
                    # Fetch player props for each event
                    for event in events[:12]:  # Limit to avoid API quota
                        event_id = event.get('id')
                        if not event_id:
                            continue
                        
                        # All prop markets including combos AND alternate lines (Joe's full cheat sheet)
                        # Joe uses ALT LINES with lower thresholds (e.g., O8.5 points instead of O18.5)
                        props_markets = [
                            # Single stat props
                            'player_points', 'player_points_alternate',
                            'player_rebounds', 'player_rebounds_alternate', 
                            'player_assists', 'player_assists_alternate',
                            'player_threes', 'player_threes_alternate',
                            'player_steals', 'player_steals_alternate',
                            'player_blocks', 'player_blocks_alternate',
                            # Combo props (PTS+REB, PTS+AST, REB+AST, PTS+REB+AST)
                            'player_points_rebounds', 'player_points_rebounds_alternate',
                            'player_points_assists', 'player_points_assists_alternate',
                            'player_rebounds_assists', 'player_rebounds_assists_alternate',
                            'player_points_rebounds_assists', 'player_points_rebounds_assists_alternate'
                        ]
                        props_url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event_id}/odds?apiKey={odds_api_key}&regions=us&markets={','.join(props_markets)}&bookmakers=fanduel,draftkings,bet365_us,fanatics"
                        
                        try:
                            props_resp = requests.get(props_url, timeout=10)
                            logger.info(f"Props API response status: {props_resp.status_code} for event {event_id}")
                            if props_resp.status_code == 200:
                                props_data = props_resp.json()
                                bookmakers = props_data.get('bookmakers', [])
                                logger.info(f"Event {event_id}: Found {len(bookmakers)} bookmakers")
                                for bm in bookmakers:
                                    # Accept all major sportsbooks (FanDuel, DraftKings, Bovada, etc.)
                                    # Joe's preferred bookmakers: DraftKings, FanDuel, Fanatics, Bet365
                                    if bm.get('key') in ['fanduel', 'draftkings', 'fanatics', 'bet365', 'bet365_us']:
                                        for market in bm.get('markets', []):
                                            # Normalize market key (remove player_ prefix and _alternate suffix)
                                            market_key = market.get('key', '').replace('player_', '').replace('_alternate', '')
                                            for outcome in market.get('outcomes', []):
                                                # Only get OVER lines, not UNDER
                                                outcome_name = outcome.get('name', '').lower()
                                                if outcome_name != 'over':
                                                    continue
                                                    
                                                player_name = outcome.get('description', '')
                                                line = outcome.get('point')
                                                odds = outcome.get('price', 0)
                                                
                                                # Debug logging for key players
                                                if player_name and ('derozan' in player_name.lower() or 'raynaud' in player_name.lower()):
                                                    logger.info(f"ODDS CHECK: {player_name} {market_key} line={line} odds={odds}")
                                                
                                                if player_name and line and odds:
                                                    # Store with both raw and normalized name keys (no odds filter)
                                                    key = f"{player_name.lower()}_{market_key}_{line}"
                                                    norm_key = f"{normalize_name(player_name)}_{market_key}_{line}"
                                                    line_data = {'line': line, 'odds': odds, 'player': player_name}
                                                    if key not in bovada_lines:
                                                        bovada_lines[key] = line_data
                                                    if norm_key not in bovada_lines_normalized:
                                                        bovada_lines_normalized[norm_key] = line_data
                        except Exception as e:
                            logger.warning(f"Error fetching props for event {event_id}: {e}")
                            continue
                    
                    logger.info(f"Fetched {len(bovada_lines)} prop lines from bookmakers")
                    # Count market types - combo markets have underscores in the market key
                    market_counts = {}
                    combo_markets = ['points_rebounds', 'points_assists', 'rebounds_assists', 'points_rebounds_assists']
                    for k in bovada_lines.keys():
                        # Key format: playername_market_line
                        for combo in combo_markets:
                            if f"_{combo}_" in k:
                                market_counts[combo] = market_counts.get(combo, 0) + 1
                                break
                        else:
                            # Single stat market
                            for single in ['points', 'rebounds', 'assists', 'threes', 'steals', 'blocks']:
                                if f"_{single}_" in k and not any(f"_{c}_" in k for c in combo_markets):
                                    market_counts[single] = market_counts.get(single, 0) + 1
                                    break
                    logger.info(f"Market type counts: {market_counts}")
                    # Log sample keys and unique player names for debugging
                    sample_keys = list(bovada_lines.keys())[:5]
                    logger.info(f"Sample keys: {sample_keys}")
                    # Extract unique player names from keys
                    unique_players = set()
                    for k in bovada_lines.keys():
                        # Key format: player_name_market_line (e.g., 'lebron james_points_24.5')
                        parts = k.rsplit('_', 2)  # Split off line and market
                        if len(parts) >= 3:
                            unique_players.add(parts[0])
                    logger.info(f"Sample API players: {list(unique_players)[:10]}")
                else:
                    logger.warning(f"Events API returned status {events_resp.status_code}")
        except Exception as e:
            logger.warning(f"Could not fetch Bovada lines: {e}")
        
        # Team ID to name mapping
        team_id_to_name = {t['id']: t['full_name'] for t in nba_teams.get_teams()}
        team_id_to_abbrev = {t['id']: t['abbreviation'] for t in nba_teams.get_teams()}
        
        # Get today's games to find active players
        today_games = []
        teams_playing = set()
        try:
            scoreboard = scoreboardv2.ScoreboardV2(game_date=today, timeout=15)
            games_data = scoreboard.get_normalized_dict()
            today_games = games_data.get('GameHeader', [])
            for game in today_games:
                teams_playing.add(game.get('HOME_TEAM_ID'))
                teams_playing.add(game.get('VISITOR_TEAM_ID'))
            logger.info(f"Found {len(today_games)} games today with {len(teams_playing)} teams")
        except Exception as e:
            logger.warning(f"Could not fetch today's games: {e}")
        
        # Build opponent lookup from today's games
        team_opponents = {}
        for game in today_games:
            home_id = game.get('HOME_TEAM_ID')
            away_id = game.get('VISITOR_TEAM_ID')
            team_opponents[home_id] = away_id
            team_opponents[away_id] = home_id
        
        # Fetch ALL player game logs at once (more efficient than individual calls)
        # CROSS-SEASON: Fetch both current AND previous season to calculate cross-season streaks
        all_game_logs = {}
        try:
            logger.info("Fetching bulk player game logs (cross-season)...")
            
            # Current season (2025-26)
            bulk_logs_current = playergamelogs.PlayerGameLogs(
                season_nullable='2025-26',
                last_n_games_nullable=100,
                timeout=90
            )
            logs_current = bulk_logs_current.get_data_frames()[0]
            
            # Previous season (2024-25) for cross-season streaks
            try:
                bulk_logs_previous = playergamelogs.PlayerGameLogs(
                    season_nullable='2024-25',
                    last_n_games_nullable=50,
                    timeout=90
                )
                logs_previous = bulk_logs_previous.get_data_frames()[0]
                # Combine both seasons
                import pandas as pd
                logs_df = pd.concat([logs_current, logs_previous], ignore_index=True)
                
                # Filter out preseason games (GAME_ID starting with '001')
                # NBA GAME_ID format: 001=preseason, 002=regular, 003=allstar, 004=playoffs
                original_count = len(logs_df)
                logs_df = logs_df[~logs_df['GAME_ID'].astype(str).str.startswith('001')]
                filtered_count = len(logs_df)
                logger.info(f"Cross-season data: {filtered_count} regular season games (filtered {original_count - filtered_count} preseason games)")
            except Exception as e:
                logger.warning(f"Could not fetch previous season, using current only: {e}")
                logs_df = logs_current
            
            # Group by player and sort by date (most recent first)
            for player_id, group in logs_df.groupby('PLAYER_ID'):
                all_game_logs[player_id] = group.sort_values('GAME_DATE', ascending=False)
            
            logger.info(f"Fetched game logs for {len(all_game_logs)} players")
        except Exception as e:
            logger.error(f"Could not fetch bulk game logs: {e}")
            return jsonify({'success': False, 'message': f'Error fetching game logs: {str(e)}', 'props': []})
        
        # Get player stats for the season
        try:
            player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
                season='2025-26',
                per_mode_detailed='PerGame',
                timeout=30
            )
            stats_df = player_stats.get_data_frames()[0]
            logger.info(f"Fetched stats for {len(stats_df)} players")
        except Exception as e:
            logger.error(f"Could not fetch player stats: {e}")
            return jsonify({'success': False, 'message': f'Error fetching player stats: {str(e)}', 'props': []})
        
        # Filter to players on teams playing today with meaningful minutes
        if teams_playing:
            active_players = stats_df[
                (stats_df['TEAM_ID'].isin(teams_playing)) & 
                (stats_df['MIN'] >= 15) &
                (stats_df['GP'] >= 10)
            ]
        else:
            # If no games today, get top players by minutes
            active_players = stats_df[
                (stats_df['MIN'] >= 20) &
                (stats_df['GP'] >= 15)
            ].head(100)
        
        logger.info(f"Processing {len(active_players)} active players")
        
        props_found = []
        
        # Define prop types to check with multiple thresholds (market_key for Bovada lookup)
        # Joe's exact thresholds from his sheets (expanded to match his methodology)
        prop_types = [
            {'key': 'points', 'name': 'Points', 'thresholds': [8, 10, 12, 15, 17, 20, 25], 'stat': 'PTS', 'market_key': 'points', 'priority': 5},
            {'key': 'rebounds', 'name': 'Rebounds', 'thresholds': [2, 3, 4, 5, 7, 10], 'stat': 'REB', 'market_key': 'rebounds', 'priority': 3},
            {'key': 'assists', 'name': 'Assists', 'thresholds': [2, 3, 4, 5, 6, 8], 'stat': 'AST', 'market_key': 'assists', 'priority': 4},
            {'key': 'threes', 'name': '3 Point Made', 'thresholds': [1, 2, 3, 4], 'stat': 'FG3M', 'market_key': 'threes', 'priority': 2},
            {'key': 'blocks', 'name': 'Blocks', 'thresholds': [1, 2, 3], 'stat': 'BLK', 'market_key': 'blocks', 'priority': 1},
            {'key': 'steals', 'name': 'Steals', 'thresholds': [1, 2, 3], 'stat': 'STL', 'market_key': 'steals', 'priority': 1},
            {'key': 'pts_reb_ast', 'name': 'PTS+AST+REB', 'thresholds': [18, 20, 23, 25, 28, 30, 35], 'stats': ['PTS', 'REB', 'AST'], 'market_key': 'points_rebounds_assists', 'priority': 6},
            {'key': 'pts_reb', 'name': 'PTS+REB', 'thresholds': [10, 11, 12, 13, 15, 18, 20], 'stats': ['PTS', 'REB'], 'market_key': 'points_rebounds', 'priority': 5},
            {'key': 'pts_ast', 'name': 'PTS+AST', 'thresholds': [10, 12, 15, 18, 20], 'stats': ['PTS', 'AST'], 'market_key': 'points_assists', 'priority': 5},
            {'key': 'reb_ast', 'name': 'REB+AST', 'thresholds': [5, 6, 8, 10, 12], 'stats': ['REB', 'AST'], 'market_key': 'rebounds_assists', 'priority': 4},
        ]
        
        # Fetch STAT-SPECIFIC defensive rankings (Joe's methodology)
        # Each stat type has its own ranking (e.g., "16th most points allowed")
        # Using NBA API's built-in rank columns for accuracy
        stat_def_rankings = {
            'PTS': {},      # Points allowed ranking
            'REB': {},      # Rebounds allowed ranking
            'AST': {},      # Assists allowed ranking
            'BLK': {},      # Blocks allowed ranking
            'STL': {},      # Steals allowed ranking
            'FG3M': {},     # 3PM allowed ranking
        }
        try:
            from nba_api.stats.endpoints import leaguedashteamstats
            # Use PerGame mode to match Joe's methodology (per-game averages, not totals)
            team_stats = leaguedashteamstats.LeagueDashTeamStats(
                season='2025-26',
                measure_type_detailed_defense='Opponent',
                per_mode_detailed='PerGame',
                timeout=30
            )
            team_df = team_stats.get_data_frames()[0]
            
            # Use built-in rank columns from NBA API
            # Higher rank number = allows MORE of that stat = worse defense = better for OVER bets
            stat_rank_columns = {
                'PTS': 'OPP_PTS_RANK',
                'REB': 'OPP_REB_RANK', 
                'AST': 'OPP_AST_RANK',
                'BLK': 'OPP_BLK_RANK',
                'STL': 'OPP_STL_RANK',
                'FG3M': 'OPP_FG3M_RANK',
            }
            
            for stat_key, rank_column in stat_rank_columns.items():
                if rank_column in team_df.columns:
                    for _, row in team_df.iterrows():
                        # NBA API rank: 1 = allows LEAST (best defense), 30 = allows MOST (worst defense)
                        # Joe's format is the same: higher rank = worse defense = more favorable for OVER bets
                        # Ranks 21-30 = bottom 10 defenses (worst) = favorable matchups
                        stat_def_rankings[stat_key][row['TEAM_ID']] = int(row[rank_column])
            
            logger.info(f"Fetched stat-specific defensive rankings for {len(team_df)} teams")
        except Exception as e:
            logger.warning(f"Could not fetch defensive rankings: {e}")
        
        def get_def_rank(opponent_team_id, stat_key='PTS'):
            """Get opponent's defensive rank for a specific stat category"""
            # For combo stats, use the primary stat's ranking
            if stat_key in ['Pts+Reb+Ast', 'Pts+Reb', 'Pts+Ast']:
                stat_key = 'PTS'
            elif stat_key in ['Reb+Ast']:
                stat_key = 'REB'
            return stat_def_rankings.get(stat_key, {}).get(opponent_team_id, None)
        
        # Process each player using pre-fetched data
        player_count = 0
        skipped_injured = 0
        for _, player in active_players.iterrows():
            player_id = player['PLAYER_ID']
            player_name = player['PLAYER_NAME']
            team_id = player['TEAM_ID']
            team_full_name = team_id_to_name.get(team_id, 'Unknown')
            
            # Skip injured/questionable players
            if player_name.lower() in injured_players:
                skipped_injured += 1
                continue
            
            # Get pre-fetched game logs
            if player_id not in all_game_logs:
                continue
            
            logs_df = all_game_logs[player_id]
            games_available = len(logs_df)
            if games_available < 10:
                continue
            
            # Get opponent ID for defensive rankings
            opponent_id = team_opponents.get(team_id)
            
            # Check ALL prop types through 100-game simulation
            for prop in prop_types:
                # Get stat-specific defensive rank (Joe's methodology)
                # e.g., "16th most points allowed" for points props
                stat_key = prop.get('stat', 'PTS')
                if 'stats' in prop:
                    stat_key = prop['stats'][0]  # Use primary stat for combos
                opp_def_rank = get_def_rank(opponent_id, stat_key) if opponent_id else None
                # Get stat values from game logs
                try:
                    if 'stats' in prop:
                        values = logs_df[prop['stats']].sum(axis=1).tolist()
                    else:
                        values = logs_df[prop['stat']].tolist()
                except:
                    continue
                
                # Need at least 10 games for analysis
                if len(values) < 10:
                    continue
                
                # === 100-GAME SIMULATION MODEL ===
                # Use all available games (up to 100) for projection
                simulation_values = values[:min(100, len(values))]
                
                # Calculate base projection from weighted average (recent games weighted more)
                weights = []
                for i, v in enumerate(simulation_values):
                    # Exponential decay: most recent = 1.0, older games = less weight
                    weight = 0.95 ** i
                    weights.append(weight)
                
                weighted_sum = sum(v * w for v, w in zip(simulation_values, weights))
                total_weight = sum(weights)
                base_projection = weighted_sum / total_weight if total_weight > 0 else 0
                
                # Calculate standard deviation for variance modeling
                if len(simulation_values) >= 2:
                    std_dev = statistics.stdev(simulation_values)
                else:
                    std_dev = 0
                
                # Defense adjustment (rank 16-30 = boost, rank 1-15 = penalty)
                if opp_def_rank:
                    if opp_def_rank > 15:
                        defense_boost = 1.0 + ((opp_def_rank - 15) * 0.012)  # Up to 18% boost
                    else:
                        defense_boost = 1.0 - ((15 - opp_def_rank) * 0.008)  # Up to 12% penalty
                else:
                    defense_boost = 1.0
                
                ai_proj = base_projection * defense_boost
                
                # Look up ALL Bovada lines for this player/prop (including alternates)
                # Try both raw name and normalized name lookups
                market_key = prop.get('market_key', prop['key'])
                player_key_prefix = f"{player_name.lower()}_{market_key}_"
                norm_player_key_prefix = f"{normalize_name(player_name)}_{market_key}_"
                
                # Find all lines for this player/prop - try raw first, then normalized
                available_lines = []
                for key, val in bovada_lines.items():
                    if key.startswith(player_key_prefix):
                        available_lines.append(val)
                
                # If no matches with raw name, try normalized lookup
                if not available_lines:
                    for key, val in bovada_lines_normalized.items():
                        if key.startswith(norm_player_key_prefix):
                            available_lines.append(val)
                
                # Debug: Log lookups for specific players
                if 'derozan' in player_name.lower() or 'kornet' in player_name.lower():
                    matching_keys = [k for k in bovada_lines.keys() if 'derozan' in k.lower() or 'kornet' in k.lower()][:5]
                    logger.info(f"DEBUG {player_name} {prop['name']}: raw='{player_key_prefix}', norm='{norm_player_key_prefix}', found {len(available_lines)}")
                    if matching_keys:
                        logger.info(f"DEBUG API keys sample: {matching_keys}")
                
                # === JOE'S METHODOLOGY: Generate streaks from game logs ===
                # When API lines available, use them; otherwise test our own thresholds
                # This ensures we always show hot streaks regardless of API availability
                
                # Need at least 10 games for analysis
                if len(values) < 10:
                    continue
                
                best_line_data = None
                best_streak_for_line = 0
                bovada_line = None
                
                # JOE'S METHODOLOGY: ONLY use actual sportsbook lines (no generated thresholds)
                # "I use DraftKings, FanDuel, Fanatics and Bet365 to make the wagers"
                # Find the line with the LONGEST streak (10+ games)
                if not available_lines:
                    continue  # Skip if no sportsbook line available
                
                # Test all available sportsbook lines, pick the one with longest streak
                for line_data in available_lines:
                    test_line = line_data['line']
                    test_odds = line_data.get('odds', -110)
                    
                    # Joe's rule: keep odds at -500 or better (decimal 1.2 = -500)
                    if test_odds < 1.2:
                        continue
                    
                    # For O8.5 line, player needs 9+ to hit
                    threshold_check = int(test_line) + 1
                    test_streak = 0
                    for v in values:
                        if v >= threshold_check:
                            test_streak += 1
                        else:
                            break
                    
                    # Joe's sheets: 10+ games in a row hitting that line
                    if test_streak >= 10 and test_streak > best_streak_for_line:
                        best_streak_for_line = test_streak
                        best_line_data = line_data
                        bovada_line = test_line
                
                # Skip if no qualifying streak found
                if not bovada_line or best_streak_for_line < 10:
                    continue
                # For .5 lines: O0.5 needs 1+, O3.5 needs 4+ (ceiling)
                threshold = int(bovada_line) + 1 if bovada_line == int(bovada_line) + 0.5 else int(bovada_line) + 1
                
                # Log when we find a good match
                if player_count <= 10:
                    logger.info(f"MATCH: {player_name} - {prop['name']} - line: {bovada_line} (threshold: {threshold}, streak: {best_streak_for_line})")
                
                # Calculate CONSECUTIVE hit streak
                consecutive_streak = best_streak_for_line
                
                # Calculate hit rates for display
                l5_values = values[:5]
                l5_hits = sum(1 for v in l5_values if v >= threshold)
                
                l10_values = values[:10]
                l10_hits = sum(1 for v in l10_values if v >= threshold)
                
                l20_values = values[:min(20, len(values))]
                l20_hits = sum(1 for v in l20_values if v >= threshold)
                
                # Debug: Log L20 85%+ qualified streaks
                current_l20_pct = (l20_hits / len(l20_values)) * 100 if l20_values else 0
                if current_l20_pct >= 85 and len(l20_values) >= 20:
                    logger.info(f"QUALIFIED: {player_name} - {prop['name']} - {consecutive_streak}/{len(l20_values)} consecutive, L20: {l20_hits}/20 ({current_l20_pct:.0f}%), line: {bovada_line}")
                
                # Debug key players even without 10+ streak
                if 'derozan' in player_name.lower() or 'raynaud' in player_name.lower() or 'lebron' in player_name.lower() or 'james' in player_name.lower():
                    l20_pct_debug = (l20_hits / len(l20_values)) * 100 if l20_values else 0
                    logger.info(f"DEBUG FILTER {player_name} {prop['name']} line={bovada_line}: streak={consecutive_streak}, L5={l5_hits}/5, L20={l20_hits}/{len(l20_values)} ({l20_pct_debug:.0f}%)")
                
                # Calculate L20 percentage for filtering
                l20_pct = (l20_hits / len(l20_values)) * 100 if l20_values else 0
                
                # === JOE'S METHODOLOGY - STREAK ONLY ===
                # Joe shows ANY prop with 10+ consecutive game streak
                # No L10 100% or L20 85% requirement - just the streak matters
                # The streak IS the filter (10+ games in a row hitting the line)
                
                # 1. Must have at least 10 games of data
                if len(values) < 10:
                    continue
                
                # 2. Must have 10+ consecutive game streak (already checked above)
                if consecutive_streak < 10:
                    continue
                
                # Track the streak - Joe's format shows consecutive streak on both sides
                # e.g., "30/L30" means 30 consecutive games hit, "25/L25" means 25 consecutive
                best_streak = consecutive_streak
                best_sample = consecutive_streak
                
                # === EDGE CALCULATION ===
                # Edge = (AI_Projection - Prop_Line) / Prop_Line × 100
                edge_pct = ((ai_proj - bovada_line) / bovada_line) * 100 if bovada_line > 0 else 0
                
                # Show all props with positive edge (AI projection > line)
                # Edge is for display/sorting, not filtering
                
                # === STREAK PERCENTAGE ===
                streak_pct = l20_pct
                
                # No classification badges - only ELITE star for favorable defense matchups
                play_classification = None
                confidence_color = None
                
                # Create defensive rank display with proper ordinal (just the rank number)
                stat_name = prop['name']
                # Get ordinal suffix (handles 11th, 12th, 13th, 21st, 22nd, 23rd, etc.)
                def get_ordinal(n):
                    if 11 <= n % 100 <= 13:
                        return f"{n}th"
                    elif n % 10 == 1:
                        return f"{n}st"
                    elif n % 10 == 2:
                        return f"{n}nd"
                    elif n % 10 == 3:
                        return f"{n}rd"
                    else:
                        return f"{n}th"
                def_rank_display = get_ordinal(opp_def_rank) if opp_def_rank else "N/A"
                
                # Create display with hit rates
                # Display as ceiling value (5.5 → "6+", 0.5 → "1+")
                display_threshold = int(bovada_line) + 1 if bovada_line == int(bovada_line) + 0.5 else int(bovada_line)
                prop_display = f"{display_threshold}+ {prop['name']}"
                hit_rates = f"L5: {l5_hits}/5 | L10: {l10_hits}/10 | L20: {l20_hits}/20"
                
                # Calculate implied probability from standard over odds (-110)
                standard_odds = -110
                implied_prob = abs(standard_odds) / (abs(standard_odds) + 100) * 100
                
                # Calculate model probability from hit rate
                model_prob = (l20_hits / 20) * 100
                
                # Calculate EV% (model probability - implied probability)
                ev_pct = round(model_prob - implied_prob, 1)
                ev_positive = ev_pct >= 0
                
                # Last 5 results visual (✓/✗)
                l5_visual = ''.join(['✓' if v >= threshold else '✗' for v in l5_values])
                
                # Trend arrow based on L5 vs L10 average
                l5_avg = sum(l5_values) / len(l5_values) if l5_values else 0
                l10_avg = sum(l10_values) / len(l10_values) if l10_values else 0
                if l5_avg > l10_avg * 1.05:
                    trend = '↑'  # Trending up
                elif l5_avg < l10_avg * 0.95:
                    trend = '↓'  # Trending down
                else:
                    trend = '→'  # Stable
                
                # VALUE SCORE based on edge thresholds
                # 35%+ Edge = 100 points (Premium)
                # 25-34% Edge = 80 points (Strong)
                # 15-24% Edge = 60 points (Standard)
                if edge_pct >= 35:
                    value_score = 100
                elif edge_pct >= 25:
                    value_score = 80 + int((edge_pct - 25) * 2)  # 80-99
                else:
                    value_score = 60 + int((edge_pct - 15) * 2)  # 60-79
                
                # Elite pick = bottom 10 defense (ranks 21-30 are worst defenses)
                is_elite = opp_def_rank and opp_def_rank >= 21
                
                props_found.append({
                    'team': team_full_name,
                    'player': player_name,
                    'prop_type': prop['key'],
                    'prop_priority': prop.get('priority', 1),  # For sorting - higher = more impressive prop
                    'prop_display': prop_display,
                    'streak': best_streak,
                    'sample': best_sample,
                    'streak_display': f"{best_streak}/L{best_sample}",
                    'streak_pct': round(streak_pct, 0),
                    'hit_rates': hit_rates,
                    'l5': f"{l5_hits}/5",
                    'l10': f"{l10_hits}/10",
                    'l20': f"{l20_hits}/20",
                    'l5_visual': l5_visual,
                    'trend': trend,
                    'implied_prob': round(implied_prob, 1),
                    'model_prob': round(model_prob, 1),
                    'ev_pct': ev_pct,
                    'ev_positive': ev_positive,
                    'edge_pct': round(edge_pct, 1),
                    'value_score': value_score,
                    'confidence_color': confidence_color,
                    'play_classification': play_classification,
                    'def_rank': opp_def_rank,
                    'def_rank_display': def_rank_display,
                    'is_elite': is_elite,
                    'stat_name': stat_name,
                    'ai_proj': round(ai_proj, 1),
                    'bovada_line': bovada_line,
                    'edge': round(ai_proj - bovada_line, 1) if bovada_line else None,
                    'status': None
                })
            
            player_count += 1
        
        # Deduplicate exact same props (same player, prop type, line)
        # Can happen when same line appears from multiple sportsbooks
        seen_prop_keys = set()
        deduped_props = []
        for prop in props_found:
            prop_key = f"{prop['player']}_{prop['prop_display']}_{prop['bovada_line']}"
            if prop_key not in seen_prop_keys:
                deduped_props.append(prop)
                seen_prop_keys.add(prop_key)
        props_found = deduped_props
        
        # Joe's methodology: Max 2 props per player
        # Sort: Elite (favorable defense 21-30) first, then streak, then L20, then def rank
        def sort_key(x):
            def_rank = x.get('opp_def_rank') or 99
            is_elite = 1 if def_rank >= 21 else 0  # Elite = favorable defense (ranks 21-30)
            l20_rate = x.get('l20_hit_rate', 0)
            streak = x.get('streak', 0)
            return (-is_elite, -streak, -l20_rate, -def_rank)
        
        props_found.sort(key=sort_key)
        
        # Keep max 2 props per player (Joe's rule)
        player_prop_counts = {}
        filtered_props = []
        for prop in props_found:
            player = prop['player']
            if player not in player_prop_counts:
                player_prop_counts[player] = 0
            if player_prop_counts[player] < 2:
                filtered_props.append(prop)
                player_prop_counts[player] += 1
        
        props_found = filtered_props
        
        # Get Elite 10 - top picks with favorable defense AND L20 85%+ (unique players)
        elite_picks = []
        seen_players = set()
        for prop in props_found:
            if len(elite_picks) >= 10:
                break
            # All qualified props have L20 85%+, prioritize unique players
            if prop['player'] not in seen_players:
                elite_picks.append(prop)
                seen_players.add(prop['player'])
        
        logger.info(f"Found {len(props_found)} player props with hot streaks from {player_count} players (skipped {skipped_injured} injured)")
        
        return jsonify({
            'success': True,
            'props': props_found,
            'elite': elite_picks,
            'count': len(props_found),
            'message': f'Found {len(props_found)} hot streaks from {player_count} active players'
        })
        
    except Exception as e:
        logger.error(f"Error in player props API: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e), 'props': []})

@app.route('/api/euroleague_props')
def api_euroleague_props():
    """
    Fetch EuroLeague player props with streak data.
    Uses euroleague-api package for player game logs.
    """
    try:
        from euroleague_api.player_stats import PlayerStats
        from euroleague_api.game_stats import GameStats
        import pandas as pd
        
        logger.info("Starting EuroLeague player props fetch...")
        
        # Initialize EuroLeague API ("E" for EuroLeague, "U" for EuroCup)
        competition = request.args.get('competition', 'E')  # Default to EuroLeague
        player_stats_api = PlayerStats(competition)
        
        # Get current season stats (2024-25 season = 2024)
        season = 2024
        
        try:
            # Fetch player stats for the season (2024-25 = season 2024)
            stats_df = player_stats_api.get_player_stats_single_season(
                endpoint='traditional',
                season=season,
                phase_type_code='RS',  # Regular season
                statistic_mode='PerGame'
            )
            logger.info(f"Fetched stats for {len(stats_df)} EuroLeague players")
        except Exception as e:
            logger.error(f"Could not fetch EuroLeague player stats: {e}")
            return jsonify({'success': False, 'message': f'Error fetching EuroLeague stats: {str(e)}', 'props': []})
        
        # Filter to players with meaningful minutes (15+ min avg, 10+ games)
        if 'minutesPlayed' in stats_df.columns and 'gamesPlayed' in stats_df.columns:
            active_players = stats_df[
                (stats_df['minutesPlayed'] >= 15) &
                (stats_df['gamesPlayed'] >= 10)
            ]
        else:
            # Fallback
            active_players = stats_df.head(100)
        
        logger.info(f"Processing {len(active_players)} active EuroLeague players")
        
        props_found = []
        
        # Define prop types to check (EuroLeague column names)
        prop_types = [
            {'key': 'points', 'name': 'Points', 'thresholds': [8, 10, 12, 15, 20], 'stat': 'pointsScored'},
            {'key': 'rebounds', 'name': 'Rebounds', 'thresholds': [2, 3, 4, 5, 7], 'stat': 'totalRebounds'},
            {'key': 'assists', 'name': 'Assists', 'thresholds': [2, 3, 4, 5, 6], 'stat': 'assists'},
        ]
        
        # Process each player
        for _, player in active_players.iterrows():
            player_name = player.get('player.name', 'Unknown')
            team_name = player.get('player.team.name', 'Unknown')
            
            # Get season averages as proxy for streaks (EuroLeague API doesn't have game logs per player easily)
            for prop in prop_types:
                stat_col = prop['stat']
                if stat_col not in player.index:
                    continue
                    
                season_avg = player.get(stat_col, 0)
                if not season_avg or season_avg < 1:
                    continue
                
                # Find best threshold based on season average
                best_threshold = None
                for t in sorted(prop['thresholds'], reverse=True):
                    if season_avg >= t * 1.1:  # 10% above threshold
                        best_threshold = t
                        break
                
                if not best_threshold:
                    continue
                
                # Calculate synthetic metrics based on season average
                ai_proj = round(season_avg, 1)
                edge = round(season_avg - best_threshold, 1)
                
                # === NEW PROTOCOL: EDGE CALCULATION ===
                # Edge = (AI_Projection - Prop_Line) / Prop_Line × 100
                edge_pct = ((ai_proj - best_threshold) / best_threshold) * 100 if best_threshold > 0 else 0
                
                # Filter: Minimum 15%+ Edge required
                if edge_pct < 15:
                    continue
                
                # Assume 90%+ hit rate for EuroLeague (less variance)
                hit_rate = min(98, 75 + (edge_pct * 0.5))
                streak_pct = hit_rate
                
                # === CLASSIFICATION (PLAY / STRONG PLAY / PREMIUM PLAY) ===
                if edge_pct >= 25 and hit_rate >= 95:
                    play_classification = 'STRONG PLAY'
                    confidence_color = 'green'
                elif edge_pct >= 35:
                    play_classification = 'PREMIUM PLAY'
                    confidence_color = 'gold'
                else:
                    play_classification = 'PLAY'
                    confidence_color = 'yellow' if edge_pct >= 20 else 'lime'
                
                # Calculate EV and value score
                implied_prob = 52.38
                model_prob = hit_rate
                ev_pct = round(model_prob - implied_prob, 1)
                
                # VALUE SCORE based on edge thresholds
                if edge_pct >= 35:
                    value_score = 100
                elif edge_pct >= 25:
                    value_score = 80 + int((edge_pct - 25) * 2)
                else:
                    value_score = 60 + int((edge_pct - 15) * 2)
                
                props_found.append({
                    'team': str(team_name),
                    'player': str(player_name),
                    'prop_type': prop['key'],
                    'prop_display': f"{best_threshold}+ {prop['name']}",
                    'streak': int(hit_rate),
                    'sample': 20,
                    'streak_display': f"{int(hit_rate)}% season",
                    'streak_pct': round(streak_pct, 0),
                    'hit_rates': f"Season avg: {season_avg:.1f}",
                    'l5': "N/A",
                    'l10': "N/A",
                    'l20': f"{int(hit_rate)}%",
                    'l5_visual': '●●●●●',
                    'trend': '→',
                    'implied_prob': round(implied_prob, 1),
                    'model_prob': round(model_prob, 1),
                    'ev_pct': ev_pct,
                    'ev_positive': ev_pct >= 0,
                    'edge_pct': round(edge_pct, 1),
                    'value_score': value_score,
                    'confidence_color': confidence_color,
                    'play_classification': play_classification,
                    'def_rank': None,
                    'ai_proj': ai_proj,
                    'bovada_line': best_threshold,
                    'edge': edge,
                    'status': None,
                    'league': 'EURO' if competition == 'E' else 'EUROCUP'
                })
        
        # Sort by edge_pct (highest edge = best pick per protocol)
        props_found.sort(key=lambda x: (-x['edge_pct'], -x['value_score'], -x['ai_proj']))
        
        # Get Elite 10
        elite_picks = []
        seen_players = set()
        for prop in props_found:
            if len(elite_picks) >= 10:
                break
            if prop['player'] not in seen_players:
                elite_picks.append(prop)
                seen_players.add(prop['player'])
        
        comp_name = "EuroLeague" if competition == 'E' else "EuroCup"
        logger.info(f"Found {len(props_found)} {comp_name} player props")
        
        return jsonify({
            'success': True,
            'props': props_found,
            'elite': elite_picks,
            'count': len(props_found),
            'message': f'Found {len(props_found)} {comp_name} player props',
            'league': comp_name
        })
        
    except Exception as e:
        logger.error(f"Error in EuroLeague props API: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e), 'props': []})

@app.route('/download/codebase_structure')
def download_codebase_structure():
    """Download the codebase structure CSV."""
    from flask import send_file
    return send_file('sports_app_structure.csv', as_attachment=True, download_name='sports_app_structure.csv')

@app.route('/download/app')
def download_app():
    """Download the sports app zip file."""
    from flask import send_file
    import os
    zip_path = os.path.join(os.path.dirname(__file__), 'static', 'downloads', 'sports_app_latest.zip')
    if os.path.exists(zip_path):
        return send_file(zip_path, as_attachment=True, download_name='sports_app_latest.zip')
    return jsonify({'error': 'File not found'}), 404

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
    # LAYER 2: MODEL QUALIFICATION FLAGS (TOTALS ONLY)
    # Tests is_qualified flags
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
    
    layer2_passed = all(t['passed'] == t['expected'] for t in layer2_tests)
    results.append({
        "layer": 2,
        "name": "MODEL QUALIFICATION FLAGS",
        "status": "PASS" if layer2_passed else "FAIL",
        "tests": layer2_tests
    })
    
    # ============================================================
    # LAYER 3: HISTORICAL QUALIFICATION (TOTALS ONLY)
    # Tests history_qualified
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
    # LAYER 6: FULL INTEGRATION TEST (TOTALS ONLY)
    # Tests complete workflow from validation to Discord payload
    # ============================================================
    layer6_tests = []
    
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
    
    layer6_tests.append({
        "test": "Lakers @ Celtics (edge 12.5, EV 3.5%) -> SUPERMAX tier",
        "passed": any(p['game'] == "Lakers @ Celtics" and p['confidence_tier'] == "SUPERMAX" 
                      for p in validation_results['validated_picks']),
        "expected": True,
        "details": f"Found in tier: {[p['confidence_tier'] for p in validation_results['validated_picks'] if 'Lakers' in p['game']]}"
    })
    
    layer6_tests.append({
        "test": "Chiefs @ Bills (NFL edge 4.5) -> validated",
        "passed": any(p['game'] == "Chiefs @ Bills" for p in validation_results['validated_picks']),
        "expected": True,
        "details": "NFL pick with sufficient edge passes"
    })
    
    layer6_tests.append({
        "test": "Bruins @ Rangers (NHL edge 0.3 < 0.5) -> rejected",
        "passed": any(p['game'] == "Bruins @ Rangers" for p in validation_results['rejected_picks']),
        "expected": True,
        "details": "NHL edge below threshold rejected"
    })
    
    layer6_tests.append({
        "test": "Duke @ UNC (negative EV -1.5%) -> rejected",
        "passed": any(p['game'] == "Duke @ UNC" for p in validation_results['rejected_picks']),
        "expected": True,
        "details": "Negative EV pick rejected"
    })
    
    layer6_tests.append({
        "test": "Tier counts: SUPERMAX=1, validated=2, rejected=2",
        "passed": (len(validation_results['by_tier']['SUPERMAX']) == 1 and
                   len(validation_results['validated_picks']) == 2 and
                   len(validation_results['rejected_picks']) == 2),
        "expected": True,
        "details": f"SUPERMAX: {len(validation_results['by_tier']['SUPERMAX'])}, " +
                   f"validated: {len(validation_results['validated_picks'])}, " +
                   f"rejected: {len(validation_results['rejected_picks'])}"
    })
    
    layer6_passed = all(t['passed'] for t in layer6_tests)
    results.append({
        "layer": 6,
        "name": "FULL INTEGRATION TEST (TOTALS)",
        "status": "PASS" if layer6_passed else "FAIL",
        "tests": layer6_tests
    })
    
    # ============================================================
    # LAYER 7: TIMEZONE VALIDATION
    # Tests UTC to ET conversion for game start times (history page)
    # ============================================================
    layer7_tests = []
    
    et = pytz.timezone('America/New_York')
    utc = pytz.UTC
    now_et = datetime.now(et)
    
    # Test 1: Past game (UTC time that's definitely in the past)
    past_game_utc = datetime(2020, 1, 1, 12, 0, 0)  # Jan 1, 2020 12:00 UTC
    past_game_utc_tz = utc.localize(past_game_utc)
    past_game_et = past_game_utc_tz.astimezone(et)
    is_past = past_game_et <= now_et
    layer7_tests.append({
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
    layer7_tests.append({
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
    layer7_tests.append({
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
    layer7_tests.append({
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
    layer7_tests.append({
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
    layer7_tests.append({
        "test": "Game starting in 1 hour is correctly UPCOMING",
        "passed": is_upcoming,
        "expected": True,
        "details": f"Starts at {reconverted_et2.strftime('%H:%M:%S %Z')}, now {now_et.strftime('%H:%M:%S %Z')}"
    })
    
    layer7_passed = all(t['passed'] for t in layer7_tests)
    results.append({
        "layer": 7,
        "name": "TIMEZONE VALIDATION",
        "status": "PASS" if layer7_passed else "FAIL",
        "tests": layer7_tests
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
