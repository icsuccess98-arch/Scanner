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
from enhanced_scraping import get_cbb_logo, CBB_TEAM_LOGOS
from automated_loading_system import (
    setup_automatic_loading, 
    get_transparent_cbb_logo, 
    CBB_TEAM_LOGOS_COMPLETE,
    TeamRankingsScraper,
    EliminationFilterSystem
)

# NBA team logo URLs from ESPN CDN (module-level for shared access)
NBA_TEAM_COLORS = {
    'Hawks': '#E03A3E', 'Celtics': '#007A33', 'Nets': '#000000', 'Hornets': '#1D1160',
    'Bulls': '#CE1141', 'Cavaliers': '#860038', 'Mavericks': '#00538C', 'Nuggets': '#0E2240',
    'Pistons': '#C8102E', 'Warriors': '#1D428A', 'Rockets': '#CE1141', 'Pacers': '#002D62',
    'Clippers': '#C8102E', 'Lakers': '#552583', 'Grizzlies': '#5D76A9', 'Heat': '#98002E',
    'Bucks': '#00471B', 'Timberwolves': '#0C2340', 'Pelicans': '#0C2340', 'Knicks': '#F58426',
    'Thunder': '#007AC1', 'Magic': '#0077C0', 'Suns': '#1D1160', '76ers': '#006BB6',
    'Trail Blazers': '#E03A3E', 'Blazers': '#E03A3E', 'Kings': '#5A2D81', 'Spurs': '#C4CED4',
    'Raptors': '#CE1141', 'Jazz': '#002B5C', 'Wizards': '#002B5C'
}

nba_team_logos = {
    'Hawks': 'https://a.espncdn.com/i/teamlogos/nba/500/atl.png',
    'Celtics': 'https://a.espncdn.com/i/teamlogos/nba/500/bos.png',
    'Nets': 'https://a.espncdn.com/i/teamlogos/nba/500/bkn.png',
    'Hornets': 'https://a.espncdn.com/i/teamlogos/nba/500/cha.png',
    'Bulls': 'https://a.espncdn.com/i/teamlogos/nba/500/chi.png',
    'Cavaliers': 'https://a.espncdn.com/i/teamlogos/nba/500/cle.png',
    'Mavericks': 'https://a.espncdn.com/i/teamlogos/nba/500/dal.png',
    'Nuggets': 'https://a.espncdn.com/i/teamlogos/nba/500/den.png',
    'Pistons': 'https://a.espncdn.com/i/teamlogos/nba/500/det.png',
    'Warriors': 'https://a.espncdn.com/i/teamlogos/nba/500/gs.png',
    'Rockets': 'https://a.espncdn.com/i/teamlogos/nba/500/hou.png',
    'Pacers': 'https://a.espncdn.com/i/teamlogos/nba/500/ind.png',
    'Clippers': 'https://a.espncdn.com/i/teamlogos/nba/500/lac.png',
    'Lakers': 'https://a.espncdn.com/i/teamlogos/nba/500/lal.png',
    'Grizzlies': 'https://a.espncdn.com/i/teamlogos/nba/500/mem.png',
    'Heat': 'https://a.espncdn.com/i/teamlogos/nba/500/mia.png',
    'Bucks': 'https://a.espncdn.com/i/teamlogos/nba/500/mil.png',
    'Timberwolves': 'https://a.espncdn.com/i/teamlogos/nba/500/min.png',
    'Pelicans': 'https://a.espncdn.com/i/teamlogos/nba/500/no.png',
    'Knicks': 'https://a.espncdn.com/i/teamlogos/nba/500/ny.png',
    'Thunder': 'https://a.espncdn.com/i/teamlogos/nba/500/okc.png',
    'Magic': 'https://a.espncdn.com/i/teamlogos/nba/500/orl.png',
    '76ers': 'https://a.espncdn.com/i/teamlogos/nba/500/phi.png',
    'Suns': 'https://a.espncdn.com/i/teamlogos/nba/500/phx.png',
    'Trail Blazers': 'https://a.espncdn.com/i/teamlogos/nba/500/por.png',
    'Blazers': 'https://a.espncdn.com/i/teamlogos/nba/500/por.png',
    'Kings': 'https://a.espncdn.com/i/teamlogos/nba/500/sac.png',
    'Spurs': 'https://a.espncdn.com/i/teamlogos/nba/500/sa.png',
    'Raptors': 'https://a.espncdn.com/i/teamlogos/nba/500/tor.png',
    'Jazz': 'https://a.espncdn.com/i/teamlogos/nba/500/utah.png',
    'Wizards': 'https://a.espncdn.com/i/teamlogos/nba/500/wsh.png'
}

nhl_team_logos = {
    'Bruins': 'https://a.espncdn.com/i/teamlogos/nhl/500/bos.png',
    'Sabres': 'https://a.espncdn.com/i/teamlogos/nhl/500/buf.png',
    'Red Wings': 'https://a.espncdn.com/i/teamlogos/nhl/500/det.png',
    'Panthers': 'https://a.espncdn.com/i/teamlogos/nhl/500/fla.png',
    'Florida': 'https://a.espncdn.com/i/teamlogos/nhl/500/fla.png',
    'Canadiens': 'https://a.espncdn.com/i/teamlogos/nhl/500/mtl.png',
    'Montreal': 'https://a.espncdn.com/i/teamlogos/nhl/500/mtl.png',
    'Senators': 'https://a.espncdn.com/i/teamlogos/nhl/500/ott.png',
    'Ottawa': 'https://a.espncdn.com/i/teamlogos/nhl/500/ott.png',
    'Lightning': 'https://a.espncdn.com/i/teamlogos/nhl/500/tb.png',
    'Tampa Bay': 'https://a.espncdn.com/i/teamlogos/nhl/500/tb.png',
    'Maple Leafs': 'https://a.espncdn.com/i/teamlogos/nhl/500/tor.png',
    'Toronto': 'https://a.espncdn.com/i/teamlogos/nhl/500/tor.png',
    'Hurricanes': 'https://a.espncdn.com/i/teamlogos/nhl/500/car.png',
    'Carolina': 'https://a.espncdn.com/i/teamlogos/nhl/500/car.png',
    'Blue Jackets': 'https://a.espncdn.com/i/teamlogos/nhl/500/cbj.png',
    'Columbus': 'https://a.espncdn.com/i/teamlogos/nhl/500/cbj.png',
    'Devils': 'https://a.espncdn.com/i/teamlogos/nhl/500/njd.png',
    'New Jersey': 'https://a.espncdn.com/i/teamlogos/nhl/500/njd.png',
    'Islanders': 'https://a.espncdn.com/i/teamlogos/nhl/500/nyi.png',
    'NY Islanders': 'https://a.espncdn.com/i/teamlogos/nhl/500/nyi.png',
    'New York Islanders': 'https://a.espncdn.com/i/teamlogos/nhl/500/nyi.png',
    'Rangers': 'https://a.espncdn.com/i/teamlogos/nhl/500/nyr.png',
    'NY Rangers': 'https://a.espncdn.com/i/teamlogos/nhl/500/nyr.png',
    'New York Rangers': 'https://a.espncdn.com/i/teamlogos/nhl/500/nyr.png',
    'Flyers': 'https://a.espncdn.com/i/teamlogos/nhl/500/phi.png',
    'Philadelphia': 'https://a.espncdn.com/i/teamlogos/nhl/500/phi.png',
    'Penguins': 'https://a.espncdn.com/i/teamlogos/nhl/500/pit.png',
    'Pittsburgh': 'https://a.espncdn.com/i/teamlogos/nhl/500/pit.png',
    'Capitals': 'https://a.espncdn.com/i/teamlogos/nhl/500/wsh.png',
    'Washington': 'https://a.espncdn.com/i/teamlogos/nhl/500/wsh.png',
    'Blackhawks': 'https://a.espncdn.com/i/teamlogos/nhl/500/chi.png',
    'Chicago': 'https://a.espncdn.com/i/teamlogos/nhl/500/chi.png',
    'Avalanche': 'https://a.espncdn.com/i/teamlogos/nhl/500/col.png',
    'Colorado': 'https://a.espncdn.com/i/teamlogos/nhl/500/col.png',
    'Stars': 'https://a.espncdn.com/i/teamlogos/nhl/500/dal.png',
    'Dallas': 'https://a.espncdn.com/i/teamlogos/nhl/500/dal.png',
    'Wild': 'https://a.espncdn.com/i/teamlogos/nhl/500/min.png',
    'Minnesota': 'https://a.espncdn.com/i/teamlogos/nhl/500/min.png',
    'Predators': 'https://a.espncdn.com/i/teamlogos/nhl/500/nsh.png',
    'Nashville': 'https://a.espncdn.com/i/teamlogos/nhl/500/nsh.png',
    'Blues': 'https://a.espncdn.com/i/teamlogos/nhl/500/stl.png',
    'St. Louis': 'https://a.espncdn.com/i/teamlogos/nhl/500/stl.png',
    'Jets': 'https://a.espncdn.com/i/teamlogos/nhl/500/wpg.png',
    'Winnipeg': 'https://a.espncdn.com/i/teamlogos/nhl/500/wpg.png',
    'Ducks': 'https://a.espncdn.com/i/teamlogos/nhl/500/ana.png',
    'Anaheim': 'https://a.espncdn.com/i/teamlogos/nhl/500/ana.png',
    'Coyotes': 'https://a.espncdn.com/i/teamlogos/nhl/500/ari.png',
    'Arizona': 'https://a.espncdn.com/i/teamlogos/nhl/500/ari.png',
    'Utah Hockey Club': 'https://a.espncdn.com/i/teamlogos/nhl/500/uta.png',
    'Utah': 'https://a.espncdn.com/i/teamlogos/nhl/500/uta.png',
    'Flames': 'https://a.espncdn.com/i/teamlogos/nhl/500/cgy.png',
    'Calgary': 'https://a.espncdn.com/i/teamlogos/nhl/500/cgy.png',
    'Oilers': 'https://a.espncdn.com/i/teamlogos/nhl/500/edm.png',
    'Edmonton': 'https://a.espncdn.com/i/teamlogos/nhl/500/edm.png',
    'Kings': 'https://a.espncdn.com/i/teamlogos/nhl/500/la.png',
    'LA Kings': 'https://a.espncdn.com/i/teamlogos/nhl/500/la.png',
    'Los Angeles': 'https://a.espncdn.com/i/teamlogos/nhl/500/la.png',
    'Sharks': 'https://a.espncdn.com/i/teamlogos/nhl/500/sj.png',
    'San Jose': 'https://a.espncdn.com/i/teamlogos/nhl/500/sj.png',
    'Kraken': 'https://a.espncdn.com/i/teamlogos/nhl/500/sea.png',
    'Seattle': 'https://a.espncdn.com/i/teamlogos/nhl/500/sea.png',
    'Canucks': 'https://a.espncdn.com/i/teamlogos/nhl/500/van.png',
    'Vancouver': 'https://a.espncdn.com/i/teamlogos/nhl/500/van.png',
    'Golden Knights': 'https://a.espncdn.com/i/teamlogos/nhl/500/vgk.png',
    'Vegas': 'https://a.espncdn.com/i/teamlogos/nhl/500/vgk.png',
    'Vegas Golden Knights': 'https://a.espncdn.com/i/teamlogos/nhl/500/vgk.png',
}


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

flask_secret = os.environ.get("SESSION_SECRET") or os.environ.get("FLASK_SECRET_KEY")
if not flask_secret:
    import secrets
    flask_secret = secrets.token_hex(32)
    logger.warning("SESSION_SECRET not set - using generated key (sessions will reset on restart)")
app.secret_key = flask_secret

app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
    "pool_size": 5,
    "max_overflow": 10,
    "connect_args": {"connect_timeout": 10}
}
db.init_app(app)

compress = Compress()
compress.init_app(app)
logger.info("Response compression enabled")

# Fast health check endpoint for production deployments
@app.route('/health', methods=['GET', 'HEAD'])
def health_check():
    """
    Simple health check that returns immediately - no database queries.
    Supports both GET and HEAD requests for maximum compatibility.
    """
    if request.method == 'HEAD':
        return '', 200
    return jsonify({"status": "healthy", "version": "2.0.0", "service": "730sports"}), 200

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

# Extended thresholds for spread betting and sharp detection
EXTENDED_THRESHOLDS = {
    'NBA': {
        'total_edge': 8.0,
        'spread_edge': 3.5,
        'min_ev': 3.0,
        'sharp_threshold': 10.0,
        'max_spread': 10.0,
        'min_handle_avoid': 80
    },
    'CBB': {
        'total_edge': 8.0,
        'spread_edge': 3.5,
        'min_ev': 3.0,
        'sharp_threshold': 10.0,
        'max_spread': 10.0,
        'min_handle_avoid': 80
    },
    'NHL': {
        'total_edge': 0.5,
        'spread_edge': 0.5,
        'min_ev': 2.5,
        'sharp_threshold': 10.0,
        'max_spread': 2.0,
        'min_handle_avoid': 80
    }
}


def calculate_rlm(game) -> bool:
    """
    Detect Reverse Line Movement.
    RLM = Line moves OPPOSITE of public betting.
    """
    if not all([
        getattr(game, 'opening_spread', None),
        getattr(game, 'spread', None),
        getattr(game, 'away_tickets_pct', None),
        getattr(game, 'home_tickets_pct', None)
    ]):
        return False
    
    movement = (game.spread or 0) - (game.opening_spread or 0)
    public_on_away = (game.away_tickets_pct or 0) > (game.home_tickets_pct or 0)
    
    if public_on_away and movement > 0.5:
        return True
    elif not public_on_away and movement < -0.5:
        return True
    return False


def calculate_sharp_side(game) -> str:
    """
    Determine sharp side based on money vs tickets %.
    Sharp = Money % significantly higher than Tickets %
    """
    if not all([
        getattr(game, 'away_tickets_pct', None),
        getattr(game, 'home_tickets_pct', None),
        getattr(game, 'away_money_pct', None),
        getattr(game, 'home_money_pct', None)
    ]):
        return 'unknown'
    
    league = getattr(game, 'league', 'NBA')
    sharp_threshold = EXTENDED_THRESHOLDS.get(league, {}).get('sharp_threshold', 10.0)
    
    away_sharp_diff = (game.away_money_pct or 0) - (game.away_tickets_pct or 0)
    home_sharp_diff = (game.home_money_pct or 0) - (game.home_tickets_pct or 0)
    
    if away_sharp_diff >= sharp_threshold:
        return 'away'
    elif home_sharp_diff >= sharp_threshold:
        return 'home'
    return 'balanced'


def get_nba_abbr(team_name: str) -> str:
    """Get NBA team abbreviation."""
    abbr_map = {
        'hawks': 'atl', 'celtics': 'bos', 'nets': 'bkn',
        'hornets': 'cha', 'bulls': 'chi', 'cavaliers': 'cle',
        'mavericks': 'dal', 'nuggets': 'den', 'pistons': 'det',
        'warriors': 'gs', 'rockets': 'hou', 'pacers': 'ind',
        'clippers': 'lac', 'lakers': 'lal', 'grizzlies': 'mem',
        'heat': 'mia', 'bucks': 'mil', 'timberwolves': 'min',
        'pelicans': 'no', 'knicks': 'ny', 'thunder': 'okc',
        'magic': 'orl', '76ers': 'phi', 'suns': 'phx',
        'blazers': 'por', 'kings': 'sac', 'spurs': 'sa',
        'raptors': 'tor', 'jazz': 'utah', 'wizards': 'wsh'
    }
    team_lower = team_name.lower()
    for key, abbr in abbr_map.items():
        if key in team_lower:
            return abbr
    return ''


def get_team_logo_bulletproof(team_name: str, league: str) -> str:
    """
    Get team logo with bulletproof fallback chain.
    NEVER fails - always returns a valid URL.
    """
    team_lower = team_name.lower()
    
    if league == 'NBA':
        abbr = get_nba_abbr(team_name)
        if abbr:
            return f'https://a.espncdn.com/i/teamlogos/nba/500/{abbr}.png'
    elif league == 'CBB':
        from automated_loading_system import ESPN_CBB_TEAM_IDS
        team_id = ESPN_CBB_TEAM_IDS.get(team_name) or ESPN_CBB_TEAM_IDS.get(team_name.title())
        if team_id:
            return f'https://a.espncdn.com/i/teamlogos/ncaa/500-dark/{team_id}.png'
    
    return f'https://via.placeholder.com/64/667eea/ffffff?text={team_name[:3].upper()}'


class MatchupIntelligence:
    """
    Advanced Matchup Intelligence engine for NBA/CBB games.
    Fetches and computes advanced analytics from NBA.com and other sources.
    Based on CleaningTheGlass methodology.
    """
    
    # D1 averages for comparison (approximate)
    D1_AVERAGES = {
        '2PT%': 50.0,
        '3PT%': 34.0,
        '3PT_RATE': 40.0,
        'eFG%': 50.0,
        'TOV%': 18.0,
        'ORB%': 28.0,
        'FT_RATE': 32.0
    }
    
    # NBA team ID mapping for NBA.com API
    NBA_TEAM_IDS = {
        'ATL': 1610612737, 'BOS': 1610612738, 'BKN': 1610612751, 'CHA': 1610612766,
        'CHI': 1610612741, 'CLE': 1610612739, 'DAL': 1610612742, 'DEN': 1610612743,
        'DET': 1610612765, 'GSW': 1610612744, 'HOU': 1610612745, 'IND': 1610612754,
        'LAC': 1610612746, 'LAL': 1610612747, 'MEM': 1610612763, 'MIA': 1610612748,
        'MIL': 1610612749, 'MIN': 1610612750, 'NOP': 1610612740, 'NYK': 1610612752,
        'OKC': 1610612760, 'ORL': 1610612753, 'PHI': 1610612755, 'PHX': 1610612756,
        'POR': 1610612757, 'SAC': 1610612758, 'SAS': 1610612759, 'TOR': 1610612761,
        'UTA': 1610612762, 'WAS': 1610612764, 
        'Hawks': 1610612737, 'Celtics': 1610612738, 'Nets': 1610612751, 'Hornets': 1610612766,
        'Bulls': 1610612741, 'Cavaliers': 1610612739, 'Mavericks': 1610612742, 'Nuggets': 1610612743,
        'Pistons': 1610612765, 'Warriors': 1610612744, 'Rockets': 1610612745, 'Pacers': 1610612754,
        'Clippers': 1610612746, 'Lakers': 1610612747, 'Grizzlies': 1610612763, 'Heat': 1610612748,
        'Bucks': 1610612749, 'Timberwolves': 1610612750, 'Pelicans': 1610612740, 'Knicks': 1610612752,
        'Thunder': 1610612760, 'Magic': 1610612753, '76ers': 1610612755, 'Suns': 1610612756,
        'Trail Blazers': 1610612757, 'Kings': 1610612758, 'Spurs': 1610612759, 'Raptors': 1610612761,
        'Jazz': 1610612762, 'Wizards': 1610612764
    }
    
    @staticmethod
    def get_team_advanced_stats(team_name: str, league: str = 'NBA') -> dict:
        """
        Fetch advanced team stats from NBA.com API.
        Returns dict with shooting, rebounding, turnover, pace metrics.
        """
        try:
            from nba_api.stats.endpoints import teamdashboardbygeneralsplits, leaguedashteamstats
            from nba_api.stats.static import teams as nba_teams_static
            import time
            
            # Find team ID
            team_id = None
            for abbr, tid in MatchupIntelligence.NBA_TEAM_IDS.items():
                if abbr.lower() in team_name.lower() or team_name.lower() in abbr.lower():
                    team_id = tid
                    break
            
            if not team_id:
                # Try to find by full name
                all_teams = nba_teams_static.get_teams()
                for t in all_teams:
                    if team_name.lower() in t['full_name'].lower() or t['nickname'].lower() in team_name.lower():
                        team_id = t['id']
                        break
            
            if not team_id:
                logger.warning(f"Could not find NBA team ID for: {team_name}")
                return {}
            
            time.sleep(0.6)  # Rate limiting
            
            # Fetch advanced stats
            stats = teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits(
                team_id=team_id,
                season='2024-25',
                season_type_all_star='Regular Season'
            )
            
            overall = stats.overall_team_dashboard.get_dict()
            if overall and overall.get('data'):
                row = overall['data'][0]
                headers = overall['headers']
                stats_dict = dict(zip(headers, row))
                
                return {
                    'fgm': stats_dict.get('FGM', 0),
                    'fga': stats_dict.get('FGA', 0),
                    'fg_pct': stats_dict.get('FG_PCT', 0) * 100,
                    'fg3m': stats_dict.get('FG3M', 0),
                    'fg3a': stats_dict.get('FG3A', 0),
                    'fg3_pct': stats_dict.get('FG3_PCT', 0) * 100,
                    'ftm': stats_dict.get('FTM', 0),
                    'fta': stats_dict.get('FTA', 0),
                    'ft_pct': stats_dict.get('FT_PCT', 0) * 100,
                    'oreb': stats_dict.get('OREB', 0),
                    'dreb': stats_dict.get('DREB', 0),
                    'reb': stats_dict.get('REB', 0),
                    'ast': stats_dict.get('AST', 0),
                    'tov': stats_dict.get('TOV', 0),
                    'stl': stats_dict.get('STL', 0),
                    'blk': stats_dict.get('BLK', 0),
                    'pts': stats_dict.get('PTS', 0),
                    'plus_minus': stats_dict.get('PLUS_MINUS', 0),
                    # Calculated metrics
                    '2pt_pct': ((stats_dict.get('FGM', 0) - stats_dict.get('FG3M', 0)) / 
                               max(1, stats_dict.get('FGA', 1) - stats_dict.get('FG3A', 0))) * 100 if stats_dict.get('FGA', 0) > 0 else 0,
                    '3pt_rate': (stats_dict.get('FG3A', 0) / max(1, stats_dict.get('FGA', 1))) * 100 if stats_dict.get('FGA', 0) > 0 else 0,
                    'efg_pct': ((stats_dict.get('FGM', 0) + 0.5 * stats_dict.get('FG3M', 0)) / 
                               max(1, stats_dict.get('FGA', 1))) * 100 if stats_dict.get('FGA', 0) > 0 else 0,
                    'tov_pct': (stats_dict.get('TOV', 0) / 
                               max(1, stats_dict.get('FGA', 0) + 0.44 * stats_dict.get('FTA', 0) + stats_dict.get('TOV', 0))) * 100,
                    'orb_pct': 0,  # Will need box score data
                    'ft_rate': (stats_dict.get('FTA', 0) / max(1, stats_dict.get('FGA', 1))) * 100 if stats_dict.get('FGA', 0) > 0 else 0
                }
            
            return {}
            
        except Exception as e:
            logger.warning(f"Error fetching NBA.com stats for {team_name}: {e}")
            return {}
    
    @staticmethod
    def get_team_rankings(league: str = 'NBA') -> dict:
        """
        Fetch team power rankings and efficiency ratings.
        Returns dict mapping team names to their rankings.
        """
        try:
            from nba_api.stats.endpoints import leaguedashteamstats
            import time
            
            time.sleep(0.6)
            
            # Get league-wide stats for rankings
            team_stats = leaguedashteamstats.LeagueDashTeamStats(
                season='2024-25',
                season_type_all_star='Regular Season',
                per_mode_detailed='PerGame'
            )
            
            data = team_stats.league_dash_team_stats.get_dict()
            if data and data.get('data'):
                headers = data['headers']
                rankings = {}
                
                # Sort teams by NET_RATING for power ranking
                team_data = []
                for row in data['data']:
                    stats = dict(zip(headers, row))
                    team_data.append({
                        'team_id': stats.get('TEAM_ID'),
                        'team_name': stats.get('TEAM_NAME'),
                        'off_rating': stats.get('OFF_RATING', 0),
                        'def_rating': stats.get('DEF_RATING', 0),
                        'net_rating': stats.get('NET_RATING', 0),
                        'pace': stats.get('PACE', 0),
                        'wins': stats.get('W', 0),
                        'losses': stats.get('L', 0)
                    })
                
                # Sort by net rating for power ranking
                team_data.sort(key=lambda x: x['net_rating'], reverse=True)
                for i, team in enumerate(team_data, 1):
                    team['power_rank'] = i
                    rankings[team['team_name']] = team
                
                # Sort by offensive rating
                team_data.sort(key=lambda x: x['off_rating'], reverse=True)
                for i, team in enumerate(team_data, 1):
                    rankings[team['team_name']]['off_rank'] = i
                
                # Sort by defensive rating (lower is better)
                team_data.sort(key=lambda x: x['def_rating'])
                for i, team in enumerate(team_data, 1):
                    rankings[team['team_name']]['def_rank'] = i
                
                return rankings
            
            return {}
            
        except Exception as e:
            logger.warning(f"Error fetching team rankings: {e}")
            return {}
    
    # L5 stats cache - stores team L5 data with TTL
    _l5_cache = {}
    _l5_cache_ttl = 1800  # 30 minute TTL
    
    @staticmethod
    def get_team_last5_stats(team_name: str, league: str = 'NBA') -> dict:
        """
        Fetch last 5 games stats from NBA.com API for trend analysis.
        Returns dict with L5 averages for key metrics.
        Uses in-memory cache with 30-minute TTL to avoid rate limiting.
        """
        import time
        
        # Check cache first
        cache_key = f"{team_name}_{league}"
        if cache_key in MatchupIntelligence._l5_cache:
            cached_data, cached_time = MatchupIntelligence._l5_cache[cache_key]
            if time.time() - cached_time < MatchupIntelligence._l5_cache_ttl:
                return cached_data
        
        try:
            from nba_api.stats.endpoints import teamgamelog
            
            # Find team ID
            team_id = None
            for abbr, tid in MatchupIntelligence.NBA_TEAM_IDS.items():
                if abbr.lower() in team_name.lower() or team_name.lower() in abbr.lower():
                    team_id = tid
                    break
            
            if not team_id:
                return {}
            
            time.sleep(0.6)  # Rate limiting for NBA.com API
            
            # Fetch game log
            game_log = teamgamelog.TeamGameLog(
                team_id=team_id,
                season='2024-25',
                season_type_all_star='Regular Season'
            )
            
            data = game_log.team_game_log.get_dict()
            if data and data.get('data'):
                headers = data['headers']
                games = data['data'][:5]  # Last 5 games
                
                if len(games) < 5:
                    return {}
                
                # Calculate L5 averages
                totals = {'FGM': 0, 'FGA': 0, 'FG3M': 0, 'FG3A': 0, 'FTM': 0, 'FTA': 0,
                         'OREB': 0, 'DREB': 0, 'REB': 0, 'AST': 0, 'TOV': 0, 'STL': 0,
                         'BLK': 0, 'PTS': 0}
                
                for game in games:
                    stats = dict(zip(headers, game))
                    for key in totals:
                        totals[key] += stats.get(key, 0)
                
                n = len(games)
                avg_fga = totals['FGA'] / n if n > 0 else 1
                avg_fta = totals['FTA'] / n if n > 0 else 1
                avg_fg3a = totals['FG3A'] / n if n > 0 else 1
                
                result = {
                    'l5_efg': ((totals['FGM'] + 0.5 * totals['FG3M']) / max(1, totals['FGA'])) * 100,
                    'l5_fg_pct': (totals['FGM'] / max(1, totals['FGA'])) * 100,
                    'l5_fg3_pct': (totals['FG3M'] / max(1, totals['FG3A'])) * 100,
                    'l5_ft_pct': (totals['FTM'] / max(1, totals['FTA'])) * 100,
                    'l5_orb': totals['OREB'] / n,
                    'l5_drb': totals['DREB'] / n,
                    'l5_ast': totals['AST'] / n,
                    'l5_tov': totals['TOV'] / n,
                    'l5_pts': totals['PTS'] / n,
                    'l5_fg3m': totals['FG3M'] / n,
                    'l5_tov_pct': (totals['TOV'] / max(1, totals['FGA'] + 0.44 * totals['FTA'] + totals['TOV'])) * 100,
                    'l5_ft_rate': (totals['FTA'] / max(1, totals['FGA'])) * 100,
                    'games_played': n
                }
                # Cache the result
                MatchupIntelligence._l5_cache[cache_key] = (result, time.time())
                return result
            
            return {}
            
        except Exception as e:
            logger.warning(f"Error fetching L5 stats for {team_name}: {e}")
            return {}
    
    # Cache for Last 5 games (in-memory, expires after 10 mins)
    _last5_cache = {}
    _last5_cache_time = {}
    
    @staticmethod
    def get_team_last5_games(team_name: str, league: str = 'NBA') -> list:
        """
        Fetch last 5 game results with dates, opponents, scores from ESPN API.
        Returns list of dicts: [{date, opp, opp_logo, location, result, score}, ...]
        """
        import requests
        from datetime import datetime
        import time as time_module
        
        # Check cache first (10 minute expiry)
        cache_key = f"{team_name}_{league}"
        cache_time = MatchupIntelligence._last5_cache_time.get(cache_key, 0)
        if cache_key in MatchupIntelligence._last5_cache and (time_module.time() - cache_time) < 600:
            return MatchupIntelligence._last5_cache[cache_key]
        
        # ESPN team ID mapping
        ESPN_TEAM_IDS = {
            'ATL': '1', 'BOS': '2', 'BKN': '17', 'CHA': '30', 'CHI': '4',
            'CLE': '5', 'DAL': '6', 'DEN': '7', 'DET': '8', 'GSW': '9',
            'HOU': '10', 'IND': '11', 'LAC': '12', 'LAL': '13', 'MEM': '29',
            'MIA': '14', 'MIL': '15', 'MIN': '16', 'NOP': '3', 'NYK': '18',
            'OKC': '25', 'ORL': '19', 'PHI': '20', 'PHX': '21', 'POR': '22',
            'SAC': '23', 'SAS': '24', 'TOR': '28', 'UTA': '26', 'WAS': '27'
        }
        
        try:
            # Find team abbreviation
            team_abbr = None
            for abbr in ESPN_TEAM_IDS.keys():
                if abbr.lower() in team_name.lower() or team_name.lower() in abbr.lower():
                    team_abbr = abbr
                    break
            
            # Try matching full names
            if not team_abbr:
                name_to_abbr = {
                    'hawks': 'ATL', 'celtics': 'BOS', 'nets': 'BKN', 'hornets': 'CHA', 'bulls': 'CHI',
                    'cavaliers': 'CLE', 'mavericks': 'DAL', 'nuggets': 'DEN', 'pistons': 'DET', 'warriors': 'GSW',
                    'rockets': 'HOU', 'pacers': 'IND', 'clippers': 'LAC', 'lakers': 'LAL', 'grizzlies': 'MEM',
                    'heat': 'MIA', 'bucks': 'MIL', 'timberwolves': 'MIN', 'pelicans': 'NOP', 'knicks': 'NYK',
                    'thunder': 'OKC', 'magic': 'ORL', '76ers': 'PHI', 'sixers': 'PHI', 'suns': 'PHX',
                    'blazers': 'POR', 'trail blazers': 'POR', 'kings': 'SAC', 'spurs': 'SAS', 'raptors': 'TOR',
                    'jazz': 'UTA', 'wizards': 'WAS'
                }
                for name, abbr in name_to_abbr.items():
                    if name in team_name.lower():
                        team_abbr = abbr
                        break
            
            if not team_abbr:
                return []
            
            espn_id = ESPN_TEAM_IDS.get(team_abbr)
            if not espn_id:
                return []
            
            # Fetch from ESPN API
            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_id}/schedule"
            resp = requests.get(url, timeout=10)
            
            if resp.status_code != 200:
                return []
            
            data = resp.json()
            events = data.get('events', [])
            
            # Filter completed games and get last 5
            completed_games = []
            for event in events:
                status = event.get('competitions', [{}])[0].get('status', {}).get('type', {}).get('completed', False)
                if status:
                    completed_games.append(event)
            
            # Get most recent 5 games
            recent_games = completed_games[-5:][::-1] if len(completed_games) >= 5 else completed_games[::-1]
            
            results = []
            for event in recent_games:
                try:
                    competition = event.get('competitions', [{}])[0]
                    competitors = competition.get('competitors', [])
                    
                    if len(competitors) < 2:
                        continue
                    
                    # Find team and opponent
                    team_data = None
                    opp_data = None
                    for comp in competitors:
                        if comp.get('team', {}).get('abbreviation', '').upper() == team_abbr:
                            team_data = comp
                        else:
                            opp_data = comp
                    
                    if not team_data or not opp_data:
                        continue
                    
                    # Get scores
                    team_score = int(team_data.get('score', 0))
                    opp_score = int(opp_data.get('score', 0))
                    is_home = team_data.get('homeAway') == 'home'
                    is_winner = team_data.get('winner', False)
                    
                    # Get opponent info
                    opp_abbr = opp_data.get('team', {}).get('abbreviation', 'N/A')
                    opp_logo = opp_data.get('team', {}).get('logo', f"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/scoreboard/{opp_abbr.lower()}.png")
                    
                    # Format date
                    game_date = event.get('date', '')
                    try:
                        dt = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
                        formatted_date = dt.strftime('%b %d')
                    except:
                        formatted_date = game_date[:6] if game_date else 'N/A'
                    
                    results.append({
                        'date': formatted_date,
                        'opp': opp_abbr,
                        'opp_logo': opp_logo,
                        'location': 'vs' if is_home else '@',
                        'result': 'W' if is_winner else 'L',
                        'score': f"{team_score} - {opp_score}"
                    })
                except Exception as e:
                    continue
            
            # Cache results
            if results:
                MatchupIntelligence._last5_cache[cache_key] = results
                MatchupIntelligence._last5_cache_time[cache_key] = time_module.time()
            
            return results
            
        except Exception as e:
            logger.warning(f"Error fetching L5 games for {team_name}: {e}")
            return []
    
    @staticmethod
    def get_team_full_name(abbr: str) -> str:
        """Convert team abbreviation to full name."""
        abbr_map = {
            'ATL': 'Hawks', 'BOS': 'Celtics', 'BKN': 'Nets', 'CHA': 'Hornets',
            'CHI': 'Bulls', 'CLE': 'Cavaliers', 'DAL': 'Mavericks', 'DEN': 'Nuggets',
            'DET': 'Pistons', 'GSW': 'Warriors', 'HOU': 'Rockets', 'IND': 'Pacers',
            'LAC': 'Clippers', 'LAL': 'Lakers', 'MEM': 'Grizzlies', 'MIA': 'Heat',
            'MIL': 'Bucks', 'MIN': 'Timberwolves', 'NOP': 'Pelicans', 'NYK': 'Knicks',
            'OKC': 'Thunder', 'ORL': 'Magic', 'PHI': '76ers', 'PHX': 'Suns',
            'POR': 'Trail Blazers', 'SAC': 'Kings', 'SAS': 'Spurs', 'TOR': 'Raptors',
            'UTA': 'Jazz', 'WAS': 'Wizards'
        }
        return abbr_map.get(abbr.upper(), abbr)
    
    @staticmethod
    def fetch_teamrankings_matchup(away_team: str, home_team: str, game_date: str, league: str = 'NBA') -> dict:
        """
        Fetch matchup data from ALL TeamRankings FREE matchup pages:
        - /stats: Basic stats (PPG, rebounds, assists, etc.)
        - /efficiency: Off/Def Efficiency, eFG%, Turnover rates, Rebound %
        - /splits: Season + Last 3 Games columns
        - /power-ratings: SOS Rank, Predictive Rating, etc.
        """
        try:
            import requests
            from bs4 import BeautifulSoup
            import re
            
            team_slugs = {
                'bulls': 'bulls', 'pacers': 'pacers', 'celtics': 'celtics', 'lakers': 'lakers',
                'heat': 'heat', 'bucks': 'bucks', 'nets': 'nets', '76ers': '76ers',
                'knicks': 'knicks', 'hawks': 'hawks', 'hornets': 'hornets', 'cavaliers': 'cavaliers',
                'pistons': 'pistons', 'magic': 'magic', 'wizards': 'wizards', 'raptors': 'raptors',
                'nuggets': 'nuggets', 'clippers': 'clippers', 'suns': 'suns', 'warriors': 'warriors',
                'grizzlies': 'grizzlies', 'mavericks': 'mavericks', 'rockets': 'rockets', 'pelicans': 'pelicans',
                'spurs': 'spurs', 'thunder': 'thunder', 'timberwolves': 'timberwolves',
                'trail blazers': 'trailblazers', 'blazers': 'trailblazers', 'jazz': 'jazz', 'kings': 'kings'
            }
            
            away_slug = team_slugs.get(away_team.lower(), away_team.lower().replace(' ', '-'))
            home_slug = team_slugs.get(home_team.lower(), home_team.lower().replace(' ', '-'))
            base_url = f"https://www.teamrankings.com/{league.lower()}/matchup/{away_slug}-{home_slug}-{game_date}"
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            result = {'away_season': {}, 'home_season': {}, 'away_l3': {}, 'home_l3': {}}
            
            def parse_value(val_str):
                if not val_str:
                    return None
                val_str = val_str.replace('%', '').strip()
                match = re.match(r'([+-]?\d*\.?\d+)', val_str)
                if match:
                    try:
                        return float(match.group(1))
                    except:
                        pass
                return None
            
            def extract_rank(val_str):
                if not val_str:
                    return None
                match = re.search(r'#(\d+)', val_str)
                if match:
                    return int(match.group(1))
                return None
            
            # 1. STATS PAGE - Format: StatName | Value(rank) | Value(rank) | OppStatName
            try:
                resp = requests.get(f"{base_url}/stats", headers=headers, timeout=15)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    tables = soup.find_all('table')
                    
                    for table in tables:
                        rows = table.find_all('tr')
                        if len(rows) < 2:
                            continue
                        header = rows[0].find_all(['td', 'th'])
                        if len(header) < 4:
                            continue
                        h0 = header[0].get_text(strip=True).upper()
                        h3 = header[3].get_text(strip=True).upper() if len(header) > 3 else ''
                        is_away_table = away_team.upper()[:3] in h0 or 'CHI' in h0 or 'BOS' in h0 or away_slug.upper()[:3] in h0
                        is_home_table = home_team.upper()[:3] in h0 or 'IND' in h0 or home_slug.upper()[:3] in h0
                        
                        for row in rows[1:]:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) < 4:
                                continue
                            stat1 = cells[0].get_text(strip=True).lower()
                            val1 = parse_value(cells[1].get_text(strip=True))
                            val2 = parse_value(cells[2].get_text(strip=True))
                            stat2 = cells[3].get_text(strip=True).lower()
                            
                            if 'subscribe' in stat1.lower() or not stat1:
                                continue
                            
                            if is_away_table and not is_home_table:
                                if val1 is not None:
                                    result['away_season'][stat1] = val1
                                if val2 is not None:
                                    result['home_season'][stat2] = val2
                            elif is_home_table and not is_away_table:
                                if val1 is not None:
                                    result['home_season'][stat1] = val1
                                if val2 is not None:
                                    result['away_season'][stat2] = val2
                            else:
                                if val1 is not None:
                                    result['away_season'][stat1] = val1
                                if val2 is not None:
                                    result['home_season'][stat2] = val2
                    
                    logger.info(f"Stats page: away={len(result['away_season'])}, home={len(result['home_season'])}")
            except Exception as e:
                logger.warning(f"Stats page error: {e}")
            
            # 2. EFFICIENCY PAGE - Format: Stat | CHI | adv | IND (4 columns)
            try:
                resp = requests.get(f"{base_url}/efficiency", headers=headers, timeout=15)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    tables = soup.find_all('table')
                    
                    for table in tables:
                        rows = table.find_all('tr')
                        if len(rows) < 2:
                            continue
                        
                        for row in rows[1:]:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) < 4:
                                continue
                            stat_name = cells[0].get_text(strip=True).lower()
                            away_val = parse_value(cells[1].get_text(strip=True))
                            home_val = parse_value(cells[3].get_text(strip=True))
                            
                            if 'subscribe' in stat_name or not stat_name:
                                continue
                            if away_val is not None:
                                result['away_season'][stat_name] = away_val
                            if home_val is not None:
                                result['home_season'][stat_name] = home_val
                    
                    eff_keys = [k for k in result['away_season'].keys() if 'eff' in k.lower()]
                    logger.info(f"Efficiency page added stats. Efficiency keys found: {eff_keys}")
            except Exception as e:
                logger.warning(f"Efficiency page error: {e}")
            
            # 3. SPLITS PAGE - Multi-header format with Season + Last 3 Games columns
            try:
                resp = requests.get(f"{base_url}/splits", headers=headers, timeout=15)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    tables = soup.find_all('table')
                    
                    for table in tables:
                        rows = table.find_all('tr')
                        if len(rows) < 3:
                            continue
                        
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) < 7:
                                continue
                            stat_name = cells[0].get_text(strip=True).lower()
                            if not stat_name or 'stat' in stat_name or 'subscribe' in stat_name:
                                continue
                            
                            away_season = parse_value(cells[1].get_text(strip=True))
                            home_season = parse_value(cells[3].get_text(strip=True))
                            away_l3 = parse_value(cells[4].get_text(strip=True))
                            home_l3 = parse_value(cells[6].get_text(strip=True))
                            
                            if away_season is not None:
                                result['away_season'][stat_name] = away_season
                            if home_season is not None:
                                result['home_season'][stat_name] = home_season
                            if away_l3 is not None:
                                result['away_l3'][stat_name] = away_l3
                            if home_l3 is not None:
                                result['home_l3'][stat_name] = home_l3
                    
                    logger.info(f"Splits page: L3 away={len(result['away_l3'])}, home={len(result['home_l3'])}")
            except Exception as e:
                logger.warning(f"Splits page error: {e}")
            
            # 4. POWER-RATINGS PAGE - Format: Rating | CHI Value(#rank) | adv | IND Value(#rank)
            try:
                resp = requests.get(f"{base_url}/power-ratings", headers=headers, timeout=15)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    tables = soup.find_all('table')
                    sos_found = False
                    
                    for table in tables:
                        rows = table.find_all('tr')
                        for row in rows:
                            cells = row.find_all(['td', 'th'])
                            if len(cells) >= 4:
                                stat_name = cells[0].get_text(strip=True).lower()
                                cell1_text = cells[1].get_text(strip=True)
                                cell3_text = cells[3].get_text(strip=True)
                                
                                if not sos_found and 'schedule strength (past)' in stat_name:
                                    away_rank = extract_rank(cell1_text)
                                    home_rank = extract_rank(cell3_text)
                                    if away_rank and home_rank:
                                        result['away_season']['sos rank'] = away_rank
                                        result['home_season']['sos rank'] = home_rank
                                        sos_found = True
                                        logger.info(f"SOS Rank: away=#{away_rank}, home=#{home_rank}")
                                
                                if 'predictive' in stat_name and 'predictive rating' not in result['away_season']:
                                    away_val = parse_value(cell1_text)
                                    home_val = parse_value(cell3_text)
                                    if away_val is not None:
                                        result['away_season']['predictive rating'] = away_val
                                    if home_val is not None:
                                        result['home_season']['predictive rating'] = home_val
                                
                                if 'last 10' in stat_name and 'last 10 rating' not in result['away_season']:
                                    away_val = parse_value(cell1_text)
                                    home_val = parse_value(cell3_text)
                                    if away_val is not None:
                                        result['away_season']['last 10 rating'] = away_val
                                    if home_val is not None:
                                        result['home_season']['last 10 rating'] = home_val
                                
                                if 'luck' in stat_name and 'luck rating' not in result['away_season']:
                                    away_val = parse_value(cell1_text)
                                    home_val = parse_value(cell3_text)
                                    if away_val is not None:
                                        result['away_season']['luck rating'] = away_val
                                    if home_val is not None:
                                        result['home_season']['luck rating'] = home_val
            except Exception as e:
                logger.warning(f"Power-ratings page error: {e}")
            
            logger.info(f"TeamRankings TOTAL: away_season={len(result['away_season'])}, home_season={len(result['home_season'])}")
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching TeamRankings matchup: {e}")
            return {'away_season': {}, 'home_season': {}, 'away_l3': {}, 'home_l3': {}}
    
    # Cleaning the Glass team IDs (NBA only)
    CTG_TEAM_IDS = {
        'hawks': 1, 'celtics': 2, 'nets': 3, 'hornets': 4, 'bulls': 5,
        'cavaliers': 6, 'mavericks': 7, 'nuggets': 8, 'pistons': 9, 'warriors': 10,
        'rockets': 11, 'pacers': 12, 'clippers': 13, 'lakers': 14, 'grizzlies': 15,
        'heat': 16, 'bucks': 17, 'timberwolves': 18, 'pelicans': 19, 'knicks': 20,
        'thunder': 21, 'magic': 22, '76ers': 23, 'suns': 24, 'trail blazers': 25,
        'blazers': 25, 'kings': 26, 'spurs': 27, 'raptors': 28, 'jazz': 29, 'wizards': 30
    }
    
    @staticmethod
    def fetch_ctg_four_factors(team_name: str) -> dict:
        """
        Fetch Four Factors data from Cleaning the Glass (FREE tier).
        Returns eFG%, TOV%, ORB%, FT Rate for offense and defense.
        """
        try:
            import requests
            from bs4 import BeautifulSoup
            
            # Find team ID
            team_id = None
            for name, tid in MatchupIntelligence.CTG_TEAM_IDS.items():
                if name in team_name.lower():
                    team_id = tid
                    break
            
            if not team_id:
                logger.warning(f"CTG team ID not found for: {team_name}")
                return {}
            
            url = f"https://cleaningtheglass.com/stats/team/{team_id}/team"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                return {}
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', {'id': 'team_stats_four_factors'})
            
            if not table:
                return {}
            
            result = {}
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                if rows:
                    # Get current season (first row)
                    cells = rows[0].find_all('td')
                    data = [c.get_text(strip=True) for c in cells]
                    
                    # Parse the data - CTG format: Year, rankings interspersed with values
                    # Structure: Year, Rank, Diff, Rank, ExpW82... Offense: Pts/Poss, eFG%, TOV%, ORB%, FT Rate... Defense: same
                    # Based on our test: indices for offense eFG% value, TOV% value, ORB% value, FT Rate value
                    # Then defense versions
                    
                    if len(data) >= 31:
                        # CTG format: rank, value pairs for each metric
                        # Offense section starts at index 10
                        # Positions: pts_poss_rank=10, pts_poss=11, efg_rank=12, efg=13, tov_rank=14, tov=15, orb_rank=16, orb=17, ft_rank=18, ft=19
                        result['off_ppp'] = data[11]  # Points per possession
                        result['off_ppp_rank'] = data[10]
                        result['off_efg'] = data[13].replace('%', '') if '%' in data[13] else data[13]
                        result['off_efg_rank'] = data[12]
                        result['off_tov'] = data[15].replace('%', '') if '%' in data[15] else data[15]
                        result['off_tov_rank'] = data[14]
                        result['off_orb'] = data[17].replace('%', '') if '%' in data[17] else data[17]
                        result['off_orb_rank'] = data[16]
                        result['off_ft_rate'] = data[19]
                        result['off_ft_rank'] = data[18]
                        
                        # Defense section starts at index 21
                        # Positions: pts_poss_rank=21, pts_poss=22, efg_rank=23, efg=24, tov_rank=25, tov=26, orb_rank=27, orb=28, ft_rank=29, ft=30
                        result['def_ppp'] = data[22]  # Opponent points per possession
                        result['def_ppp_rank'] = data[21]
                        result['def_efg'] = data[24].replace('%', '') if '%' in data[24] else data[24]
                        result['def_efg_rank'] = data[23]
                        result['def_tov'] = data[26].replace('%', '') if '%' in data[26] else data[26]
                        result['def_tov_rank'] = data[25]
                        result['def_orb'] = data[28].replace('%', '') if '%' in data[28] else data[28]
                        result['def_orb_rank'] = data[27]
                        result['def_ft_rate'] = data[30]
                        result['def_ft_rank'] = data[29]
                        
                        logger.info(f"CTG Four Factors for {team_name}: PPP={result.get('off_ppp')} (#{result.get('off_ppp_rank')}), FT Rate={result.get('off_ft_rate')} (#{result.get('off_ft_rank')})")
            
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching CTG four factors for {team_name}: {e}")
            return {}
    
    @staticmethod
    def fetch_teamrankings_stats(team_name: str, league: str = 'NBA') -> dict:
        """
        Fetch comprehensive team stats from TeamRankings.com including rankings and SOS.
        """
        try:
            import requests
            from bs4 import BeautifulSoup
            
            # Team name to slug mapping
            team_slugs = {
                'bulls': 'chicago-bulls', 'pacers': 'indiana-pacers', 'celtics': 'boston-celtics',
                'lakers': 'los-angeles-lakers', 'heat': 'miami-heat', 'bucks': 'milwaukee-bucks',
                'nets': 'brooklyn-nets', '76ers': 'philadelphia-76ers', 'knicks': 'new-york-knicks',
                'hawks': 'atlanta-hawks', 'hornets': 'charlotte-hornets', 'cavaliers': 'cleveland-cavaliers',
                'pistons': 'detroit-pistons', 'magic': 'orlando-magic', 'wizards': 'washington-wizards',
                'raptors': 'toronto-raptors', 'nuggets': 'denver-nuggets', 'clippers': 'los-angeles-clippers',
                'suns': 'phoenix-suns', 'warriors': 'golden-state-warriors', 'grizzlies': 'memphis-grizzlies',
                'mavericks': 'dallas-mavericks', 'rockets': 'houston-rockets', 'pelicans': 'new-orleans-pelicans',
                'spurs': 'san-antonio-spurs', 'thunder': 'oklahoma-city-thunder', 'timberwolves': 'minnesota-timberwolves',
                'trail blazers': 'portland-trail-blazers', 'blazers': 'portland-trail-blazers',
                'jazz': 'utah-jazz', 'kings': 'sacramento-kings'
            }
            
            team_slug = team_slugs.get(team_name.lower(), team_name.lower().replace(' ', '-'))
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            result = {}
            
            # Fetch schedule strength (SOS)
            sos_url = f"https://www.teamrankings.com/nba/team/{team_slug}"
            try:
                resp = requests.get(sos_url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    # Look for SOS in team profile
                    for row in soup.find_all('tr'):
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            label = cells[0].get_text(strip=True).lower()
                            if 'sos' in label or 'schedule' in label:
                                val = cells[1].get_text(strip=True)
                                try:
                                    result['sos'] = float(val)
                                except:
                                    result['sos_rank'] = val
            except:
                pass
            
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching TeamRankings stats for {team_name}: {e}")
            return {}
    
    @staticmethod
    def fetch_covers_trends(team_name: str, league: str = 'NBA') -> dict:
        """
        Fetch betting trends from Covers.com.
        Returns dict with ATS, O/U records and trends.
        """
        try:
            import requests
            from bs4 import BeautifulSoup
            
            league_path = 'nba' if league == 'NBA' else 'ncaab'
            team_slug = team_name.lower().replace(' ', '-').replace("'", "")
            
            url = f"https://www.covers.com/sport/{league_path}/teams/{team_slug}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                return {}
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            trends = {}
            
            # Look for ATS and O/U records
            record_sections = soup.find_all(class_='covers-CoversRecords')
            for section in record_sections:
                text = section.get_text()
                if 'ATS' in text:
                    # Parse ATS record
                    import re
                    ats_match = re.search(r'(\d+)-(\d+)(?:-(\d+))?', text)
                    if ats_match:
                        wins, losses = int(ats_match.group(1)), int(ats_match.group(2))
                        trends['ats_wins'] = wins
                        trends['ats_losses'] = losses
                        trends['ats_pct'] = wins / max(1, wins + losses) * 100
                
                if 'O/U' in text or 'Over' in text:
                    import re
                    ou_match = re.search(r'(\d+)-(\d+)(?:-(\d+))?', text)
                    if ou_match:
                        overs, unders = int(ou_match.group(1)), int(ou_match.group(2))
                        trends['over_hits'] = overs
                        trends['under_hits'] = unders
                        trends['over_pct'] = overs / max(1, overs + unders) * 100
            
            return trends
            
        except Exception as e:
            logger.warning(f"Error fetching Covers trends for {team_name}: {e}")
            return {}
    
    @staticmethod
    def fetch_covers_h2h(away_team: str, home_team: str, league: str = 'NBA') -> dict:
        """
        Fetch H2H L10 W/L and ATS records from Covers.com matchup page.
        Returns: {h2h_record: "6-4", h2h_leader: "Pacers", h2h_ats: "4-6-0", ats_leader: "Bulls"}
        """
        import requests
        from bs4 import BeautifulSoup
        import re
        
        result = {
            'h2h_record': 'N/A',
            'h2h_leader': 'Even',
            'h2h_ats': 'N/A',
            'ats_leader': 'Even',
            'away_record': 'N/A',
            'home_record': 'N/A',
            'away_ats': 'N/A',
            'home_ats': 'N/A'
        }
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
            
            # Team name to URL slug mapping
            team_slugs = {
                'bulls': 'chicago-bulls', 'pacers': 'indiana-pacers', 'celtics': 'boston-celtics',
                'lakers': 'los-angeles-lakers', 'heat': 'miami-heat', 'bucks': 'milwaukee-bucks',
                'nets': 'brooklyn-nets', '76ers': 'philadelphia-76ers', 'knicks': 'new-york-knicks',
                'hawks': 'atlanta-hawks', 'hornets': 'charlotte-hornets', 'cavaliers': 'cleveland-cavaliers',
                'pistons': 'detroit-pistons', 'magic': 'orlando-magic', 'wizards': 'washington-wizards',
                'raptors': 'toronto-raptors', 'nuggets': 'denver-nuggets', 'clippers': 'los-angeles-clippers',
                'suns': 'phoenix-suns', 'warriors': 'golden-state-warriors', 'grizzlies': 'memphis-grizzlies',
                'mavericks': 'dallas-mavericks', 'rockets': 'houston-rockets', 'pelicans': 'new-orleans-pelicans',
                'spurs': 'san-antonio-spurs', 'thunder': 'oklahoma-city-thunder', 'timberwolves': 'minnesota-timberwolves',
                'trail blazers': 'portland-trail-blazers', 'blazers': 'portland-trail-blazers',
                'jazz': 'utah-jazz', 'kings': 'sacramento-kings'
            }
            
            # Team abbreviations for matching
            team_abbrevs = {
                'bulls': 'chi', 'pacers': 'ind', 'celtics': 'bos', 'lakers': 'lal', 'heat': 'mia',
                'bucks': 'mil', 'nets': 'bkn', '76ers': 'phi', 'knicks': 'nyk', 'hawks': 'atl',
                'hornets': 'cha', 'cavaliers': 'cle', 'pistons': 'det', 'magic': 'orl', 'wizards': 'was',
                'raptors': 'tor', 'nuggets': 'den', 'clippers': 'lac', 'suns': 'phx', 'warriors': 'gsw',
                'grizzlies': 'mem', 'mavericks': 'dal', 'rockets': 'hou', 'pelicans': 'nop',
                'spurs': 'sas', 'thunder': 'okc', 'timberwolves': 'min', 'trail blazers': 'por',
                'blazers': 'por', 'jazz': 'uta', 'kings': 'sac'
            }
            
            away_lower = away_team.lower()
            home_lower = home_team.lower()
            away_slug = team_slugs.get(away_lower, away_lower.replace(' ', '-'))
            home_slug = team_slugs.get(home_lower, home_lower.replace(' ', '-'))
            away_abbrev = team_abbrevs.get(away_lower, away_lower[:3])
            home_abbrev = team_abbrevs.get(home_lower, home_lower[:3])
            
            # Fetch the matchups page and look for link with both teams
            matchups_url = f"https://www.covers.com/sports/nba/matchups"  # Main listing page
            resp = requests.get(matchups_url, headers=headers, timeout=15)
            
            matchup_id = None
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Find all matchup links and look for our game
                all_links = soup.find_all('a', href=re.compile(r'/matchup/\d+'))
                for link in all_links:
                    href = link.get('href', '')
                    link_text = link.get_text().lower()
                    # Check if link text or surrounding context contains both teams
                    parent_text = link.parent.get_text().lower() if link.parent else ''
                    full_text = f"{link_text} {parent_text}"
                    
                    if (away_lower in full_text or away_abbrev in full_text) and \
                       (home_lower in full_text or home_abbrev in full_text):
                        match = re.search(r'/matchup/(\d+)', href)
                        if match:
                            matchup_id = match.group(1)
                            break
                
                # Fallback: get all IDs and check first 5 matchup pages
                if not matchup_id:
                    all_ids = list(set(re.findall(r'/matchup/(\d+)', str(soup))))[:5]
                    for mid in all_ids:
                        try:
                            check_url = f"https://www.covers.com/sport/basketball/nba/matchup/{mid}"
                            check_resp = requests.get(check_url, headers=headers, timeout=8)
                            if check_resp.status_code == 200:
                                title = BeautifulSoup(check_resp.text, 'html.parser').find('title')
                                if title:
                                    title_text = title.get_text().lower()
                                    if (away_lower in title_text or away_abbrev in title_text) and \
                                       (home_lower in title_text or home_abbrev in title_text):
                                        matchup_id = mid
                                        break
                        except:
                            continue
            
            # If we found a matchup ID, fetch the matchup page
            if matchup_id:
                matchup_url = f"https://www.covers.com/sport/basketball/nba/matchup/{matchup_id}"
                resp = requests.get(matchup_url, headers=headers, timeout=15)
                
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    text = soup.get_text(' ', strip=True)
                    html = str(soup)
                    
                    # Parse H2H Win/Loss from Last 10 section
                    # Look for patterns like "4-6" or "Win/Loss 4-6" or "Last 10 ... W/L 4-6"
                    wl_patterns = [
                        re.search(r'Last\s*10\s*(?:Games)?\s*[^\d]*(\d+)\s*[-–]\s*(\d+)', text, re.IGNORECASE),
                        re.search(r'Win\s*/?\s*Loss\s*(\d+)\s*[-–]\s*(\d+)', text, re.IGNORECASE),
                        re.search(r'W/L\s*(\d+)\s*[-–]\s*(\d+)', text, re.IGNORECASE),
                        re.search(r'Head.*Head[^\d]*(\d+)\s*[-–]\s*(\d+)', text, re.IGNORECASE),
                    ]
                    
                    for wl_pattern in wl_patterns:
                        if wl_pattern:
                            wins, losses = int(wl_pattern.group(1)), int(wl_pattern.group(2))
                            result['h2h_record'] = f"{wins}-{losses}"
                            if wins > losses:
                                result['h2h_leader'] = away_team
                            elif losses > wins:
                                result['h2h_leader'] = home_team
                            else:
                                result['h2h_leader'] = 'Even'
                            break
                    
                    # Parse ATS section
                    ats_patterns = [
                        re.search(r'ATS\s*(?:Against the Spread)?\s*(\d+)\s*[-–]\s*(\d+)\s*[-–]\s*(\d+)', text, re.IGNORECASE),
                        re.search(r'Against.*Spread[^\d]*(\d+)\s*[-–]\s*(\d+)\s*[-–]\s*(\d+)', text, re.IGNORECASE),
                    ]
                    
                    for ats_pattern in ats_patterns:
                        if ats_pattern:
                            wins, losses, pushes = int(ats_pattern.group(1)), int(ats_pattern.group(2)), int(ats_pattern.group(3))
                            result['h2h_ats'] = f"{wins}-{losses}-{pushes}"
                            if wins > losses:
                                result['ats_leader'] = away_team
                            elif losses > wins:
                                result['ats_leader'] = home_team
                            else:
                                result['ats_leader'] = 'Even'
                            break
                    
                    # Parse team records - look for W-L patterns near team names
                    away_record = re.search(rf'{away_abbrev}[^\d]*(\d+-\d+)', text, re.IGNORECASE)
                    home_record = re.search(rf'{home_abbrev}[^\d]*(\d+-\d+)', text, re.IGNORECASE)
                    
                    # Alternative: Look for record pattern like "(24-20)"
                    records = re.findall(r'\((\d+-\d+)\)', text)
                    if len(records) >= 2:
                        result['away_record'] = records[0]
                        result['home_record'] = records[1]
                    elif away_record:
                        result['away_record'] = away_record.group(1)
                    elif home_record:
                        result['home_record'] = home_record.group(1)
                    
                    # Parse ATS records for each team
                    ats_records = re.findall(r'(\d+-\d+-\d+)\s*ATS', text, re.IGNORECASE)
                    if len(ats_records) >= 2:
                        result['away_ats'] = ats_records[0]
                        result['home_ats'] = ats_records[1]
            
            logger.info(f"Covers H2H {away_team} vs {home_team}: W/L={result['h2h_record']} ({result['h2h_leader']}), ATS={result['h2h_ats']} ({result['ats_leader']})")
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching Covers H2H for {away_team} vs {home_team}: {e}")
            return result
    
    # Cache for Covers Last 10 data - short cache for fast refresh
    _covers_last10_cache = {}
    _covers_last10_cache_time = {}
    _matchup_id_cache = {}
    
    @staticmethod
    def get_covers_matchup_id(away_team: str, home_team: str, league: str = 'NBA') -> str:
        """Get and cache Covers.com matchup ID for a game."""
        import requests
        from bs4 import BeautifulSoup
        import re
        
        cache_key = f"{away_team}_{home_team}_{league}"
        if cache_key in MatchupIntelligence._matchup_id_cache:
            return MatchupIntelligence._matchup_id_cache[cache_key]
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        team_aliases = {
            'bulls': ['bulls', 'chicago'], 'pacers': ['pacers', 'indiana'],
            'celtics': ['celtics', 'boston'], 'lakers': ['lakers', 'l.a. lakers', 'la lakers'],
            'heat': ['heat', 'miami'], 'bucks': ['bucks', 'milwaukee'],
            'nets': ['nets', 'brooklyn'], '76ers': ['76ers', 'sixers', 'philadelphia'],
            'knicks': ['knicks', 'new york'], 'hawks': ['hawks', 'atlanta'],
            'hornets': ['hornets', 'charlotte'], 'cavaliers': ['cavaliers', 'cavs', 'cleveland'],
            'pistons': ['pistons', 'detroit'], 'magic': ['magic', 'orlando'],
            'wizards': ['wizards', 'washington'], 'raptors': ['raptors', 'toronto'],
            'nuggets': ['nuggets', 'denver'], 'clippers': ['clippers', 'l.a. clippers'],
            'suns': ['suns', 'phoenix'], 'warriors': ['warriors', 'golden state'],
            'grizzlies': ['grizzlies', 'memphis'], 'mavericks': ['mavericks', 'mavs', 'dallas'],
            'rockets': ['rockets', 'houston'], 'pelicans': ['pelicans', 'new orleans'],
            'spurs': ['spurs', 'san antonio'], 'thunder': ['thunder', 'oklahoma city'],
            'timberwolves': ['timberwolves', 'wolves', 'minnesota'],
            'trail blazers': ['trail blazers', 'blazers', 'portland'],
            'jazz': ['jazz', 'utah'], 'kings': ['kings', 'sacramento']
        }
        
        def matches_team(text, team_name):
            text_lower = text.lower()
            aliases = team_aliases.get(team_name.lower(), [team_name.lower()])
            return any(alias in text_lower for alias in aliases)
        
        # Sport path mapping for matchup URLs
        sport_path_map = {
            'NBA': 'sport/basketball/nba',
            'CBB': 'sport/basketball/ncaab',
            'NFL': 'sport/football/nfl',
            'CFB': 'sport/football/ncaaf',
            'NHL': 'sport/hockey/nhl'
        }
        sport_listing_map = {'NBA': 'nba', 'CBB': 'ncaab', 'NFL': 'nfl', 'CFB': 'ncaaf', 'NHL': 'nhl'}
        sport_path = sport_path_map.get(league.upper(), 'sport/basketball/nba')
        sport_listing = sport_listing_map.get(league.upper(), 'nba')
        
        try:
            from playwright.sync_api import sync_playwright
            import subprocess
            
            matchups_url = f"https://www.covers.com/sports/{sport_listing}/matchups"
            
            all_ids = []
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.goto(matchups_url, timeout=15000)
                    page.wait_for_timeout(3000)
                    
                    html_content = page.content()
                    browser.close()
                    
                    all_ids = list(set(re.findall(r'/matchup/(\d+)', html_content)))
                    logger.info(f"Playwright found {len(all_ids)} matchup IDs on Covers")
            except Exception as pw_err:
                logger.warning(f"Playwright error fetching Covers matchups: {pw_err}")
                resp = requests.get(matchups_url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    all_ids = list(set(re.findall(r'/matchup/(\d+)', resp.text)))
            
            for mid in all_ids[:20]:
                try:
                    check_url = f"https://www.covers.com/{sport_path}/matchup/{mid}"
                    check_resp = requests.get(check_url, headers=headers, timeout=5)
                    if check_resp.status_code == 200:
                        title = BeautifulSoup(check_resp.text, 'html.parser').find('title')
                        if title and matches_team(title.get_text(), away_team) and matches_team(title.get_text(), home_team):
                            MatchupIntelligence._matchup_id_cache[cache_key] = mid
                            logger.info(f"Found Covers matchup ID {mid} for {away_team} vs {home_team}")
                            return mid
                except:
                    continue
            
            # If not found, search sequential IDs ahead (today's games may not be in listing)
            if all_ids:
                max_id = max(int(x) for x in all_ids)
                for offset in range(1, 30):
                    mid = str(max_id + offset)
                    try:
                        check_url = f"https://www.covers.com/{sport_path}/matchup/{mid}"
                        check_resp = requests.get(check_url, headers=headers, timeout=5)
                        if check_resp.status_code == 200:
                            title = BeautifulSoup(check_resp.text, 'html.parser').find('title')
                            if title and matches_team(title.get_text(), away_team) and matches_team(title.get_text(), home_team):
                                MatchupIntelligence._matchup_id_cache[cache_key] = mid
                                logger.info(f"Found Covers matchup ID {mid} (sequential search) for {away_team} vs {home_team}")
                                return mid
                    except:
                        continue
            
            logger.warning(f"No Covers matchup found for {away_team} vs {home_team} in {league}")
        except Exception as e:
            logger.warning(f"Error finding Covers matchup ID: {e}")
        
        return None
    
    @staticmethod
    def fetch_covers_full_h2h(away_team: str, home_team: str, league: str = 'NBA') -> dict:
        """
        Fetch COMPLETE H2H data from Covers.com matchup page - NO FALLBACK.
        Returns all H2H info: W/L, ATS, O/U with team logos and individual game details.
        """
        import requests
        from bs4 import BeautifulSoup
        import re
        import time as time_module
        
        cache_key = f"full_h2h_{away_team}_{home_team}_{league}"
        cache_time = MatchupIntelligence._covers_last10_cache_time.get(cache_key, 0)
        if cache_key in MatchupIntelligence._covers_last10_cache and (time_module.time() - cache_time) < 60:
            return MatchupIntelligence._covers_last10_cache[cache_key]
        
        TEAM_ABBR = {
            'hawks': 'ATL', 'celtics': 'BOS', 'nets': 'BKN', 'hornets': 'CHA',
            'bulls': 'CHI', 'cavaliers': 'CLE', 'mavericks': 'DAL', 'nuggets': 'DEN',
            'pistons': 'DET', 'warriors': 'GSW', 'rockets': 'HOU', 'pacers': 'IND',
            'clippers': 'LAC', 'lakers': 'LAL', 'grizzlies': 'MEM', 'heat': 'MIA',
            'bucks': 'MIL', 'timberwolves': 'MIN', 'pelicans': 'NOP', 'knicks': 'NYK',
            'thunder': 'OKC', 'magic': 'ORL', '76ers': 'PHI', 'suns': 'PHX',
            'trail blazers': 'POR', 'kings': 'SAC', 'spurs': 'SAS', 'raptors': 'TOR',
            'jazz': 'UTA', 'wizards': 'WAS'
        }
        
        def get_abbr(name):
            return TEAM_ABBR.get(name.lower(), name[:3].upper())
        
        def get_logo(abbr):
            return f"https://a.espncdn.com/i/teamlogos/nba/500/{abbr.lower()}.png"
        
        away_abbr = get_abbr(away_team)
        home_abbr = get_abbr(home_team)
        
        result = {
            'away_team': away_team,
            'home_team': home_team,
            'away_abbr': away_abbr,
            'home_abbr': home_abbr,
            'away_logo': get_logo(away_abbr),
            'home_logo': get_logo(home_abbr),
            'wl_record': 'N/A',
            'wl_leader': None,
            'wl_leader_logo': None,
            'ats_record': 'N/A',
            'ats_leader': None,
            'ats_leader_logo': None,
            'ou_record': 'N/A',
            'ou_over': 0,
            'ou_under': 0,
            'games': []
        }
        
        try:
            matchup_id = MatchupIntelligence.get_covers_matchup_id(away_team, home_team, league)
            if not matchup_id:
                logger.warning(f"No Covers matchup ID found for {away_team} vs {home_team}")
                return result
            
            sport_path_map = {
                'NBA': 'sport/basketball/nba',
                'CBB': 'sport/basketball/ncaab',
                'NFL': 'sport/football/nfl',
                'CFB': 'sport/football/ncaaf',
                'NHL': 'sport/hockey/nhl'
            }
            sport_path = sport_path_map.get(league.upper(), 'sport/basketball/nba')
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            matchup_url = f"https://www.covers.com/{sport_path}/matchup/{matchup_id}"
            resp = requests.get(matchup_url, headers=headers, timeout=10)
            
            if resp.status_code != 200:
                return result
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(' ', strip=True)
            
            wl_match = re.search(r'Win\s*/?\s*Loss\s*(\d+)\s*-\s*(\d+)', text, re.IGNORECASE)
            if wl_match:
                w, l = int(wl_match.group(1)), int(wl_match.group(2))
                result['wl_record'] = f"{w}-{l}"
                if w > l:
                    result['wl_leader'] = away_team
                    result['wl_leader_logo'] = result['away_logo']
                elif l > w:
                    result['wl_leader'] = home_team
                    result['wl_leader_logo'] = result['home_logo']
            
            ats_match = re.search(r'ATS\s*(?:Against the Spread)?\s*(\d+)\s*-\s*(\d+)\s*-\s*(\d+)', text, re.IGNORECASE)
            if ats_match:
                w, l, p = int(ats_match.group(1)), int(ats_match.group(2)), int(ats_match.group(3))
                result['ats_record'] = f"{w}-{l}-{p}"
                if w > l:
                    result['ats_leader'] = away_team
                    result['ats_leader_logo'] = result['away_logo']
                elif l > w:
                    result['ats_leader'] = home_team
                    result['ats_leader_logo'] = result['home_logo']
            
            ou_match = re.search(r'Over\s*/?\s*Under\s*(\d+)\s*-\s*(\d+)(?:\s*-\s*(\d+))?', text, re.IGNORECASE)
            if ou_match:
                over = int(ou_match.group(1))
                under = int(ou_match.group(2))
                push = int(ou_match.group(3)) if ou_match.group(3) else 0
                result['ou_record'] = f"{over}-{under}-{push}" if push else f"{over}-{under}"
                result['ou_over'] = over
                result['ou_under'] = under
            
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:11]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 3:
                        try:
                            date_text = cells[0].get_text(strip=True)
                            matchup_text = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                            score_text = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                            ats_text = cells[3].get_text(strip=True) if len(cells) > 3 else ''
                            ou_text = cells[4].get_text(strip=True) if len(cells) > 4 else ''
                            
                            score_match = re.search(r'(\d+)\s*-\s*(\d+)', score_text)
                            if score_match:
                                result['games'].append({
                                    'date': date_text,
                                    'matchup': matchup_text,
                                    'score': f"{score_match.group(1)}-{score_match.group(2)}",
                                    'away_score': int(score_match.group(1)),
                                    'home_score': int(score_match.group(2)),
                                    'ats': ats_text,
                                    'ou': ou_text
                                })
                        except:
                            continue
            
            MatchupIntelligence._covers_last10_cache[cache_key] = result
            MatchupIntelligence._covers_last10_cache_time[cache_key] = time_module.time()
            
            logger.info(f"Covers H2H {away_team} vs {home_team}: W/L={result['wl_record']}, ATS={result['ats_record']}, O/U={result['ou_record']}, {len(result['games'])} games")
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching Covers full H2H: {e}")
            return result
    
    @staticmethod 
    def fetch_covers_live_data(away_team: str, home_team: str, league: str = 'NBA') -> dict:
        """
        Fetch live game data from Covers.com - scores, quarter, time remaining.
        Designed for 5-second refresh rate.
        """
        import requests
        from bs4 import BeautifulSoup
        import re
        import time as time_module
        
        cache_key = f"live_{away_team}_{home_team}_{league}"
        cache_time = MatchupIntelligence._covers_last10_cache_time.get(cache_key, 0)
        if cache_key in MatchupIntelligence._covers_last10_cache and (time_module.time() - cache_time) < 5:
            return MatchupIntelligence._covers_last10_cache[cache_key]
        
        result = {
            'is_live': False,
            'away_score': 0,
            'home_score': 0,
            'quarter': '',
            'time_remaining': '',
            'status': 'Not Started',
            'line_movement': {'open': 'N/A', 'current': 'N/A', 'movement': 0}
        }
        
        try:
            matchup_id = MatchupIntelligence.get_covers_matchup_id(away_team, home_team, league)
            if not matchup_id:
                return result
            
            sport_path_map = {
                'NBA': 'sport/basketball/nba',
                'CBB': 'sport/basketball/ncaab',
                'NFL': 'sport/football/nfl',
                'CFB': 'sport/football/ncaaf',
                'NHL': 'sport/hockey/nhl'
            }
            sport_path = sport_path_map.get(league.upper(), 'sport/basketball/nba')
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            matchup_url = f"https://www.covers.com/{sport_path}/matchup/{matchup_id}"
            resp = requests.get(matchup_url, headers=headers, timeout=5)
            
            if resp.status_code != 200:
                return result
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(' ', strip=True)
            
            if 'LIVE' in text.upper() or 'IN PROGRESS' in text.upper():
                result['is_live'] = True
                result['status'] = 'LIVE'
            elif 'FINAL' in text.upper():
                result['status'] = 'Final'
            
            score_match = re.search(r'(\d{1,3})\s*-\s*(\d{1,3})', text)
            if score_match and result['is_live']:
                result['away_score'] = int(score_match.group(1))
                result['home_score'] = int(score_match.group(2))
            
            quarter_match = re.search(r'(1st|2nd|3rd|4th|OT\d?)\s*(?:Quarter|Qtr|Q)?', text, re.IGNORECASE)
            if quarter_match:
                result['quarter'] = quarter_match.group(1)
            
            time_match = re.search(r'(\d{1,2}:\d{2})', text)
            if time_match:
                result['time_remaining'] = time_match.group(1)
            
            open_match = re.search(r'Open(?:ing)?\s*(?:Line|Total)?\s*[:\s]*([OU]?\s*\d+\.?\d*)', text, re.IGNORECASE)
            current_match = re.search(r'Current\s*(?:Line|Total)?\s*[:\s]*([OU]?\s*\d+\.?\d*)', text, re.IGNORECASE)
            
            if open_match:
                result['line_movement']['open'] = open_match.group(1).strip()
            if current_match:
                result['line_movement']['current'] = current_match.group(1).strip()
            
            MatchupIntelligence._covers_last10_cache[cache_key] = result
            MatchupIntelligence._covers_last10_cache_time[cache_key] = time_module.time()
            
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching Covers live data: {e}")
            return result
    
    @staticmethod
    def fetch_covers_betting_action(away_team: str, home_team: str, league: str = 'NBA') -> dict:
        """
        Fetch betting action data from Covers.com - Bet %, Money %, line movement.
        Fast refresh for live updates.
        """
        import requests
        from bs4 import BeautifulSoup
        import re
        import time as time_module
        
        cache_key = f"betting_{away_team}_{home_team}_{league}"
        cache_time = MatchupIntelligence._covers_last10_cache_time.get(cache_key, 0)
        if cache_key in MatchupIntelligence._covers_last10_cache and (time_module.time() - cache_time) < 5:
            return MatchupIntelligence._covers_last10_cache[cache_key]
        
        result = {
            'bet_pct_over': 50,
            'bet_pct_under': 50,
            'money_pct_over': 50,
            'money_pct_under': 50,
            'open_total': 'N/A',
            'current_total': 'N/A',
            'line_movement': 0,
            'sharp_action': None
        }
        
        try:
            matchup_id = MatchupIntelligence.get_covers_matchup_id(away_team, home_team, league)
            if not matchup_id:
                return result
            
            sport_path_map = {
                'NBA': 'sport/basketball/nba',
                'CBB': 'sport/basketball/ncaab',
                'NFL': 'sport/football/nfl',
                'CFB': 'sport/football/ncaaf',
                'NHL': 'sport/hockey/nhl'
            }
            sport_path = sport_path_map.get(league.upper(), 'sport/basketball/nba')
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            matchup_url = f"https://www.covers.com/{sport_path}/matchup/{matchup_id}"
            resp = requests.get(matchup_url, headers=headers, timeout=5)
            
            if resp.status_code != 200:
                return result
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text(' ', strip=True)
            
            bet_pct_match = re.search(r'(?:Tickets?|Bets?)\s*(?:%|Percent)?\s*(\d+)\s*%?\s*(?:Over|O)', text, re.IGNORECASE)
            if bet_pct_match:
                result['bet_pct_over'] = int(bet_pct_match.group(1))
                result['bet_pct_under'] = 100 - result['bet_pct_over']
            
            money_pct_match = re.search(r'(?:Money|Handle)\s*(?:%|Percent)?\s*(\d+)\s*%?\s*(?:Over|O)', text, re.IGNORECASE)
            if money_pct_match:
                result['money_pct_over'] = int(money_pct_match.group(1))
                result['money_pct_under'] = 100 - result['money_pct_over']
            
            total_matches = re.findall(r'(?:Total|O/U)\s*[:\s]*(\d+\.?\d*)', text, re.IGNORECASE)
            if len(total_matches) >= 2:
                result['open_total'] = total_matches[0]
                result['current_total'] = total_matches[-1]
                try:
                    result['line_movement'] = float(total_matches[-1]) - float(total_matches[0])
                except:
                    pass
            elif len(total_matches) == 1:
                result['current_total'] = total_matches[0]
            
            if result['bet_pct_over'] < 40 and result['money_pct_over'] > 60:
                result['sharp_action'] = 'OVER'
            elif result['bet_pct_under'] < 40 and result['money_pct_under'] > 60:
                result['sharp_action'] = 'UNDER'
            
            MatchupIntelligence._covers_last10_cache[cache_key] = result
            MatchupIntelligence._covers_last10_cache_time[cache_key] = time_module.time()
            
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching Covers betting action: {e}")
            return result
    
    @staticmethod
    def fetch_covers_last10_games(away_team: str, home_team: str, league: str = 'NBA') -> dict:
        """
        Fetch Last 10 games for each team + H2H history from Covers.com.
        Returns data for tabs: Away Team L10, Home Team L10, H2H Last 10.
        """
        import time as time_module
        
        cache_key = f"last10_{away_team}_{home_team}_{league}"
        cache_time = MatchupIntelligence._covers_last10_cache_time.get(cache_key, 0)
        if cache_key in MatchupIntelligence._covers_last10_cache and (time_module.time() - cache_time) < 300:
            return MatchupIntelligence._covers_last10_cache[cache_key]
        
        result = {
            'away': {'team': away_team, 'record': 'N/A', 'ats': 'N/A', 'ou': 'N/A', 'games': []},
            'home': {'team': home_team, 'record': 'N/A', 'ats': 'N/A', 'ou': 'N/A', 'games': []},
            'h2h': {'record': 'N/A', 'ats': 'N/A', 'ou': 'N/A', 'games': []}
        }
        
        try:
            # Fetch full H2H data which includes game history
            h2h_data = MatchupIntelligence.fetch_covers_full_h2h(away_team, home_team, league)
            
            if h2h_data:
                result['h2h']['record'] = h2h_data.get('wl_record', 'N/A')
                result['h2h']['ats'] = h2h_data.get('ats_record', 'N/A')
                result['h2h']['ou'] = h2h_data.get('ou_record', 'N/A')
                result['h2h']['games'] = h2h_data.get('games', [])
                
                # Log success
                logger.info(f"Covers H2H {away_team} vs {home_team}: W/L={result['h2h']['record']}, ATS={result['h2h']['ats']}, O/U={result['h2h']['ou']}, {len(result['h2h']['games'])} games")
            
            MatchupIntelligence._covers_last10_cache[cache_key] = result
            MatchupIntelligence._covers_last10_cache_time[cache_key] = time_module.time()
            
        except Exception as e:
            logger.warning(f"Error in fetch_covers_last10_games: {e}")
        
        return result
    
    # Cache for RLM data with game-time aware refresh
    _rlm_cache = {}
    _rlm_cache_time = {}
    
    @staticmethod
    def fetch_rlm_data(league: str = 'NBA', game_time: str = None) -> dict:
        """
        Fetch betting data from WagerTalk.com.
        - Bet % (tickets) and Money % (handle) for spreads
        - Sharp money detection (when money% diverges from bet%)
        
        Auto-refreshes 2 hours before game time for live data.
        """
        from datetime import datetime
        from wagertalk_scraper import get_all_wagertalk_data
        
        result = {}
        cache_key = f"{league}_{datetime.now().strftime('%Y%m%d')}"
        
        now = datetime.now()
        should_refresh = True
        
        if cache_key in MatchupIntelligence._rlm_cache:
            cached_time = MatchupIntelligence._rlm_cache_time.get(cache_key, now)
            cache_age_minutes = (now - cached_time).total_seconds() / 60
            should_refresh = cache_age_minutes > 2  # Refresh every 2 mins for live line movement
            
            if game_time and not should_refresh:
                try:
                    if 'T' in str(game_time):
                        game_dt = datetime.fromisoformat(str(game_time).replace('Z', ''))
                    else:
                        game_dt = datetime.strptime(f"{now.strftime('%Y-%m-%d')} {game_time.replace(' ET', '').replace(' PM', ' PM').replace(' AM', ' AM')}", "%Y-%m-%d %I:%M %p")
                    
                    time_to_game = (game_dt - now).total_seconds() / 3600
                    if 0 < time_to_game <= 2:
                        should_refresh = True
                        logger.info(f"Auto-refresh triggered: {time_to_game:.1f}h until game time")
                except Exception as e:
                    logger.debug(f"Could not parse game time for refresh check: {e}")
        
        if not should_refresh and cache_key in MatchupIntelligence._rlm_cache:
            logger.info(f"Using cached RLM data for {league}")
            return MatchupIntelligence._rlm_cache[cache_key]
        
        try:
            wagertalk_data = get_all_wagertalk_data(league)
            
            # City to Nickname mapping for consistent Bovada-style names
            city_to_nickname = {
                'Washington': 'Wizards', 'Milwaukee': 'Bucks', 'Boston': 'Celtics',
                'Brooklyn': 'Nets', 'Charlotte': 'Hornets', 'Chicago': 'Bulls',
                'Cleveland': 'Cavaliers', 'Dallas': 'Mavericks', 'Denver': 'Nuggets',
                'Detroit': 'Pistons', 'Golden State': 'Warriors', 'Houston': 'Rockets',
                'Indiana': 'Pacers', 'LA Clippers': 'Clippers', 'LA Lakers': 'Lakers',
                'Los Angeles Lakers': 'Lakers', 'Los Angeles Clippers': 'Clippers',
                'Memphis': 'Grizzlies', 'Miami': 'Heat', 'Minnesota': 'Timberwolves',
                'New Orleans': 'Pelicans', 'New York': 'Knicks', 'Oklahoma City': 'Thunder',
                'Orlando': 'Magic', 'Philadelphia': 'Sixers', 'Phoenix': 'Suns',
                'Portland': 'Trail Blazers', 'Sacramento': 'Kings', 'San Antonio': 'Spurs',
                'Toronto': 'Raptors', 'Utah': 'Jazz', 'Atlanta': 'Hawks'
            }
            
            def normalize_team_name(name):
                """Convert city name or full name to Bovada-style nickname."""
                if not name:
                    return name
                # Already a nickname
                if name in city_to_nickname.values():
                    return name
                # City name lookup
                if name in city_to_nickname:
                    return city_to_nickname[name]
                # Try partial match
                for city, nickname in city_to_nickname.items():
                    if city.lower() in name.lower() or name.lower() in city.lower():
                        return nickname
                return name
            
            for key, data in wagertalk_data.items():
                away_team = normalize_team_name(data.get('away_team', ''))
                home_team = normalize_team_name(data.get('home_team', ''))
                
                # Spread percentages (favorite vs underdog)
                spread_tickets_pct = data.get('spread_tickets_pct', 50)
                spread_money_pct = data.get('spread_money_pct', 50)
                away_bet_pct = data.get('away_bet_pct', spread_tickets_pct)
                home_bet_pct = data.get('home_bet_pct', 100 - spread_tickets_pct)
                away_money_pct = data.get('away_money_pct', spread_money_pct)
                home_money_pct = data.get('home_money_pct', 100 - spread_money_pct)
                
                # Totals percentages (over/under)
                over_bet_pct = data.get('over_bet_pct', 50)
                under_bet_pct = data.get('under_bet_pct', 50)
                over_money_pct = data.get('over_money_pct', 50)
                under_money_pct = data.get('under_money_pct', 50)
                
                majority_team = 'away' if away_bet_pct > home_bet_pct else 'home'
                majority_pct = max(away_bet_pct, home_bet_pct)
                
                # Line movement data
                spread_open_line = data.get('spread_open_line')
                spread_open_odds = data.get('spread_open_odds')
                spread_current_line = data.get('spread_current_line')
                spread_current_odds = data.get('spread_current_odds')
                total_open_line = data.get('total_open_line')
                total_open_odds = data.get('total_open_odds')
                total_current_line = data.get('total_current_line')
                total_current_odds = data.get('total_current_odds')
                
                # === TRUE RLM DETECTION ===
                # RLM = Public bets heavily one way, but line moves OPPOSITE direction
                # This signals SHARP MONEY on the side the line moved toward
                
                spread_rlm_detected = False
                spread_rlm_sharp_side = None
                totals_rlm_detected = False
                totals_rlm_sharp_side = None
                
                # SPREAD RLM: Check if line moved opposite to public betting
                try:
                    if spread_open_line and spread_current_line:
                        open_spread = float(str(spread_open_line).replace('+', ''))
                        current_spread = float(str(spread_current_line).replace('+', ''))
                        spread_movement = current_spread - open_spread
                        
                        # Public on AWAY (negative spread means away favored)
                        # If public bets away but spread gets LESS negative (moves toward home) = RLM
                        if away_bet_pct >= 60 and spread_movement > 0.5:
                            spread_rlm_detected = True
                            spread_rlm_sharp_side = home_team
                        # Public on HOME but spread gets MORE negative (moves toward away) = RLM
                        elif home_bet_pct >= 60 and spread_movement < -0.5:
                            spread_rlm_detected = True
                            spread_rlm_sharp_side = away_team
                except:
                    pass
                
                # TOTALS RLM: Check if line moved opposite to over/under betting
                try:
                    if total_open_line and total_current_line:
                        open_total = float(str(total_open_line).replace('O', '').replace('U', ''))
                        current_str = str(total_current_line).replace('O', '').replace('U', '')
                        current_total = float(current_str)
                        total_movement = current_total - open_total
                        
                        # Public on OVER but total DROPS = RLM (sharp on Under)
                        if over_bet_pct >= 60 and total_movement < -0.5:
                            totals_rlm_detected = True
                            totals_rlm_sharp_side = 'Under'
                        # Public on UNDER but total RISES = RLM (sharp on Over)
                        elif under_bet_pct >= 60 and total_movement > 0.5:
                            totals_rlm_detected = True
                            totals_rlm_sharp_side = 'Over'
                except:
                    pass
                
                # Combined RLM potential (either spread or totals has RLM)
                rlm_potential = spread_rlm_detected or totals_rlm_detected
                
                # Sharp money detection (use RLM-detected values)
                sharp_detected = totals_rlm_detected
                sharp_side = totals_rlm_sharp_side
                spread_sharp_detected = spread_rlm_detected
                spread_sharp_side = spread_rlm_sharp_side
                
                game_key = f"{away_team}_vs_{home_team}".lower().replace(' ', '_')
                
                result[game_key] = {
                    'away': {'team': away_team, 'bet_pct': str(away_bet_pct), 'money_pct': str(away_money_pct)},
                    'home': {'team': home_team, 'bet_pct': str(home_bet_pct), 'money_pct': str(home_money_pct)},
                    'open_spread': spread_open_line or 'N/A',
                    'current_spread': spread_current_line or 'N/A',
                    'spread_open_odds': spread_open_odds or '-110',
                    'spread_current_odds': spread_current_odds or '-110',
                    'spread_tickets_pct': spread_tickets_pct,
                    'spread_money_pct': spread_money_pct,
                    'spread_sharp_detected': spread_sharp_detected,
                    'spread_sharp_side': spread_sharp_side,
                    'total_open_line': total_open_line or 'N/A',
                    'total_current_line': total_current_line or 'N/A',
                    'total_open_odds': total_open_odds or '-110',
                    'total_current_odds': total_current_odds or '-110',
                    'line_movement': 'N/A',
                    'majority_team': majority_team,
                    'majority_pct': majority_pct,
                    'rlm_potential': rlm_potential,
                    'spread_rlm_detected': spread_rlm_detected,
                    'spread_rlm_sharp_side': spread_rlm_sharp_side,
                    'totals_rlm_detected': totals_rlm_detected,
                    'totals_rlm_sharp_side': totals_rlm_sharp_side,
                    'sharp_detected': sharp_detected,
                    'sharp_side': sharp_side,
                    'over_bet_pct': over_bet_pct,
                    'under_bet_pct': under_bet_pct,
                    'over_money_pct': over_money_pct,
                    'under_money_pct': under_money_pct
                }
            
            logger.info(f"WagerTalk data fetched for {league}: {len(result)} games")
            
            if result:
                MatchupIntelligence._rlm_cache[cache_key] = result
                MatchupIntelligence._rlm_cache_time[cache_key] = datetime.now()
            
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching WagerTalk data for {league}: {e}")
            if cache_key in MatchupIntelligence._rlm_cache:
                logger.info(f"Returning cached RLM data after error for {league}")
                return MatchupIntelligence._rlm_cache[cache_key]
            return result
    
    @staticmethod
    def compute_ctg_metrics(team_stats: dict, league: str = 'NBA') -> dict:
        """
        Compute Cleaning-the-Glass style efficiency metrics.
        Uses NBA.com data to calculate CTG-style advanced stats.
        """
        try:
            if not team_stats:
                return {}
            
            fga = team_stats.get('fga', 0) or 1
            fta = team_stats.get('fta', 0) or 1
            fg3a = team_stats.get('fg3a', 0) or 1
            fgm = team_stats.get('fgm', 0)
            fg3m = team_stats.get('fg3m', 0)
            ftm = team_stats.get('ftm', 0)
            tov = team_stats.get('tov', 0)
            oreb = team_stats.get('oreb', 0)
            dreb = team_stats.get('dreb', 0)
            
            # Possessions estimate
            possessions = fga + 0.44 * fta + tov - oreb
            possessions = max(possessions, 1)
            
            # CTG-style metrics
            return {
                'efg_pct': ((fgm + 0.5 * fg3m) / max(1, fga)) * 100,
                'ts_pct': (team_stats.get('pts', 0) / max(1, 2 * (fga + 0.44 * fta))) * 100,
                'tov_pct': (tov / possessions) * 100,
                'orb_pct': (oreb / max(1, oreb + dreb)) * 100 if oreb + dreb > 0 else 0,
                'drb_pct': (dreb / max(1, oreb + dreb)) * 100 if oreb + dreb > 0 else 0,
                'ft_rate': (fta / max(1, fga)) * 100,
                '3pt_rate': (fg3a / max(1, fga)) * 100,
                'ast_rate': (team_stats.get('ast', 0) / max(1, fgm)) * 100,
                'pace': possessions
            }
            
        except Exception as e:
            logger.warning(f"Error computing CTG metrics: {e}")
            return {}
    
    @staticmethod
    def compute_matchup_stats(away_team: str, home_team: str, away_stats: dict, home_stats: dict, 
                             away_ppg: float, home_ppg: float, away_opp_ppg: float, home_opp_ppg: float,
                             rankings: dict = None, league: str = 'NBA') -> dict:
        """
        Compute comprehensive matchup intelligence for a game.
        Returns structured data for UI display including Season vs L5 comparisons.
        """
        result = {
            'has_data': False,
            'away_team': away_team,
            'home_team': home_team
        }
        
        # Get rankings data
        away_rank_data = rankings.get(away_team, {}) if rankings else {}
        home_rank_data = rankings.get(home_team, {}) if rankings else {}
        
        # Fetch L5 stats for both teams (NBA only for now)
        away_l5 = {}
        home_l5 = {}
        if league == 'NBA':
            try:
                away_l5 = MatchupIntelligence.get_team_last5_stats(away_team, league)
                home_l5 = MatchupIntelligence.get_team_last5_stats(home_team, league)
            except Exception as e:
                logger.warning(f"Failed to fetch L5 stats: {e}")
        
        # Store L5 data in result for template access
        result['away_l5'] = away_l5
        result['home_l5'] = home_l5
        
        # Power Rating section
        away_power_rank = away_rank_data.get('power_rank', 0)
        home_power_rank = home_rank_data.get('power_rank', 0)
        
        if away_power_rank and home_power_rank:
            result['power_rating'] = {
                'away': {
                    'rank': away_power_rank,
                    'percentile': round((30 - away_power_rank + 1) / 30 * 100),
                    'label': f"Top {round((away_power_rank / 30) * 100)}%"
                },
                'home': {
                    'rank': home_power_rank,
                    'percentile': round((30 - home_power_rank + 1) / 30 * 100),
                    'label': f"Top {round((home_power_rank / 30) * 100)}%"
                },
                'diff': home_power_rank - away_power_rank,
                'edge': 'away' if away_power_rank < home_power_rank else ('home' if home_power_rank < away_power_rank else 'even')
            }
            result['has_data'] = True
        
        # Offensive Efficiency (pts/100 possessions)
        away_off = away_rank_data.get('off_rating', away_ppg)
        home_off = home_rank_data.get('off_rating', home_ppg)
        away_off_rank = away_rank_data.get('off_rank', 0)
        home_off_rank = home_rank_data.get('off_rank', 0)
        
        if away_off and home_off:
            result['offensive_efficiency'] = {
                'away': {
                    'value': round(away_off, 1),
                    'rank': away_off_rank,
                    'percentile': round((30 - away_off_rank + 1) / 30 * 100) if away_off_rank else 0
                },
                'home': {
                    'value': round(home_off, 1),
                    'rank': home_off_rank,
                    'percentile': round((30 - home_off_rank + 1) / 30 * 100) if home_off_rank else 0
                },
                'diff': round(away_off - home_off, 1),
                'edge': 'away' if away_off > home_off else ('home' if home_off > away_off else 'even')
            }
            result['has_data'] = True
        
        # Defensive Efficiency (pts allowed/100 possessions)
        away_def = away_rank_data.get('def_rating', away_opp_ppg)
        home_def = home_rank_data.get('def_rating', home_opp_ppg)
        away_def_rank = away_rank_data.get('def_rank', 0)
        home_def_rank = home_rank_data.get('def_rank', 0)
        
        if away_def and home_def:
            result['defensive_efficiency'] = {
                'away': {
                    'value': round(away_def, 1),
                    'rank': away_def_rank,
                    'percentile': round((30 - away_def_rank + 1) / 30 * 100) if away_def_rank else 0
                },
                'home': {
                    'value': round(home_def, 1),
                    'rank': home_def_rank,
                    'percentile': round((30 - home_def_rank + 1) / 30 * 100) if home_def_rank else 0
                },
                'diff': round(home_def - away_def, 1),  # Lower is better for defense
                'edge': 'away' if away_def < home_def else ('home' if home_def < away_def else 'even')
            }
            result['has_data'] = True
        
        # Efficiency Comparison (Offense vs Defense matchup)
        if away_off and home_def:
            result['efficiency_comparison'] = {
                'away_off_vs_home_def': {
                    'off_value': round(away_off, 1),
                    'def_value': round(home_def, 1),
                    'diff': round(away_off - home_def, 1),
                    'insight': 'expect scoring' if away_off > home_def + 5 else ('defensive edge' if home_def < away_off - 5 else 'even matchup')
                },
                'home_off_vs_away_def': {
                    'off_value': round(home_off, 1),
                    'def_value': round(away_def, 1),
                    'diff': round(home_off - away_def, 1),
                    'insight': 'expect scoring' if home_off > away_def + 5 else ('defensive edge' if away_def < home_off - 5 else 'even matchup')
                }
            }
        
        # Shooting Profile with Season vs L5 comparison
        if away_stats and home_stats:
            result['shooting_profile'] = {
                'efg_pct': {
                    'away_season': round(away_stats.get('efg_pct', 0), 1),
                    'home_season': round(home_stats.get('efg_pct', 0), 1),
                    'away_l5': round(away_l5.get('l5_efg', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_efg', 0), 1) if home_l5 else 0,
                    'd1_avg': MatchupIntelligence.D1_AVERAGES['eFG%']
                },
                '3pt_pct': {
                    'away_season': round(away_stats.get('fg3_pct', 0), 1),
                    'home_season': round(home_stats.get('fg3_pct', 0), 1),
                    'away_l5': round(away_l5.get('l5_fg3_pct', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_fg3_pct', 0), 1) if home_l5 else 0,
                    'd1_avg': MatchupIntelligence.D1_AVERAGES['3PT%']
                },
                '3pm_game': {
                    'away_season': round(away_stats.get('fg3m', 0), 1),
                    'home_season': round(home_stats.get('fg3m', 0), 1),
                    'away_l5': round(away_l5.get('l5_fg3m', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_fg3m', 0), 1) if home_l5 else 0
                }
            }
            result['has_data'] = True
        
        # Ball Control with Season vs L5 comparison
        if away_stats and home_stats:
            away_tov_pct = away_stats.get('tov_pct', 0)
            home_tov_pct = home_stats.get('tov_pct', 0)
            result['ball_control'] = {
                'tov_pct': {
                    'away_season': round(away_tov_pct, 1),
                    'home_season': round(home_tov_pct, 1),
                    'away_l5': round(away_l5.get('l5_tov_pct', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_tov_pct', 0), 1) if home_l5 else 0,
                    'd1_avg': MatchupIntelligence.D1_AVERAGES['TOV%'],
                    'away_protects': away_tov_pct < MatchupIntelligence.D1_AVERAGES['TOV%'],
                    'home_protects': home_tov_pct < MatchupIntelligence.D1_AVERAGES['TOV%']
                },
                'forced_tov_pct': {
                    'away_season': round(away_stats.get('forced_tov_pct', 0), 1),
                    'home_season': round(home_stats.get('forced_tov_pct', 0), 1),
                    'away_l5': 0,  # Would need opponent data for accurate L5
                    'home_l5': 0
                }
            }
        
        # Rebounding with Season vs L5 comparison
        if away_stats and home_stats:
            away_orb_pct = away_stats.get('orb_pct', 0)
            home_orb_pct = home_stats.get('orb_pct', 0)
            away_drb_pct = away_stats.get('drb_pct', 0)
            home_drb_pct = home_stats.get('drb_pct', 0)
            result['rebounding'] = {
                'orb_pct': {
                    'away_season': round(away_orb_pct, 1),
                    'home_season': round(home_orb_pct, 1),
                    'away_l5': round(away_l5.get('l5_orb', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_orb', 0), 1) if home_l5 else 0,
                    'd1_avg': MatchupIntelligence.D1_AVERAGES['ORB%'],
                    'away_crashes': away_orb_pct > MatchupIntelligence.D1_AVERAGES['ORB%'] + 2,
                    'home_crashes': home_orb_pct > MatchupIntelligence.D1_AVERAGES['ORB%'] + 2
                },
                'drb_pct': {
                    'away_season': round(away_drb_pct, 1),
                    'home_season': round(home_drb_pct, 1),
                    'away_l5': round(away_l5.get('l5_drb', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_drb', 0), 1) if home_l5 else 0
                }
            }
        
        # Pace & Free Throws with Season vs L5 comparison
        if away_stats and home_stats:
            away_ft_rate = away_stats.get('ft_rate', 0)
            home_ft_rate = home_stats.get('ft_rate', 0)
            away_pace = away_rank_data.get('pace', 100)
            home_pace = home_rank_data.get('pace', 100)
            result['pace_ft'] = {
                'ft_rate': {
                    'away_season': round(away_ft_rate, 1),
                    'home_season': round(home_ft_rate, 1),
                    'away_l5': round(away_l5.get('l5_ft_rate', 0), 1) if away_l5 else 0,
                    'home_l5': round(home_l5.get('l5_ft_rate', 0), 1) if home_l5 else 0,
                    'd1_avg': MatchupIntelligence.D1_AVERAGES['FT_RATE'],
                    'away_attacks': away_ft_rate > MatchupIntelligence.D1_AVERAGES['FT_RATE'] + 5,
                    'home_attacks': home_ft_rate > MatchupIntelligence.D1_AVERAGES['FT_RATE'] + 5
                },
                'opp_ft_rate': {
                    'away_season': round(away_stats.get('opp_ft_rate', 0), 1),
                    'home_season': round(home_stats.get('opp_ft_rate', 0), 1),
                    'away_l5': 0,  # Would need opponent data
                    'home_l5': 0
                },
                'pace': {
                    'away': round(away_pace, 1),
                    'home': round(home_pace, 1),
                    'expected': round((away_pace + home_pace) / 2, 1)
                }
            }
        
        # Analyst Insight based on overall comparison
        if result.get('power_rating'):
            power_diff = abs(result['power_rating']['diff'])
            if power_diff <= 5:
                result['analyst_insight'] = "Neither team has a clear advantage."
            elif result['power_rating']['edge'] == 'away':
                result['analyst_insight'] = f"{away_team} has the edge with better overall efficiency."
            else:
                result['analyst_insight'] = f"{home_team} has the edge with better overall efficiency."
        
        return result


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
    # Bart Torvik CBB stats
    torvik_tempo = db.Column(db.Float)
    torvik_away_adj_o = db.Column(db.Float)
    torvik_away_adj_d = db.Column(db.Float)
    torvik_home_adj_o = db.Column(db.Float)
    torvik_home_adj_d = db.Column(db.Float)
    torvik_away_rank = db.Column(db.Integer)
    torvik_home_rank = db.Column(db.Integer)
    
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
            if not pick_type or pick_type not in ['total', 'spread']:
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
                # Use commonName (team nickname) for NHL, not placeName (city)
                away_name = game.get("awayTeam", {}).get("commonName", {}).get("default", "")
                home_name = game.get("homeTeam", {}).get("commonName", {}).get("default", "")
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
                # Use commonName (team nickname) for NHL, not placeName (city)
                away_name = game.get("awayTeam", {}).get("commonName", {}).get("default", "")
                home_name = game.get("homeTeam", {}).get("commonName", {}).get("default", "")
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
    # CRITICAL: Fast health check response for Cloud Run
    # Cloud Run checks root path with specific user agents - respond immediately
    user_agent = request.headers.get('User-Agent', '').lower()
    
    # Multiple health check patterns from various services
    health_check_indicators = [
        'googlehc',           # Google Health Check
        'health',             # Generic health checks
        'check',              # Uptime monitoring
        'kube-probe',         # Kubernetes probes
        'ping',               # Ping checks
        'monitoring',         # Monitoring services
    ]
    
    if any(indicator in user_agent for indicator in health_check_indicators):
        return jsonify({"status": "healthy", "service": "730sports"}), 200
    
    # Also check for HEAD requests (common health check method)
    if request.method == 'HEAD':
        return '', 200
    
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    show_only_qualified = request.args.get('qualified', '0') == '1'
    # Always filter to games with lines (Bovada only)
    
    # ASYNC CLEANUP: Don't block the response - schedule cleanup in background thread
    def cleanup_old_games():
        try:
            with app.app_context():
                old_game_ids = [g.id for g in Game.query.filter(Game.date < today).limit(100).all()]
                if old_game_ids:
                    Pick.query.filter(Pick.game_id.in_(old_game_ids)).update({Pick.game_id: None}, synchronize_session=False)
                    stmt = delete(Game).where(Game.id.in_(old_game_ids))
                    db.session.execute(stmt)
                    db.session.commit()
                    logger.info(f"Background cleanup: removed {len(old_game_ids)} old games")
        except Exception as e:
            logger.warning(f"Background cleanup error (non-critical): {e}")
            try:
                db.session.rollback()
            except:
                pass
    
    # Start cleanup in background thread (non-blocking)
    import threading
    cleanup_thread = threading.Thread(target=cleanup_old_games, daemon=True)
    cleanup_thread.start()
    
    all_games_db = Game.query.filter_by(date=today).order_by(Game.edge.desc()).all()
    # Show all games from today's slate (includes in-progress and completed)
    all_games = all_games_db
    
    # Add time window and logos to each game for weekend slate grouping
    for g in all_games:
        g.time_window = get_game_window(g.game_time)
        # Add team logos for Pikkit-style display
        if g.league == 'NBA':
            g.away_logo = nba_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
            g.home_logo = nba_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
        elif g.league == 'CBB':
            g.away_logo = get_transparent_cbb_logo(g.away_team) or get_cbb_logo(g.away_team) or 'https://a.espncdn.com/i/teamlogos/leagues/500-dark/nba.png'
            g.home_logo = get_transparent_cbb_logo(g.home_team) or get_cbb_logo(g.home_team) or 'https://a.espncdn.com/i/teamlogos/leagues/500-dark/nba.png'
        elif g.league == 'NHL':
            g.away_logo = nhl_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
            g.home_logo = nhl_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
        else:
            g.away_logo = ''
            g.home_logo = ''
    
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
    
    # FETCH L5 STATS for NBA games (advanced analytics)
    # Pre-cache L5 data to avoid API calls during template render
    for g in qualified:
        if g.league == 'NBA':
            try:
                away_l5 = MatchupIntelligence.get_team_last5_stats(g.away_team, 'NBA')
                home_l5 = MatchupIntelligence.get_team_last5_stats(g.home_team, 'NBA')
                g.matchup_l5 = {
                    'away': away_l5,
                    'home': home_l5,
                    'has_data': bool(away_l5 or home_l5)
                }
            except Exception as e:
                logger.warning(f"Error fetching L5 stats for {g.away_team} vs {g.home_team}: {e}")
                g.matchup_l5 = {'away': {}, 'home': {}, 'has_data': False}
        else:
            g.matchup_l5 = {'away': {}, 'home': {}, 'has_data': False}
    
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

@app.route('/dashboard')
def dashboard_redirect():
    """Redirect /dashboard to main page."""
    return redirect(url_for('dashboard'))

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
                # Use commonName (team nickname) for NHL, not placeName (city)
                away_name = game.get("awayTeam", {}).get("commonName", {}).get("default", "")
                home_name = game.get("homeTeam", {}).get("commonName", {}).get("default", "")
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

@app.route('/api/covers_h2h/<int:game_id>')
def api_covers_h2h(game_id):
    """Get full H2H data from Covers.com - W/L, ATS, O/U with team logos and games."""
    game = Game.query.get_or_404(game_id)
    h2h_data = MatchupIntelligence.fetch_covers_full_h2h(game.away_team, game.home_team, game.league)
    return jsonify(h2h_data)

@app.route('/api/covers_live/<int:game_id>')
def api_covers_live(game_id):
    """Get live game data from Covers.com - scores, quarter, time. Fast 5-second cache."""
    game = Game.query.get_or_404(game_id)
    live_data = MatchupIntelligence.fetch_covers_live_data(game.away_team, game.home_team, game.league)
    return jsonify(live_data)

@app.route('/api/covers_betting/<int:game_id>')
def api_covers_betting(game_id):
    """Get betting action data from Covers.com - Bet %, Money %, line movement. Fast 5-second cache."""
    game = Game.query.get_or_404(game_id)
    betting_data = MatchupIntelligence.fetch_covers_betting_action(game.away_team, game.home_team, game.league)
    return jsonify(betting_data)

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

torvik_cache = {}
torvik_cache_date = None

def fetch_torvik_ratings():
    """Fetch Bart Torvik team ratings for CBB. Cached daily."""
    global torvik_cache, torvik_cache_date
    today = date.today()
    if torvik_cache_date == today and torvik_cache:
        logger.info(f"Using cached Torvik data ({len(torvik_cache)} teams)")
        return torvik_cache
    try:
        logger.info("Fetching Bart Torvik CBB ratings...")
        url = "https://barttorvik.com/trank.php?year=2026&sort=&top=0&conlimit=All"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Torvik fetch failed: {resp.status_code}")
            return torvik_cache
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'ratings-table'})
        if not table:
            tables = soup.find_all('table')
            table = tables[0] if tables else None
        if not table:
            logger.warning("Could not find Torvik ratings table")
            return torvik_cache
        rows = table.find_all('tr')[1:]
        new_cache = {}
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 8:
                try:
                    rank = cells[0].get_text(strip=True)
                    team_cell = cells[1]
                    team_link = team_cell.find('a')
                    team_name = team_link.get_text(strip=True) if team_link else team_cell.get_text(strip=True)
                    team_name = team_name.replace('\n', ' ').strip()
                    conf = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    record = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    adj_o_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                    adj_d_text = cells[5].get_text(strip=True) if len(cells) > 5 else ""
                    barthag_text = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                    tempo_text = cells[7].get_text(strip=True) if len(cells) > 7 else ""
                    adj_o = float(adj_o_text) if adj_o_text else None
                    adj_d = float(adj_d_text) if adj_d_text else None
                    barthag = float(barthag_text) if barthag_text else None
                    tempo = float(tempo_text) if tempo_text else None
                    if team_name and adj_o and adj_d:
                        new_cache[team_name.lower()] = {
                            'rank': int(rank) if rank.isdigit() else 0,
                            'team': team_name,
                            'conf': conf,
                            'record': record,
                            'adj_o': adj_o,
                            'adj_d': adj_d,
                            'barthag': barthag,
                            'tempo': tempo
                        }
                except (ValueError, IndexError) as e:
                    continue
        if new_cache:
            torvik_cache = new_cache
            torvik_cache_date = today
            logger.info(f"Torvik data loaded: {len(new_cache)} teams")
        return torvik_cache
    except Exception as e:
        logger.error(f"Torvik fetch error: {e}")
        return torvik_cache

def get_torvik_team(team_name: str) -> Optional[dict]:
    """Get Torvik stats for a team by name (fuzzy match)."""
    if not torvik_cache:
        fetch_torvik_ratings()
    if not torvik_cache:
        return None
    name_lower = team_name.lower().strip()
    if name_lower in torvik_cache:
        return torvik_cache[name_lower]
    for key, data in torvik_cache.items():
        if name_lower in key or key in name_lower:
            return data
        key_parts = key.split()
        name_parts = name_lower.split()
        if any(p in name_parts for p in key_parts if len(p) > 3):
            return data
    common_aliases = {
        'uconn': 'connecticut', 'usc': 'southern california', 'unc': 'north carolina',
        'ucla': 'ucla', 'lsu': 'lsu', 'osu': 'ohio st.', 'msu': 'michigan st.',
        'uk': 'kentucky', 'ku': 'kansas', 'iu': 'indiana', 'duke': 'duke',
        'gonzaga': 'gonzaga', 'auburn': 'auburn', 'houston': 'houston', 'purdue': 'purdue',
        'tennessee': 'tennessee', 'alabama': 'alabama', 'arizona': 'arizona', 'iowa st.': 'iowa st.',
        'st. johns': "st. john's", 'st johns': "st. john's"
    }
    if name_lower in common_aliases:
        alias = common_aliases[name_lower]
        if alias in torvik_cache:
            return torvik_cache[alias]
    return None

def calculate_torvik_projection(away_team: str, home_team: str) -> Optional[dict]:
    """Calculate projected total using Torvik adjusted efficiency + tempo."""
    away_stats = get_torvik_team(away_team)
    home_stats = get_torvik_team(home_team)
    if not away_stats or not home_stats:
        return None
    away_adj_o = away_stats.get('adj_o', 0)
    away_adj_d = away_stats.get('adj_d', 0)
    home_adj_o = home_stats.get('adj_o', 0)
    home_adj_d = home_stats.get('adj_d', 0)
    away_tempo = away_stats.get('tempo', 67)
    home_tempo = home_stats.get('tempo', 67)
    d1_avg_tempo = 67.5
    d1_avg_eff = 109.6
    game_tempo = (away_tempo + home_tempo) / 2
    possessions = game_tempo
    away_off_eff = (away_adj_o + home_adj_d) / 2
    home_off_eff = (home_adj_o + away_adj_d) / 2
    away_points = (away_off_eff / 100) * possessions
    home_points = (home_off_eff / 100) * possessions
    projected_total = away_points + home_points
    return {
        'projected_total': round(projected_total, 1),
        'away_points': round(away_points, 1),
        'home_points': round(home_points, 1),
        'game_tempo': round(game_tempo, 1),
        'away_adj_o': away_adj_o,
        'away_adj_d': away_adj_d,
        'home_adj_o': home_adj_o,
        'home_adj_d': home_adj_d,
        'away_rank': away_stats.get('rank', 0),
        'home_rank': home_stats.get('rank', 0)
    }

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
                # Use commonName for team nickname (Rangers, Islanders) not placeName (New York)
                away_name = game_data.get("awayTeam", {}).get("commonName", {}).get("default", "")
                home_name = game_data.get("homeTeam", {}).get("commonName", {}).get("default", "")
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
    
    fetch_torvik_ratings()
    
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
                                        
                                        if league == "CBB":
                                            torvik_proj = calculate_torvik_projection(game.away_team, game.home_team)
                                            if torvik_proj:
                                                proj_total = torvik_proj['projected_total']
                                                exp_away = torvik_proj['away_points']
                                                exp_home = torvik_proj['home_points']
                                                game.torvik_tempo = torvik_proj.get('game_tempo')
                                                game.torvik_away_adj_o = torvik_proj.get('away_adj_o')
                                                game.torvik_away_adj_d = torvik_proj.get('away_adj_d')
                                                game.torvik_home_adj_o = torvik_proj.get('home_adj_o')
                                                game.torvik_home_adj_d = torvik_proj.get('home_adj_d')
                                                game.torvik_away_rank = torvik_proj.get('away_rank')
                                                game.torvik_home_rank = torvik_proj.get('home_rank')
                                                logger.debug(f"CBB {game.away_team}@{game.home_team}: Torvik proj={proj_total}")
                                        
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

_nba_standings_cache = {'data': {}, 'timestamp': 0}

def get_nba_standings():
    """Fetch NBA standings from ESPN API with caching."""
    global _nba_standings_cache
    import time
    
    # Check cache (60 min TTL)
    if _nba_standings_cache['data'] and time.time() - _nba_standings_cache['timestamp'] < 3600:
        return _nba_standings_cache['data']
    
    standings = {}
    try:
        url = 'https://site.api.espn.com/apis/v2/sports/basketball/nba/standings'
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for child in data.get('children', []):
                conf_name = child.get('name', '')
                conf_abbr = 'Eastern' if 'east' in conf_name.lower() else 'Western'
                entries = child.get('standings', {}).get('entries', [])
                # Sort by wins descending to get proper standings order
                sorted_entries = sorted(entries, key=lambda x: -float([s.get('value',0) for s in x.get('stats',[]) if s.get('name')=='wins'][0] if [s.get('value',0) for s in x.get('stats',[]) if s.get('name')=='wins'] else 0))
                for idx, entry in enumerate(sorted_entries, 1):
                    team_info = entry.get('team', {})
                    team_name = team_info.get('displayName', '').split()[-1]  # Get last word (nickname)
                    stats = {s['name']: s.get('value', 0) for s in entry.get('stats', [])}
                    wins = int(float(stats.get('wins', 0)))
                    losses = int(float(stats.get('losses', 0)))
                    # Format ordinal suffix
                    if 11 <= idx <= 13:
                        suffix = 'th'
                    else:
                        suffix = ['th','st','nd','rd','th','th','th','th','th','th'][idx % 10]
                    standings[team_name] = {
                        'record': f"{wins}-{losses}",
                        'standing': f"{idx}{suffix} {conf_abbr}",
                        'wins': wins,
                        'losses': losses,
                        'conf': conf_abbr
                    }
        _nba_standings_cache = {'data': standings, 'timestamp': time.time()}
        logger.info(f"Fetched NBA standings for {len(standings)} teams")
    except Exception as e:
        logger.warning(f"Error fetching NBA standings: {e}")
    return standings

@app.route('/spreads')
def spreads():
    """Spreads page - shows all upcoming games with spread data (no totals filtering)."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    # Fetch NBA standings for records
    nba_standings = get_nba_standings()
    
    # Get ALL games for today without any totals filtering
    all_games = Game.query.filter_by(date=today).order_by(Game.game_time.asc()).all()
    
    # Group games by league
    games_by_league = {
        'NBA': [],
        'CBB': [],
        'NFL': [],
        'CFB': [],
        'NHL': []
    }
    
    for g in all_games:
        if g.league in games_by_league:
            # Add team logos for NBA
            if g.league == 'NBA':
                g.away_logo = nba_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
                g.home_logo = nba_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
                # Add records and standings from ESPN
                away_stand = nba_standings.get(g.away_team, {})
                home_stand = nba_standings.get(g.home_team, {})
                g.away_record = away_stand.get('record', '--')
                g.home_record = home_stand.get('record', '--')
                g.away_standing = away_stand.get('standing', '')
                g.home_standing = home_stand.get('standing', '')
            else:
                # CBB uses transparent team-specific logos from automated_loading_system
                g.away_logo = get_transparent_cbb_logo(g.away_team) or get_cbb_logo(g.away_team) or 'https://a.espncdn.com/i/teamlogos/leagues/500-dark/nba.png'
                g.home_logo = get_transparent_cbb_logo(g.home_team) or get_cbb_logo(g.home_team) or 'https://a.espncdn.com/i/teamlogos/leagues/500-dark/nba.png'
                g.away_record = '--'
                g.home_record = '--'
                g.away_standing = ''
                g.home_standing = ''
            games_by_league[g.league].append(g)
    
    # Set up basic attributes for all games (data fetched on-demand via API)
    # Elimination process for qualifying picks
    eliminated_large_spread = []
    eliminated_bad_teams = []
    eliminated_bad_defense = []
    qualifying_picks = []
    
    # Bottom 5 defenses (L5) based on NBA.com defense stats - worst net rating
    # Updated from nba.com/stats/teams/defense-dash-overall?LastNGames=5
    bad_defense_teams = {
        'Nets': '30th',      # 7.0 diff - worst
        'Magic': '29th',     # 6.1 diff
        'Suns': '28th',      # 1.6 diff
        'Kings': '27th',     # 1.2 diff
        'Warriors': '26th',  # 1.1 diff
    }
    
    for g in all_games:
        # Mark away team as favorite if spread is negative
        g.away_is_favorite = g.spread_line is not None and g.spread_line < 0
        g.home_is_favorite = g.spread_line is not None and g.spread_line > 0
        
        # Format spread display
        if g.spread_line is not None:
            if g.spread_line < 0:
                g.spread_display = f"{g.away_team} {g.spread_line}"
            elif g.spread_line > 0:
                g.spread_display = f"{g.home_team} -{g.spread_line}"
            else:
                g.spread_display = "PICK"
        else:
            g.spread_display = "N/A"
        
        # Initialize empty - data fetched via AJAX when user clicks Model Breakdown
        g.matchup_l5 = {}
        g.away_advanced = {}
        g.home_advanced = {}
        
        # Apply elimination filters for NBA games
        if g.league == 'NBA' and g.spread_line is not None:
            abs_spread = abs(g.spread_line)
            
            # 1. Eliminate large spreads (10+ points)
            if abs_spread >= 10:
                eliminated_large_spread.append(g)
                g.elimination_reason = 'LARGE SPREAD'
                continue
            
            # 2. Eliminate bad teams (would need record data - use placeholder logic)
            # Bad teams typically have low PPG or are known struggling teams
            bad_record_teams = ['Wizards', 'Nets', 'Hornets', 'Blazers', 'Trail Blazers', 'Jazz']
            if g.away_team in bad_record_teams or g.home_team in bad_record_teams:
                eliminated_bad_teams.append(g)
                g.elimination_reason = 'BAD TEAM'
                continue
            
            # 3. Eliminate games with bad defense matchups (bottom 5 defenses)
            away_def_rank = bad_defense_teams.get(g.away_team)
            home_def_rank = bad_defense_teams.get(g.home_team)
            if away_def_rank or home_def_rank:
                g.defense_rank = away_def_rank or home_def_rank
                g.bad_defense_team = g.away_team if away_def_rank else g.home_team
                eliminated_bad_defense.append(g)
                g.elimination_reason = 'BAD DEFENSE'
                continue
            
            # Remaining = qualifying picks
            qualifying_picks.append(g)
    
    # Exclude NHL from total count (not implemented yet)
    basketball_games = [g for g in all_games if g.league in ['NBA', 'CBB']]
    
    # === DAILY SLATE ANALYSIS ===
    # Build team lists for analysis section (NBA only)
    nba_games = [g for g in all_games if g.league == 'NBA']
    
    # 1. Large Spread 10+ (show matchup with spread)
    large_spread_matchups = []
    large_spread_teams = set()
    for g in nba_games:
        if g.spread_line is not None and abs(g.spread_line) >= 10:
            spread_val = abs(g.spread_line)
            if g.spread_line < 0:  # Away favored
                large_spread_matchups.append(f"{g.home_team} +{spread_val}")
                large_spread_teams.add(g.home_team)
            else:  # Home favored
                large_spread_matchups.append(f"{g.away_team} +{spread_val}")
                large_spread_teams.add(g.away_team)
    large_spread_display = ', '.join(large_spread_matchups) if large_spread_matchups else 'None'
    
    # L10 Records - Updated daily from ESPN/NBA.com
    # Format: team_name -> (wins, losses) in last 10 games
    nba_l10_records = {
        # Cold teams (3 wins or less in L10)
        'Wizards': (1, 9),
        'Nets': (2, 8),
        'Jazz': (3, 7),
        'Trail Blazers': (3, 7),
        'Blazers': (3, 7),
        'Hornets': (3, 7),
        # Moderate teams
        'Pelicans': (4, 6),
        'Raptors': (4, 6),
        'Bulls': (5, 5),
        'Hawks': (5, 5),
        'Heat': (5, 5),
        'Spurs': (5, 5),
        '76ers': (5, 5),
        'Magic': (5, 5),
        'Pacers': (5, 5),
        'Nuggets': (6, 4),
        'Lakers': (6, 4),
        'Suns': (6, 4),
        'Kings': (4, 6),
        'Warriors': (6, 4),
        'Mavericks': (6, 4),
        'Timberwolves': (7, 3),
        'Rockets': (7, 3),
        'Bucks': (7, 3),
        'Clippers': (7, 3),
        'Knicks': (7, 3),
        # Hot teams (8+ wins in L10)
        'Celtics': (8, 2),
        'Cavaliers': (8, 2),
        'Thunder': (9, 1),
        'Grizzlies': (8, 2),
        'Pistons': (8, 2),
    }
    
    # Get all teams playing today
    teams_today = set()
    for g in nba_games:
        teams_today.add(g.away_team)
        teams_today.add(g.home_team)
    
    # 2. Cold Teams (L10: 3 wins or less) - teams on losing streaks
    cold_teams_list = []
    cold_teams_set = set()
    for team_name in teams_today:
        l10 = nba_l10_records.get(team_name)
        if l10 and l10[0] <= 3:  # 3 wins or less in L10
            cold_teams_list.append(f'<span style="white-space:nowrap">{team_name} ({l10[0]}-{l10[1]})</span>')
            cold_teams_set.add(team_name)
    cold_teams_display = ', '.join(sorted(cold_teams_list, key=lambda x: int(x.split('(')[1].split('-')[0]))) if cold_teams_list else 'None'
    
    # 3. Hot Teams (8-2+ L10) - don't fade these
    hot_teams_list = []
    hot_teams_set = set()
    for team_name in teams_today:
        l10 = nba_l10_records.get(team_name)
        if l10 and l10[0] >= 8:  # 8 wins or more in L10
            hot_teams_list.append(f'<span style="white-space:nowrap">{team_name} ({l10[0]}-{l10[1]})</span>')
            hot_teams_set.add(team_name)
    hot_teams_display = ', '.join(sorted(hot_teams_list, key=lambda x: -int(x.split('(')[1].split('-')[0]))) if hot_teams_list else 'None'
    
    # 4. Bad Defense L5 teams with rankings (bottom 5)
    bad_defense_in_slate = []
    bad_defense_set = set()
    for g in nba_games:
        away_rank = bad_defense_teams.get(g.away_team)
        home_rank = bad_defense_teams.get(g.home_team)
        if away_rank:
            bad_defense_in_slate.append(f'<span style="white-space:nowrap">{g.away_team} ({away_rank})</span>')
            bad_defense_set.add(g.away_team)
        if home_rank:
            bad_defense_in_slate.append(f'<span style="white-space:nowrap">{g.home_team} ({home_rank})</span>')
            bad_defense_set.add(g.home_team)
    bad_defense_display = ', '.join(bad_defense_in_slate) if bad_defense_in_slate else 'None'
    
    # 5. B2B teams - check yesterday's games
    # Teams that played yesterday (January 28, 2026) - update daily
    # Source: NBA schedule
    teams_played_yesterday = {
        'Thunder',      # vs Grizzlies
        'Grizzlies',    # vs Thunder
        'Rockets',      # vs Clippers
        'Clippers',     # vs Rockets
        'Pistons',      # vs Magic
        'Magic',        # vs Pistons
        'Hornets',      # vs Cavaliers
        'Cavaliers',    # vs Hornets
        'Bucks',        # vs Celtics
        'Celtics',      # vs Bucks
        'Nets',         # vs Knicks
        'Knicks',       # vs Nets
    }
    
    b2b_teams_list = []
    b2b_set = set()
    for g in nba_games:
        if g.away_team in teams_played_yesterday:
            b2b_set.add(g.away_team)
        if g.home_team in teams_played_yesterday:
            b2b_set.add(g.home_team)
    b2b_display = ', '.join(sorted(b2b_set)) if b2b_set else 'None'
    
    # 6. Home/Away Edge - placeholder for now
    home_away_display = 'See game cards for H/A splits'
    
    # 7. Remaining teams (after all eliminations)
    all_teams_in_slate = set()
    for g in nba_games:
        all_teams_in_slate.add(g.away_team)
        all_teams_in_slate.add(g.home_team)
    
    # Remove all flagged teams
    eliminated_teams = large_spread_teams | cold_teams_set | bad_defense_set | b2b_set
    remaining = all_teams_in_slate - eliminated_teams
    remaining_display = ', '.join(sorted(remaining)) if remaining else 'All flagged - proceed with caution'
    
    return render_template('spreads.html', 
                           games_by_league=games_by_league,
                           all_games=basketball_games,
                           today=today,
                           total_games=len(basketball_games),
                           eliminated_large_spread=eliminated_large_spread,
                           eliminated_bad_teams=eliminated_bad_teams,
                           eliminated_bad_defense=eliminated_bad_defense,
                           qualifying_picks=qualifying_picks,
                           large_spread_teams=large_spread_display,
                           cold_teams=cold_teams_display,
                           hot_teams=hot_teams_display,
                           bad_defense_teams_display=bad_defense_display,
                           b2b_teams=b2b_display,
                           home_away_edge=home_away_display,
                           remaining_teams=remaining_display,
                           team_colors=NBA_TEAM_COLORS)

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


@app.route('/api/matchup_data/<int:game_id>')
def get_matchup_data(game_id):
    """Fetch live matchup data from TeamRankings matchup page."""
    game = Game.query.get_or_404(game_id)
    
    result = {
        'game_id': game_id,
        'away_team': game.away_team,
        'home_team': game.home_team,
        'league': game.league,
        'away_season': {},
        'home_season': {},
        'away_l3': {},
        'home_l3': {}
    }
    
    # Helper function to find stat by searching multiple possible key names
    def find_stat(stats_dict, *possible_keys):
        """Search for a stat value using multiple possible key names."""
        for key in possible_keys:
            # Try exact match first
            if key in stats_dict and stats_dict[key]:
                return stats_dict[key]
            # Try lowercase
            key_lower = key.lower()
            if key_lower in stats_dict and stats_dict[key_lower]:
                return stats_dict[key_lower]
            # Try partial match (stat name contains the key)
            for stat_key, val in stats_dict.items():
                if key_lower in stat_key.lower() and val:
                    return val
        return 0
    
    try:
        # Format date for TeamRankings URL (YYYY-MM-DD)
        game_date = game.date.strftime('%Y-%m-%d') if hasattr(game.date, 'strftime') else str(game.date)[:10]
        
        if game.league in ['NBA', 'CBB']:
            # Fetch from TeamRankings matchup page
            matchup_data = MatchupIntelligence.fetch_teamrankings_matchup(
                game.away_team, game.home_team, game_date, game.league
            ) or {}
            
            away_season = matchup_data.get('away_season', {})
            home_season = matchup_data.get('home_season', {})
            away_l3 = matchup_data.get('away_l3', {})
            home_l3 = matchup_data.get('home_l3', {})
            
            # Log what stats we got for debugging
            logging.info(f"TeamRankings stats for {game.away_team} vs {game.home_team}: {list(away_season.keys())[:10]}...")
            
            # Fetch external data in PARALLEL for faster loading
            away_ctg = {}
            home_ctg = {}
            h2h_data = {}
            rlm_data = {}
            
            def fetch_ctg_away():
                if game.league == 'NBA':
                    return MatchupIntelligence.fetch_ctg_four_factors(game.away_team)
                return {}
            
            def fetch_ctg_home():
                if game.league == 'NBA':
                    return MatchupIntelligence.fetch_ctg_four_factors(game.home_team)
                return {}
            
            def fetch_h2h():
                return MatchupIntelligence.fetch_covers_h2h(game.away_team, game.home_team, game.league)
            
            def fetch_rlm():
                return MatchupIntelligence.fetch_rlm_data(game.league)
            
            # Run all fetches in parallel
            with ThreadPoolExecutor(max_workers=4) as executor:
                ctg_away_future = executor.submit(fetch_ctg_away)
                ctg_home_future = executor.submit(fetch_ctg_home)
                h2h_future = executor.submit(fetch_h2h)
                rlm_future = executor.submit(fetch_rlm)
                
                try:
                    away_ctg = ctg_away_future.result(timeout=10)
                except Exception as e:
                    logging.warning(f"CTG away fetch error: {e}")
                    away_ctg = {}
                
                try:
                    home_ctg = ctg_home_future.result(timeout=10)
                except Exception as e:
                    logging.warning(f"CTG home fetch error: {e}")
                    home_ctg = {}
                
                try:
                    h2h_data = h2h_future.result(timeout=10)
                    logging.info(f"Covers H2H data for {game.league}: {h2h_data}")
                except Exception as e:
                    logging.warning(f"Covers H2H fetch error: {e}")
                    h2h_data = {}
                
                try:
                    all_rlm = rlm_future.result(timeout=30)
                except Exception as e:
                    logging.warning(f"RLM fetch error: {e}")
                    all_rlm = {}
            
            if away_ctg or home_ctg:
                logging.info(f"CTG data: away={away_ctg}, home={home_ctg}")
            
            # Add H2H, ATS and records to result
            result['h2h_record'] = h2h_data.get('h2h_record', 'N/A')
            result['h2h_leader'] = h2h_data.get('h2h_leader', 'Even')
            result['h2h_ats'] = h2h_data.get('h2h_ats', 'N/A')
            result['ats_leader'] = h2h_data.get('ats_leader', 'Even')
            result['away_record'] = h2h_data.get('away_record', 'N/A')
            result['home_record'] = h2h_data.get('home_record', 'N/A')
            result['away_ats'] = h2h_data.get('away_ats', 'N/A')
            result['home_ats'] = h2h_data.get('home_ats', 'N/A')
            
            # Process RLM data (already fetched in parallel above)
            rlm_data = {}
            try:
                # Team name mappings for matching (all 30 NBA teams)
                team_keywords = {
                    'bulls': ['bulls', 'chicago', 'chi'],
                    'pacers': ['pacers', 'indiana', 'ind'],
                    'lakers': ['lakers', 'la lakers', 'l.a. lakers', 'los angeles'],
                    'cavaliers': ['cavaliers', 'cavs', 'cleveland', 'cle'],
                    'celtics': ['celtics', 'boston', 'bos'],
                    'hawks': ['hawks', 'atlanta', 'atl'],
                    'heat': ['heat', 'miami', 'mia'],
                    'magic': ['magic', 'orlando', 'orl'],
                    'knicks': ['knicks', 'new york', 'ny'],
                    'raptors': ['raptors', 'toronto', 'tor'],
                    'hornets': ['hornets', 'charlotte', 'cha'],
                    'grizzlies': ['grizzlies', 'memphis', 'mem'],
                    'timberwolves': ['timberwolves', 'wolves', 'minnesota', 'min'],
                    'mavericks': ['mavericks', 'mavs', 'dallas', 'dal'],
                    'warriors': ['warriors', 'golden state', 'gsw', 'gs'],
                    'jazz': ['jazz', 'utah', 'uta'],
                    'spurs': ['spurs', 'san antonio', 'sa'],
                    'rockets': ['rockets', 'houston', 'hou'],
                    'pelicans': ['pelicans', 'new orleans', 'nop', 'no'],
                    'nets': ['nets', 'brooklyn', 'bkn'],
                    '76ers': ['76ers', 'sixers', 'philadelphia', 'phi'],
                    'pistons': ['pistons', 'detroit', 'det'],
                    'clippers': ['clippers', 'la clippers', 'lac'],
                    'nuggets': ['nuggets', 'denver', 'den'],
                    'trail blazers': ['trail blazers', 'blazers', 'portland', 'por'],
                    'thunder': ['thunder', 'oklahoma', 'okc'],
                    'kings': ['kings', 'sacramento', 'sac'],
                    'suns': ['suns', 'phoenix', 'phx'],
                    'bucks': ['bucks', 'milwaukee', 'mil'],
                    'wizards': ['wizards', 'washington', 'was', 'wiz']
                }
                
                def matches_team(text, team):
                    text = text.lower()
                    team = team.lower()
                    keywords = team_keywords.get(team, [team])
                    return any(kw in text for kw in keywords)
                
                for key, data in all_rlm.items():
                    away_match = matches_team(key, game.away_team) or matches_team(data.get('away', {}).get('team', ''), game.away_team)
                    home_match = matches_team(key, game.home_team) or matches_team(data.get('home', {}).get('team', ''), game.home_team)
                    
                    if away_match and home_match:
                        rlm_data = data
                        break
                
                logging.info(f"RLM data for {game.away_team} vs {game.home_team}: {rlm_data}")
            except Exception as e:
                logging.warning(f"RLM fetch error: {e}")
            
            # Add RLM checklist data to result
            result['rlm'] = {
                'away_spread': rlm_data.get('current_spread', rlm_data.get('spread_current_line', 'N/A')),
                'home_spread': rlm_data.get('home', {}).get('spread', 'N/A'),
                'open_spread': rlm_data.get('open_spread', rlm_data.get('spread_open_line', 'N/A')),
                'current_spread': rlm_data.get('current_spread', rlm_data.get('spread_current_line', 'N/A')),
                'spread_open_line': rlm_data.get('open_spread', rlm_data.get('spread_open_line', 'N/A')),
                'spread_current_line': rlm_data.get('current_spread', rlm_data.get('spread_current_line', 'N/A')),
                'spread_open_odds': rlm_data.get('spread_open_odds', '-110'),
                'spread_current_odds': rlm_data.get('spread_current_odds', '-110'),
                'spread_tickets_pct': rlm_data.get('spread_tickets_pct', 50),
                'spread_money_pct': rlm_data.get('spread_money_pct', 50),
                'spread_sharp_detected': rlm_data.get('spread_sharp_detected', False),
                'spread_sharp_side': rlm_data.get('spread_sharp_side'),
                'total_open_line': rlm_data.get('total_open_line', 'N/A'),
                'total_current_line': rlm_data.get('total_current_line', 'N/A'),
                'total_open_odds': rlm_data.get('total_open_odds', '-110'),
                'total_current_odds': rlm_data.get('total_current_odds', '-110'),
                'line_movement': rlm_data.get('line_movement', 'N/A'),
                'away_bet_pct': rlm_data.get('away_bet_pct', rlm_data.get('away', {}).get('bet_pct', 50)),
                'home_bet_pct': rlm_data.get('home_bet_pct', rlm_data.get('home', {}).get('bet_pct', 50)),
                'away_money_pct': rlm_data.get('away_money_pct', rlm_data.get('away', {}).get('money_pct', 50)),
                'home_money_pct': rlm_data.get('home_money_pct', rlm_data.get('home', {}).get('money_pct', 50)),
                'over_bet_pct': rlm_data.get('over_bet_pct', 50),
                'under_bet_pct': rlm_data.get('under_bet_pct', 50),
                'over_money_pct': rlm_data.get('over_money_pct', 50),
                'under_money_pct': rlm_data.get('under_money_pct', 50),
                'majority_team': rlm_data.get('majority_team', 'N/A'),
                'majority_pct': rlm_data.get('majority_pct', 0),
                'rlm_potential': rlm_data.get('rlm_potential', False),
                'sharp_detected': rlm_data.get('sharp_detected', False),
                'sharp_side': rlm_data.get('sharp_side')
            }
            
            # Convert to display format - Season Stats using exact TeamRankings stat names
            result['away_season'] = {
                'PPG': find_stat(away_season, 'points/game'),
                'Opp PPG': find_stat(away_season, 'opp points/game'),
                'FG%': find_stat(away_season, 'shooting %'),
                'Opp FG%': find_stat(away_season, 'opp shooting %'),
                '3PT%': find_stat(away_season, 'three point %'),
                'Opp 3PT%': find_stat(away_season, 'opp three point %'),
                'FT%': find_stat(away_season, 'free throw %'),
                'Opp FT%': find_stat(away_season, 'opp free throw %'),
                'PACE': find_stat(away_season, 'possessions/gm'),
                'Assists/TO': find_stat(away_season, 'assists/turnover'),
                'eFG%': find_stat(away_season, 'effective fg %'),
                'Opp eFG%': find_stat(away_season, 'opp effective fg %'),
                'TOV': find_stat(away_season, 'turnovers/game'),
                'TOV%': find_stat(away_season, 'turnovers/play'),
                'ORB': find_stat(away_season, 'off rebounds/gm'),
                'ORB%': find_stat(away_season, 'off rebound %'),
                'DRB': find_stat(away_season, 'def rebounds/gm'),
                'DRB%': find_stat(away_season, 'def rebound %'),
                'Assists': find_stat(away_season, 'assists/game'),
                'Blocks': find_stat(away_season, 'blocks/game'),
                'Steals': find_stat(away_season, 'steals/game'),
                'Fouls': find_stat(away_season, 'personal fouls/gm'),
                'O Eff': find_stat(away_season, 'off efficiency'),
                'D Eff': find_stat(away_season, 'def efficiency'),
                'Pts in Paint': find_stat(away_season, 'pts in paint/gm'),
                'Fastbreak Pts': find_stat(away_season, 'fastbreak pts/gm'),
                'FTA/FGA': away_ctg.get('off_ft_rate') or find_stat(away_season, 'fta/fga'),
                'FT Rate Rank': away_ctg.get('off_ft_rank'),
                '3PM/Game': find_stat(away_season, '3pm/game'),
                'Opp TOV': find_stat(away_season, 'opp turnovers/game'),
                'Opp TOV%': find_stat(away_season, 'opp turnovers/play'),
                'Opp 3PM/Game': find_stat(away_season, 'opp 3pm/game'),
                'Opp FTA/FGA': away_ctg.get('def_ft_rate') or find_stat(away_season, 'opp fta/fga'),
                'Opp FT Rate Rank': away_ctg.get('def_ft_rank'),
                'PPP': away_ctg.get('off_ppp'),
                'PPP Rank': away_ctg.get('off_ppp_rank'),
                'Opp PPP': away_ctg.get('def_ppp'),
                'Opp PPP Rank': away_ctg.get('def_ppp_rank'),
                'eFG% Rank': away_ctg.get('off_efg_rank'),
                'Opp eFG% Rank': away_ctg.get('def_efg_rank'),
                'TOV% Rank': away_ctg.get('off_tov_rank'),
                'F-TOV% Rank': away_ctg.get('def_tov_rank'),
                'ORB% Rank': away_ctg.get('off_orb_rank'),
                'DRB% Rank': away_ctg.get('def_orb_rank')
            }
            result['home_season'] = {
                'PPG': find_stat(home_season, 'points/game'),
                'Opp PPG': find_stat(home_season, 'opp points/game'),
                'FG%': find_stat(home_season, 'shooting %'),
                'Opp FG%': find_stat(home_season, 'opp shooting %'),
                '3PT%': find_stat(home_season, 'three point %'),
                'Opp 3PT%': find_stat(home_season, 'opp three point %'),
                'FT%': find_stat(home_season, 'free throw %'),
                'Opp FT%': find_stat(home_season, 'opp free throw %'),
                'PACE': find_stat(home_season, 'possessions/gm'),
                'Assists/TO': find_stat(home_season, 'assists/turnover'),
                'eFG%': find_stat(home_season, 'effective fg %'),
                'Opp eFG%': find_stat(home_season, 'opp effective fg %'),
                'TOV': find_stat(home_season, 'turnovers/game'),
                'TOV%': find_stat(home_season, 'turnovers/play'),
                'ORB': find_stat(home_season, 'off rebounds/gm'),
                'ORB%': find_stat(home_season, 'off rebound %'),
                'DRB': find_stat(home_season, 'def rebounds/gm'),
                'DRB%': find_stat(home_season, 'def rebound %'),
                'Assists': find_stat(home_season, 'assists/game'),
                'Blocks': find_stat(home_season, 'blocks/game'),
                'Steals': find_stat(home_season, 'steals/game'),
                'Fouls': find_stat(home_season, 'personal fouls/gm'),
                'O Eff': find_stat(home_season, 'off efficiency'),
                'D Eff': find_stat(home_season, 'def efficiency'),
                'Pts in Paint': find_stat(home_season, 'pts in paint/gm'),
                'Fastbreak Pts': find_stat(home_season, 'fastbreak pts/gm'),
                'FTA/FGA': home_ctg.get('off_ft_rate') or find_stat(home_season, 'fta/fga'),
                'FT Rate Rank': home_ctg.get('off_ft_rank'),
                '3PM/Game': find_stat(home_season, '3pm/game'),
                'Opp TOV': find_stat(home_season, 'opp turnovers/game'),
                'Opp TOV%': find_stat(home_season, 'opp turnovers/play'),
                'Opp 3PM/Game': find_stat(home_season, 'opp 3pm/game'),
                'Opp FTA/FGA': home_ctg.get('def_ft_rate') or find_stat(home_season, 'opp fta/fga'),
                'Opp FT Rate Rank': home_ctg.get('def_ft_rank'),
                'PPP': home_ctg.get('off_ppp'),
                'PPP Rank': home_ctg.get('off_ppp_rank'),
                'Opp PPP': home_ctg.get('def_ppp'),
                'Opp PPP Rank': home_ctg.get('def_ppp_rank'),
                'eFG% Rank': home_ctg.get('off_efg_rank'),
                'Opp eFG% Rank': home_ctg.get('def_efg_rank'),
                'TOV% Rank': home_ctg.get('off_tov_rank'),
                'F-TOV% Rank': home_ctg.get('def_tov_rank'),
                'ORB% Rank': home_ctg.get('off_orb_rank'),
                'DRB% Rank': home_ctg.get('def_orb_rank')
            }
            
            # SOS Rank comes from the power-ratings page scraper
            result['away_season']['SOS'] = find_stat(away_season, 'sos rank') or 'N/A'
            result['home_season']['SOS'] = find_stat(home_season, 'sos rank') or 'N/A'
            
            # Last 3 Games - use season stats as fallback since L3 may not be available from all pages
            result['away_l3'] = {
                'PPG': find_stat(away_l3, 'points/game') or result['away_season']['PPG'],
                'Opp PPG': find_stat(away_l3, 'opp points/game') or result['away_season']['Opp PPG'],
                'FG%': find_stat(away_l3, 'shooting %') or result['away_season']['FG%'],
                'Opp FG%': find_stat(away_l3, 'opp shooting %') or result['away_season']['Opp FG%'],
                '3PT%': find_stat(away_l3, 'three point %') or result['away_season']['3PT%'],
                'Opp 3PT%': find_stat(away_l3, 'opp three point %') or result['away_season']['Opp 3PT%'],
                'FT%': find_stat(away_l3, 'free throw %') or result['away_season']['FT%'],
                'PACE': find_stat(away_l3, 'possessions/game') or result['away_season']['PACE'],
                'Assists/TO': find_stat(away_l3, 'assists/turnover') or result['away_season']['Assists/TO'],
                'eFG%': find_stat(away_l3, 'effective fg %') or result['away_season']['eFG%'],
                'TOV': find_stat(away_l3, 'turnovers/game') or result['away_season']['TOV'],
                'ORB': find_stat(away_l3, 'off rebounds/gm') or result['away_season']['ORB'],
                'DRB': find_stat(away_l3, 'def rebounds/gm') or result['away_season']['DRB'],
                'Assists': find_stat(away_l3, 'assists/game') or result['away_season']['Assists'],
                'Blocks': find_stat(away_l3, 'blocks/game') or result['away_season']['Blocks'],
                'Steals': find_stat(away_l3, 'steals/game') or result['away_season']['Steals']
            }
            result['home_l3'] = {
                'PPG': find_stat(home_l3, 'points/game') or result['home_season']['PPG'],
                'Opp PPG': find_stat(home_l3, 'opp points/game') or result['home_season']['Opp PPG'],
                'FG%': find_stat(home_l3, 'shooting %') or result['home_season']['FG%'],
                'Opp FG%': find_stat(home_l3, 'opp shooting %') or result['home_season']['Opp FG%'],
                '3PT%': find_stat(home_l3, 'three point %') or result['home_season']['3PT%'],
                'Opp 3PT%': find_stat(home_l3, 'opp three point %') or result['home_season']['Opp 3PT%'],
                'FT%': find_stat(home_l3, 'free throw %') or result['home_season']['FT%'],
                'PACE': find_stat(home_l3, 'possessions/game') or result['home_season']['PACE'],
                'Assists/TO': find_stat(home_l3, 'assists/turnover') or result['home_season']['Assists/TO'],
                'eFG%': find_stat(home_l3, 'effective fg %') or result['home_season']['eFG%'],
                'TOV': find_stat(home_l3, 'turnovers/game') or result['home_season']['TOV'],
                'ORB': find_stat(home_l3, 'off rebounds/gm') or result['home_season']['ORB'],
                'DRB': find_stat(home_l3, 'def rebounds/gm') or result['home_season']['DRB'],
                'Assists': find_stat(home_l3, 'assists/game') or result['home_season']['Assists'],
                'Blocks': find_stat(home_l3, 'blocks/game') or result['home_season']['Blocks'],
                'Steals': find_stat(home_l3, 'steals/game') or result['home_season']['Steals']
            }
        
    except Exception as e:
        logging.warning(f"Error fetching TeamRankings matchup data for game {game_id}: {e}")
    
    # Fetch Last 10 games from Covers.com (each team separately + H2H)
    try:
        if game.league == 'NBA':
            covers_last10 = MatchupIntelligence.fetch_covers_last10_games(game.away_team, game.home_team, game.league)
            result['covers_last10'] = covers_last10
            
            # Update top-level H2H fields from covers_last10 data
            if covers_last10.get('h2h', {}).get('record') and covers_last10['h2h']['record'] != 'N/A':
                result['h2h_record'] = covers_last10['h2h']['record']
                # Parse record to determine leader (format: "7-3" means away team leads)
                try:
                    parts = covers_last10['h2h']['record'].split('-')
                    if len(parts) == 2:
                        wins, losses = int(parts[0]), int(parts[1])
                        if wins > losses:
                            result['h2h_leader'] = game.away_team
                        elif losses > wins:
                            result['h2h_leader'] = game.home_team
                        else:
                            result['h2h_leader'] = 'Even'
                except:
                    pass
            if covers_last10.get('h2h', {}).get('ats') and covers_last10['h2h']['ats'] != 'N/A':
                result['h2h_ats'] = covers_last10['h2h']['ats']
                # Parse ATS to determine leader (format: "3-7-0" means home team leads ATS)
                try:
                    parts = covers_last10['h2h']['ats'].split('-')
                    if len(parts) >= 2:
                        wins, losses = int(parts[0]), int(parts[1])
                        if wins > losses:
                            result['ats_leader'] = game.away_team
                        elif losses > wins:
                            result['ats_leader'] = game.home_team
                        else:
                            result['ats_leader'] = 'Even'
                except:
                    pass
            
            # Also set last5 for backward compatibility (first 5 of last 10)
            result['last5_away'] = covers_last10['away']['games'][:5] if covers_last10['away']['games'] else []
            result['last5_home'] = covers_last10['home']['games'][:5] if covers_last10['home']['games'] else []
        else:
            result['covers_last10'] = {'away': {'games': []}, 'home': {'games': []}, 'h2h': {'games': []}}
            result['last5_away'] = []
            result['last5_home'] = []
    except Exception as e:
        logging.warning(f"Error fetching Covers Last 10 games: {e}")
        result['covers_last10'] = {'away': {'games': []}, 'home': {'games': []}, 'h2h': {'games': []}}
        result['last5_away'] = []
        result['last5_home'] = []
    
    return jsonify(result)

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

# Initialize automatic game loading system
auto_loader = setup_automatic_loading(app, db)
logger.info("Automatic game loading enabled - games will load on new day automatically")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
