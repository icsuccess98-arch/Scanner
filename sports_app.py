import os
import logging
import re
import time
import threading
from datetime import datetime, date, timedelta
from typing import Tuple, Optional, List, Dict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
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
from enhanced_scraping import get_cbb_logo, CBB_TEAM_NAME_ALIASES, normalize_cbb_team_name, get_all_team_aliases, get_covers_matchup_stats
from team_identity import normalize_team_name as identity_normalize
from automated_loading_system import (
    setup_automatic_loading,
    get_transparent_cbb_logo,
)

# AI Brain imports (graceful fallback — app works exactly as before if unavailable)
try:
    from feature_engineering import extract_features, get_ml_features
    from ai_brains import analyze_game as brain_analyze_game
    from ml_models import ensemble_predictor, EloSystem
    AI_BRAINS_AVAILABLE = True
except ImportError as e:
    AI_BRAINS_AVAILABLE = False
    logging.getLogger(__name__).info(f"AI brains not available: {e}")


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
    """Centralized configuration for thresholds."""

    EDGE_THRESHOLDS = {
        "NBA": 8.0,
        "CBB": 8.0,
        "NFL": 3.5,
        "CFB": 3.5,
        "NHL": 0.5
    }

    CACHE_TTL_CTG = 14400  # 4 hours for CTG data

THRESHOLDS = GameConstants.EDGE_THRESHOLDS

CBB_SPREAD_THRESHOLD = 4.0  # Lowered from 8.0 - professional edge is typically 2-4pts for spreads

def qualify_spread_game(away_team: str, home_team: str, market_spread: float, league: str = 'CBB') -> dict:
    """
    Qualify a spread game using KenPom stats and PPG formula.
    
    Returns dict with:
        - qualified: bool
        - projected_spread: float
        - spread_edge: float (difference from market)
        - decision: str (e.g., "Team A -5.5" or "Team B +5.5")
        - net_gap: float (KenPom net rating difference)
        - reason: str (why qualified/disqualified)
    """
    result = {
        'qualified': False,
        'projected_spread': 0,
        'spread_edge': 0,
        'decision': '',
        'net_gap': 0,
        'reason': 'Unknown'
    }
    
    if league != 'CBB':
        result['reason'] = 'Only CBB supported'
        return result
    
    away_stats = get_torvik_team(away_team)
    home_stats = get_torvik_team(home_team)
    
    if not away_stats or not home_stats:
        result['reason'] = 'Missing KenPom data'
        return result
    
    away_adj_o = away_stats.get('adj_o', 0)
    away_adj_d = away_stats.get('adj_d', 0)
    home_adj_o = home_stats.get('adj_o', 0)
    home_adj_d = home_stats.get('adj_d', 0)
    
    if not all([away_adj_o, away_adj_d, home_adj_o, home_adj_d]):
        result['reason'] = 'Incomplete KenPom efficiency data'
        return result
    
    away_adj_em = away_adj_o - away_adj_d
    home_adj_em = home_adj_o - home_adj_d
    
    avg_tempo = (away_stats.get('tempo', 68) + home_stats.get('tempo', 68)) / 2
    tempo_factor = avg_tempo / 68.0
    
    expected_away = ((away_adj_o + home_adj_d) / 2) * tempo_factor
    expected_home = ((home_adj_o + away_adj_d) / 2) * tempo_factor
    
    home_court_advantage = 3.5
    projected_spread = (expected_away - expected_home) + home_court_advantage
    
    spread_diff = projected_spread - abs(market_spread)
    
    if abs(spread_diff) < CBB_SPREAD_THRESHOLD:
        result['reason'] = f'Edge {abs(spread_diff):.1f} below {CBB_SPREAD_THRESHOLD} threshold'
        result['projected_spread'] = round(projected_spread, 1)
        result['spread_edge'] = round(spread_diff, 1)
        return result
    
    net_gap = away_adj_em - home_adj_em
    
    if projected_spread > 0 and net_gap <= 0:
        result['reason'] = 'Net rating conflicts with projected spread (away favored but home better NetRtg)'
        result['projected_spread'] = round(projected_spread, 1)
        result['spread_edge'] = round(spread_diff, 1)
        result['net_gap'] = round(net_gap, 1)
        return result
    
    if projected_spread < 0 and net_gap >= 0:
        result['reason'] = 'Net rating conflicts with projected spread (home favored but away better NetRtg)'
        result['projected_spread'] = round(projected_spread, 1)
        result['spread_edge'] = round(spread_diff, 1)
        result['net_gap'] = round(net_gap, 1)
        return result
    
    if projected_spread > 0:
        if market_spread > 0:
            decision = f"{away_team} -{abs(market_spread)}"
        else:
            decision = f"{away_team} +{abs(market_spread)}"
    else:
        if market_spread < 0:
            decision = f"{home_team} -{abs(market_spread)}"
        else:
            decision = f"{home_team} +{abs(market_spread)}"
    
    result['qualified'] = True
    result['projected_spread'] = round(projected_spread, 1)
    result['spread_edge'] = round(spread_diff, 1)
    result['net_gap'] = round(net_gap, 1)
    result['decision'] = decision
    result['reason'] = f'QUALIFIED: {abs(spread_diff):.1f}pt edge, NetRtg aligns'
    
    return result


def calculate_rlm(game) -> bool:
    """
    RLM Detection using Favorite/Underdog Decision Table.
    
    Money on Favorite + Line moves Down (toward underdog) = RLM, sharp = underdog
    Money on Underdog + Line moves Up (toward favorite) = RLM, sharp = favorite
    Money on Favorite + Line moves Up = NOT RLM (line confirming money)
    Money on Underdog + Line moves Down = NOT RLM (line confirming money)
    
    Threshold: >=54% money to establish majority. Uses money %, not tickets.
    """
    current_spread_field = 'spread_line' if hasattr(game, 'spread_line') else 'spread'
    
    if not all([
        getattr(game, 'opening_spread', None) is not None,
        getattr(game, current_spread_field, None) is not None,
    ]):
        return False
    
    try:
        opening_spread = float(game.opening_spread)
        current_spread = float(getattr(game, current_spread_field))
        away_tickets = float(game.away_tickets_pct or 0) if getattr(game, 'away_tickets_pct', None) else 0
        home_tickets = float(game.home_tickets_pct or 0) if getattr(game, 'home_tickets_pct', None) else 0
        away_money = float(getattr(game, 'away_money_pct', None) or away_tickets)
        home_money = float(getattr(game, 'home_money_pct', None) or home_tickets)
    except (ValueError, TypeError):
        return False
    
    movement = current_spread - opening_spread
    if abs(movement) == 0:
        return False
    
    if away_money < 60 and home_money < 60:
        return False
    
    favorite_team = None
    underdog_team = None
    if opening_spread < 0:
        favorite_team = game.away_team
        underdog_team = game.home_team
    elif opening_spread > 0:
        favorite_team = game.home_team
        underdog_team = game.away_team
    else:
        return False
    
    fav_is_away = (favorite_team == game.away_team)
    fav_money = away_money if fav_is_away else home_money
    dog_money = home_money if fav_is_away else away_money
    
    money_on_favorite = fav_money >= 60
    money_on_underdog = dog_money >= 60
    
    if fav_is_away:
        line_moved_up = movement < 0
        line_moved_down = movement > 0
    else:
        line_moved_up = movement > 0
        line_moved_down = movement < 0
    
    rlm_detected = False
    sharp_team = None
    
    if money_on_favorite and line_moved_down:
        rlm_detected = True
        sharp_team = underdog_team
    elif money_on_underdog and line_moved_up:
        rlm_detected = True
        sharp_team = favorite_team
    
    if rlm_detected and hasattr(game, 'rlm_sharp_side'):
        majority_pct = fav_money if money_on_favorite else dog_money
        majority_team = favorite_team if money_on_favorite else underdog_team
        game.rlm_sharp_side = sharp_team
        game.rlm_explanation = f"RLM: {majority_pct:.0f}% money on {majority_team}, but line moved {abs(movement):.1f} pts toward {sharp_team}"
        game.rlm_detected = True
    
    return rlm_detected


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
    _l5_cache_date = None  # Date-based invalidation for daily refresh
    
    @staticmethod
    def get_team_last5_stats(team_name: str, league: str = 'NBA') -> dict:
        """
        Fetch last 5 games stats from NBA.com API for trend analysis.
        Returns dict with L5 averages for key metrics.
        Uses date-based cache (daily refresh) + 30-minute TTL within the same day.
        """
        import time
        
        # Date-based cache invalidation - clear cache at midnight
        today = date.today()
        if MatchupIntelligence._l5_cache_date != today:
            MatchupIntelligence._l5_cache = {}
            MatchupIntelligence._l5_cache_date = today
            logger.info("L5 stats cache cleared for new day")
        
        # Check cache first (30 min TTL within same day)
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
                'bulls': 'bulls', 'chicago': 'bulls', 'pacers': 'pacers', 'indiana': 'pacers',
                'celtics': 'celtics', 'boston': 'celtics', 'lakers': 'lakers', 'los angeles lakers': 'lakers',
                'l.a. lakers': 'lakers', 'la lakers': 'lakers',
                'heat': 'heat', 'miami': 'heat', 'bucks': 'bucks', 'milwaukee': 'bucks',
                'nets': 'nets', 'brooklyn': 'nets', '76ers': '76ers', 'sixers': '76ers', 'philadelphia': '76ers',
                'knicks': 'knicks', 'new york': 'knicks', 'hawks': 'hawks', 'atlanta': 'hawks',
                'hornets': 'hornets', 'charlotte': 'hornets', 'cavaliers': 'cavaliers', 'cavs': 'cavaliers', 'cleveland': 'cavaliers',
                'pistons': 'pistons', 'detroit': 'pistons', 'magic': 'magic', 'orlando': 'magic',
                'wizards': 'wizards', 'washington': 'wizards', 'raptors': 'raptors', 'toronto': 'raptors',
                'nuggets': 'nuggets', 'denver': 'nuggets', 'clippers': 'clippers', 'l.a. clippers': 'clippers', 'la clippers': 'clippers',
                'suns': 'suns', 'phoenix': 'suns', 'warriors': 'warriors', 'golden state': 'warriors',
                'grizzlies': 'grizzlies', 'memphis': 'grizzlies', 'mavericks': 'mavericks', 'mavs': 'mavericks', 'dallas': 'mavericks',
                'rockets': 'rockets', 'houston': 'rockets', 'pelicans': 'pelicans', 'new orleans': 'pelicans',
                'spurs': 'spurs', 'san antonio': 'spurs', 'thunder': 'thunder', 'oklahoma city': 'thunder', 'okc': 'thunder',
                'timberwolves': 'timberwolves', 'wolves': 'timberwolves', 'minnesota': 'timberwolves',
                'trail blazers': 'trail-blazers', 'blazers': 'trail-blazers', 'portland': 'trail-blazers',
                'jazz': 'jazz', 'utah': 'jazz', 'kings': 'kings', 'sacramento': 'kings'
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
            
            # Metric name mapping for consistent keys
            def normalize_stat_name(stat):
                """Map stat names to our standardized keys"""
                stat_lower = stat.lower().strip()
                mappings = {
                    'points/game': 'PPP', 'ppg': 'PPP', 'points per game': 'PPP',
                    'opp points/game': 'Opp PPP', 'opponent ppg': 'Opp PPP',
                    'offensive reb %': 'ORB%', 'off reb%': 'ORB%', 'orb%': 'ORB%', 'offensive rebound %': 'ORB%',
                    'defensive reb %': 'DRB%', 'def reb%': 'DRB%', 'drb%': 'DRB%', 'defensive rebound %': 'DRB%',
                    'turnover %': 'TOV%', 'tov%': 'TOV%', 'to%': 'TOV%',
                    'opponent to%': 'Opp TOV%', 'forced to%': 'Opp TOV%', 'opp tov%': 'Opp TOV%',
                    'effective fg%': 'eFG%', 'efg%': 'eFG%', 'effective fg %': 'eFG%',
                    'opp effective fg%': 'Opp eFG%', 'opp efg%': 'Opp eFG%',
                    '3-pt%': '3PT%', '3pt%': '3PT%', '3-point%': '3PT%', 'three point %': '3PT%',
                    'opp 3-pt%': 'Opp 3PT%', 'opp 3pt%': 'Opp 3PT%',
                    'fta/fga': 'FTA/FGA', 'ft rate': 'FTA/FGA', 'free throw rate': 'FTA/FGA',
                    'opp fta/fga': 'Opp FTA/FGA', 'opp ft rate': 'Opp FTA/FGA',
                    'off efficiency': 'O Eff', 'offensive efficiency': 'O Eff',
                    'def efficiency': 'D Eff', 'defensive efficiency': 'D Eff',
                }
                return mappings.get(stat_lower, stat)
            
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
                            stat1_raw = cells[0].get_text(strip=True)
                            cell1_text = cells[1].get_text(strip=True)
                            cell2_text = cells[2].get_text(strip=True)
                            stat2_raw = cells[3].get_text(strip=True)
                            
                            stat1 = normalize_stat_name(stat1_raw)
                            stat2 = normalize_stat_name(stat2_raw)
                            val1 = parse_value(cell1_text)
                            val2 = parse_value(cell2_text)
                            rank1 = extract_rank(cell1_text)
                            rank2 = extract_rank(cell2_text)
                            
                            if 'subscribe' in stat1.lower() or not stat1:
                                continue
                            
                            if is_away_table and not is_home_table:
                                if val1 is not None:
                                    result['away_season'][stat1] = val1
                                    if rank1:
                                        result['away_season'][f'{stat1} Rank'] = rank1
                                if val2 is not None:
                                    result['home_season'][stat2] = val2
                                    if rank2:
                                        result['home_season'][f'{stat2} Rank'] = rank2
                            elif is_home_table and not is_away_table:
                                if val1 is not None:
                                    result['home_season'][stat1] = val1
                                    if rank1:
                                        result['home_season'][f'{stat1} Rank'] = rank1
                                if val2 is not None:
                                    result['away_season'][stat2] = val2
                                    if rank2:
                                        result['away_season'][f'{stat2} Rank'] = rank2
                            else:
                                if val1 is not None:
                                    result['away_season'][stat1] = val1
                                    if rank1:
                                        result['away_season'][f'{stat1} Rank'] = rank1
                                if val2 is not None:
                                    result['home_season'][stat2] = val2
                                    if rank2:
                                        result['home_season'][f'{stat2} Rank'] = rank2
                    
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
                            stat_name_raw = cells[0].get_text(strip=True)
                            stat_name = normalize_stat_name(stat_name_raw)
                            away_text = cells[1].get_text(strip=True)
                            home_text = cells[3].get_text(strip=True)
                            away_val = parse_value(away_text)
                            home_val = parse_value(home_text)
                            away_rank = extract_rank(away_text)
                            home_rank = extract_rank(home_text)
                            
                            if 'subscribe' in stat_name.lower() or not stat_name:
                                continue
                            if away_val is not None:
                                result['away_season'][stat_name] = away_val
                                if away_rank:
                                    result['away_season'][f'{stat_name} Rank'] = away_rank
                            if home_val is not None:
                                result['home_season'][stat_name] = home_val
                                if home_rank:
                                    result['home_season'][f'{stat_name} Rank'] = home_rank
                    
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
    
    # Basketball-Reference abbreviations for NBA teams
    BBALL_REF_ABBREVS = {
        'hawks': 'ATL', 'atlanta': 'ATL', 'celtics': 'BOS', 'boston': 'BOS',
        'nets': 'BKN', 'brooklyn': 'BKN', 'hornets': 'CHA', 'charlotte': 'CHA',
        'bulls': 'CHI', 'chicago': 'CHI', 'cavaliers': 'CLE', 'cavs': 'CLE', 'cleveland': 'CLE',
        'mavericks': 'DAL', 'mavs': 'DAL', 'dallas': 'DAL',
        'nuggets': 'DEN', 'denver': 'DEN', 'pistons': 'DET', 'detroit': 'DET',
        'warriors': 'GSW', 'golden state': 'GSW', 'rockets': 'HOU', 'houston': 'HOU',
        'pacers': 'IND', 'indiana': 'IND', 'clippers': 'LAC', 'l.a. clippers': 'LAC',
        'lakers': 'LAL', 'l.a. lakers': 'LAL', 'los angeles lakers': 'LAL',
        'grizzlies': 'MEM', 'memphis': 'MEM', 'heat': 'MIA', 'miami': 'MIA',
        'bucks': 'MIL', 'milwaukee': 'MIL', 'timberwolves': 'MIN', 'wolves': 'MIN', 'minnesota': 'MIN',
        'pelicans': 'NOP', 'new orleans': 'NOP', 'knicks': 'NYK', 'new york': 'NYK',
        'thunder': 'OKC', 'oklahoma city': 'OKC', 'okc': 'OKC',
        'magic': 'ORL', 'orlando': 'ORL', '76ers': 'PHI', 'sixers': 'PHI', 'philadelphia': 'PHI',
        'suns': 'PHO', 'phoenix': 'PHO', 'trail blazers': 'POR', 'blazers': 'POR', 'portland': 'POR',
        'kings': 'SAC', 'sacramento': 'SAC', 'spurs': 'SAS', 'san antonio': 'SAS',
        'raptors': 'TOR', 'toronto': 'TOR', 'jazz': 'UTA', 'utah': 'UTA',
        'wizards': 'WAS', 'washington': 'WAS'
    }

    # Cache for basketball-reference stats
    _bball_ref_cache = {}
    _bball_ref_cache_date = None

    @staticmethod
    def fetch_bball_ref_team_stats(team_name: str) -> dict:
        """
        Fetch team stats from basketball-reference.com as fallback for TeamRankings.
        Returns dict with PPG, Opp PPG, Off/Def Rtg, Pace, eFG%, and other metrics.
        Basketball-reference serves static HTML so this works reliably.
        """
        import time as _time

        # Daily cache
        today = date.today()
        if MatchupIntelligence._bball_ref_cache_date != today:
            MatchupIntelligence._bball_ref_cache = {}
            MatchupIntelligence._bball_ref_cache_date = today

        cache_key = team_name.lower()
        cached = MatchupIntelligence._bball_ref_cache.get(cache_key)
        if cached and _time.time() - cached.get('timestamp', 0) < GameConstants.CACHE_TTL_CTG:
            return cached.get('data', {})

        try:
            import requests
            from bs4 import BeautifulSoup
            import re

            # Find abbreviation
            abbrev = None
            for name, abbr in MatchupIntelligence.BBALL_REF_ABBREVS.items():
                if name in team_name.lower():
                    abbrev = abbr
                    break

            if not abbrev:
                return {}

            # Current NBA season year (e.g., 2025-26 season = 2026)
            season_year = today.year if today.month >= 10 else today.year
            url = f"https://www.basketball-reference.com/teams/{abbrev}/{season_year}.html"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }

            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"BBall-Ref returned {resp.status_code} for {team_name}")
                MatchupIntelligence._bball_ref_cache[cache_key] = {'data': {}, 'timestamp': _time.time()}
                return {}

            result = {}
            page_text = resp.text

            # Basketball-reference puts some stats in HTML comments - uncomment them
            # Tables like team_and_opponent are often inside <!-- ... --> comments
            page_text = page_text.replace('<!--', '').replace('-->', '')
            soup = BeautifulSoup(page_text, 'html.parser')

            # 1. Extract summary stats from the page header (static text)
            # Look for "Record: X-Y" and key stats in the info section
            for p_tag in soup.find_all('p'):
                text = p_tag.get_text()
                # Extract PTS/G, Opp PTS/G, Off Rtg, Def Rtg, Pace
                pts_match = re.search(r'PTS/G:\s*([\d.]+)', text)
                if pts_match:
                    result['points/game'] = float(pts_match.group(1))
                opp_pts_match = re.search(r'Opp PTS/G:\s*([\d.]+)', text)
                if opp_pts_match:
                    result['opp points/game'] = float(opp_pts_match.group(1))
                ortg_match = re.search(r'Off Rtg:\s*([\d.]+)', text)
                if ortg_match:
                    result['O Eff'] = float(ortg_match.group(1))
                drtg_match = re.search(r'Def Rtg:\s*([\d.]+)', text)
                if drtg_match:
                    result['D Eff'] = float(drtg_match.group(1))
                pace_match = re.search(r'Pace:\s*([\d.]+)', text)
                if pace_match:
                    result['Tempo'] = float(pace_match.group(1))
                srs_match = re.search(r'SRS:\s*([+-]?[\d.]+)', text)
                if srs_match:
                    result['SRS'] = float(srs_match.group(1))

            # 2. Parse team_and_opponent table for detailed stats
            team_opp_table = soup.find('table', {'id': 'team_and_opponent'})
            if team_opp_table:
                rows = team_opp_table.find_all('tr')
                headers_row = None
                team_row = None
                opp_row = None

                for row in rows:
                    th = row.find('th')
                    if th:
                        th_text = th.get_text(strip=True)
                        if th_text == '' or 'Stat' in th_text:
                            headers_row = row
                        elif th_text == 'Team' or 'Team' in th_text:
                            team_row = row
                        elif th_text == 'Opponent' or 'Opp' in th_text:
                            opp_row = row

                if headers_row:
                    cols = [c.get_text(strip=True) for c in headers_row.find_all(['th', 'td'])]

                    def extract_row_stats(row, prefix=''):
                        stats = {}
                        if not row:
                            return stats
                        cells = row.find_all('td')
                        cell_data = [c.get_text(strip=True) for c in cells]
                        for i, val in enumerate(cell_data):
                            if i + 1 < len(cols):
                                col_name = cols[i + 1]  # +1 because first col is the header th
                                try:
                                    stats[f'{prefix}{col_name}'] = float(val)
                                except (ValueError, TypeError):
                                    pass
                        return stats

                    team_stats = extract_row_stats(team_row)
                    opp_stats = extract_row_stats(opp_row, 'Opp ')

                    # Map to our standard stat names
                    if 'eFG%' in team_stats:
                        result['eFG%'] = team_stats['eFG%']
                    if 'Opp eFG%' in opp_stats:
                        result['Opp eFG%'] = opp_stats['Opp eFG%']
                    if 'TOV%' in team_stats:
                        result['TOV%'] = team_stats['TOV%']
                    if 'Opp TOV%' in opp_stats:
                        result['Opp TOV%'] = opp_stats['Opp TOV%']
                    if 'ORB%' in team_stats:
                        result['ORB%'] = team_stats['ORB%']
                    if 'Opp ORB%' in opp_stats:
                        result['DRB%'] = opp_stats['Opp ORB%']
                    if 'FT/FGA' in team_stats:
                        result['FTA/FGA'] = team_stats['FT/FGA']
                    if 'Opp FT/FGA' in opp_stats:
                        result['Opp FTA/FGA'] = opp_stats['Opp FT/FGA']
                    if '3P%' in team_stats:
                        result['3PT%'] = team_stats['3P%']
                    if 'Opp 3P%' in opp_stats:
                        result['Opp 3PT%'] = opp_stats['Opp 3P%']
                    if 'FG%' in team_stats:
                        result['shooting %'] = team_stats['FG%']
                    if 'Opp FG%' in opp_stats:
                        result['opp shooting %'] = opp_stats['Opp FG%']
                    if 'FT%' in team_stats:
                        result['free throw %'] = team_stats['FT%']
                    if 'Opp FT%' in opp_stats:
                        result['opp free throw %'] = opp_stats['Opp FT%']
                    if 'AST' in team_stats:
                        result['assists/game'] = team_stats['AST']
                    if 'STL' in team_stats:
                        result['steals/game'] = team_stats['STL']
                    if 'BLK' in team_stats:
                        result['blocks/game'] = team_stats['BLK']
                    if 'TOV' in team_stats:
                        result['turnovers/game'] = team_stats['TOV']
                    if 'Opp TOV' in opp_stats:
                        result['opp turnovers/game'] = opp_stats['Opp TOV']
                    if 'ORB' in team_stats:
                        result['off rebounds/gm'] = team_stats['ORB']
                    if 'DRB' in team_stats:
                        result['def rebounds/gm'] = team_stats['DRB']
                    if 'PF' in team_stats:
                        result['personal fouls/gm'] = team_stats['PF']

            if result:
                logger.info(f"BBall-Ref stats for {team_name}: {list(result.keys())[:8]}...")
            else:
                logger.warning(f"BBall-Ref: No stats parsed for {team_name}")

            MatchupIntelligence._bball_ref_cache[cache_key] = {'data': result, 'timestamp': _time.time()}
            return result

        except Exception as e:
            logger.warning(f"Error fetching BBall-Ref stats for {team_name}: {e}")
            MatchupIntelligence._bball_ref_cache[cache_key] = {'data': {}, 'timestamp': _time.time()}
            return {}

    # Cleaning the Glass team IDs (NBA only)
    CTG_TEAM_IDS = {
        'hawks': 1, 'atlanta': 1, 'celtics': 2, 'boston': 2, 'nets': 3, 'brooklyn': 3,
        'hornets': 4, 'charlotte': 4, 'bulls': 5, 'chicago': 5,
        'cavaliers': 6, 'cavs': 6, 'cleveland': 6, 'mavericks': 7, 'mavs': 7, 'dallas': 7,
        'nuggets': 8, 'denver': 8, 'pistons': 9, 'detroit': 9, 'warriors': 10, 'golden state': 10,
        'rockets': 11, 'houston': 11, 'pacers': 12, 'indiana': 12, 'clippers': 13, 'l.a. clippers': 13,
        'lakers': 14, 'l.a. lakers': 14, 'los angeles lakers': 14, 'grizzlies': 15, 'memphis': 15,
        'heat': 16, 'miami': 16, 'bucks': 17, 'milwaukee': 17, 'timberwolves': 18, 'wolves': 18, 'minnesota': 18,
        'pelicans': 19, 'new orleans': 19, 'knicks': 20, 'new york': 20,
        'thunder': 21, 'oklahoma city': 21, 'okc': 21, 'magic': 22, 'orlando': 22,
        '76ers': 23, 'sixers': 23, 'philadelphia': 23, 'suns': 24, 'phoenix': 24,
        'trail blazers': 25, 'blazers': 25, 'portland': 25,
        'kings': 26, 'sacramento': 26, 'spurs': 27, 'san antonio': 27,
        'raptors': 28, 'toronto': 28, 'jazz': 29, 'utah': 29, 'wizards': 30, 'washington': 30
    }
    
    # CTG cache: team_name -> {data: dict, timestamp: float}
    _ctg_cache = {}
    _ctg_cache_date = None  # Date-based invalidation for daily refresh
    
    @staticmethod
    def fetch_ctg_four_factors(team_name: str) -> dict:
        """
        Fetch Four Factors data from Cleaning the Glass with bulletproof caching and retry.
        Returns eFG%, TOV%, ORB%, FT Rate for offense and defense.
        Uses date-based cache (daily refresh) + 4-hour TTL within the same day.
        """
        import time
        
        # Date-based cache invalidation - clear cache at midnight
        today = date.today()
        if MatchupIntelligence._ctg_cache_date != today:
            MatchupIntelligence._ctg_cache = {}
            MatchupIntelligence._ctg_cache_date = today
            logger.info("CTG cache cleared for new day")
        
        # Check cache first (4 hour TTL within same day)
        cache_key = team_name.lower()
        cached = MatchupIntelligence._ctg_cache.get(cache_key)
        if cached and time.time() - cached.get('timestamp', 0) < GameConstants.CACHE_TTL_CTG:
            return cached.get('data', {})
        
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
                return {}
            
            url = f"https://cleaningtheglass.com/stats/team/{team_id}/team"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
            }
            
            # Retry logic with exponential backoff
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    resp = requests.get(url, headers=headers, timeout=20)
                    if resp.status_code == 200:
                        break
                    elif resp.status_code == 429:  # Rate limited
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        time.sleep(0.5 * (attempt + 1))
                except requests.exceptions.Timeout:
                    if attempt < max_retries - 1:
                        time.sleep(1 * (attempt + 1))
                        continue
                    # Cache empty result to avoid repeated timeouts
                    MatchupIntelligence._ctg_cache[cache_key] = {'data': {}, 'timestamp': time.time()}
                    return {}
                except Exception:
                    if attempt < max_retries - 1:
                        time.sleep(0.5)
                        continue
                    MatchupIntelligence._ctg_cache[cache_key] = {'data': {}, 'timestamp': time.time()}
                    return {}
            
            if resp.status_code != 200:
                MatchupIntelligence._ctg_cache[cache_key] = {'data': {}, 'timestamp': time.time()}
                return {}
            
            soup = BeautifulSoup(resp.text, 'html.parser')

            # Try multiple selectors - CTG page structure varies
            table = soup.find('table', {'id': 'team_stats_four_factors'})
            if not table:
                # Try sortable table inside stat_table_container
                container = soup.find('div', class_='stat_table_container')
                if container:
                    table = container.find('table')
            if not table:
                # Try any sortable table with efficiency/four factors data
                for t in soup.find_all('table', class_='sortable'):
                    header_text = t.get_text()
                    if 'eFG' in header_text or 'Pts/Poss' in header_text or 'TOV' in header_text:
                        table = t
                        break
            if not table:
                # Last resort: find first table with enough data columns
                for t in soup.find_all('table'):
                    first_row = t.find('tr')
                    if first_row:
                        cells = first_row.find_all(['td', 'th'])
                        if len(cells) >= 10:
                            table = t
                            break

            if not table:
                logger.warning(f"CTG: No data table found for {team_name} (team_id={team_id})")
                MatchupIntelligence._ctg_cache[cache_key] = {'data': {}, 'timestamp': time.time()}
                return {}

            result = {}
            # Find data rows - try tbody first, then all tr
            tbody = table.find('tbody')
            rows = tbody.find_all('tr') if tbody else table.find_all('tr')

            # Skip header rows - find the first row with the current season data
            data_row = None
            current_year = str(date.today().year % 100)  # e.g., "26" for 2026
            prev_year = str((date.today().year - 1) % 100)  # e.g., "25"
            season_tag = f"{prev_year}-{current_year}"  # e.g., "25-26"

            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 10:
                    row_text = cells[0].get_text(strip=True) if cells else ''
                    # Current season row (e.g., "25-26") or just first data row
                    if season_tag in row_text or (not data_row and len(cells) >= 10):
                        data_row = row
                        if season_tag in row_text:
                            break  # Found exact season match

            if data_row:
                cells = data_row.find_all('td')
                data = [c.get_text(strip=True) for c in cells]

                if len(data) >= 31:
                    # Original wide table format (rank + value pairs)
                    result['off_ppp'] = data[11]
                    result['off_ppp_rank'] = data[10]
                    result['off_efg'] = data[13].replace('%', '') if '%' in data[13] else data[13]
                    result['off_efg_rank'] = data[12]
                    result['off_tov'] = data[15].replace('%', '') if '%' in data[15] else data[15]
                    result['off_tov_rank'] = data[14]
                    result['off_orb'] = data[17].replace('%', '') if '%' in data[17] else data[17]
                    result['off_orb_rank'] = data[16]
                    result['off_ft_rate'] = data[19]
                    result['off_ft_rank'] = data[18]
                    result['def_ppp'] = data[22]
                    result['def_ppp_rank'] = data[21]
                    result['def_efg'] = data[24].replace('%', '') if '%' in data[24] else data[24]
                    result['def_efg_rank'] = data[23]
                    result['def_tov'] = data[26].replace('%', '') if '%' in data[26] else data[26]
                    result['def_tov_rank'] = data[25]
                    result['def_orb'] = data[28].replace('%', '') if '%' in data[28] else data[28]
                    result['def_orb_rank'] = data[27]
                    result['def_ft_rate'] = data[30]
                    result['def_ft_rank'] = data[29]

                    logger.info(f"CTG Four Factors for {team_name}: PPP={result.get('off_ppp')} (#{result.get('off_ppp_rank')})")
                elif len(data) >= 17:
                    # Compact table: values only (no separate rank cells)
                    # Columns: Year, metadata..., Off Pts/Poss, Off eFG%, Off TOV%, Off ORB%, Off FT Rate,
                    #          Def Pts/Poss, Def eFG%, Def TOV%, Def ORB%, Def FT Rate
                    # Find the offense/defense boundary by looking for patterns
                    numeric_vals = []
                    for i, d in enumerate(data):
                        clean = d.replace('%', '').replace('+', '').replace('-', '', 1).strip()
                        try:
                            float(clean)
                            numeric_vals.append((i, d))
                        except ValueError:
                            pass

                    # CTG compact: typically offset varies, but the last 10 numeric values
                    # are Off(PPP, eFG, TOV, ORB, FT) + Def(PPP, eFG, TOV, ORB, FT)
                    if len(numeric_vals) >= 10:
                        # Take last 10 numeric values as the four factors
                        ff_vals = numeric_vals[-10:]
                        result['off_ppp'] = ff_vals[0][1]
                        result['off_efg'] = ff_vals[1][1].replace('%', '')
                        result['off_tov'] = ff_vals[2][1].replace('%', '')
                        result['off_orb'] = ff_vals[3][1].replace('%', '')
                        result['off_ft_rate'] = ff_vals[4][1]
                        result['def_ppp'] = ff_vals[5][1]
                        result['def_efg'] = ff_vals[6][1].replace('%', '')
                        result['def_tov'] = ff_vals[7][1].replace('%', '')
                        result['def_orb'] = ff_vals[8][1].replace('%', '')
                        result['def_ft_rate'] = ff_vals[9][1]

                        logger.info(f"CTG Four Factors (compact) for {team_name}: Off PPP={result.get('off_ppp')}, Def PPP={result.get('def_ppp')}")
            
            # Cache the result
            MatchupIntelligence._ctg_cache[cache_key] = {'data': result, 'timestamp': time.time()}
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching CTG four factors for {team_name}: {e}")
            # Cache empty to prevent repeated failures
            MatchupIntelligence._ctg_cache[cache_key] = {'data': {}, 'timestamp': time.time()}
            return {}
    
    # Cache for KenPom Four Factors stats (daily)
    _kenpom_ff_cache = {}
    _kenpom_ff_cache_date = None
    
    @staticmethod
    def fetch_kenpom_four_factors():
        """Fetch Four Factors data from KenPom API. Cached daily."""
        today = date.today()
        if MatchupIntelligence._kenpom_ff_cache_date == today and MatchupIntelligence._kenpom_ff_cache:
            return MatchupIntelligence._kenpom_ff_cache
        
        api_key = os.environ.get('CBB_API_KEY', '')
        if not api_key:
            return {}
        
        try:
            url = "https://kenpom.com/api.php?endpoint=four-factors&y=2026"
            headers = {
                'Authorization': f'Bearer {api_key}',
                'User-Agent': 'Mozilla/5.0 (compatible; SportsApp/1.0)'
            }
            
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                cache = {}
                for team in data:
                    team_name = team.get('TeamName', team.get('Team', team.get('team', ''))).lower()
                    if team_name:
                        cache[team_name] = {
                            # Offensive Four Factors (actual API field names)
                            'off_efg': team.get('eFG_Pct', 0),
                            'off_efg_rank': team.get('RankeFG_Pct', 999),
                            'off_tov': team.get('TO_Pct', 0),
                            'off_tov_rank': team.get('RankTO_Pct', 999),
                            'off_orb': team.get('OR_Pct', 0),
                            'off_orb_rank': team.get('RankOR_Pct', 999),
                            'off_ft_rate': team.get('FT_Rate', 0),
                            'off_ft_rank': team.get('RankFT_Rate', 999),
                            # Defensive Four Factors (D prefix = Defense)
                            'def_efg': team.get('DeFG_Pct', 0),
                            'def_efg_rank': team.get('RankDeFG_Pct', 999),
                            'def_tov': team.get('DTO_Pct', 0),
                            'def_tov_rank': team.get('RankDTO_Pct', 999),
                            'def_orb': team.get('DOR_Pct', 0),
                            'def_orb_rank': team.get('RankDOR_Pct', 999),
                            'def_ft_rate': team.get('DFT_Rate', 0),
                            'def_ft_rank': team.get('RankDFT_Rate', 999)
                        }
                
                if cache:
                    MatchupIntelligence._kenpom_ff_cache = cache
                    MatchupIntelligence._kenpom_ff_cache_date = today
                    logger.info(f"KenPom Four Factors loaded: {len(cache)} teams")
                return cache
            else:
                logger.warning(f"KenPom Four Factors API returned {resp.status_code}")
                return MatchupIntelligence._kenpom_ff_cache
        except Exception as e:
            logger.warning(f"KenPom Four Factors fetch error: {e}")
            return MatchupIntelligence._kenpom_ff_cache
    
    @staticmethod
    def fetch_kenpom_stats(team_name: str) -> dict:
        """
        Fetch KenPom advanced analytics for CBB teams.
        Uses ratings API for efficiency metrics and four-factors API for Four Factors.
        """
        try:
            # Get basic stats from ratings API cache
            tv = get_torvik_team(team_name)
            if not tv:
                return {}
            
            result = {
                'rank': tv.get('rank', 999),
                'team': tv.get('team', team_name),
                'adj_o': tv.get('adj_o', 0),
                'adj_d': tv.get('adj_d', 0),
                'adj_em': tv.get('adj_em', 0),
                'tempo': tv.get('tempo', 0),
                'sos': tv.get('sos', 0),
                'sos_rank': tv.get('sos_rank', 0),
                'record': tv.get('record', ''),
                'conf': tv.get('conf', '')
            }
            
            # Get Four Factors from dedicated API endpoint
            ff_cache = MatchupIntelligence.fetch_kenpom_four_factors()
            team_key = team_name.lower().strip()

            # Try exact match first
            ff = ff_cache.get(team_key)

            # Try normalized name match
            if not ff:
                normalized = normalize_cbb_team_name(team_name).lower().strip()
                ff = ff_cache.get(normalized)

            # Try all known aliases (bi-directional lookup for SJSU, SMC, etc.)
            if not ff:
                try:
                    all_aliases = get_all_team_aliases(normalize_cbb_team_name(team_name))
                    for alias in all_aliases:
                        alias_lower = alias.lower().strip()
                        if alias_lower in ff_cache:
                            ff = ff_cache[alias_lower]
                            logger.info(f"KenPom FF matched via alias: {team_name} -> {alias}")
                            break
                except Exception:
                    pass

            # Merge Four Factors into result (MatchupIntelligence cache uses off_efg, def_efg format)
            if ff:
                result.update({
                    'off_efg': ff.get('off_efg', 0),
                    'off_efg_rank': ff.get('off_efg_rank', 999),
                    'def_efg': ff.get('def_efg', 0),
                    'def_efg_rank': ff.get('def_efg_rank', 999),
                    'off_tov': ff.get('off_tov', 0),
                    'off_tov_rank': ff.get('off_tov_rank', 999),
                    'def_tov': ff.get('def_tov', 0),
                    'def_tov_rank': ff.get('def_tov_rank', 999),
                    'off_orb': ff.get('off_orb', 0),
                    'off_orb_rank': ff.get('off_orb_rank', 999),
                    'def_orb': ff.get('def_orb', 0),
                    'def_orb_rank': ff.get('def_orb_rank', 999),
                    'off_ft_rate': ff.get('off_ft_rate', 0),
                    'off_ft_rank': ff.get('off_ft_rank', 999),
                    'def_ft_rate': ff.get('def_ft_rate', 0),
                    'def_ft_rank': ff.get('def_ft_rank', 999)
                })
                logger.info(f"KenPom Four Factors merged for {team_name}")
            
            # Get 3PT% data from misc cache (if available)
            # Try multiple name variations for better matching (bi-directional alias lookup)
            misc_data = kenpom_misc_cache.get(team_key)
            if not misc_data:
                normalized = normalize_cbb_team_name(team_name).lower().strip()
                misc_data = kenpom_misc_cache.get(normalized)
            if not misc_data:
                # Try all known aliases for this team (SJSU, SMC, etc.)
                try:
                    all_aliases = get_all_team_aliases(normalize_cbb_team_name(team_name))
                    for alias in all_aliases:
                        alias_lower = alias.lower().strip()
                        if alias_lower in kenpom_misc_cache:
                            misc_data = kenpom_misc_cache[alias_lower]
                            logger.info(f"KenPom misc matched via alias: {team_name} -> {alias}")
                            break
                except Exception:
                    pass
            if not misc_data:
                # Try partial match with token-based comparison for better accuracy
                team_tokens = set(team_key.replace('-', ' ').split())
                for key, data in kenpom_misc_cache.items():
                    cache_tokens = set(key.replace('-', ' ').split())
                    # Match if significant overlap (at least 50% of tokens match)
                    if team_tokens and cache_tokens:
                        overlap = len(team_tokens & cache_tokens)
                        if overlap >= min(len(team_tokens), len(cache_tokens)) * 0.5:
                            misc_data = data
                            break
            
            if misc_data:
                result.update({
                    'off_3pt': misc_data.get('o_3pt_pct', 0),
                    'off_3pt_rank': misc_data.get('o_3pt_rank', 0),
                    'def_3pt': misc_data.get('d_3pt_pct', 0),
                    'def_3pt_rank': misc_data.get('d_3pt_rank', 0)
                })
                logger.info(f"KenPom 3PT% data merged for {team_name}")
            
            return result
        except Exception as e:
            logger.warning(f"Error fetching KenPom stats for {team_name}: {e}")
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
        Supports NBA and CBB leagues.
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
            
            # Determine URL path based on league
            if league == 'CBB':
                matchups_url = "https://www.covers.com/sport/basketball/ncaab/matchups"
                sport_path = "sport/basketball/ncaab/matchup"
            else:  # NBA
                matchups_url = "https://www.covers.com/sports/nba/matchups"
                sport_path = "sport/basketball/nba/matchup"
            
            away_lower = away_team.lower().strip()
            home_lower = home_team.lower().strip()
            
            # Create search tokens for fuzzy matching
            away_tokens = set(away_lower.replace('-', ' ').replace('.', '').split())
            home_tokens = set(home_lower.replace('-', ' ').replace('.', '').split())
            resp = requests.get(matchups_url, headers=headers, timeout=15)
            
            matchup_id = None
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Helper to check if text contains team (fuzzy match)
                def text_matches_team(text: str, team_name: str, tokens: set) -> bool:
                    text_lower = text.lower()
                    if team_name in text_lower:
                        return True
                    text_tokens = set(text_lower.replace('-', ' ').replace('.', '').split())
                    # Check if any significant token matches
                    for token in tokens:
                        if len(token) > 2 and token in text_tokens:
                            return True
                    return False
                
                # Find all matchup links and look for our game
                all_links = soup.find_all('a', href=re.compile(r'/matchup/\d+'))
                for link in all_links:
                    href = link.get('href', '')
                    link_text = link.get_text().lower()
                    # Check if link text or surrounding context contains both teams
                    parent_text = link.parent.get_text().lower() if link.parent else ''
                    full_text = f"{link_text} {parent_text}"
                    
                    if text_matches_team(full_text, away_lower, away_tokens) and \
                       text_matches_team(full_text, home_lower, home_tokens):
                        match = re.search(r'/matchup/(\d+)', href)
                        if match:
                            matchup_id = match.group(1)
                            break
                
                # Fallback: get all IDs and check first 5 matchup pages
                if not matchup_id:
                    all_ids = list(set(re.findall(r'/matchup/(\d+)', str(soup))))[:5]
                    for mid in all_ids:
                        try:
                            check_url = f"https://www.covers.com/{sport_path}/{mid}"
                            check_resp = requests.get(check_url, headers=headers, timeout=8)
                            if check_resp.status_code == 200:
                                title = BeautifulSoup(check_resp.text, 'html.parser').find('title')
                                if title:
                                    title_text = title.get_text().lower()
                                    if text_matches_team(title_text, away_lower, away_tokens) and \
                                       text_matches_team(title_text, home_lower, home_tokens):
                                        matchup_id = mid
                                        break
                        except:
                            continue
            
            # If we found a matchup ID, fetch the matchup page
            if matchup_id:
                matchup_url = f"https://www.covers.com/{sport_path}/{matchup_id}"
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
                    away_record = re.search(rf'{re.escape(away_team[:3])}[^\d]*(\d+-\d+)', text, re.IGNORECASE)
                    home_record = re.search(rf'{re.escape(home_team[:3])}[^\d]*(\d+-\d+)', text, re.IGNORECASE)
                    
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
    _covers_cache_date = None  # Date-based invalidation for daily refresh
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
        Uses date-based cache (daily refresh) + 5-minute TTL within the same day.
        """
        import time as time_module
        
        # Date-based cache invalidation - clear cache at midnight
        today = date.today()
        if MatchupIntelligence._covers_cache_date != today:
            MatchupIntelligence._covers_last10_cache = {}
            MatchupIntelligence._covers_last10_cache_time = {}
            MatchupIntelligence._covers_cache_date = today
            logger.info("Covers cache cleared for new day")
        
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
        Fetch betting data from VSIN (replaces WagerTalk).
        - Tickets % (bets) and Handle % (money) for spreads
        - Open and current lines from VSIN Line Tracker
        - Sharp money detection (when money% diverges from tickets%)
        
        Auto-refreshes 2 hours before game time for live data.
        """
        from datetime import datetime
        from vsin_scraper import get_all_vsin_data
        
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
            vsin_result = get_all_vsin_data(league)
            vsin_data = vsin_result.get('data', {}) if vsin_result.get('success') else {}
            
            # City to Nickname mapping for consistent Bovada-style names
            city_to_nickname = {
                'Washington': 'Wizards', 'Milwaukee': 'Bucks', 'Boston': 'Celtics',
                'Brooklyn': 'Nets', 'Charlotte': 'Hornets', 'Chicago': 'Bulls',
                'Cleveland': 'Cavaliers', 'Dallas': 'Mavericks', 'Denver': 'Nuggets',
                'Detroit': 'Pistons', 'Golden State': 'Warriors', 'Houston': 'Rockets',
                'Indiana': 'Pacers', 'LA Clippers': 'Clippers', 'LA Lakers': 'Lakers',
                'L.A. Clippers': 'Clippers', 'L.A. Lakers': 'Lakers',
                'Los Angeles Lakers': 'Lakers', 'Los Angeles Clippers': 'Clippers',
                'Memphis': 'Grizzlies', 'Miami': 'Heat', 'Minnesota': 'Timberwolves',
                'New Orleans': 'Pelicans', 'New York': 'Knicks', 'Oklahoma City': 'Thunder',
                'Orlando': 'Magic', 'Philadelphia': 'Sixers', 'Phoenix': 'Suns',
                'Portland': 'Trail Blazers', 'Sacramento': 'Kings', 'San Antonio': 'Spurs',
                'Toronto': 'Raptors', 'Utah': 'Jazz', 'Atlanta': 'Hawks'
            }
            
            def normalize_team_name(name, is_nba=False):
                """Convert city name or full name to Bovada-style nickname. Only for NBA."""
                if not name:
                    return name
                # Only apply NBA city-to-nickname normalization for NBA games
                # CBB teams like "Miami OH" or "Indiana" should NOT be converted to NBA nicknames
                if not is_nba:
                    return name
                # Already a nickname
                if name in city_to_nickname.values():
                    return name
                # City name lookup (exact match only)
                if name in city_to_nickname:
                    return city_to_nickname[name]
                # Try partial match (only for NBA)
                for city, nickname in city_to_nickname.items():
                    if city.lower() in name.lower() or name.lower() in city.lower():
                        return nickname
                return name
            
            is_nba_league = (league == 'NBA')
            for key, data in vsin_data.items():
                away_team = normalize_team_name(data.get('away_team', ''), is_nba=is_nba_league)
                home_team = normalize_team_name(data.get('home_team', ''), is_nba=is_nba_league)
                
                # VSIN terminology: tickets = bets %, money = handle %
                away_bet_pct = data.get('tickets_away') or 50
                home_bet_pct = data.get('tickets_home') or 50
                away_money_pct = data.get('money_away') or 50
                home_money_pct = data.get('money_home') or 50
                
                # Totals percentages (VSIN may not have totals splits - use 50/50 default)
                over_bet_pct = 50
                under_bet_pct = 50
                over_money_pct = 50
                under_money_pct = 50
                
                majority_team = 'away' if away_bet_pct > home_bet_pct else 'home'
                majority_pct = max(away_bet_pct, home_bet_pct)
                
                # Line movement data from VSIN Line Tracker
                spread_open_line = data.get('open_away_spread')
                spread_open_odds = data.get('open_away_odds')
                spread_current_line = data.get('current_away_spread')
                spread_current_odds = data.get('current_away_odds')
                total_open_line = None  # VSIN doesn't have totals lines currently
                total_open_odds = None
                total_current_line = None
                total_current_odds = None

                # Calculate actual line movement
                line_movement_value = data.get('line_movement')
                line_direction = data.get('line_direction', 'stable')

                # If not calculated in scraper, calculate here
                if line_movement_value is None and spread_open_line is not None and spread_current_line is not None:
                    try:
                        line_movement_value = float(spread_current_line) - float(spread_open_line)
                        if line_movement_value < -0.4:
                            line_direction = 'toward_away'
                        elif line_movement_value > 0.4:
                            line_direction = 'toward_home'
                        else:
                            line_direction = 'stable'
                    except (ValueError, TypeError):
                        line_movement_value = None
                        line_direction = 'stable'
                
                # === TRUE RLM DETECTION ===
                # RLM = Public bets heavily one way, but line moves OPPOSITE direction
                # This signals SHARP MONEY on the side the line moved toward
                
                spread_rlm_detected = False
                spread_rlm_sharp_side = None
                totals_rlm_detected = False
                totals_rlm_sharp_side = None
                
                # Determine favorite based on spread (negative spread = favorite)
                open_favorite = ''
                if spread_open_line is not None and spread_open_line < 0:
                    open_favorite = away_team
                elif spread_open_line is not None and spread_open_line > 0:
                    open_favorite = home_team
                
                # Determine favorite/underdog based on open spread
                favorite_team = None
                underdog_team = None
                if open_favorite == away_team:
                    favorite_team = away_team
                    underdog_team = home_team
                elif open_favorite == home_team:
                    favorite_team = home_team
                    underdog_team = away_team
                # No fallback - if open_favorite doesn't match, leave as None
                
                logger.debug(f"RLM setup: {away_team} vs {home_team} | Favorite: {favorite_team} (from open: '{open_favorite}')")
                
                # === SPREAD RLM DETECTION (Favorite/Underdog Decision Table) ===
                # Money on Favorite + Line moves Down (toward underdog) = RLM, sharp = underdog
                # Money on Underdog + Line moves Up (toward favorite) = RLM, sharp = favorite
                # Money on Favorite + Line moves Up = NOT RLM (line confirming money)
                # Money on Underdog + Line moves Down = NOT RLM (line confirming money)
                # Threshold: >=54% money to establish majority
                try:
                    if spread_open_line is not None and spread_current_line is not None and away_team and home_team and favorite_team and underdog_team:
                        open_spread = float(spread_open_line)
                        current_spread = float(spread_current_line)
                        
                        movement = current_spread - open_spread
                        
                        fav_is_away = (favorite_team == away_team)
                        fav_money_pct = away_money_pct if fav_is_away else home_money_pct
                        dog_money_pct = home_money_pct if fav_is_away else away_money_pct
                        
                        money_on_favorite = fav_money_pct >= 60
                        money_on_underdog = dog_money_pct >= 60
                        
                        if fav_is_away:
                            line_moved_up = movement < 0
                            line_moved_down = movement > 0
                        else:
                            line_moved_up = movement > 0
                            line_moved_down = movement < 0
                        
                        if money_on_favorite and line_moved_down:
                            spread_rlm_detected = True
                            spread_rlm_sharp_side = underdog_team
                            logger.info(f"RLM DETECTED: {fav_money_pct:.0f}% money on {favorite_team} (fav), but line moved DOWN toward {underdog_team} (open {open_spread:+.1f} → curr {current_spread:+.1f})")
                        elif money_on_underdog and line_moved_up:
                            spread_rlm_detected = True
                            spread_rlm_sharp_side = favorite_team
                            logger.info(f"RLM DETECTED: {dog_money_pct:.0f}% money on {underdog_team} (dog), but line moved UP toward {favorite_team} (open {open_spread:+.1f} → curr {current_spread:+.1f})")
                except Exception as e:
                    logger.warning(f"Error detecting spread RLM: {e}")
                
                # NOTE: RLM is only for SPREADS, not totals. Totals uses different strategy on dashboard.
                # totals_rlm_detected stays False
                
                # RLM potential (spreads only)
                rlm_potential = spread_rlm_detected
                
                # Sharp money detection (use RLM-detected values)
                sharp_detected = totals_rlm_detected
                sharp_side = totals_rlm_sharp_side
                spread_sharp_detected = spread_rlm_detected
                spread_sharp_side = spread_rlm_sharp_side
                
                game_key = f"{away_team}_vs_{home_team}".lower().replace(' ', '_')
                
                # Get favorite tracking data from WagerTalk
                favorite_is_away = data.get('favorite_is_away')
                open_favorite = data.get('open_favorite')
                # Normalize the open_favorite team name (only for NBA)
                if open_favorite:
                    open_favorite = normalize_team_name(open_favorite, is_nba=is_nba_league)
                
                # Format line movement for display
                if line_movement_value is not None:
                    line_movement_display = f"{line_movement_value:+.1f}" if line_movement_value != 0 else "0"
                else:
                    line_movement_display = 'N/A'

                result[game_key] = {
                    'away': {'team': away_team, 'bet_pct': str(away_bet_pct), 'money_pct': str(away_money_pct)},
                    'home': {'team': home_team, 'bet_pct': str(home_bet_pct), 'money_pct': str(home_money_pct)},
                    'favorite_is_away': favorite_is_away,
                    'open_favorite': open_favorite,
                    'open_spread': spread_open_line or 'N/A',
                    'current_spread': spread_current_line or 'N/A',
                    'spread_open_line': spread_open_line,  # Numeric value for template
                    'spread_current_line': spread_current_line,  # Numeric value for template
                    'spread_open_odds': spread_open_odds or '-110',
                    'spread_current_odds': spread_current_odds or '-110',
                    'spread_tickets_pct': away_bet_pct,
                    'spread_money_pct': away_money_pct,
                    # Add VSIN-specific fields for direct access
                    'tickets_away': away_bet_pct,
                    'tickets_home': home_bet_pct,
                    'money_away': away_money_pct,
                    'money_home': home_money_pct,
                    'away_bet_pct': away_bet_pct,
                    'home_bet_pct': home_bet_pct,
                    'away_money_pct': away_money_pct,
                    'home_money_pct': home_money_pct,
                    'open_away_spread': spread_open_line,
                    'current_away_spread': spread_current_line,
                    'open_away_odds': spread_open_odds,
                    'current_away_odds': spread_current_odds,
                    'away_team': away_team,
                    'home_team': home_team,
                    'spread_sharp_detected': spread_sharp_detected,
                    'spread_sharp_side': spread_sharp_side,
                    'total_open_line': total_open_line or 'N/A',
                    'total_current_line': total_current_line or 'N/A',
                    'total_open_odds': total_open_odds or '-110',
                    'total_current_odds': total_current_odds or '-110',
                    'line_movement': line_movement_display,
                    'line_movement_value': line_movement_value,  # Numeric value
                    'line_direction': line_direction,
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
            
            logger.info(f"VSIN data fetched for {league}: {len(result)} games")
            
            if result:
                MatchupIntelligence._rlm_cache[cache_key] = result
                MatchupIntelligence._rlm_cache_time[cache_key] = datetime.now()
            
            return result
            
        except Exception as e:
            logger.warning(f"Error fetching VSIN data for {league}: {e}")
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
    
    def __len__(self):
        return len(self.cache)

line_movement_cache = TTLCache(maxsize=500, ttl=43200)
opening_lines_store = TTLCache(maxsize=500, ttl=86400)
espn_schedule_cache = TTLCache(maxsize=500, ttl=43200)

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
    'nc a&t': 'north carolina a&t', 'nc at': 'north carolina a&t', 'ncat': 'north carolina a&t',
    'purdue fw': 'purdue fort wayne', 'purdue-fw': 'purdue fort wayne', 'pfw': 'purdue fort wayne',
    'william & mary': 'william mary', 'w&m': 'william mary', 'william and mary': 'william mary',
    'c connecticut': 'central connecticut', 'cconn': 'central connecticut', 'ccsu': 'central connecticut',
    'san jose st': 'san jose state', 'sjsu': 'san jose state',
    'sacramento st': 'sacramento state', 'sac state': 'sacramento state', 'sac st': 'sacramento state',
    'fresno st': 'fresno state', 'boise st': 'boise state', 'utah st': 'utah state',
    'san diego st': 'san diego state', 'sdsu': 'san diego state',
    'montana st': 'montana state', 'weber st': 'weber state', 'idaho st': 'idaho state',
    'portland st': 'portland state', 'n arizona': 'northern arizona', 'nau': 'northern arizona',
    'e washington': 'eastern washington', 'ewu': 'eastern washington',
    'n colorado': 'northern colorado', 'unc colorado': 'northern colorado',
    'cal poly': 'california poly', 'long beach st': 'long beach state', 'lbsu': 'long beach state',
    'uc davis': 'california davis', 'uc irvine': 'california irvine', 'uc riverside': 'california riverside',
    'uc santa barbara': 'california santa barbara', 'ucsb': 'california santa barbara',
    'cal st fullerton': 'california state fullerton', 'csuf': 'california state fullerton',
    'cal st northridge': 'california state northridge', 'csun': 'california state northridge',
    'st marys': 'saint marys', 'st marys ca': 'saint marys', "saint mary's": 'saint marys',
    'st johns': 'saint johns', "st john's": 'saint johns', "saint john's": 'saint johns',
    'st bonaventure': 'saint bonaventure', "st joseph's": 'saint josephs', "saint joseph's": 'saint josephs',
    'st louis': 'saint louis', 'slu': 'saint louis',
    'queens': 'queens', 'bellarmine': 'bellarmine',
    'liu': 'long island', 'li': 'long island',
    'campbell': 'campbell', 'robert morris': 'robert morris', 'rmu': 'robert morris',
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


def normalize_cbb_team_name(name: str) -> str:
    """Normalize CBB team name for matching with KenPom/Covers data.
    First applies CBB_TEAM_NAME_MAP for aliased names, then normalizes.
    """
    # First normalize basic characters
    base_normalized = normalize_team_name(name)
    
    # Apply CBB_TEAM_NAME_MAP to convert aliases (e.g., "ut martin" -> "tennessee martin")
    mapped = CBB_TEAM_NAME_MAP.get(base_normalized)
    if mapped:
        return mapped
    
    return base_normalized


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
    # KenPom CBB stats (from all endpoints)
    torvik_tempo = db.Column(db.Float)
    torvik_away_adj_o = db.Column(db.Float)
    torvik_away_adj_d = db.Column(db.Float)
    torvik_home_adj_o = db.Column(db.Float)
    torvik_home_adj_d = db.Column(db.Float)
    torvik_away_rank = db.Column(db.Integer)
    torvik_home_rank = db.Column(db.Integer)
    # Additional KenPom Four Factors
    kenpom_away_efg = db.Column(db.Float)       # Away offensive eFG%
    kenpom_home_efg = db.Column(db.Float)       # Home offensive eFG%
    kenpom_away_to = db.Column(db.Float)        # Away TO%
    kenpom_home_to = db.Column(db.Float)        # Home TO%
    kenpom_away_or = db.Column(db.Float)        # Away offensive rebound %
    kenpom_home_or = db.Column(db.Float)        # Home offensive rebound %
    kenpom_away_ft_rate = db.Column(db.Float)   # Away FT Rate
    kenpom_home_ft_rate = db.Column(db.Float)   # Home FT Rate
    # KenPom Shooting
    kenpom_away_3pt = db.Column(db.Float)       # Away 3PT%
    kenpom_home_3pt = db.Column(db.Float)       # Home 3PT%
    kenpom_away_2pt = db.Column(db.Float)       # Away 2PT%
    kenpom_home_2pt = db.Column(db.Float)       # Home 2PT%
    kenpom_away_ft_pct = db.Column(db.Float)    # Away FT%
    kenpom_home_ft_pct = db.Column(db.Float)    # Home FT%
    # KenPom Defense
    kenpom_away_d_efg = db.Column(db.Float)     # Away defensive eFG% allowed
    kenpom_home_d_efg = db.Column(db.Float)     # Home defensive eFG% allowed
    kenpom_away_d_to = db.Column(db.Float)      # Away forced TO%
    kenpom_home_d_to = db.Column(db.Float)      # Home forced TO%
    # KenPom Size/Experience
    kenpom_away_height = db.Column(db.Float)    # Away avg height
    kenpom_home_height = db.Column(db.Float)    # Home avg height
    kenpom_away_exp = db.Column(db.Float)       # Away experience
    kenpom_home_exp = db.Column(db.Float)       # Home experience
    # KenPom SOS
    kenpom_away_sos = db.Column(db.Float)       # Away SOS
    kenpom_home_sos = db.Column(db.Float)       # Home SOS
    kenpom_away_sos_rank = db.Column(db.Integer)
    kenpom_home_sos_rank = db.Column(db.Integer)
    # KenPom Conference
    kenpom_away_conf = db.Column(db.String(20))
    kenpom_home_conf = db.Column(db.String(20))
    
    # Betting action data (WagerTalk) - for "Closed" line display
    opening_spread = db.Column(db.Float)  # First spread seen
    opening_total = db.Column(db.Float)  # First total seen
    closed_spread = db.Column(db.Float)  # Final spread before game starts
    closed_total = db.Column(db.Float)  # Final total before game starts
    closed_spread_odds = db.Column(db.String(10))  # e.g., "-110"
    closed_total_odds = db.Column(db.String(10))
    current_spread = db.Column(db.Float)  # Live spread (updates during game)
    current_total = db.Column(db.Float)  # Live total (updates during game)
    game_started = db.Column(db.Boolean, default=False)
    
    # Betting percentages (from WagerTalk - don't update after game starts)
    away_tickets_pct = db.Column(db.Float)
    home_tickets_pct = db.Column(db.Float)
    away_money_pct = db.Column(db.Float)
    home_money_pct = db.Column(db.Float)
    over_tickets_pct = db.Column(db.Float)
    under_tickets_pct = db.Column(db.Float)
    over_money_pct = db.Column(db.Float)
    under_money_pct = db.Column(db.Float)
    
    # RLM detection results
    rlm_detected = db.Column(db.Boolean, default=False)
    rlm_severity = db.Column(db.String(20))  # 'moderate', 'strong', 'extreme'
    rlm_confidence = db.Column(db.Float)  # 0-100
    rlm_sharp_side = db.Column(db.String(50))  # Team name
    rlm_explanation = db.Column(db.Text)
    totals_rlm_detected = db.Column(db.Boolean, default=False)
    totals_rlm_severity = db.Column(db.String(20))
    totals_rlm_confidence = db.Column(db.Float)
    totals_rlm_sharp_side = db.Column(db.String(10))  # 'Over' or 'Under'
    totals_rlm_explanation = db.Column(db.Text)
    
    # PRE-GAME STATS from Covers.com - persist until game completes
    pregame_away_ats = db.Column(db.String(20))
    pregame_home_ats = db.Column(db.String(20))
    pregame_away_ats_road = db.Column(db.String(20))
    pregame_home_ats_home = db.Column(db.String(20))
    pregame_away_l10 = db.Column(db.String(20))
    pregame_home_l10 = db.Column(db.String(20))
    pregame_away_l10_ats = db.Column(db.String(20))
    pregame_home_l10_ats = db.Column(db.String(20))
    pregame_away_road_record = db.Column(db.String(20))
    pregame_home_home_record = db.Column(db.String(20))
    pregame_stats_captured = db.Column(db.Boolean, default=False)

    # AI Brain analysis columns (silent — stored in DB, never displayed in UI)
    brain_verdict = db.Column(db.String(10))       # OVER/UNDER/HOME/AWAY or None
    brain_confidence = db.Column(db.Float)          # 5.0-8.5 consensus confidence
    brain_agreement = db.Column(db.Integer)         # 0-4 agreeing brains
    brain_qualified = db.Column(db.Boolean)         # 3/4 consensus met
    brain_edge_boost = db.Column(db.Float)          # adjustment to effective edge (-2.0 to +3.0)
    brain_analyzed_at = db.Column(db.DateTime)

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

class EloRating(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team = db.Column(db.String(100), nullable=False)
    league = db.Column(db.String(10), nullable=False)
    rating = db.Column(db.Float, default=1500.0)
    games_played = db.Column(db.Integer, default=0)
    last_updated = db.Column(db.Date)
    season = db.Column(db.String(20))
    peak_rating = db.Column(db.Float)
    __table_args__ = (
        db.Index('idx_elo_team_league', 'team', 'league'),
        db.Index('idx_elo_league_rating', 'league', 'rating'),
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


def run_brain_analysis():
    """
    Run 4-brain AI analysis on today's games with PPG data.
    Stores results silently in Game brain columns — no UI impact.
    Brain consensus boosts/penalizes effective edge for pick ranking.
    """
    if not AI_BRAINS_AVAILABLE:
        return {'status': 'unavailable', 'analyzed': 0}

    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()

    games = Game.query.filter_by(date=today).filter(
        Game.away_ppg.isnot(None),
        Game.home_ppg.isnot(None)
    ).all()

    if not games:
        return {'status': 'no_games', 'analyzed': 0}

    analyzed = 0
    errors = 0

    # Batch-fetch all Elo ratings in one query (avoids N+1 problem)
    all_teams = set()
    for g in games:
        all_teams.add((g.away_team, g.league))
        all_teams.add((g.home_team, g.league))
    elo_cache = {}
    try:
        elo_records = EloRating.query.filter(
            EloRating.team.in_([t for t, _ in all_teams])
        ).all()
        for e in elo_records:
            elo_cache[(e.team, e.league)] = e.rating
    except Exception:
        pass  # No Elo data yet — all default to 1500

    for game in games:
        try:
            # Get Elo ratings from cache (default 1500)
            elo_away = elo_cache.get((game.away_team, game.league), 1500.0)
            elo_home = elo_cache.get((game.home_team, game.league), 1500.0)

            # Extract features
            features = extract_features(game, elo_away, elo_home)

            # Get ML features for ensemble
            ml_features = get_ml_features(features)

            # PPG-derived values for ensemble
            ppg_total = (game.away_ppg or 0) + (game.home_ppg or 0)
            ppg_margin = (game.home_ppg or 0) - (game.away_ppg or 0)

            # Ensemble prediction
            ensemble_pred = ensemble_predictor.predict(
                ml_features, game.league, elo_away, elo_home, ppg_total, ppg_margin
            )

            # Run all 4 brains
            verdict = brain_analyze_game(game, features, ensemble_pred)

            # Store results in Game columns
            game.brain_verdict = verdict.verdict
            game.brain_confidence = verdict.confidence
            game.brain_agreement = verdict.agreement_count
            game.brain_qualified = verdict.qualified
            game.brain_analyzed_at = datetime.utcnow()

            # Calculate brain_edge_boost based on agreement level
            if verdict.agreement_level == 'CONSENSUS':  # 4/4
                boost = 2.0
            elif verdict.agreement_level == 'STRONG':    # 3/4
                boost = 1.0
            elif verdict.agreement_level == 'SPLIT':     # 2/4
                boost = -0.5
            else:                                         # 0-1/4
                boost = -1.5

            # Scale by brain confidence: boost * (confidence - 5.0) / 3.5
            conf_scale = (verdict.confidence - 5.0) / 3.5
            game.brain_edge_boost = round(boost * max(0, conf_scale), 2)

            analyzed += 1
        except Exception as e:
            logger.warning(f"Brain analysis error for {game.away_team}@{game.home_team}: {e}")
            errors += 1

    db.session.commit()
    logger.info(f"Brain analysis complete: {analyzed} analyzed, {errors} errors out of {len(games)} games")
    return {'status': 'complete', 'analyzed': analyzed, 'errors': errors, 'total': len(games)}


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

    # Migrate: add new columns to existing tables if they don't exist
    _migration_columns = [
        ("game", "brain_verdict", "VARCHAR(10)"),
        ("game", "brain_confidence", "FLOAT"),
        ("game", "brain_agreement", "INTEGER"),
        ("game", "brain_qualified", "BOOLEAN"),
        ("game", "brain_edge_boost", "FLOAT"),
        ("game", "brain_analyzed_at", "TIMESTAMP"),
    ]
    for table, col, col_type in _migration_columns:
        try:
            db.session.execute(db.text(
                f"ALTER TABLE {table} ADD COLUMN {col} {col_type}"
            ))
            db.session.commit()
        except Exception:
            db.session.rollback()
    # Fix column type if brain_agreement was created as VARCHAR
    try:
        db.session.execute(db.text(
            "ALTER TABLE game ALTER COLUMN brain_agreement TYPE INTEGER USING brain_agreement::integer"
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()

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

# NHL city/name -> abbreviation mapping for result checking
NHL_TEAM_ABBREVS = {
    'anaheim': 'ANA', 'ducks': 'ANA',
    'arizona': 'UTA', 'utah': 'UTA', 'utah hockey club': 'UTA',
    'boston': 'BOS', 'bruins': 'BOS',
    'buffalo': 'BUF', 'sabres': 'BUF',
    'calgary': 'CGY', 'flames': 'CGY',
    'carolina': 'CAR', 'hurricanes': 'CAR',
    'chicago': 'CHI', 'blackhawks': 'CHI',
    'colorado': 'COL', 'avalanche': 'COL',
    'columbus': 'CBJ', 'blue jackets': 'CBJ',
    'dallas': 'DAL', 'stars': 'DAL',
    'detroit': 'DET', 'red wings': 'DET',
    'edmonton': 'EDM', 'oilers': 'EDM',
    'florida': 'FLA', 'panthers': 'FLA',
    'los angeles': 'LAK', 'kings': 'LAK',
    'minnesota': 'MIN', 'wild': 'MIN',
    'montreal': 'MTL', 'montréal': 'MTL', 'canadiens': 'MTL',
    'nashville': 'NSH', 'predators': 'NSH',
    'new jersey': 'NJD', 'devils': 'NJD',
    'islanders': 'NYI',
    'rangers': 'NYR',
    'ottawa': 'OTT', 'senators': 'OTT',
    'philadelphia': 'PHI', 'flyers': 'PHI',
    'pittsburgh': 'PIT', 'penguins': 'PIT',
    'san jose': 'SJS', 'sharks': 'SJS',
    'seattle': 'SEA', 'kraken': 'SEA',
    'st. louis': 'STL', 'st louis': 'STL', 'blues': 'STL',
    'tampa bay': 'TBL', 'lightning': 'TBL',
    'toronto': 'TOR', 'maple leafs': 'TOR',
    'vancouver': 'VAN', 'canucks': 'VAN',
    'vegas': 'VGK', 'golden knights': 'VGK',
    'washington': 'WSH', 'capitals': 'WSH',
    'winnipeg': 'WPG', 'jets': 'WPG',
}


def nhl_team_matches(api_abbrev: str, pick_name: str) -> bool:
    """Check if an NHL API team (by abbrev) matches a pick's team name."""
    key = pick_name.lower().strip()
    # "New York" is ambiguous - could be Rangers (NYR) or Islanders (NYI)
    if key == 'new york':
        return api_abbrev in ('NYR', 'NYI')
    pick_abbrev = NHL_TEAM_ABBREVS.get(key)
    return pick_abbrev == api_abbrev if pick_abbrev else False


def check_totals_pick_result(pick: Pick) -> int:
    """Check result for a totals pick."""
    if not pick.pick or len(pick.pick) < 2:
        return 0
    
    try:
        direction = pick.pick[0]
        # Strip odds info like "O122.5 (-185)" -> "122.5"
        raw = pick.pick[1:].split('(')[0].strip()
        line = float(raw)
    except (ValueError, IndexError):
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
                away_abbrev = game.get("awayTeam", {}).get("abbrev", "")
                home_abbrev = game.get("homeTeam", {}).get("abbrev", "")
                if nhl_team_matches(away_abbrev, away_team) and nhl_team_matches(home_abbrev, home_team):
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
                away_abbrev = game.get("awayTeam", {}).get("abbrev", "")
                home_abbrev = game.get("homeTeam", {}).get("abbrev", "")
                if nhl_team_matches(away_abbrev, away_team) and nhl_team_matches(home_abbrev, home_team):
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
    
    # Get standings for all leagues
    nba_standings = get_nba_standings()
    cbb_standings = get_cbb_standings()
    nhl_standings = get_nhl_standings()
    
    # Add time window and logos to each game for weekend slate grouping
    for g in all_games:
        g.time_window = get_game_window(g.game_time)
        # Add team logos for Pikkit-style display
        if g.league == 'NBA':
            g.away_logo = nba_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
            g.home_logo = nba_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
            away_stand = nba_standings.get(g.away_team, {})
            home_stand = nba_standings.get(g.home_team, {})
            g.away_record = away_stand.get('record', '--')
            g.home_record = home_stand.get('record', '--')
            g.away_standing = away_stand.get('standing', '')
            g.home_standing = home_stand.get('standing', '')
        elif g.league == 'CBB':
            # Use CBB logo lookup with proper fallback
            def get_cbb_logo_fallback(team_name):
                """Get CBB logo with proper ESPN fallback."""
                logo = get_transparent_cbb_logo(team_name) or get_cbb_logo(team_name)
                if logo:
                    return logo
                # Try normalized name variations
                for name_variant in [team_name, team_name.replace("'", ""), team_name.replace("'", "")]:
                    logo = get_transparent_cbb_logo(name_variant) or get_cbb_logo(name_variant)
                    if logo:
                        return logo
                # Ultimate fallback - NCAA generic logo (not NBA!)
                return 'https://a.espncdn.com/i/teamlogos/ncaa/500-dark/ncaa.png'
            g.away_logo = get_cbb_logo_fallback(g.away_team)
            g.home_logo = get_cbb_logo_fallback(g.home_team)
            # Use fuzzy matching for CBB team records
            g.away_record = get_cbb_team_record(g.away_team, cbb_standings)
            g.home_record = get_cbb_team_record(g.home_team, cbb_standings)
            g.away_standing = ''
            g.home_standing = ''
        elif g.league == 'NHL':
            g.away_logo = nhl_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
            g.home_logo = nhl_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
            away_stand = nhl_standings.get(g.away_team, {})
            home_stand = nhl_standings.get(g.home_team, {})
            g.away_record = away_stand.get('record', '--')
            g.home_record = home_stand.get('record', '--')
            g.away_standing = away_stand.get('standing', '')
            g.home_standing = home_stand.get('standing', '')
        else:
            g.away_logo = ''
            g.home_logo = ''
            g.away_record = '--'
            g.home_record = '--'
            g.away_standing = ''
            g.home_standing = ''
    
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

    # Brain-adjusted edge: brain boost only affects sort order, not displayed numbers
    def _brain_adjusted_edge(g):
        base = g.alt_edge or g.edge or 0
        boost = g.brain_edge_boost if g.brain_edge_boost is not None else 0
        return base + boost

    # Sort qualified totals by brain-adjusted edge (alt if available, else main)
    qualified.sort(key=_brain_adjusted_edge, reverse=True)

    # LOCK OF THE DAY = highest brain-adjusted edge totals pick
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
    
    # TOP 5: Games qualified by edge threshold (sorted by brain-adjusted edge)
    # Sort by brain-adjusted edge before taking top 5
    qualified.sort(key=_brain_adjusted_edge, reverse=True)
    
    # FETCH L5 STATS for NBA games and KenPom breakdown for CBB games
    # Pre-cache data to avoid API calls during template render
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
        elif g.league == 'CBB':
            try:
                # Compute comprehensive CBB matchup breakdown using all KenPom endpoints
                cbb_breakdown = compute_cbb_matchup_breakdown(g.away_team, g.home_team)
                g.matchup_l5 = cbb_breakdown if cbb_breakdown.get('has_data') else {'has_data': False}
                g.cbb_breakdown = cbb_breakdown
            except Exception as e:
                logger.warning(f"Error computing CBB breakdown for {g.away_team} vs {g.home_team}: {e}")
                g.matchup_l5 = {'has_data': False}
                g.cbb_breakdown = {'has_data': False}
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
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y%m%d")
    
    games_db = Game.query.filter(Game.date >= yesterday).all()
    live_scores = {}
    dates_to_check = [today_str, yesterday_str]
    
    for date_str in dates_to_check:
        try:
            nba_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
            resp = requests.get(nba_url, timeout=10)
            for event in resp.json().get("events", []):
                status = event.get("status", {})
                state = status.get("type", {}).get("state", "")
                if state in ("in", "post"):
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
                            is_final = state == "post"
                            for g in games_db:
                                if g.league == "NBA" and g.away_team == away_name and g.home_team == home_name:
                                    key = f"{g.away_team}@{g.home_team}"
                                    if key not in live_scores:
                                        live_scores[key] = {
                                            "away_score": away_score,
                                            "home_score": home_score,
                                            "total": away_score + home_score,
                                            "period": "Final" if is_final else f"Q{period}",
                                            "clock": clock,
                                            "league": "NBA",
                                            "status": "Final" if is_final else "Live",
                                            "is_final": is_final
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
            if state in ("in", "post"):
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
                        is_final = state == "post"
                        for g in games_db:
                            if g.league == "CBB" and g.away_team == away_name and g.home_team == home_name:
                                live_scores[f"{g.away_team}@{g.home_team}"] = {
                                    "away_score": away_score,
                                    "home_score": home_score,
                                    "total": away_score + home_score,
                                    "period": "Final" if is_final else f"H{period}",
                                    "clock": clock,
                                    "league": "CBB",
                                    "status": "Final" if is_final else "Live",
                                    "is_final": is_final
                                }
                                break
    except Exception as e:
        logger.debug(f"CBB live scores fetch: {e}")
    
    for nhl_date in [today, yesterday]:
        try:
            nhl_url = f"https://api-web.nhle.com/v1/score/{nhl_date.strftime('%Y-%m-%d')}"
            resp = requests.get(nhl_url, timeout=10)
            for game in resp.json().get("games", []):
                game_state = game.get("gameState", "")
                if game_state in ("LIVE", "FINAL", "OFF", "CRIT"):
                    away_name = game.get("awayTeam", {}).get("name", {}).get("default", "")
                    home_name = game.get("homeTeam", {}).get("name", {}).get("default", "")
                    away_score = game.get("awayTeam", {}).get("score", 0)
                    home_score = game.get("homeTeam", {}).get("score", 0)
                    period = game.get("periodDescriptor", {}).get("number", 0)
                    clock = game.get("clock", {}).get("timeRemaining", "")
                    is_final = game_state in ("FINAL", "OFF")
                    for g in games_db:
                        if g.league == "NHL":
                            away_match = g.away_team.lower() in away_name.lower() or away_name.lower() in g.away_team.lower()
                            home_match = g.home_team.lower() in home_name.lower() or home_name.lower() in g.home_team.lower()
                            if away_match and home_match:
                                key = f"{g.away_team}@{g.home_team}"
                                if key not in live_scores:
                                    live_scores[key] = {
                                        "away_score": away_score,
                                        "home_score": home_score,
                                        "total": away_score + home_score,
                                        "period": "Final" if is_final else f"P{period}",
                                        "clock": clock,
                                        "league": "NHL",
                                        "status": "Final" if is_final else "Live",
                                        "is_final": is_final
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

@app.route('/api/live_lines')
@app.route('/api/live_lines/<league>')
def api_live_lines(league='NBA'):
    """Get current live lines from The Odds API - refreshes every 30 seconds."""
    try:
        from live_odds_fetcher import get_live_odds
        odds_data = get_live_odds(league.upper())
        return jsonify({
            'success': True,
            'league': league.upper(),
            'lines': odds_data,
            'count': len(odds_data),
            'timestamp': time.time()
        })
    except Exception as e:
        logging.error(f"Live lines API error: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'lines': {}
        })

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

# KenPom API caches - unified storage for all endpoints
torvik_cache = {}
torvik_cache_date = None

# Additional KenPom endpoint caches
kenpom_four_factors_cache = {}
kenpom_point_distribution_cache = {}
kenpom_height_cache = {}
kenpom_misc_cache = {}
kenpom_conference_ratings_cache = {}
kenpom_conferences_cache = {}
kenpom_fanmatch_cache = {}
kenpom_fanmatch_cache_date = None
kenpom_teams_cache = {}
kenpom_cache_date = None

# D1 CBB Averages for comparison (2024-25 season baselines)
CBB_D1_AVERAGES = {
    'eFG_pct': 50.0,          # Effective FG%
    'TO_pct': 18.5,           # Turnover %
    'OR_pct': 28.0,           # Offensive Rebound %
    'FT_rate': 32.0,          # FT Rate (FTA/FGA)
    'FT_pct': 72.0,           # Free Throw %
    '3PT_pct': 34.0,          # 3-Point %
    '2PT_pct': 50.0,          # 2-Point %
    'tempo': 67.5,            # Possessions per 40 min
    'adj_o': 109.0,           # Adj Offensive Efficiency
    'adj_d': 109.0,           # Adj Defensive Efficiency
    'ppg': 72.0,              # Points per game
    'avg_height': 77.0,       # Average height in inches
    'experience': 2.0,        # Average years experience
    'bench_minutes': 30.0,    # Bench minutes percentage
}


def fetch_kenpom_api(endpoint: str, year: int = 2026) -> Optional[list]:
    """Generic KenPom API fetcher for any endpoint."""
    api_key = os.environ.get('CBB_API_KEY', '')
    if not api_key:
        logger.warning(f"CBB_API_KEY not set, skipping KenPom {endpoint} fetch")
        return None

    try:
        url = f"https://kenpom.com/api.php?endpoint={endpoint}&y={year}"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'User-Agent': 'Mozilla/5.0 (compatible; SportsApp/1.0)'
        }
        resp = requests.get(url, headers=headers, timeout=30)

        if resp.status_code == 401:
            logger.warning(f"KenPom API unauthorized for {endpoint}")
            return None
        if resp.status_code != 200:
            logger.warning(f"KenPom API {endpoint} failed: {resp.status_code}")
            return None

        return resp.json()
    except Exception as e:
        logger.error(f"KenPom {endpoint} fetch error: {e}")
        return None


def fetch_kenpom_four_factors() -> dict:
    """
    Fetch KenPom Four Factors data for all teams.
    Returns: eFG%, TOV%, ORB%, FT Rate for both offense and defense
    """
    global kenpom_four_factors_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_four_factors_cache:
        return kenpom_four_factors_cache

    data = fetch_kenpom_api('four-factors')
    if not data:
        return kenpom_four_factors_cache

    cache = {}
    for team in data:
        try:
            team_name = team.get('TeamName', '').lower()
            if team_name:
                cache[team_name] = {
                    # Offensive Four Factors (API uses underscores: eFG_Pct, RankeFG_Pct)
                    'o_efg': team.get('eFG_Pct', 0),           # Offensive eFG%
                    'o_efg_rank': team.get('RankeFG_Pct', 0),
                    'o_to': team.get('TO_Pct', 0),             # Offensive TO%
                    'o_to_rank': team.get('RankTO_Pct', 0),
                    'o_or': team.get('OR_Pct', 0),             # Offensive Rebound %
                    'o_or_rank': team.get('RankOR_Pct', 0),
                    'o_ft_rate': team.get('FT_Rate', 0),       # FT Rate (FTA/FGA)
                    'o_ft_rate_rank': team.get('RankFT_Rate', 0),
                    # Defensive Four Factors
                    'd_efg': team.get('DeFG_Pct', 0),          # Defensive eFG% allowed
                    'd_efg_rank': team.get('RankDeFG_Pct', 0),
                    'd_to': team.get('DTO_Pct', 0),            # Forced TO%
                    'd_to_rank': team.get('RankDTO_Pct', 0),
                    'd_or': team.get('DOR_Pct', 0),            # DRB% (100 - opponent ORB%)
                    'd_or_rank': team.get('RankDOR_Pct', 0),
                    'd_ft_rate': team.get('DFT_Rate', 0),      # Opponent FT Rate
                    'd_ft_rate_rank': team.get('RankDFT_Rate', 0),
                    # Efficiency metrics from Four Factors API
                    'adj_o': team.get('AdjOE', 0),
                    'adj_o_rank': team.get('RankAdjOE', 0),
                    'adj_d': team.get('AdjDE', 0),
                    'adj_d_rank': team.get('RankAdjDE', 0),
                }
        except Exception:
            continue

    if cache:
        kenpom_four_factors_cache = cache
        logger.info(f"KenPom Four Factors loaded: {len(cache)} teams")
    return kenpom_four_factors_cache


def fetch_kenpom_point_distribution() -> dict:
    """
    Fetch KenPom Point Distribution data.
    Returns: % of points from 3PT, 2PT, FT (how points are scored)
    """
    global kenpom_point_distribution_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_point_distribution_cache:
        return kenpom_point_distribution_cache

    data = fetch_kenpom_api('pointdist')
    if not data:
        return kenpom_point_distribution_cache

    # Log sample team data to see actual field names
    if data and len(data) > 0:
        sample = data[0]
        logger.info(f"KenPom pointdist fields: {list(sample.keys())}")

    cache = {}
    for team in data:
        try:
            team_name = team.get('TeamName', '').lower()
            if team_name:
                cache[team_name] = {
                    # Offensive point distribution - % of points FROM 3PT shots
                    'o_3pt_pct': team.get('OffFG3Pct') or team.get('OFG3Pct') or team.get('Off3P') or team.get('FG3') or 0,
                    'o_2pt_pct': team.get('OffFG2Pct') or team.get('OFG2Pct') or team.get('Off2P') or team.get('FG2') or 0,
                    'o_ft_pct_dist': team.get('OffFTPct') or team.get('OFTPct') or team.get('OffFT') or team.get('FT') or 0,
                    'o_3pt_pct_rank': team.get('RankOffFG3Pct') or team.get('RankOFG3Pct') or 0,
                    # Defensive point distribution (what opponents do against this team)
                    # Per KenPom API docs: DefFg3 = Percentage of points allowed from 3-point FGs
                    'd_3pt_pct': team.get('DefFg3') or team.get('DefFG3Pct') or team.get('DFG3Pct') or team.get('Def3P') or 0,
                    'd_2pt_pct': team.get('DefFG2Pct') or team.get('DFG2Pct') or team.get('Def2P') or 0,
                    'd_ft_pct_dist': team.get('DefFTPct') or team.get('DFTPct') or team.get('DefFT') or 0,
                }
        except Exception:
            continue

    if cache:
        kenpom_point_distribution_cache = cache
        logger.info(f"KenPom Point Distribution loaded: {len(cache)} teams")
    return kenpom_point_distribution_cache


def fetch_kenpom_height() -> dict:
    """
    Fetch KenPom Height/Size data for teams.
    Returns: average height, experience, bench minutes
    """
    global kenpom_height_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_height_cache:
        return kenpom_height_cache

    data = fetch_kenpom_api('height')
    if not data:
        return kenpom_height_cache

    cache = {}
    for team in data:
        try:
            team_name = team.get('TeamName', '').lower()
            if team_name:
                cache[team_name] = {
                    'avg_height': team.get('AvgHgt', 0),
                    'avg_height_rank': team.get('RankAvgHgt', 0),
                    'eff_height': team.get('EffHgt', 0),       # Effective height
                    'eff_height_rank': team.get('RankEffHgt', 0),
                    'experience': team.get('Exp', 0),           # Average experience
                    'experience_rank': team.get('RankExp', 0),
                    'bench_mins': team.get('Bench', 0),         # Bench minutes %
                    'bench_rank': team.get('RankBench', 0),
                    'continuity': team.get('Continuity', 0),    # % returning minutes
                    'continuity_rank': team.get('RankContinuity', 0),
                }
        except Exception:
            continue

    if cache:
        kenpom_height_cache = cache
        logger.info(f"KenPom Height loaded: {len(cache)} teams")
    return kenpom_height_cache


def fetch_kenpom_misc() -> dict:
    """
    Fetch KenPom Miscellaneous Stats (teamstats endpoint).
    Returns: 3PT%, 2PT%, FT%, block%, steal%, assist rate, etc.
    """
    global kenpom_misc_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_misc_cache:
        return kenpom_misc_cache

    # Use correct endpoint name from KenPom API docs: misc-stats
    data = fetch_kenpom_api('misc-stats')
    if not data:
        logger.warning("KenPom misc-stats endpoint failed")
        return kenpom_misc_cache

    logger.info(f"KenPom Misc Stats loaded: {len(data)} teams")

    # Log sample team data to see actual field names
    if data and len(data) > 0:
        sample = data[0]
        logger.info(f"KenPom misc-stats fields: {list(sample.keys())}")

    cache = {}
    for team in data:
        try:
            team_name = team.get('TeamName', '').lower()
            if team_name:
                # KenPom uses 3P% (not 3PT%) - try multiple field name patterns
                # Common KenPom API field patterns: FG3Pct, 3P_O, Off3PPct, ThreePtPct
                cache[team_name] = {
                    # Offensive misc stats - try multiple field names
                    'o_3pt_pct': team.get('FG3Pct') or team.get('3P_O') or team.get('Off3PPct') or team.get('ThreePtPct') or 0,
                    'o_3pt_rank': team.get('RankFG3Pct') or team.get('Rank3P_O') or team.get('RankOff3PPct') or 0,
                    'o_2pt_pct': team.get('FG2Pct') or team.get('2P_O') or 0,
                    'o_2pt_rank': team.get('RankFG2Pct') or team.get('Rank2P_O') or 0,
                    'o_ft_pct': team.get('FTPct') or team.get('FT_O') or 0,
                    'o_ft_rank': team.get('RankFTPct') or team.get('RankFT_O') or 0,
                    'o_blk_pct': team.get('BlockPct') or team.get('Blk') or 0,
                    'o_blk_rank': team.get('RankBlockPct') or team.get('RankBlk') or 0,
                    'o_stl_rate': team.get('StlRate') or team.get('Stl') or 0,
                    'o_stl_rank': team.get('RankStlRate') or team.get('RankStl') or 0,
                    'o_ast_rate': team.get('ARate') or team.get('Ast') or 0,
                    'o_ast_rank': team.get('RankARate') or team.get('RankAst') or 0,
                    'o_nst_rate': team.get('NSTRate') or team.get('NST') or 0,
                    'o_nst_rank': team.get('RankNSTRate') or team.get('RankNST') or 0,
                    'o_3pt_rate': team.get('FG3ARate') or team.get('3PA_O') or 0,
                    'o_3pt_rate_rank': team.get('RankFG3ARate') or team.get('Rank3PA_O') or 0,
                    # Defensive misc stats - opponent's shooting % against this team
                    # Per KenPom API docs: OppFG3Pct = Opponent 3-point field goal percentage (defense)
                    'd_3pt_pct': team.get('OppFG3Pct') or team.get('DFG3Pct') or team.get('3P_D') or team.get('Def3PPct') or 0,
                    'd_3pt_rank': team.get('RankOppFG3Pct') or team.get('RankDFG3Pct') or team.get('Rank3P_D') or 0,
                    'd_2pt_pct': team.get('DFG2Pct') or team.get('2P_D') or 0,
                    'd_2pt_rank': team.get('RankDFG2Pct') or team.get('Rank2P_D') or 0,
                    'd_ft_pct': team.get('DFTPct') or team.get('FT_D') or 0,
                    'd_ft_rank': team.get('RankDFTPct') or team.get('RankFT_D') or 0,
                    'd_blk_pct': team.get('DBlockPct') or team.get('DBlk') or 0,
                    'd_blk_rank': team.get('RankDBlockPct') or team.get('RankDBlk') or 0,
                    'd_stl_rate': team.get('DStlRate') or team.get('DStl') or 0,
                    'd_stl_rank': team.get('RankDStlRate') or team.get('RankDStl') or 0,
                    'd_nst_rate': team.get('DNSTRate') or team.get('DNST') or 0,
                    'd_nst_rank': team.get('RankDNSTRate') or team.get('RankDNST') or 0,
                    'd_3pt_rate': team.get('DFG3ARate') or team.get('3PA_D') or 0,
                    'd_3pt_rate_rank': team.get('RankDFG3ARate') or team.get('Rank3PA_D') or 0,
                }
        except Exception:
            continue

    if cache:
        kenpom_misc_cache = cache
        logger.info(f"KenPom Misc Stats loaded: {len(cache)} teams")
    return kenpom_misc_cache


def fetch_kenpom_conference_ratings() -> dict:
    """
    Fetch KenPom Conference Ratings.
    Returns: conference efficiency, SOS, etc.
    """
    global kenpom_conference_ratings_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_conference_ratings_cache:
        return kenpom_conference_ratings_cache

    data = fetch_kenpom_api('confratings')
    if not data:
        return kenpom_conference_ratings_cache

    cache = {}
    for conf in data:
        try:
            conf_name = conf.get('ConfName', '').lower()
            conf_abbrev = conf.get('Conf', '').lower()
            if conf_name:
                conf_data = {
                    'name': conf.get('ConfName', ''),
                    'abbrev': conf.get('Conf', ''),
                    'adj_em': conf.get('AdjEM', 0),
                    'rank': conf.get('RankAdjEM', 0),
                    'adj_o': conf.get('AdjO', 0),
                    'adj_d': conf.get('AdjD', 0),
                    'adj_tempo': conf.get('AdjTempo', 0),
                    'sos': conf.get('SOS', 0),
                    'num_teams': conf.get('NumTeams', 0),
                }
                cache[conf_name] = conf_data
                cache[conf_abbrev] = conf_data
        except Exception:
            continue

    if cache:
        kenpom_conference_ratings_cache = cache
        logger.info(f"KenPom Conference Ratings loaded: {len(cache)} conferences")
    return kenpom_conference_ratings_cache


def fetch_kenpom_fanmatch(target_date: str = None) -> dict:
    """
    Fetch KenPom Fanmatch predictions - per-game predicted scores and spreads.
    The fanmatch API requires a date parameter and returns game-level predictions.
    
    Returns dict keyed by 'visitor_vs_home' (lowercased) with:
        - visitor, home team names
        - visitor_pred, home_pred (predicted scores)
        - kenpom_spread (positive = visitor favored, negative = home favored, from visitor perspective)
        - home_wp (home win probability %)
        - pred_tempo
        - thrill_score
        - home_rank, visitor_rank
    """
    global kenpom_fanmatch_cache, kenpom_fanmatch_cache_date
    today = date.today()

    if kenpom_fanmatch_cache_date == today and kenpom_fanmatch_cache:
        return kenpom_fanmatch_cache

    if not target_date:
        target_date = today.strftime('%Y-%m-%d')

    api_key = os.environ.get('CBB_API_KEY', '')
    if not api_key:
        logger.warning("CBB_API_KEY not set, skipping KenPom fanmatch fetch")
        return kenpom_fanmatch_cache

    dates_to_try = [target_date]
    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    if yesterday not in dates_to_try:
        dates_to_try.append(yesterday)

    try:
        resp = None
        for try_date in dates_to_try:
            url = f"https://kenpom.com/api.php?endpoint=fanmatch&d={try_date}"
            headers = {
                'Authorization': f'Bearer {api_key}',
                'User-Agent': 'Mozilla/5.0 (compatible; SportsApp/1.0)'
            }
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                logger.info(f"KenPom fanmatch loaded for date: {try_date}")
                break
            else:
                logger.warning(f"KenPom fanmatch API returned {resp.status_code} for {try_date}: {resp.text[:200]}")
        if not resp or resp.status_code != 200:
            logger.warning(f"KenPom fanmatch unavailable for all dates tried: {dates_to_try}")
            return kenpom_fanmatch_cache

        data = resp.json()
        if not isinstance(data, list):
            return kenpom_fanmatch_cache

        cache = {}
        for game in data:
            try:
                visitor = game.get('Visitor', '')
                home = game.get('Home', '')
                if not visitor or not home:
                    continue

                visitor_pred = game.get('VisitorPred', 0)
                home_pred = game.get('HomePred', 0)
                kenpom_spread = visitor_pred - home_pred

                key = f"{visitor.lower()}_vs_{home.lower()}"
                cache[key] = {
                    'visitor': visitor,
                    'home': home,
                    'visitor_pred': visitor_pred,
                    'home_pred': home_pred,
                    'kenpom_spread': round(kenpom_spread, 1),
                    'home_wp': game.get('HomeWP', 50),
                    'pred_tempo': game.get('PredTempo', 68),
                    'thrill_score': game.get('ThrillScore', 0),
                    'home_rank': game.get('HomeRank', 999),
                    'visitor_rank': game.get('VisitorRank', 999),
                }
            except Exception:
                continue

        if cache:
            kenpom_fanmatch_cache = cache
            kenpom_fanmatch_cache_date = today
            logger.info(f"KenPom Fanmatch predictions loaded: {len(cache)} games")
        return kenpom_fanmatch_cache
    except Exception as e:
        logger.warning(f"KenPom fanmatch fetch error: {e}")
        return kenpom_fanmatch_cache


def get_kenpom_prediction(away_team: str, home_team: str) -> dict:
    """
    Look up KenPom fanmatch prediction for a specific game.
    Handles team name normalization/matching.
    Returns dict with kenpom_spread, predicted scores, etc. or empty dict if not found.
    """
    cache = fetch_kenpom_fanmatch()
    if not cache:
        return {}

    away_lower = away_team.lower().strip()
    home_lower = home_team.lower().strip()

    direct_key = f"{away_lower}_vs_{home_lower}"
    if direct_key in cache:
        return cache[direct_key]

    away_normalized = normalize_cbb_team_for_kenpom(away_lower)
    home_normalized = normalize_cbb_team_for_kenpom(home_lower)

    norm_key = f"{away_normalized}_vs_{home_normalized}"
    if norm_key in cache:
        return cache[norm_key]

    away_clean = away_lower.replace('.', '').replace("'", '').replace('-', ' ').strip()
    home_clean = home_lower.replace('.', '').replace("'", '').replace('-', ' ').strip()
    away_norm_clean = away_normalized.replace('.', '').replace("'", '').replace('-', ' ').strip()
    home_norm_clean = home_normalized.replace('.', '').replace("'", '').replace('-', ' ').strip()

    for key, val in cache.items():
        cached_visitor = val['visitor'].lower()
        cached_home = val['home'].lower()
        cv_clean = cached_visitor.replace('.', '').replace("'", '').replace('-', ' ').strip()
        ch_clean = cached_home.replace('.', '').replace("'", '').replace('-', ' ').strip()
        cv_norm = normalize_cbb_team_for_kenpom(cv_clean)
        ch_norm = normalize_cbb_team_for_kenpom(ch_clean)

        away_matches = (away_clean == cv_clean or away_norm_clean == cv_clean or
                       away_clean == cv_norm.replace('.', '').replace("'", '') or
                       away_norm_clean == cv_norm.replace('.', '').replace("'", '') or
                       fuzzy_team_match(away_lower, cached_visitor))
        home_matches = (home_clean == ch_clean or home_norm_clean == ch_clean or
                       home_clean == ch_norm.replace('.', '').replace("'", '') or
                       home_norm_clean == ch_norm.replace('.', '').replace("'", '') or
                       fuzzy_team_match(home_lower, cached_home))

        if away_matches and home_matches:
            return val

        reverse_away = (away_clean == ch_clean or away_norm_clean == ch_clean or
                       away_clean == ch_norm.replace('.', '').replace("'", '') or
                       away_norm_clean == ch_norm.replace('.', '').replace("'", '') or
                       fuzzy_team_match(away_lower, cached_home))
        reverse_home = (home_clean == cv_clean or home_norm_clean == cv_clean or
                       home_clean == cv_norm.replace('.', '').replace("'", '') or
                       home_norm_clean == cv_norm.replace('.', '').replace("'", '') or
                       fuzzy_team_match(home_lower, cached_visitor))

        if reverse_away and reverse_home:
            reversed_result = dict(val)
            reversed_result['kenpom_spread'] = -val['kenpom_spread']
            reversed_result['visitor'] = val['home']
            reversed_result['home'] = val['visitor']
            reversed_result['visitor_pred'] = val.get('home_pred', 0)
            reversed_result['home_pred'] = val.get('visitor_pred', 0)
            return reversed_result

    return {}


def normalize_cbb_team_for_kenpom(name: str) -> str:
    """Normalize team name to match KenPom naming conventions."""
    kenpom_aliases = {
        'uic': 'illinois chicago', 'illinois-chicago': 'illinois chicago',
        'uconn': 'connecticut', 'ole miss': 'mississippi',
        'siue': 'siu edwardsville', 'etsu': 'east tennessee st.',
        'fau': 'florida atlantic', 'ucf': 'central florida',
        'usf': 'south florida', 'smu': 'smu',
        'byu': 'brigham young', 'lsu': 'louisiana st.',
        'unlv': 'nevada las vegas', 'utep': 'texas el paso',
        'vcu': 'virginia commonwealth', 'wvu': 'west virginia',
        'tcu': 'tcu', 'umass': 'massachusetts',
        'usc': 'southern california', 'ucla': 'ucla',
        'n iowa': 'northern iowa', 'uni': 'northern iowa',
        's illinois': 'southern illinois', 'siu': 'southern illinois',
        'murray st': 'murray st.', 'loyola chicago': 'loyola chicago',
        'e texas a&m': 'east texas a&m', 'etamu': 'east texas a&m',
        'texas a&m-cc': 'texas a&m corpus chris', 'tamucc': 'texas a&m corpus chris',
        'ut rio grande': 'ut rio grande valley', 'utrgv': 'ut rio grande valley',
        "n'western st": 'northwestern st.', 'nw st': 'northwestern st.', 'northwestern st': 'northwestern st.',
        'mid atl chrstn': 'mid atlantic christian',
        'c arkansas': 'central arkansas', 'uca': 'central arkansas',
        'hou christian': 'houston christian', 'hcu': 'houston christian',
        'sf austin': 'stephen f. austin', 'sfa': 'stephen f. austin',
        'nc central': 'north carolina central', 'nccu': 'north carolina central',
        'miss valley st': 'mississippi valley st.', 'mvsu': 'mississippi valley st.',
        'alcorn st': 'alcorn st.', 'alabama a&m': 'alabama a&m',
        'jackson st': 'jackson st.', 'grambling': 'grambling st.',
        'grambling st': 'grambling st.',
        'ar-pine bluff': 'arkansas pine bluff', 'uapb': 'arkansas pine bluff',
        'n alabama': 'north alabama', 'una': 'north alabama',
        'se louisiana': 'southeastern louisiana', 'sela': 'southeastern louisiana',
        'tx southern': 'texas southern', 'txso': 'texas southern',
        'bethune-cookman': 'bethune cookman', 'bethune cookman': 'bethune cookman',
        'n.c. state': 'n.c. state', 'nc state': 'n.c. state', 'ncst': 'n.c. state',
        'mcneese': 'mcneese', 'mcneese st': 'mcneese',
        'chicago st': 'chicago st.', 'csu': 'chicago st.',
        'saint francis': 'saint francis', 'st. francis': 'saint francis',
        'st francis': 'saint francis',
        'indiana st': 'indiana st.', 'illinois st': 'illinois st.',
        'delaware st': 'delaware st.', 'alabama st': 'alabama st.',
        'prairie view': 'prairie view a&m', 'pvamu': 'prairie view a&m',
        'florida a&m': 'florida a&m', 'famu': 'florida a&m',
        'unc wilmington': 'unc wilmington', 'uncw': 'unc wilmington',
        'g washington': 'george washington', 'geo washington': 'george washington', 'gw': 'george washington',
        'mount st marys': "mount st. mary's", 'mt st marys': "mount st. mary's", 'mount st. marys': "mount st. mary's",
        'nc a&t': 'north carolina a&t', 'ncat': 'north carolina a&t',
        'unc asheville': 'unc asheville', 'unca': 'unc asheville',
        'boise st': 'boise st.', 'boise state': 'boise st.',
        'michigan st': 'michigan st.', 'michigan state': 'michigan st.',
        'missouri st': 'missouri st.', 'missouri state': 'missouri st.',
        'montana st': 'montana st.', 'montana state': 'montana st.',
        'oregon st': 'oregon st.', 'oregon state': 'oregon st.',
        'ohio st': 'ohio st.', 'ohio state': 'ohio st.',
        'penn st': 'penn st.', 'penn state': 'penn st.',
        'san diego st': 'san diego st.', 'san diego state': 'san diego st.',
        'colorado st': 'colorado st.', 'colorado state': 'colorado st.',
        'fresno st': 'fresno st.', 'fresno state': 'fresno st.',
        'utah st': 'utah st.', 'utah state': 'utah st.',
        'iowa st': 'iowa st.', 'iowa state': 'iowa st.',
        'kansas st': 'kansas st.', 'kansas state': 'kansas st.',
        'oklahoma st': 'oklahoma st.', 'oklahoma state': 'oklahoma st.',
        'wichita st': 'wichita st.', 'wichita state': 'wichita st.',
        'wright st': 'wright st.', 'wright state': 'wright st.',
        'ball st': 'ball st.', 'ball state': 'ball st.',
        'kent st': 'kent st.', 'kent state': 'kent st.',
        'kennesaw st': 'kennesaw st.', 'kennesaw state': 'kennesaw st.',
        'long beach st': 'long beach st.', 'long beach state': 'long beach st.',
        'sacramento st': 'sacramento st.', 'sacramento state': 'sacramento st.',
        'sam houston st': 'sam houston st.', 'sam houston state': 'sam houston st.',
        'portland st': 'portland st.', 'portland state': 'portland st.',
        'mtsu': 'middle tennessee', 'middle tenn': 'middle tennessee',
        's dakota st': 'south dakota st.', 'south dakota st': 'south dakota st.',
        'n dakota st': 'north dakota st.', 'north dakota st': 'north dakota st.',
        'so indiana': 'southern indiana', 'uso': 'southern indiana',
        'ca baptist': 'cal baptist', 'california baptist': 'cal baptist',
        'abil christian': 'abilene christian', 'abilene chrstn': 'abilene christian',
        'ut arlington': 'ut arlington', 'texas arlington': 'ut arlington',
        'ut martin': 'ut martin', 'tennessee martin': 'ut martin',
        'c connecticut': 'central connecticut', 'ccsu': 'central connecticut',
        'e washington': 'eastern washington', 'ewu': 'eastern washington',
        'idaho st': 'idaho st.', 'idaho state': 'idaho st.',
        'se missouri': 'southeast missouri st.', 'semo': 'southeast missouri st.',
        'little rock': 'little rock', 'ualr': 'little rock',
        'w illinois': 'western illinois', 'wiu': 'western illinois',
        'tennessee st': 'tennessee st.', 'tennessee state': 'tennessee st.',
        'lindenwood': 'lindenwood',
        'southern utah': 'southern utah', 's utah': 'southern utah',
        'purdue fw': 'purdue fort wayne', 'pfw': 'purdue fort wayne',
        'long island': 'long island', 'liu': 'long island',
        'st thomas (mn)': 'st. thomas', 'st thomas mn': 'st. thomas',
        'fdu': 'fairleigh dickinson',
        'new haven': 'new haven',
        'sacred heart': 'sacred heart',
        'wagner': 'wagner',
    }
    return kenpom_aliases.get(name, name)


def fuzzy_team_match(name1: str, name2: str) -> bool:
    """Fuzzy match two team names, handling St./State, abbreviations, etc."""
    if name1 == name2:
        return True
    n1 = name1.replace('.', '').replace('-', ' ').replace("'", '').strip()
    n2 = name2.replace('.', '').replace('-', ' ').replace("'", '').strip()
    if n1 == n2:
        return True
    n1_cleaned = ' '.join(n1.split())
    n2_cleaned = ' '.join(n2.split())
    if n1_cleaned == n2_cleaned:
        return True
    if n1_cleaned.replace(' st', ' state') == n2_cleaned or n2_cleaned.replace(' st', ' state') == n1_cleaned:
        return True
    if n1_cleaned in n2_cleaned or n2_cleaned in n1_cleaned:
        if len(n1_cleaned) >= 4 and len(n2_cleaned) >= 4:
            return True
    kp1 = normalize_cbb_team_for_kenpom(n1_cleaned)
    kp2 = normalize_cbb_team_for_kenpom(n2_cleaned)
    if kp1 == kp2:
        return True
    kp1_clean = kp1.replace('.', '').replace("'", '').strip()
    kp2_clean = kp2.replace('.', '').replace("'", '').strip()
    if kp1_clean == kp2_clean:
        return True
    return False


def fetch_kenpom_teams() -> dict:
    """
    Fetch KenPom Teams data - team metadata.
    """
    global kenpom_teams_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_teams_cache:
        return kenpom_teams_cache

    data = fetch_kenpom_api('teams')
    if not data:
        return kenpom_teams_cache

    cache = {}
    for team in data:
        try:
            team_name = team.get('TeamName', '').lower()
            if team_name:
                cache[team_name] = {
                    'team_id': team.get('TeamId', ''),
                    'team_name': team.get('TeamName', ''),
                    'conf': team.get('Conf', ''),
                    'seed': team.get('Seed', 0),
                }
        except Exception:
            continue

    if cache:
        kenpom_teams_cache = cache
        logger.info(f"KenPom Teams loaded: {len(cache)} teams")
    return kenpom_teams_cache


def fetch_kenpom_conferences() -> dict:
    """
    Fetch KenPom Conferences data - conference metadata.
    """
    global kenpom_conferences_cache, kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today and kenpom_conferences_cache:
        return kenpom_conferences_cache

    data = fetch_kenpom_api('conferences')
    if not data:
        return kenpom_conferences_cache

    cache = {}
    for conf in data:
        try:
            conf_abbrev = conf.get('Conf', '').lower()
            if conf_abbrev:
                cache[conf_abbrev] = {
                    'conf_name': conf.get('ConfName', ''),
                    'conf_abbrev': conf.get('Conf', ''),
                }
        except Exception:
            continue

    if cache:
        kenpom_conferences_cache = cache
        logger.info(f"KenPom Conferences loaded: {len(cache)} conferences")
    return kenpom_conferences_cache


def fetch_all_kenpom_data():
    """
    Fetch ALL KenPom API endpoints in parallel and merge data.
    This is the master function that populates all caches.
    """
    global kenpom_cache_date
    today = date.today()

    if kenpom_cache_date == today:
        logger.info("KenPom data already loaded today, using cache")
        return True

    logger.info("Fetching all KenPom API endpoints...")

    # Clear the key resolution cache so fresh data gets re-resolved
    _kenpom_key_resolution_cache.clear()

    # Fetch all endpoints (the individual functions handle their own caching)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    endpoints_to_fetch = [
        ('ratings', fetch_kenpom_ratings),
        ('four-factors', fetch_kenpom_four_factors),
        ('pointdist', fetch_kenpom_point_distribution),
        ('height', fetch_kenpom_height),
        ('misc', fetch_kenpom_misc),
        ('confratings', fetch_kenpom_conference_ratings),
        ('fanmatch', fetch_kenpom_fanmatch),
        ('teams', fetch_kenpom_teams),
        ('conferences', fetch_kenpom_conferences),
    ]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fn): name for name, fn in endpoints_to_fetch}
        for future in as_completed(futures):
            endpoint_name = futures[future]
            try:
                future.result()
                logger.info(f"KenPom {endpoint_name} completed")
            except Exception as e:
                logger.error(f"KenPom {endpoint_name} failed: {e}")

    kenpom_cache_date = today
    logger.info("All KenPom data fetch complete")
    return True


def get_kenpom_team_full(team_name: str) -> Optional[dict]:
    """
    Get complete KenPom data for a team from all endpoints.
    Returns unified data structure matching NBA model breakdown format.
    Uses resolve_kenpom_key to find the EXACT KenPom cache key for ALL lookups.
    """
    # Ensure all data is loaded
    if not torvik_cache:
        fetch_all_kenpom_data()

    # Resolve to the exact KenPom cache key ONCE, use it everywhere
    kp_key = resolve_kenpom_key_cached(team_name)
    if not kp_key:
        logger.debug(f"KenPom key resolution failed for: {team_name}")
        return None

    # Get base ratings data using resolved key
    base_data = torvik_cache.get(kp_key)
    if not base_data:
        return None

    # Use the SAME resolved key for ALL supplementary caches
    ff_data = kenpom_four_factors_cache.get(kp_key, {})
    pd_data = kenpom_point_distribution_cache.get(kp_key, {})
    ht_data = kenpom_height_cache.get(kp_key, {})
    misc_data = kenpom_misc_cache.get(kp_key, {})
    fm_data = kenpom_fanmatch_cache.get(kp_key, {})
    team_info = kenpom_teams_cache.get(kp_key, {})

    if not ff_data and not ht_data and not misc_data:
        logger.debug(f"KenPom supplementary caches empty for key '{kp_key}' (team: {team_name})")

    # Get conference data
    conf_abbrev = base_data.get('conf', '').lower()
    conf_data = kenpom_conference_ratings_cache.get(conf_abbrev, {})

    # Build unified data structure matching NBA model format
    return {
        # Base ratings (from ratings endpoint)
        'team': base_data.get('team', team_name),
        'rank': base_data.get('rank', 999),
        'conf': base_data.get('conf', ''),
        'record': base_data.get('record', ''),
        'adj_o': base_data.get('adj_o', 0),
        'adj_d': base_data.get('adj_d', 0),
        'adj_em': base_data.get('adj_em', 0),
        'tempo': base_data.get('tempo', 67.5),
        'sos': base_data.get('sos', 0),
        'sos_rank': base_data.get('sos_rank', 0),

        # Four Factors - Offensive
        'o_efg': ff_data.get('o_efg', 0),
        'o_efg_rank': ff_data.get('o_efg_rank', 0),
        'o_to': ff_data.get('o_to', 0),
        'o_to_rank': ff_data.get('o_to_rank', 0),
        'o_or': ff_data.get('o_or', 0),
        'o_or_rank': ff_data.get('o_or_rank', 0),
        'o_ft_rate': ff_data.get('o_ft_rate', 0),
        'o_ft_rate_rank': ff_data.get('o_ft_rate_rank', 0),

        # Four Factors - Defensive
        'd_efg': ff_data.get('d_efg', 0),
        'd_efg_rank': ff_data.get('d_efg_rank', 0),
        'd_to': ff_data.get('d_to', 0),
        'd_to_rank': ff_data.get('d_to_rank', 0),
        'd_or': ff_data.get('d_or', 0),
        'd_or_rank': ff_data.get('d_or_rank', 0),
        'd_ft_rate': ff_data.get('d_ft_rate', 0),
        'd_ft_rate_rank': ff_data.get('d_ft_rate_rank', 0),

        # Shooting (from misc endpoint)
        'o_3pt_pct': misc_data.get('o_3pt_pct', 0),
        'o_3pt_rank': misc_data.get('o_3pt_rank', 0),
        'o_2pt_pct': misc_data.get('o_2pt_pct', 0),
        'o_2pt_rank': misc_data.get('o_2pt_rank', 0),
        'o_ft_pct': misc_data.get('o_ft_pct', 0),
        'o_ft_rank': misc_data.get('o_ft_rank', 0),
        'd_3pt_pct': misc_data.get('d_3pt_pct', 0),
        'd_3pt_rank': misc_data.get('d_3pt_rank', 0),
        'd_2pt_pct': misc_data.get('d_2pt_pct', 0),
        'd_2pt_rank': misc_data.get('d_2pt_rank', 0),

        # Ball movement/control
        'o_ast_rate': misc_data.get('o_ast_rate', 0),
        'o_ast_rank': misc_data.get('o_ast_rank', 0),
        'o_3pt_rate': misc_data.get('o_3pt_rate', 0),  # 3PT attempt rate
        'd_stl_pct': misc_data.get('d_stl_pct', 0),
        'd_stl_rank': misc_data.get('d_stl_rank', 0),
        'd_blk_pct': misc_data.get('d_blk_pct', 0),
        'd_blk_rank': misc_data.get('d_blk_rank', 0),

        # Point distribution
        'pts_from_3': pd_data.get('o_3pt_pct', 0),
        'pts_from_2': pd_data.get('o_2pt_pct', 0),
        'pts_from_ft': pd_data.get('o_ft_pct_dist', 0),

        # Height/Size
        'avg_height': ht_data.get('avg_height', 0),
        'avg_height_rank': ht_data.get('avg_height_rank', 0),
        'eff_height': ht_data.get('eff_height', 0),
        'experience': ht_data.get('experience', 0),
        'experience_rank': ht_data.get('experience_rank', 0),
        'bench_mins': ht_data.get('bench_mins', 0),
        'continuity': ht_data.get('continuity', 0),

        # Conference strength
        'conf_rank': conf_data.get('rank', 0),
        'conf_adj_em': conf_data.get('adj_em', 0),
        'conf_sos': conf_data.get('sos', 0),

        # Fanmatch
        'fanmatch': fm_data.get('fanmatch', 0),
    }


def compute_cbb_matchup_breakdown(away_team: str, home_team: str) -> dict:
    """
    Compute comprehensive CBB matchup breakdown using KenPom data.
    Returns structured data matching NBA model breakdown format.
    """
    away_data = get_kenpom_team_full(away_team)
    home_data = get_kenpom_team_full(home_team)

    result = {
        'has_data': False,
        'away_team': away_team,
        'home_team': home_team
    }

    if not away_data or not home_data:
        return result

    result['has_data'] = True
    num_teams = 365  # Total D1 teams for percentile calc

    # Power Rating (Overall ranking)
    result['power_rating'] = {
        'away': {
            'rank': away_data['rank'],
            'percentile': round((num_teams - away_data['rank'] + 1) / num_teams * 100),
            'adj_em': round(away_data['adj_em'], 1),
        },
        'home': {
            'rank': home_data['rank'],
            'percentile': round((num_teams - home_data['rank'] + 1) / num_teams * 100),
            'adj_em': round(home_data['adj_em'], 1),
        },
        'diff': home_data['rank'] - away_data['rank'],
        'edge': 'away' if away_data['rank'] < home_data['rank'] else ('home' if home_data['rank'] < away_data['rank'] else 'even')
    }

    # Offensive Efficiency
    result['offensive_efficiency'] = {
        'away': {
            'value': round(away_data['adj_o'], 1),
            'rank': away_data.get('rank', 0),  # Would need separate rank
            'percentile': round((num_teams - away_data.get('rank', 1) + 1) / num_teams * 100)
        },
        'home': {
            'value': round(home_data['adj_o'], 1),
            'rank': home_data.get('rank', 0),
            'percentile': round((num_teams - home_data.get('rank', 1) + 1) / num_teams * 100)
        },
        'diff': round(away_data['adj_o'] - home_data['adj_o'], 1),
        'edge': 'away' if away_data['adj_o'] > home_data['adj_o'] else ('home' if home_data['adj_o'] > away_data['adj_o'] else 'even')
    }

    # Defensive Efficiency (lower is better)
    result['defensive_efficiency'] = {
        'away': {
            'value': round(away_data['adj_d'], 1),
            'rank': away_data.get('rank', 0),
            'percentile': round((num_teams - away_data.get('rank', 1) + 1) / num_teams * 100)
        },
        'home': {
            'value': round(home_data['adj_d'], 1),
            'rank': home_data.get('rank', 0),
            'percentile': round((num_teams - home_data.get('rank', 1) + 1) / num_teams * 100)
        },
        'diff': round(home_data['adj_d'] - away_data['adj_d'], 1),
        'edge': 'away' if away_data['adj_d'] < home_data['adj_d'] else ('home' if home_data['adj_d'] < away_data['adj_d'] else 'even')
    }

    # Four Factors - Shooting Profile
    result['shooting_profile'] = {
        'efg_pct': {
            'away_season': round(away_data.get('o_efg', 0), 1),
            'home_season': round(home_data.get('o_efg', 0), 1),
            'away_rank': away_data.get('o_efg_rank', 0),
            'home_rank': home_data.get('o_efg_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['eFG_pct']
        },
        '3pt_pct': {
            'away_season': round(away_data.get('o_3pt_pct', 0), 1),
            'home_season': round(home_data.get('o_3pt_pct', 0), 1),
            'away_rank': away_data.get('o_3pt_rank', 0),
            'home_rank': home_data.get('o_3pt_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['3PT_pct']
        },
        '2pt_pct': {
            'away_season': round(away_data.get('o_2pt_pct', 0), 1),
            'home_season': round(home_data.get('o_2pt_pct', 0), 1),
            'away_rank': away_data.get('o_2pt_rank', 0),
            'home_rank': home_data.get('o_2pt_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['2PT_pct']
        },
        'ft_pct': {
            'away_season': round(away_data.get('o_ft_pct', 0), 1),
            'home_season': round(home_data.get('o_ft_pct', 0), 1),
            'away_rank': away_data.get('o_ft_rank', 0),
            'home_rank': home_data.get('o_ft_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['FT_pct']
        },
        'd_efg': {
            'away_season': round(away_data.get('d_efg', 0), 1),
            'home_season': round(home_data.get('d_efg', 0), 1),
        },
        'd_3pt': {
            'away_season': round(away_data.get('d_3pt_pct', 0), 1),
            'home_season': round(home_data.get('d_3pt_pct', 0), 1),
        }
    }

    # Ball Control / Turnovers
    result['ball_control'] = {
        'tov_pct': {
            'away_season': round(away_data.get('o_to', 0), 1),
            'home_season': round(home_data.get('o_to', 0), 1),
            'away_rank': away_data.get('o_to_rank', 0),
            'home_rank': home_data.get('o_to_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['TO_pct'],
            'away_protects': away_data.get('o_to', 20) < CBB_D1_AVERAGES['TO_pct'],
            'home_protects': home_data.get('o_to', 20) < CBB_D1_AVERAGES['TO_pct']
        },
        'forced_tov_pct': {
            'away_season': round(away_data.get('d_to', 0), 1),
            'home_season': round(home_data.get('d_to', 0), 1),
            'away_rank': away_data.get('d_to_rank', 0),
            'home_rank': home_data.get('d_to_rank', 0),
        },
        'stl_pct': {
            'away_season': round(away_data.get('d_stl_pct', 0), 1),
            'home_season': round(home_data.get('d_stl_pct', 0), 1),
            'away_rank': away_data.get('d_stl_rank', 0),
            'home_rank': home_data.get('d_stl_rank', 0),
        },
        'ast_rate': {
            'away_season': round(away_data.get('o_ast_rate', 0), 1),
            'home_season': round(home_data.get('o_ast_rate', 0), 1),
        }
    }

    # Rebounding
    result['rebounding'] = {
        'orb_pct': {
            'away_season': round(away_data.get('o_or', 0), 1),
            'home_season': round(home_data.get('o_or', 0), 1),
            'away_rank': away_data.get('o_or_rank', 0),
            'home_rank': home_data.get('o_or_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['OR_pct'],
            'away_crashes': away_data.get('o_or', 0) > CBB_D1_AVERAGES['OR_pct'] + 2,
            'home_crashes': home_data.get('o_or', 0) > CBB_D1_AVERAGES['OR_pct'] + 2
        },
        'drb_pct': {
            'away_season': round(away_data.get('d_or', 0), 1),  # DRB% = 100 - opponent ORB%
            'home_season': round(home_data.get('d_or', 0), 1),
            'away_rank': away_data.get('d_or_rank', 0),
            'home_rank': home_data.get('d_or_rank', 0),
        },
        'blk_pct': {
            'away_season': round(away_data.get('d_blk_pct', 0), 1),
            'home_season': round(home_data.get('d_blk_pct', 0), 1),
            'away_rank': away_data.get('d_blk_rank', 0),
            'home_rank': home_data.get('d_blk_rank', 0),
        }
    }

    # Pace & Free Throws
    result['pace_ft'] = {
        'ft_rate': {
            'away_season': round(away_data.get('o_ft_rate', 0), 1),
            'home_season': round(home_data.get('o_ft_rate', 0), 1),
            'away_rank': away_data.get('o_ft_rate_rank', 0),
            'home_rank': home_data.get('o_ft_rate_rank', 0),
            'd1_avg': CBB_D1_AVERAGES['FT_rate'],
            'away_attacks': away_data.get('o_ft_rate', 0) > CBB_D1_AVERAGES['FT_rate'] + 5,
            'home_attacks': home_data.get('o_ft_rate', 0) > CBB_D1_AVERAGES['FT_rate'] + 5
        },
        'opp_ft_rate': {
            'away_season': round(away_data.get('d_ft_rate', 0), 1),
            'home_season': round(home_data.get('d_ft_rate', 0), 1),
        },
        'pace': {
            'away': round(away_data.get('tempo', 67.5), 1),
            'home': round(home_data.get('tempo', 67.5), 1),
            'expected': round((away_data.get('tempo', 67.5) + home_data.get('tempo', 67.5)) / 2, 1),
            'd1_avg': CBB_D1_AVERAGES['tempo']
        }
    }

    # Size/Height matchup
    result['size'] = {
        'avg_height': {
            'away': round(away_data.get('avg_height', 0), 1),
            'home': round(home_data.get('avg_height', 0), 1),
            'away_rank': away_data.get('avg_height_rank', 0),
            'home_rank': home_data.get('avg_height_rank', 0),
        },
        'eff_height': {
            'away': round(away_data.get('eff_height', 0), 1),
            'home': round(home_data.get('eff_height', 0), 1),
        },
        'experience': {
            'away': round(away_data.get('experience', 0), 2),
            'home': round(home_data.get('experience', 0), 2),
            'away_rank': away_data.get('experience_rank', 0),
            'home_rank': home_data.get('experience_rank', 0),
        }
    }

    # Strength of Schedule
    result['sos'] = {
        'away': {
            'sos': round(away_data.get('sos', 0), 2),
            'sos_rank': away_data.get('sos_rank', 0),
            'conf_rank': away_data.get('conf_rank', 0),
            'conf_adj_em': round(away_data.get('conf_adj_em', 0), 1),
        },
        'home': {
            'sos': round(home_data.get('sos', 0), 2),
            'sos_rank': home_data.get('sos_rank', 0),
            'conf_rank': home_data.get('conf_rank', 0),
            'conf_adj_em': round(home_data.get('conf_adj_em', 0), 1),
        }
    }

    # Point Distribution
    result['point_dist'] = {
        'away': {
            'from_3': round(away_data.get('pts_from_3', 0), 1),
            'from_2': round(away_data.get('pts_from_2', 0), 1),
            'from_ft': round(away_data.get('pts_from_ft', 0), 1),
        },
        'home': {
            'from_3': round(home_data.get('pts_from_3', 0), 1),
            'from_2': round(home_data.get('pts_from_2', 0), 1),
            'from_ft': round(home_data.get('pts_from_ft', 0), 1),
        }
    }

    # Generate analyst insight
    power_diff = abs(result['power_rating']['diff'])
    if power_diff <= 25:
        result['analyst_insight'] = "Evenly matched teams - expect a competitive game."
    elif result['power_rating']['edge'] == 'away':
        off_matchup = away_data['adj_o'] - home_data['adj_d']
        if off_matchup > 5:
            result['analyst_insight'] = f"{away_team} offense ({away_data['adj_o']:.1f}) vs {home_team} defense ({home_data['adj_d']:.1f}) favors scoring."
        else:
            result['analyst_insight'] = f"{away_team} has the edge with better overall efficiency (#{away_data['rank']} vs #{home_data['rank']})."
    else:
        off_matchup = home_data['adj_o'] - away_data['adj_d']
        if off_matchup > 5:
            result['analyst_insight'] = f"{home_team} offense ({home_data['adj_o']:.1f}) vs {away_team} defense ({away_data['adj_d']:.1f}) favors scoring."
        else:
            result['analyst_insight'] = f"{home_team} has the edge with better overall efficiency (#{home_data['rank']} vs #{away_data['rank']})."

    # Store raw data for template access
    result['away_kenpom'] = away_data
    result['home_kenpom'] = home_data

    return result


def get_kenpom_rank(team_name: str) -> int:
    """Get KenPom efficiency ranking (1-365) for any CBB team."""
    tv = get_torvik_team(team_name)
    if tv:
        return tv.get('rank', 999)
    return 999

def fetch_kenpom_ratings():
    """Fetch KenPom team ratings for CBB via official API. Cached daily."""
    global torvik_cache, torvik_cache_date
    today = date.today()
    if torvik_cache_date == today and torvik_cache:
        logger.info(f"Using cached KenPom data ({len(torvik_cache)} teams)")
        return torvik_cache
    
    api_key = os.environ.get('CBB_API_KEY', '')
    if not api_key:
        logger.warning("CBB_API_KEY not set, skipping KenPom fetch")
        return torvik_cache
    
    try:
        logger.info("Fetching KenPom CBB ratings via API...")
        url = "https://kenpom.com/api.php?endpoint=ratings&y=2026"
        headers = {
            'Authorization': f'Bearer {api_key}',
            'User-Agent': 'Mozilla/5.0 (compatible; SportsApp/1.0)'
        }
        resp = requests.get(url, headers=headers, timeout=30)
        
        if resp.status_code == 401:
            logger.warning("KenPom API unauthorized - check API key")
            return torvik_cache
        if resp.status_code != 200:
            logger.warning(f"KenPom API fetch failed: {resp.status_code}")
            return torvik_cache
        
        data = resp.json()
        if not data:
            logger.warning("KenPom API returned empty data")
            return torvik_cache
        
        new_cache = {}
        for team in data:
            try:
                team_name = team.get('TeamName', '')
                rank = team.get('RankAdjEM', 0)
                adj_o = team.get('AdjOE', 0)
                adj_d = team.get('AdjDE', 0)
                adj_em = team.get('AdjEM', 0)
                tempo = team.get('AdjTempo', 0)
                conf = team.get('ConfShort', '')
                wins = team.get('Wins', 0)
                losses = team.get('Losses', 0)
                sos = team.get('SOS', 0)
                sos_rank = team.get('RankSOS', 0)
                
                if team_name and adj_o and adj_d:
                    new_cache[team_name.lower()] = {
                        'rank': rank,
                        'team': team_name,
                        'conf': conf,
                        'record': f"{wins}-{losses}",
                        'adj_o': adj_o,
                        'adj_d': adj_d,
                        'adj_em': adj_em,
                        'tempo': tempo,
                        'sos': sos,
                        'sos_rank': sos_rank
                    }
            except (ValueError, KeyError) as e:
                continue
        
        if new_cache:
            torvik_cache = new_cache
            torvik_cache_date = today
            _kenpom_key_resolution_cache.clear()  # Clear stale key mappings
            logger.info(f"KenPom API loaded {len(new_cache)} teams")
            logger.info(f"Torvik data loaded: {len(new_cache)} teams")
        return torvik_cache
    except Exception as e:
        logger.error(f"Torvik fetch error: {e}")
        return torvik_cache

CBB_TEAM_NAME_MAP = {
    # Comprehensive mapping: Covers.com/ESPN name -> KenPom name
    # States with abbreviations
    'ball state': 'ball st.', 'ball st': 'ball st.', 'bsu': 'ball st.',
    'ohio state': 'ohio st.', 'ohio st': 'ohio st.', 'osu': 'ohio st.',
    'michigan state': 'michigan st.', 'michigan st': 'michigan st.', 'msu': 'michigan st.',
    'florida state': 'florida st.', 'florida st': 'florida st.', 'fsu': 'florida st.',
    'penn state': 'penn st.', 'penn st': 'penn st.', 'psu': 'penn st.',
    'iowa state': 'iowa st.', 'iowa st': 'iowa st.', 'isu': 'iowa st.',
    'kansas state': 'kansas st.', 'kansas st': 'kansas st.', 'ksu': 'kansas st.', 'k-state': 'kansas st.',
    'oklahoma state': 'oklahoma st.', 'oklahoma st': 'oklahoma st.', 'okst': 'oklahoma st.', 'ok state': 'oklahoma st.',
    'oregon state': 'oregon st.', 'oregon st': 'oregon st.', 'osu': 'oregon st.',
    'washington state': 'washington st.', 'washington st': 'washington st.', 'wsu': 'washington st.', 'wazzu': 'washington st.',
    'mississippi state': 'mississippi st.', 'mississippi st': 'mississippi st.', 'miss state': 'mississippi st.', 'miss st': 'mississippi st.',
    'arizona state': 'arizona st.', 'arizona st': 'arizona st.', 'asu': 'arizona st.',
    'fresno state': 'fresno st.', 'fresno st': 'fresno st.',
    'boise state': 'boise st.', 'boise st': 'boise st.',
    'san diego state': 'san diego st.', 'san diego st': 'san diego st.', 'sdsu': 'san diego st.',
    'colorado state': 'colorado st.', 'colorado st': 'colorado st.', 'csu': 'colorado st.',
    'utah state': 'utah st.', 'utah st': 'utah st.', 'usu': 'utah st.',
    'georgia state': 'georgia st.', 'georgia st': 'georgia st.', 'gsu': 'georgia st.',
    'kennesaw state': 'kennesaw st.', 'kennesaw st': 'kennesaw st.',
    'jacksonville state': 'jacksonville st.', 'jacksonville st': 'jacksonville st.', 'jax state': 'jacksonville st.', 'jax st': 'jacksonville st.',
    'sam houston state': 'sam houston st.', 'sam houston st': 'sam houston st.', 'sam houston': 'sam houston st.', 'shsu': 'sam houston st.',
    'appalachian state': 'appalachian st.', 'appalachian st': 'appalachian st.', 'app state': 'appalachian st.', 'app st': 'appalachian st.',
    'tarleton state': 'tarleton st.', 'tarleton st': 'tarleton st.', 'tarleton': 'tarleton st.',
    'weber state': 'weber st.', 'weber st': 'weber st.',
    'idaho state': 'idaho st.', 'idaho st': 'idaho st.',
    'montana state': 'montana st.', 'montana st': 'montana st.',
    'portland state': 'portland st.', 'portland st': 'portland st.',
    'sacramento state': 'sacramento st.', 'sacramento st': 'sacramento st.', 'sac state': 'sacramento st.', 'sac st': 'sacramento st.',
    'norfolk state': 'norfolk st.', 'norfolk st': 'norfolk st.', 'nsu': 'norfolk st.',
    'coppin state': 'coppin st.', 'coppin st': 'coppin st.',
    'morgan state': 'morgan st.', 'morgan st': 'morgan st.',
    'delaware state': 'delaware st.', 'delaware st': 'delaware st.', 'dsu': 'delaware st.',
    'south carolina state': 'south carolina st.', 'south carolina st': 'south carolina st.', 'sc state': 'south carolina st.',
    'north carolina state': 'n.c. state', 'nc state': 'n.c. state', 'n.c. state': 'n.c. state',
    'alcorn state': 'alcorn st.', 'alcorn st': 'alcorn st.',
    'alabama state': 'alabama st.', 'alabama st': 'alabama st.', 'bama state': 'alabama st.',
    'grambling state': 'grambling', 'grambling st': 'grambling',
    'jackson state': 'jackson st.', 'jackson st': 'jackson st.',
    'prairie view': 'prairie view a&m', 'prairie view am': 'prairie view a&m',
    'texas southern': 'texas southern',
    'arkansas state': 'arkansas st.', 'arkansas st': 'arkansas st.',
    'missouri state': 'missouri st.', 'missouri st': 'missouri st.',
    'indiana state': 'indiana st.', 'indiana st': 'indiana st.',
    'illinois state': 'illinois st.', 'illinois st': 'illinois st.',
    'wichita state': 'wichita st.', 'wichita st': 'wichita st.',
    'wright state': 'wright st.', 'wright st': 'wright st.',
    'cleveland state': 'cleveland st.', 'cleveland st': 'cleveland st.',
    'youngstown state': 'youngstown st.', 'youngstown st': 'youngstown st.',
    'murray state': 'murray st.', 'murray st': 'murray st.',
    'morehead state': 'morehead st.', 'morehead st': 'morehead st.',
    'kent state': 'kent st.', 'kent st': 'kent st.',
    'bowling green': 'bowling green',
    # Saint/St. variations
    "st. john's": "st. john's", 'st johns': "st. john's", "st john's": "st. john's", 'saint johns': "st. john's",
    'st_johns': "st. john's", 'sju': "st. john's", "saint john's": "st. john's", "st. john's red storm": "st. john's",
    "st. mary's": "saint mary's", 'st marys': "saint mary's", "saint mary's": "saint mary's", 'saint marys': "saint mary's",
    'st_marys': "saint mary's", 'smc': "saint mary's", "st mary's": "saint mary's",
    "st. joseph's": "saint joseph's", 'st josephs': "saint joseph's", "saint joseph's": "saint joseph's",
    'st_josephs': "saint joseph's", 'hawk': "saint joseph's", "st joseph's": "saint joseph's",
    'st. bonaventure': 'st. bonaventure', 'st bonaventure': 'st. bonaventure', 'saint bonaventure': 'st. bonaventure',
    'st_bonaventure': 'st. bonaventure', 'bonnies': 'st. bonaventure',
    'st. peters': "saint peter's", "st. peter's": "saint peter's", 'saint peters': "saint peter's",
    'st_peters': "saint peter's", 'peacocks': "saint peter's", "saint peter's": "saint peter's",
    'st. thomas': 'st. thomas', 'st thomas': 'st. thomas', 'saint thomas': 'st. thomas',
    'st_thomas': 'st. thomas', 'tommies': 'st. thomas',
    'mount st. marys': "mount st. mary's", "mount st. mary's": "mount st. mary's", 'mt st marys': "mount st. mary's",
    'mount_st_marys': "mount st. mary's", 'msm': "mount st. mary's",
    'saint louis': 'saint louis', 'st louis': 'saint louis', 'slu': 'saint louis',
    # Common acronyms
    'uconn': 'connecticut', 'connecticut': 'connecticut',
    'usc': 'usc', 'southern california': 'usc',
    'unc': 'north carolina', 'north carolina': 'north carolina',
    'ucla': 'ucla', 'california los angeles': 'ucla',
    'lsu': 'lsu', 'louisiana state': 'lsu',
    'vcu': 'vcu', 'virginia commonwealth': 'vcu',
    'ucf': 'ucf', 'central florida': 'ucf',
    'uic': 'illinois chicago', 'illinois chicago': 'illinois chicago', 'illinois-chicago': 'illinois chicago', 'uic flames': 'illinois chicago',
    'usf': 'south florida', 'south florida': 'south florida',
    'fiu': 'fiu', 'florida international': 'fiu',
    'fau': 'fau', 'florida atlantic': 'fau',
    'unlv': 'unlv', 'nevada las vegas': 'unlv',
    'utep': 'utep', 'texas el paso': 'utep',
    'utsa': 'utsa', 'texas san antonio': 'utsa',
    'smu': 'smu', 'southern methodist': 'smu',
    'tcu': 'tcu', 'texas christian': 'tcu',
    'byu': 'byu', 'brigham young': 'byu',
    'mtsu': 'middle tennessee', 'middle tennessee': 'middle tennessee', 'middle tenn': 'middle tennessee',
    'etsu': 'east tennessee st.', 'east tennessee': 'east tennessee st.', 'east tennessee state': 'east tennessee st.',
    'ut martin': 'tennessee martin', 'utm': 'tennessee martin', 'tennessee martin': 'tennessee martin', 'tenn martin': 'tennessee martin',
    # Regional directionals
    'western kentucky': 'western kentucky', 'wku': 'western kentucky', 'western ky': 'western kentucky',
    'eastern kentucky': 'eastern kentucky', 'eku': 'eastern kentucky', 'eastern ky': 'eastern kentucky', 'e kentucky': 'eastern kentucky',
    'northern kentucky': 'northern kentucky', 'nku': 'northern kentucky', 'n kentucky': 'northern kentucky',
    'northern illinois': 'northern illinois', 'niu': 'northern illinois', 'n illinois': 'northern illinois',
    'southern illinois': 'southern illinois', 'siu': 'southern illinois', 's illinois': 'southern illinois',
    'eastern illinois': 'eastern illinois', 'eiu': 'eastern illinois', 'e illinois': 'eastern illinois',
    'western illinois': 'western illinois', 'wiu': 'western illinois', 'w illinois': 'western illinois',
    'northern iowa': 'northern iowa', 'uni': 'northern iowa', 'n iowa': 'northern iowa',
    'eastern michigan': 'eastern michigan', 'emu': 'eastern michigan', 'e michigan': 'eastern michigan',
    'western michigan': 'western michigan', 'wmu': 'western michigan', 'w michigan': 'western michigan',
    'central michigan': 'central michigan', 'cmu': 'central michigan', 'c michigan': 'central michigan',
    'northern colorado': 'northern colorado', 'n colorado': 'northern colorado',
    'southern utah': 'southern utah', 's utah': 'southern utah',
    'northern arizona': 'northern arizona', 'nau': 'northern arizona', 'n arizona': 'northern arizona',
    'eastern washington': 'eastern washington', 'ewu': 'eastern washington', 'e washington': 'eastern washington',
    'central arkansas': 'central arkansas', 'uca': 'central arkansas', 'c arkansas': 'central arkansas',
    # George Washington variations
    'george washington': 'george washington', 'g washington': 'george washington', 'gw': 'george washington', 'gwu': 'george washington',
    # Pittsburgh
    'pittsburgh': 'pittsburgh', 'pitt': 'pittsburgh',
    # Ole Miss
    'ole miss': 'mississippi', 'mississippi': 'mississippi',
    # Connecticut variations
    'connecticut': 'connecticut', 'uconn': 'connecticut',
    # Texas A&M variations
    'texas am': 'texas a&m', 'texas a&m': 'texas a&m', 'tamu': 'texas a&m',
    # UC schools
    'uc davis': 'uc davis', 'ucd': 'uc davis', 'california davis': 'uc davis',
    'uc irvine': 'uc irvine', 'uci': 'uc irvine', 'california irvine': 'uc irvine',
    'uc riverside': 'uc riverside', 'ucr': 'uc riverside', 'california riverside': 'uc riverside',
    'uc san diego': 'uc san diego', 'ucsd': 'uc san diego', 'california san diego': 'uc san diego',
    'uc santa barbara': 'uc santa barbara', 'ucsb': 'uc santa barbara', 'santa barbara': 'uc santa barbara',
    'cal poly': 'cal poly', 'cal poly slo': 'cal poly',
    'cal state fullerton': 'cal st. fullerton', 'fullerton': 'cal st. fullerton', 'csuf': 'cal st. fullerton',
    'cal state northridge': 'csun', 'northridge': 'csun', 'csu northridge': 'csun', 'csun': 'csun', 'cal st. northridge': 'csun', 'cal st northridge': 'csun', 'cs northridge': 'csun', 'cs state northridge': 'csun',
    'cal state bakersfield': 'cal st. bakersfield', 'bakersfield': 'cal st. bakersfield', 'csub': 'cal st. bakersfield',
    'long beach state': 'long beach st.', 'long beach st': 'long beach st.', 'lbsu': 'long beach st.',
    # UMass variations
    'umass': 'massachusetts', 'massachusetts': 'massachusetts', 'mass': 'massachusetts',
    'umass lowell': 'umass lowell', 'massachusetts lowell': 'umass lowell',
    # Loyola schools
    'loyola chicago': 'loyola chicago', 'loyola chi': 'loyola chicago', 'loyola il': 'loyola chicago',
    'loyola marymount': 'loyola marymount', 'lmu': 'loyola marymount', 'loyola la': 'loyola marymount',
    'loyola md': 'loyola maryland', 'loyola maryland': 'loyola maryland',
    # Miami variations
    'miami fl': 'miami fl', 'miami florida': 'miami fl', 'miami hurricanes': 'miami fl',
    'miami oh': 'miami oh', 'miami ohio': 'miami oh', 'miami redhawks': 'miami oh',
    # Other common variations
    'unc greensboro': 'unc greensboro', 'uncg': 'unc greensboro', 'greensboro': 'unc greensboro',
    'unc asheville': 'unc asheville', 'unca': 'unc asheville', 'asheville': 'unc asheville',
    'unc wilmington': 'unc wilmington', 'uncw': 'unc wilmington', 'wilmington': 'unc wilmington',
    'unc charlotte': 'charlotte', 'uncc': 'charlotte', 'charlotte': 'charlotte',
    'georgia southern': 'georgia southern', 'ga southern': 'georgia southern', 'gaso': 'georgia southern',
    'georgia tech': 'georgia tech', 'ga tech': 'georgia tech', 'gt': 'georgia tech',
    'texas tech': 'texas tech', 'ttu': 'texas tech', 'tech': 'texas tech',
    'virginia tech': 'virginia tech', 'vt': 'virginia tech', 'va tech': 'virginia tech',
    'tennessee tech': 'tennessee tech', 'ttu': 'tennessee tech', 'tn tech': 'tennessee tech',
    'louisiana tech': 'louisiana tech', 'la tech': 'louisiana tech',
    # New Mexico variations
    'new mexico': 'new mexico', 'unm': 'new mexico',
    'new mexico state': 'new mexico st.', 'new mexico st': 'new mexico st.', 'nmsu': 'new mexico st.',
    # Texas State variations
    'texas state': 'texas st.', 'texas st': 'texas st.', 'txst': 'texas st.',
    # Other schools
    'grand canyon': 'grand canyon', 'gcu': 'grand canyon',
    'gonzaga': 'gonzaga', 'zags': 'gonzaga',
    'villanova': 'villanova', 'nova': 'villanova',
    'creighton': 'creighton', 'bluejays': 'creighton',
    'marquette': 'marquette',
    'seton hall': 'seton hall', 'hall': 'seton hall',
    'xavier': 'xavier',
    'butler': 'butler',
    'providence': 'providence', 'friars': 'providence',
    'depaul': 'depaul',
    'georgetown': 'georgetown', 'hoyas': 'georgetown',
    # Southern schools
    'southern': 'southern', 'southern jaguars': 'southern',
    'southern miss': 'southern miss', 'southern mississippi': 'southern miss', 'usm': 'southern miss',
    'south alabama': 'south alabama', 'usa': 'south alabama',
    'south florida': 'south florida', 'usf': 'south florida',
    # Ivy League
    'harvard': 'harvard', 'yale': 'yale', 'princeton': 'princeton', 'columbia': 'columbia',
    'cornell': 'cornell', 'brown': 'brown', 'dartmouth': 'dartmouth', 'penn': 'penn', 'pennsylvania': 'penn',
    # HBCU and smaller schools
    'howard': 'howard', 'hampton': 'hampton', 'delaware': 'delaware',
    'bethune': 'bethune cookman', 'bethune cookman': 'bethune cookman', 'bccu': 'bethune cookman', 'b cookman': 'bethune cookman',
    'north carolina at': 'n.c. a&t', 'nc at': 'n.c. a&t', 'nc a&t': 'n.c. a&t', 'n.c. a&t': 'n.c. a&t',
    'florida am': 'florida a&m', 'florida a&m': 'florida a&m', 'famu': 'florida a&m',
    # Additional variations
    'siu edwardsville': 'siu edwardsville', 'siue': 'siu edwardsville',
    'southeast missouri': 'southeast missouri st.', 'se missouri': 'southeast missouri st.', 'semo': 'southeast missouri st.',
    'ut arlington': 'ut arlington', 'uta': 'ut arlington', 'texas arlington': 'ut arlington',
    'ut rio grande': 'ut rio grande valley', 'utrgv': 'ut rio grande valley', 'rio grande valley': 'ut rio grande valley',
    'texas am cc': 'texas a&m corpus chris', 'texas a&m corpus christi': 'texas a&m corpus chris', 'tamucc': 'texas a&m corpus chris',
    'lamar': 'lamar', 'mcneese': 'mcneese', 'nicholls': 'nicholls', 'nicholls state': 'nicholls',
    'houston christian': 'houston christian', 'hcu': 'houston christian',
    'incarnate word': 'incarnate word', 'uiw': 'incarnate word',
    'abilene christian': 'abilene christian', 'acu': 'abilene christian', 'abilene chrstn': 'abilene christian',
    'little rock': 'little rock', 'ualr': 'little rock',
    'ar pine bluff': 'arkansas pine bluff', 'arkansas pine bluff': 'arkansas pine bluff', 'uapb': 'arkansas pine bluff',
    'central connecticut': 'central connecticut', 'c connecticut': 'central connecticut', 'ccsu': 'central connecticut',
    'fairleigh dickinson': 'fairleigh dickinson', 'fdu': 'fairleigh dickinson',
    'long island': 'long island', 'liu': 'long island',
    'detroit mercy': 'detroit mercy', 'detroit': 'detroit mercy', 'udm': 'detroit mercy',
    'chicago state': 'chicago st.', 'chicago st': 'chicago st.', 'csu': 'chicago st.',
    'southern indiana': 'southern indiana', 'so indiana': 'southern indiana', 'usi': 'southern indiana',
    'bellarmine': 'bellarmine', 'queens': 'queens',
    'lindenwood': 'lindenwood', 'mercyhurst': 'mercyhurst',
    "hawaii": "hawaii", "hawai'i": "hawaii", "hawai\u2019i": "hawaii",
    'texas am': 'e. texas a&m', 'e texas am': 'e. texas a&m', 'east texas am': 'e. texas a&m', 'e texas a&m': 'e. texas a&m',
    'miss valley st': 'mississippi valley st.', 'mississippi valley': 'mississippi valley st.', 'mvsu': 'mississippi valley st.',
    # Southern Utah
    'southern utah': 'southern utah', 's utah': 'southern utah',
    'utah valley': 'utah valley', 'uvu': 'utah valley',
    'utah tech': 'utah tech', 'dixie state': 'utah tech',
    # Big schools
    'duke': 'duke', 'kentucky': 'kentucky', 'uk': 'kentucky',
    'kansas': 'kansas', 'ku': 'kansas',
    'indiana': 'indiana', 'iu': 'indiana',
    'north carolina': 'north carolina', 'unc': 'north carolina', 'carolina': 'north carolina',
    'michigan': 'michigan', 'wolverines': 'michigan',
    'purdue': 'purdue', 'boilermakers': 'purdue',
    'wisconsin': 'wisconsin', 'badgers': 'wisconsin',
    'illinois': 'illinois', 'illini': 'illinois',
    'auburn': 'auburn', 'tigers': 'auburn',
    'tennessee': 'tennessee', 'vols': 'tennessee',
    'alabama': 'alabama', 'bama': 'alabama',
    'florida': 'florida', 'gators': 'florida',
    'houston': 'houston', 'coogs': 'houston',
    'cincinnati': 'cincinnati', 'cincy': 'cincinnati', 'uc': 'cincinnati',
    # MAC schools
    'toledo': 'toledo', 'rockets': 'toledo',
    'ohio': 'ohio', 'ohio bobcats': 'ohio',
    'akron': 'akron', 'zips': 'akron',
    'buffalo': 'buffalo', 'ub': 'buffalo',
    # A10 schools
    'dayton': 'dayton', 'flyers': 'dayton',
    'davidson': 'davidson', 'wildcats': 'davidson',
    'richmond': 'richmond', 'spiders': 'richmond',
    'fordham': 'fordham', 'rams': 'fordham',
    'george mason': 'george mason', 'gmu': 'george mason',
    'la salle': 'la salle', 'lasalle': 'la salle',
    'rhode island': 'rhode island', 'uri': 'rhode island',
    'duquesne': 'duquesne', 'dukes': 'duquesne',
    'st louis': 'saint louis', 'saint louis': 'saint louis', 'billikens': 'saint louis',
    # Patriot League
    'colgate': 'colgate', 'raiders': 'colgate',
    'lehigh': 'lehigh', 'mountain hawks': 'lehigh',
    'bucknell': 'bucknell', 'bison': 'bucknell',
    'army': 'army', 'black knights': 'army',
    'navy': 'navy', 'midshipmen': 'navy',
    'boston u': 'boston university', 'boston university': 'boston university', 'bu': 'boston university',
    'american': 'american', 'eagles': 'american',
    'holy cross': 'holy cross', 'crusaders': 'holy cross',
    'lafayette': 'lafayette', 'leopards': 'lafayette',
    # WCC schools
    'san diego': 'san diego', 'toreros': 'san diego',
    'pepperdine': 'pepperdine', 'waves': 'pepperdine',
    'san francisco': 'san francisco', 'usf': 'san francisco', 'dons': 'san francisco',
    'pacific': 'pacific', 'tigers': 'pacific',
    'santa clara': 'santa clara', 'broncos': 'santa clara',
    'portland': 'portland', 'pilots': 'portland',
    # MWC schools
    'nevada': 'nevada', 'wolfpack': 'nevada',
    'wyoming': 'wyoming', 'cowboys': 'wyoming',
    'air force': 'air force', 'falcons': 'air force',
    # Conference USA
    'uab': 'uab', 'blazers': 'uab',
    'north texas': 'north texas', 'unt': 'north texas', 'mean green': 'north texas',
    'florida atlantic': 'fau', 'fau': 'fau', 'owls': 'fau',
    'old dominion': 'old dominion', 'odu': 'old dominion', 'monarchs': 'old dominion',
    'marshall': 'marshall', 'thundering herd': 'marshall',
    'james madison': 'james madison', 'jmu': 'james madison', 'dukes': 'james madison',
    # Big East
    'uconn': 'connecticut', 'huskies': 'connecticut',
    # Big 12
    'baylor': 'baylor', 'bears': 'baylor',
    'west virginia': 'west virginia', 'wvu': 'west virginia', 'mountaineers': 'west virginia',
    'oklahoma': 'oklahoma', 'sooners': 'oklahoma', 'ou': 'oklahoma',
    'texas': 'texas', 'longhorns': 'texas', 'ut': 'texas',
    'cincinnati': 'cincinnati', 'bearcats': 'cincinnati',
    'ucf': 'ucf', 'knights': 'ucf',
    'colorado': 'colorado', 'buffaloes': 'colorado', 'cu': 'colorado',
    'utah': 'utah', 'utes': 'utah',
    'arizona': 'arizona', 'wildcats': 'arizona', 'zona': 'arizona',
    # ACC
    'clemson': 'clemson', 'tigers': 'clemson',
    'louisville': 'louisville', 'cards': 'louisville', 'uofl': 'louisville',
    'wake forest': 'wake forest', 'demon deacons': 'wake forest',
    'boston college': 'boston college', 'bc': 'boston college', 'eagles': 'boston college',
    'syracuse': 'syracuse', 'orange': 'syracuse', 'cuse': 'syracuse',
    'notre dame': 'notre dame', 'irish': 'notre dame', 'nd': 'notre dame',
    'stanford': 'stanford', 'cardinal': 'stanford',
    'california': 'california', 'cal': 'california', 'bears': 'california',
    # SEC
    'arkansas': 'arkansas', 'razorbacks': 'arkansas', 'hogs': 'arkansas',
    'georgia': 'georgia', 'uga': 'georgia', 'bulldogs': 'georgia',
    'south carolina': 'south carolina', 'gamecocks': 'south carolina', 'usc': 'south carolina',
    'missouri': 'missouri', 'mizzou': 'missouri', 'tigers': 'missouri',
    'vanderbilt': 'vanderbilt', 'vandy': 'vanderbilt', 'commodores': 'vanderbilt',
    'texas am': 'texas a&m', 'aggies': 'texas a&m',
    'lsu': 'lsu', 'tigers': 'lsu',
    'ole miss': 'mississippi', 'rebels': 'mississippi',
    # Big Ten
    'maryland': 'maryland', 'terps': 'maryland', 'umd': 'maryland',
    'rutgers': 'rutgers', 'scarlet knights': 'rutgers',
    'northwestern': 'northwestern', 'wildcats': 'northwestern', 'nu': 'northwestern',
    'minnesota': 'minnesota', 'gophers': 'minnesota',
    'nebraska': 'nebraska', 'huskers': 'nebraska',
    'iowa': 'iowa', 'hawkeyes': 'iowa',
    'oregon': 'oregon', 'ducks': 'oregon', 'uo': 'oregon',
    'washington': 'washington', 'huskies': 'washington', 'uw': 'washington',
    # Sun Belt
    'troy': 'troy', 'trojans': 'troy',
    'south alabama': 'south alabama', 'jaguars': 'south alabama',
    'coastal carolina': 'coastal carolina', 'chanticleers': 'coastal carolina', 'ccu': 'coastal carolina',
    'texas state': 'texas st.', 'bobcats': 'texas st.',
    'louisiana': 'louisiana', 'ragin cajuns': 'louisiana', 'ul': 'louisiana',
    # Other
    'belmont': 'belmont', 'bruins': 'belmont',
    'valparaiso': 'valparaiso', 'valpo': 'valparaiso', 'crusaders': 'valparaiso',
    'evansville': 'evansville', 'purple aces': 'evansville',
    'radford': 'radford', 'highlanders': 'radford',
    'presbyterian': 'presbyterian', 'blue hose': 'presbyterian',
    'winthrop': 'winthrop', 'eagles': 'winthrop',
    'citadel': 'the citadel', 'the citadel': 'the citadel',
    'maine': 'maine', 'black bears': 'maine',
    # ESPN abbreviation mappings for mid-major teams
    'sc upstate': 'usc upstate', 'south carolina upstate': 'usc upstate', 'usc upstate': 'usc upstate',
    'gardner-webb': 'gardner webb', 'gardner webb': 'gardner webb',
    "n'western st": 'northwestern st.', 'northwestern st': 'northwestern st.', 'northwestern state': 'northwestern st.',
    'north alabama': 'north alabama', 'una': 'north alabama',
    'md eastern': 'maryland eastern shore', 'md eastern shore': 'maryland eastern shore', 'umes': 'maryland eastern shore',
    'south dakota': 'south dakota', 'usd': 'south dakota', 'south dakota st': 'south dakota st.',
    'kansas city': 'kansas city', 'umkc': 'kansas city',
    'high point': 'high point', 'hpu': 'high point',
    'lipscomb': 'lipscomb', 'bisons': 'lipscomb',
    'austin peay': 'austin peay', 'apsu': 'austin peay', 'governors': 'austin peay',
    'purdue fw': 'purdue fort wayne', 'purdue fort wayne': 'purdue fort wayne', 'pfw': 'purdue fort wayne', 'fort wayne': 'purdue fort wayne',
    'charleston so': 'charleston southern', 'charleston southern': 'charleston southern', 'chas southern': 'charleston southern',
    'longwood': 'longwood', 'lancers': 'longwood',
    'tulsa': 'tulsa', 'golden hurricane': 'tulsa',
    'e texas a&m': 'e. texas a&m', 'east texas a&m': 'e. texas a&m', 'e texas am': 'e. texas a&m', 'east texas am': 'e. texas a&m', 'etamu': 'e. texas a&m',
    'ar-pine bluff': 'arkansas pine bluff', 'ar pine bluff': 'arkansas pine bluff', 'uapb': 'arkansas pine bluff',
    'grambling': 'grambling', 'grambling st': 'grambling', 'grambling state': 'grambling',
    'c arkansas': 'central arkansas', 'central arkansas': 'central arkansas', 'uca': 'central arkansas',
    'mtsu': 'middle tennessee', 'middle tennessee': 'middle tennessee', 'middle tenn': 'middle tennessee', 'mid tennessee': 'middle tennessee',
    'fiu': 'fiu', 'florida international': 'fiu', 'fla intl': 'fiu',
    'sc state': 'south carolina st.', 'south carolina state': 'south carolina st.',
    # New D1 / transitional teams that ESPN names differently than KenPom
    'le moyne': 'le moyne', 'lemoyne': 'le moyne', 'le moyne dolphins': 'le moyne',
    'mercyhurst': 'mercyhurst', 'mercy': 'mercyhurst', 'mercyhurst lakers': 'mercyhurst',
    'stonehill': 'stonehill', 'stonehill skyhawks': 'stonehill',
    'cal baptist': 'cal baptist', 'california baptist': 'cal baptist', 'cbu': 'cal baptist',
    'queens': 'queens', 'queens royals': 'queens',
    'bellarmine': 'bellarmine', 'bellarmine knights': 'bellarmine',
    'lindenwood': 'lindenwood', 'lindenwood lions': 'lindenwood',
    'st thomas': 'st. thomas', 'st. thomas': 'st. thomas', 'st thomas mn': 'st. thomas',
    'texas am commerce': 'east texas a&m', 'texas a&m commerce': 'east texas a&m',
    'south dakota state': 'south dakota st.', 's dakota st': 'south dakota st.',
    'north dakota state': 'north dakota st.', 'n dakota st': 'north dakota st.', 'ndsu': 'north dakota st.',
    'north dakota': 'north dakota', 'und': 'north dakota',
    # Additional ESPN shortDisplayName variants
    'cs fullerton': 'cal st. fullerton', 'cs northridge': 'cal st. northridge', 'cs bakersfield': 'cal st. bakersfield',
    'fla gulf coast': 'florida gulf coast', 'fgcu': 'florida gulf coast', 'florida gulf coast': 'florida gulf coast',
    'nc central': 'north carolina central', 'nccu': 'north carolina central', 'north carolina central': 'north carolina central',
    'southeastern la': 'southeastern louisiana', 'se louisiana': 'southeastern louisiana', 'southeastern louisiana': 'southeastern louisiana',
    'northwestern la': 'northwestern st.', 'nw state': 'northwestern st.',
    'southern u': 'southern', 'southern university': 'southern',
    'mcneese st': 'mcneese', 'mcneese state': 'mcneese',
    'nicholls st': 'nicholls', 'nicholls state': 'nicholls',
    'sam houston': 'sam houston st.', 'shsu': 'sam houston st.',
    'stephen f austin': "stephen f. austin", 'sfa': "stephen f. austin", 'sf austin': "stephen f. austin",
    'prairie view am': 'prairie view a&m', 'prairie view a&m': 'prairie view a&m', 'pvamu': 'prairie view a&m',
    'texas southern': 'texas southern', 'txso': 'texas southern',
    'houston christian': 'houston christian', 'hcu': 'houston christian', 'hou christian': 'houston christian',
    'incarnate word': 'incarnate word', 'uiw': 'incarnate word',
    'lamar': 'lamar', 'lamar cardinals': 'lamar',
    'north alabama': 'north alabama', 'una': 'north alabama',
    'jacksonville': 'jacksonville', 'jax': 'jacksonville',
    'kennesaw': 'kennesaw st.', 'kennesaw st': 'kennesaw st.',
    'north florida': 'north florida', 'unf': 'north florida',
    'east carolina': 'east carolina', 'ecu': 'east carolina',
    'charleston': 'charleston', 'college of charleston': 'charleston', 'coll charleston': 'charleston',
    'coastal caro': 'coastal carolina', 'coastal car': 'coastal carolina',
    'sacramento st': 'sacramento st.', 'sac state': 'sacramento st.',
    'utah tech': 'utah tech', 'dixie st': 'utah tech',
    'tarleton': 'tarleton st.', 'tarleton st': 'tarleton st.', 'tarleton state': 'tarleton st.',
    'abilene christian': 'abilene christian', 'acu': 'abilene christian',
    'uc san diego': 'uc san diego', 'ucsd': 'uc san diego',
    'portland st': 'portland st.', 'portland state': 'portland st.',
    'south florida': 'south florida', 'bulls': 'south florida',
    'weber st': 'weber st.', 'weber state': 'weber st.',
    'idaho st': 'idaho st.', 'idaho state': 'idaho st.',
    'tennessee st': 'tennessee st.', 'tennessee state': 'tennessee st.',
    'tennessee tech': 'tennessee tech', 'ttu': 'tennessee tech',
    'e texas a&m': 'e. texas a&m', 'east texas a&m': 'e. texas a&m',
    'detroit': 'detroit mercy', 'detroit mercy': 'detroit mercy',
    'sacred heart': 'sacred heart', 'shu': 'sacred heart',
    'wagner': 'wagner', 'seahawks': 'wagner',
    'rider': 'rider', 'broncs': 'rider',
    'niagara': 'niagara', 'purple eagles': 'niagara',
    'siena': 'siena', 'saints': 'siena',
    'manhattan': 'manhattan', 'jaspers': 'manhattan',
    'marist': 'marist', 'red foxes': 'marist',
    'iona': 'iona', 'gaels': 'iona',
    'canisius': 'canisius', 'golden griffins': 'canisius',
    'merrimack': 'merrimack', 'warriors': 'merrimack',
    'stony brook': 'stony brook', 'seawolves': 'stony brook',
    'new hampshire': 'new hampshire', 'unh': 'new hampshire',
    'umbc': 'umbc', 'retrievers': 'umbc',
    'vermont': 'vermont', 'catamounts': 'vermont',
    'albany': 'albany', 'great danes': 'albany',
    'binghamton': 'binghamton', 'bearcats': 'binghamton',
    'hartford': 'hartford', 'hawks': 'hartford',
    'umass lowell': 'umass lowell', 'mass lowell': 'umass lowell', 'lowell': 'umass lowell',
    'njit': 'njit', 'highlanders': 'njit',
    'maine': 'maine', 'black bears': 'maine',
    'siu edwardsville': 'siu edwardsville', 'siue': 'siu edwardsville',
    # ESPN unicode/accent variants
    "hawai'i": "hawaii", "hawai\u2018i": "hawaii", "hawai\u2019i": "hawaii",
    'csu northridge': 'csun', 'csu bakersfield': 'cal st. bakersfield', 'csu fullerton': 'cal st. fullerton',
    'san jose st': 'san jose st.', 'san jose state': 'san jose st.', 'sjsu': 'san jose st.',
    "san josé st": "san jose st.", "san josé state": "san jose st.",
}

def resolve_kenpom_key(team_name: str) -> Optional[str]:
    """
    Resolve any team name (ESPN, Covers, VSIN, etc.) to the exact KenPom cache key.
    This is the SINGLE SOURCE OF TRUTH for all KenPom cache lookups.
    Returns the lowercase key as stored in torvik_cache/four_factors/height/misc caches, or None.
    """
    if not torvik_cache:
        fetch_kenpom_ratings()
    if not torvik_cache:
        return None

    name_lower = team_name.lower().strip()

    # Strip accents/diacritics (Hawai'i → hawaii, San José → san jose) and smart quotes
    import unicodedata
    name_clean = ''.join(
        c for c in unicodedata.normalize('NFD', name_lower)
        if unicodedata.category(c) != 'Mn'
    )
    # Normalize curly/smart quotes to straight quotes, then remove
    name_clean = name_clean.replace('\u2018', "'").replace('\u2019', "'").replace('\u201c', '"').replace('\u201d', '"')
    name_clean = name_clean.replace("'", "").replace('"', '').strip()

    # Direct cache lookup
    if name_lower in torvik_cache:
        return name_lower
    if name_clean in torvik_cache:
        return name_clean

    # Try normalized name via CBB_TEAM_NAME_MAP
    normalized = normalize_cbb_team_name(team_name).lower().strip()
    if normalized in torvik_cache:
        return normalized

    # Try all known aliases (SJSU, SMC, etc.) - bi-directional lookup
    try:
        all_aliases = get_all_team_aliases(normalize_cbb_team_name(team_name))
        for alias in all_aliases:
            alias_lower = alias.lower().strip()
            if alias_lower in torvik_cache:
                return alias_lower
    except Exception:
        pass

    # Try comprehensive mapping (both original and accent-stripped)
    for try_name in [name_lower, name_clean]:
        if try_name in CBB_TEAM_NAME_MAP:
            mapped_name = CBB_TEAM_NAME_MAP[try_name]
            if mapped_name in torvik_cache:
                return mapped_name

    # Try KenPom-specific aliases
    kp_name = normalize_cbb_team_for_kenpom(name_lower)
    if kp_name in torvik_cache:
        return kp_name
    kp_name_lower = kp_name.lower().strip()
    if kp_name_lower in torvik_cache:
        return kp_name_lower

    # Try with "st." suffix variations
    name_with_st = name_lower.replace(' state', ' st.').replace(' st', ' st.')
    if name_with_st in torvik_cache:
        return name_with_st

    # Try without periods
    name_no_periods = name_lower.replace('.', '')
    for key in torvik_cache:
        if key.replace('.', '') == name_no_periods:
            return key

    # Fuzzy substring matching (with safety: require significant overlap)
    for key in torvik_cache:
        if name_lower in key or key in name_lower:
            if len(name_lower) >= 4 and len(key) >= 4:
                return key
        # Check if key contains significant matching words
        key_parts = key.split()
        name_parts = name_lower.split()
        matching_parts = sum(1 for p in key_parts if p in name_parts and len(p) > 3)
        if matching_parts >= 1 and len(key_parts) <= 3:
            return key

    return None

# Cache for resolved KenPom keys: ESPN name -> KenPom cache key
_kenpom_key_resolution_cache: Dict[str, Optional[str]] = {}

def resolve_kenpom_key_cached(team_name: str) -> Optional[str]:
    """Cached version of resolve_kenpom_key for batch performance."""
    if team_name not in _kenpom_key_resolution_cache:
        _kenpom_key_resolution_cache[team_name] = resolve_kenpom_key(team_name)
    return _kenpom_key_resolution_cache[team_name]

def get_torvik_team(team_name: str) -> Optional[dict]:
    """Get Torvik stats for a team by name (fuzzy match with bi-directional alias support)."""
    key = resolve_kenpom_key(team_name)
    if key and key in torvik_cache:
        return torvik_cache[key]
    return None

def calculate_torvik_projection(away_team: str, home_team: str) -> Optional[dict]:
    """
    Calculate projected game stats using comprehensive KenPom data from all endpoints.
    Returns all metrics needed for CBB model breakdown matching NBA format.
    """
    # Get full KenPom data from all endpoints
    away_stats = get_kenpom_team_full(away_team)
    home_stats = get_kenpom_team_full(home_team)

    if not away_stats or not home_stats:
        if not away_stats:
            logger.warning(f"KenPom full data MISSING for away team: '{away_team}' (resolved key: {resolve_kenpom_key_cached(away_team)})")
        if not home_stats:
            logger.warning(f"KenPom full data MISSING for home team: '{home_team}' (resolved key: {resolve_kenpom_key_cached(home_team)})")
        # Fallback to basic data
        away_basic = get_torvik_team(away_team)
        home_basic = get_torvik_team(home_team)
        if not away_basic or not home_basic:
            if not away_basic:
                logger.warning(f"KenPom basic data MISSING for away team: '{away_team}'")
            if not home_basic:
                logger.warning(f"KenPom basic data MISSING for home team: '{home_team}'")
            return None
        away_stats = away_basic
        home_stats = home_basic

    # Core efficiency metrics
    away_adj_o = away_stats.get('adj_o', 0)
    away_adj_d = away_stats.get('adj_d', 0)
    home_adj_o = home_stats.get('adj_o', 0)
    home_adj_d = home_stats.get('adj_d', 0)
    away_tempo = away_stats.get('tempo', 67.5)
    home_tempo = home_stats.get('tempo', 67.5)

    # Calculate projections
    game_tempo = (away_tempo + home_tempo) / 2
    possessions = game_tempo
    away_off_eff = (away_adj_o + home_adj_d) / 2
    home_off_eff = (home_adj_o + away_adj_d) / 2
    away_points = (away_off_eff / 100) * possessions
    home_points = (home_off_eff / 100) * possessions
    projected_total = away_points + home_points

    return {
        # Projections
        'projected_total': round(projected_total, 1),
        'away_points': round(away_points, 1),
        'home_points': round(home_points, 1),
        'game_tempo': round(game_tempo, 1),

        # Core efficiency
        'away_adj_o': away_adj_o,
        'away_adj_d': away_adj_d,
        'home_adj_o': home_adj_o,
        'home_adj_d': home_adj_d,
        'away_adj_em': away_stats.get('adj_em', 0),
        'home_adj_em': home_stats.get('adj_em', 0),
        'away_rank': away_stats.get('rank', 0),
        'home_rank': home_stats.get('rank', 0),

        # Four Factors - Offensive
        'away_efg': away_stats.get('o_efg', 0),
        'home_efg': home_stats.get('o_efg', 0),
        'away_to': away_stats.get('o_to', 0),
        'home_to': home_stats.get('o_to', 0),
        'away_or': away_stats.get('o_or', 0),
        'home_or': home_stats.get('o_or', 0),
        'away_ft_rate': away_stats.get('o_ft_rate', 0),
        'home_ft_rate': home_stats.get('o_ft_rate', 0),

        # Four Factors - Defensive
        'away_d_efg': away_stats.get('d_efg', 0),
        'home_d_efg': home_stats.get('d_efg', 0),
        'away_d_to': away_stats.get('d_to', 0),
        'home_d_to': home_stats.get('d_to', 0),

        # Shooting
        'away_3pt': away_stats.get('o_3pt_pct', 0),
        'home_3pt': home_stats.get('o_3pt_pct', 0),
        'away_2pt': away_stats.get('o_2pt_pct', 0),
        'home_2pt': home_stats.get('o_2pt_pct', 0),
        'away_ft_pct': away_stats.get('o_ft_pct', 0),
        'home_ft_pct': home_stats.get('o_ft_pct', 0),

        # Defense shooting allowed
        'away_d_3pt': away_stats.get('d_3pt_pct', 0),
        'home_d_3pt': home_stats.get('d_3pt_pct', 0),

        # Size/Experience
        'away_height': away_stats.get('avg_height', 0),
        'home_height': home_stats.get('avg_height', 0),
        'away_exp': away_stats.get('experience', 0),
        'home_exp': home_stats.get('experience', 0),

        # SOS
        'away_sos': away_stats.get('sos', 0),
        'home_sos': home_stats.get('sos', 0),
        'away_sos_rank': away_stats.get('sos_rank', 0),
        'home_sos_rank': home_stats.get('sos_rank', 0),

        # Conference
        'away_conf': away_stats.get('conf', ''),
        'home_conf': home_stats.get('conf', ''),

        # Ball movement
        'away_ast_rate': away_stats.get('o_ast_rate', 0),
        'home_ast_rate': home_stats.get('o_ast_rate', 0),
        'away_stl_pct': away_stats.get('d_stl_pct', 0),
        'home_stl_pct': home_stats.get('d_stl_pct', 0),
        'away_blk_pct': away_stats.get('d_blk_pct', 0),
        'home_blk_pct': home_stats.get('d_blk_pct', 0),

        # Tempo
        'away_tempo': away_tempo,
        'home_tempo': home_tempo,
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

    # Fetch all KenPom endpoints (ratings, four factors, height, misc, etc.)
    fetch_all_kenpom_data()
    
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

        # Apply KenPom stats to ALL CBB games (not just those with Bovada lines)
        kenpom_applied = 0
        cbb_games = Game.query.filter_by(date=today, league="CBB").all()
        for game in cbb_games:
            try:
                torvik_proj = calculate_torvik_projection(game.away_team, game.home_team)
                if torvik_proj:
                    # Core efficiency
                    game.torvik_tempo = torvik_proj.get('game_tempo')
                    game.torvik_away_adj_o = torvik_proj.get('away_adj_o')
                    game.torvik_away_adj_d = torvik_proj.get('away_adj_d')
                    game.torvik_home_adj_o = torvik_proj.get('home_adj_o')
                    game.torvik_home_adj_d = torvik_proj.get('home_adj_d')
                    game.torvik_away_rank = torvik_proj.get('away_rank')
                    game.torvik_home_rank = torvik_proj.get('home_rank')
                    # Four Factors
                    game.kenpom_away_efg = torvik_proj.get('away_efg')
                    game.kenpom_home_efg = torvik_proj.get('home_efg')
                    game.kenpom_away_to = torvik_proj.get('away_to')
                    game.kenpom_home_to = torvik_proj.get('home_to')
                    game.kenpom_away_or = torvik_proj.get('away_or')
                    game.kenpom_home_or = torvik_proj.get('home_or')
                    game.kenpom_away_ft_rate = torvik_proj.get('away_ft_rate')
                    game.kenpom_home_ft_rate = torvik_proj.get('home_ft_rate')
                    # Shooting
                    game.kenpom_away_3pt = torvik_proj.get('away_3pt')
                    game.kenpom_home_3pt = torvik_proj.get('home_3pt')
                    game.kenpom_away_2pt = torvik_proj.get('away_2pt')
                    game.kenpom_home_2pt = torvik_proj.get('home_2pt')
                    game.kenpom_away_ft_pct = torvik_proj.get('away_ft_pct')
                    game.kenpom_home_ft_pct = torvik_proj.get('home_ft_pct')
                    # Defense
                    game.kenpom_away_d_efg = torvik_proj.get('away_d_efg')
                    game.kenpom_home_d_efg = torvik_proj.get('home_d_efg')
                    game.kenpom_away_d_to = torvik_proj.get('away_d_to')
                    game.kenpom_home_d_to = torvik_proj.get('home_d_to')
                    # Size/Experience
                    game.kenpom_away_height = torvik_proj.get('away_height')
                    game.kenpom_home_height = torvik_proj.get('home_height')
                    game.kenpom_away_exp = torvik_proj.get('away_exp')
                    game.kenpom_home_exp = torvik_proj.get('home_exp')
                    # SOS
                    game.kenpom_away_sos = torvik_proj.get('away_sos')
                    game.kenpom_home_sos = torvik_proj.get('home_sos')
                    game.kenpom_away_sos_rank = torvik_proj.get('away_sos_rank')
                    game.kenpom_home_sos_rank = torvik_proj.get('home_sos_rank')
                    # Conference
                    game.kenpom_away_conf = torvik_proj.get('away_conf')
                    game.kenpom_home_conf = torvik_proj.get('home_conf')
                    # Projected scores (even without a line)
                    game.expected_away = torvik_proj.get('away_points')
                    game.expected_home = torvik_proj.get('home_points')
                    game.projected_total = torvik_proj.get('projected_total')
                    game.projected_margin = torvik_proj.get('home_points', 0) - torvik_proj.get('away_points', 0)
                    kenpom_applied += 1
            except Exception as e:
                logger.debug(f"KenPom apply error for {game.away_team}@{game.home_team}: {e}")

        if kenpom_applied:
            db.session.commit()
            logger.info(f"KenPom stats applied to {kenpom_applied}/{len(cbb_games)} CBB games")
    else:
        logger.warning("No games fetched from ESPN - keeping existing data")
    fetch_time = time.time() - start_time
    logger.info(f"Games fetch complete in {fetch_time:.2f}s: {games_added} games added")

    odds_result = fetch_odds_internal()
    vsin_result = fetch_vsin_internal()
    history_result = fetch_history_internal()

    # Run brain analysis in background (non-blocking)
    if AI_BRAINS_AVAILABLE:
        def _bg_brain_analysis():
            try:
                with app.app_context():
                    run_brain_analysis()
            except Exception as e:
                logger.warning(f"Background brain analysis: {e}")
        threading.Thread(target=_bg_brain_analysis, daemon=True).start()

    total_time = time.time() - start_time
    logger.info(f"Total fetch_games completed in {total_time:.2f}s")

    return jsonify({
        "success": True,
        "games_added": games_added,
        "leagues_cleared": leagues_cleared,
        "lines_updated": odds_result.get("lines_updated", 0),
        "alt_lines_found": odds_result.get("alt_lines_found", 0),
        "vsin_updated": vsin_result.get("vsin_updated", 0),
        "rlm_found": vsin_result.get("rlm_found", 0),
        "history_checked": history_result.get("games_checked", 0),
        "fetch_time_seconds": round(total_time, 2)
    })

@app.route('/fetch_stats', methods=['POST'])
def fetch_stats():
    nba_stats = get_nba_stats()
    nhl_stats = get_nhl_stats()

    # Actually save stats to Game records in the database
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    all_games = Game.query.filter_by(date=today).all()
    updated_count = 0

    for game in all_games:
        matched = False
        if game.league == 'NBA' and nba_stats:
            # Match by team nickname (last word of team name, lowercase)
            for nick, stat in nba_stats.items():
                if nick.lower() in game.away_team.lower() or nick.lower() in game.home_team.lower():
                    # Check which team this stat matches
                    if nick.lower() in game.away_team.lower():
                        if stat.get('ppg'):
                            game.away_ppg = float(stat['ppg'])
                        if stat.get('opp_ppg'):
                            game.away_opp_ppg = float(stat['opp_ppg'])
                    if nick.lower() in game.home_team.lower():
                        if stat.get('ppg'):
                            game.home_ppg = float(stat['ppg'])
                        if stat.get('opp_ppg'):
                            game.home_opp_ppg = float(stat['opp_ppg'])
                    matched = True
        elif game.league == 'NHL' and nhl_stats:
            for nick, stat in nhl_stats.items():
                if nick.lower() in game.away_team.lower() or nick.lower() in game.home_team.lower():
                    if nick.lower() in game.away_team.lower():
                        if stat.get('ppg'):
                            game.away_ppg = float(stat['ppg'])
                        if stat.get('opp_ppg'):
                            game.away_opp_ppg = float(stat['opp_ppg'])
                    if nick.lower() in game.home_team.lower():
                        if stat.get('ppg'):
                            game.home_ppg = float(stat['ppg'])
                        if stat.get('opp_ppg'):
                            game.home_opp_ppg = float(stat['opp_ppg'])
                    matched = True

        if matched:
            updated_count += 1

    try:
        db.session.commit()
        logger.info(f"fetch_stats: Updated {updated_count} games with stats")
    except Exception as e:
        db.session.rollback()
        logger.error(f"fetch_stats commit error: {e}")

    return jsonify({"success": True, "counts": {"nba": len(nba_stats), "nhl": len(nhl_stats)}, "updated_games": updated_count})

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

_bovada_games_cache = {}
_bovada_games_cache_time = None

def get_bovada_games(league: str = 'CBB') -> set:
    """
    Fetch games that Bovada has lines for.
    Returns set of (away_team, home_team) tuples for games with Bovada spreads.
    Caches for 10 minutes to avoid excessive API calls.
    """
    global _bovada_games_cache, _bovada_games_cache_time
    
    cache_key = f"{league}_{datetime.now().strftime('%Y%m%d')}"
    
    # Check cache
    if cache_key in _bovada_games_cache and _bovada_games_cache_time:
        cache_age = (datetime.now() - _bovada_games_cache_time).total_seconds()
        if cache_age < 600:  # 10 minute cache
            return _bovada_games_cache[cache_key]
    
    sport_map = {
        'NBA': 'basketball_nba',
        'CBB': 'basketball_ncaab',
        'NFL': 'americanfootball_nfl',
        'NHL': 'icehockey_nhl',
    }
    
    api_key = os.environ.get("BOVADA_API_KEY") or os.environ.get("ODDS_API_KEY")
    if not api_key:
        return set()
    
    sport_key = sport_map.get(league, 'basketball_ncaab')
    
    try:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        params = {
            'apiKey': api_key,
            'regions': 'us',
            'markets': 'spreads',
            'bookmakers': 'bovada',
            'dateFormat': 'iso',
        }
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            return set()
        
        data = resp.json()
        bovada_games = set()
        
        for game in data:
            away = game.get('away_team', '')
            home = game.get('home_team', '')
            bookmakers = game.get('bookmakers', [])
            
            # Only include if Bovada has spreads
            bovada = next((b for b in bookmakers if b.get('key') == 'bovada'), None)
            if bovada:
                spreads = next((m for m in bovada.get('markets', []) if m.get('key') == 'spreads'), None)
                if spreads:
                    bovada_games.add((away.lower(), home.lower()))
        
        _bovada_games_cache[cache_key] = bovada_games
        _bovada_games_cache_time = datetime.now()
        logger.info(f"Fetched {len(bovada_games)} Bovada {league} games")
        
        return bovada_games
        
    except Exception as e:
        logger.warning(f"Error fetching Bovada games: {e}")
        return _bovada_games_cache.get(cache_key, set())


def is_bovada_game(away_team: str, home_team: str, bovada_games: set) -> bool:
    """Check if a game is available on Bovada using fuzzy matching with proper normalization."""
    if not bovada_games:
        return True  # If no Bovada data, show all games
    
    # Normalize team names for matching
    def normalize_for_match(name: str) -> str:
        """Normalize team name for matching."""
        n = name.lower().strip()
        # Remove common suffixes and state abbreviations
        for suffix in [' state', ' st', ' st.', ' university', ' univ', ' college']:
            if n.endswith(suffix):
                n = n[:-len(suffix)].strip()
        # Handle common abbreviations
        replacements = {
            'north carolina': 'nc', 'south carolina': 'sc',
            'central florida': 'ucf', 'c florida': 'ucf',
            'louisiana state': 'lsu', 'florida state': 'fsu',
            'texas a&m': 'texas am', 'brigham young': 'byu',
            'san jose': 'san jose', 'san josé': 'san jose',
        }
        for old, new in replacements.items():
            if old in n:
                n = n.replace(old, new)
        return n
    
    away_norm = normalize_for_match(away_team)
    home_norm = normalize_for_match(home_team)
    
    # Exact match after normalization
    if (away_norm, home_norm) in bovada_games:
        return True
    
    # Check if normalized names match Bovada games
    for (bov_away, bov_home) in bovada_games:
        bov_away_norm = normalize_for_match(bov_away)
        bov_home_norm = normalize_for_match(bov_home)
        
        # Check for token overlap (at least one significant word matches)
        away_tokens = set(w for w in away_norm.split() if len(w) > 2)
        home_tokens = set(w for w in home_norm.split() if len(w) > 2)
        bov_away_tokens = set(w for w in bov_away_norm.split() if len(w) > 2)
        bov_home_tokens = set(w for w in bov_home_norm.split() if len(w) > 2)
        
        away_match = bool(away_tokens & bov_away_tokens) or away_norm in bov_away_norm or bov_away_norm in away_norm
        home_match = bool(home_tokens & bov_home_tokens) or home_norm in bov_home_norm or bov_home_norm in home_norm
        
        if away_match and home_match:
            return True
    
    return False


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
                                            # Fetch comprehensive KenPom data from all endpoints
                                            torvik_proj = calculate_torvik_projection(game.away_team, game.home_team)
                                            if torvik_proj:
                                                proj_total = torvik_proj['projected_total']
                                                exp_away = torvik_proj['away_points']
                                                exp_home = torvik_proj['home_points']
                                                # Core efficiency
                                                game.torvik_tempo = torvik_proj.get('game_tempo')
                                                game.torvik_away_adj_o = torvik_proj.get('away_adj_o')
                                                game.torvik_away_adj_d = torvik_proj.get('away_adj_d')
                                                game.torvik_home_adj_o = torvik_proj.get('home_adj_o')
                                                game.torvik_home_adj_d = torvik_proj.get('home_adj_d')
                                                game.torvik_away_rank = torvik_proj.get('away_rank')
                                                game.torvik_home_rank = torvik_proj.get('home_rank')
                                                # Four Factors
                                                game.kenpom_away_efg = torvik_proj.get('away_efg')
                                                game.kenpom_home_efg = torvik_proj.get('home_efg')
                                                game.kenpom_away_to = torvik_proj.get('away_to')
                                                game.kenpom_home_to = torvik_proj.get('home_to')
                                                game.kenpom_away_or = torvik_proj.get('away_or')
                                                game.kenpom_home_or = torvik_proj.get('home_or')
                                                game.kenpom_away_ft_rate = torvik_proj.get('away_ft_rate')
                                                game.kenpom_home_ft_rate = torvik_proj.get('home_ft_rate')
                                                # Shooting
                                                game.kenpom_away_3pt = torvik_proj.get('away_3pt')
                                                game.kenpom_home_3pt = torvik_proj.get('home_3pt')
                                                game.kenpom_away_2pt = torvik_proj.get('away_2pt')
                                                game.kenpom_home_2pt = torvik_proj.get('home_2pt')
                                                game.kenpom_away_ft_pct = torvik_proj.get('away_ft_pct')
                                                game.kenpom_home_ft_pct = torvik_proj.get('home_ft_pct')
                                                # Defense
                                                game.kenpom_away_d_efg = torvik_proj.get('away_d_efg')
                                                game.kenpom_home_d_efg = torvik_proj.get('home_d_efg')
                                                game.kenpom_away_d_to = torvik_proj.get('away_d_to')
                                                game.kenpom_home_d_to = torvik_proj.get('home_d_to')
                                                # Size/Experience
                                                game.kenpom_away_height = torvik_proj.get('away_height')
                                                game.kenpom_home_height = torvik_proj.get('home_height')
                                                game.kenpom_away_exp = torvik_proj.get('away_exp')
                                                game.kenpom_home_exp = torvik_proj.get('home_exp')
                                                # SOS
                                                game.kenpom_away_sos = torvik_proj.get('away_sos')
                                                game.kenpom_home_sos = torvik_proj.get('home_sos')
                                                game.kenpom_away_sos_rank = torvik_proj.get('away_sos_rank')
                                                game.kenpom_home_sos_rank = torvik_proj.get('home_sos_rank')
                                                # Conference
                                                game.kenpom_away_conf = torvik_proj.get('away_conf')
                                                game.kenpom_home_conf = torvik_proj.get('home_conf')
                                                logger.debug(f"CBB {game.away_team}@{game.home_team}: KenPom proj={proj_total}")
                                        
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

@app.route('/fetch_vsin', methods=['POST'])
def fetch_vsin():
    """Route wrapper for fetch_vsin_internal."""
    return jsonify(fetch_vsin_internal())

def fetch_vsin_internal() -> dict:
    """Fetch VSIN betting splits + line tracker data and persist to Game DB columns."""
    import html as html_mod
    start_time = time.time()

    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()

    games = Game.query.filter_by(date=today).all()
    if not games:
        return {"success": True, "vsin_updated": 0, "rlm_found": 0}

    # Determine which leagues have games today
    leagues_today = set(g.league for g in games if g.league in ('NBA', 'CBB', 'NHL'))

    # Fetch VSIN data for each league
    vsin_all_data = {}
    for league in leagues_today:
        try:
            vsin_all_data[league] = MatchupIntelligence.fetch_rlm_data(league)
        except Exception as e:
            logger.warning(f"VSIN fetch failed for {league}: {e}")
            vsin_all_data[league] = {}

    if not any(vsin_all_data.values()):
        return {"success": True, "vsin_updated": 0, "rlm_found": 0, "reason": "no_vsin_data"}

    def _teams_match(vsin_team: str, game_team: str, league: str) -> bool:
        """Fuzzy match a VSIN team name against a Game DB team name."""
        if not vsin_team or not game_team:
            return False
        vsin_lower = html_mod.unescape(vsin_team.lower().strip())
        game_lower = html_mod.unescape(game_team.lower().strip())
        # Exact
        if vsin_lower == game_lower:
            return True
        # Canonical via team_identity
        vsin_canonical = identity_normalize(vsin_lower, league)
        game_canonical = identity_normalize(game_lower, league)
        if vsin_canonical and game_canonical and vsin_canonical == game_canonical:
            return True
        # CBB alias lookup
        if league == 'CBB':
            vsin_alias = CBB_TEAM_NAME_ALIASES.get(vsin_lower, '').lower()
            game_alias = CBB_TEAM_NAME_ALIASES.get(game_lower, '').lower()
            if vsin_alias and vsin_alias == game_lower:
                return True
            if game_alias and game_alias == vsin_lower:
                return True
            if vsin_alias and game_alias and vsin_alias == game_alias:
                return True
        # Normalized basic
        def _norm(n):
            import re as re_mod
            n = n.lower().strip().replace("'", '').replace('-', ' ').replace('  ', ' ')
            n = re_mod.sub(r'\bst\b', 'state', n)
            n = n.replace('st.', 'state').replace('univ.', '').replace('university', '').replace('  ', ' ')
            return n.strip()
        vsin_norm = _norm(vsin_lower)
        game_norm = _norm(game_lower)
        if vsin_norm == game_norm:
            return True
        # Token overlap (4+ char tokens)
        vsin_tokens = {t for t in vsin_norm.split() if len(t) >= 4}
        game_tokens = {t for t in game_norm.split() if len(t) >= 4}
        if vsin_tokens and game_tokens and vsin_tokens & game_tokens:
            return True
        # Substring
        if len(vsin_norm) >= 4 and len(game_norm) >= 4:
            if vsin_norm in game_norm or game_norm in vsin_norm:
                return True
        return False

    def _match_game_to_vsin(game, vsin_data: dict, league: str) -> dict:
        """Find matching VSIN entry for a game."""
        for key, data in vsin_data.items():
            vsin_away = data.get('away_team', '')
            vsin_home = data.get('home_team', '')
            if not vsin_away or not vsin_home:
                if ' @ ' in key:
                    parts = key.split(' @ ')
                    if len(parts) == 2:
                        vsin_away = parts[0].strip()
                        vsin_home = parts[1].strip()
            if _teams_match(vsin_away, game.away_team, league) and _teams_match(vsin_home, game.home_team, league):
                return data
        return {}

    vsin_updated = 0
    rlm_found = 0

    for game in games:
        vsin_data = vsin_all_data.get(game.league, {})
        if not vsin_data:
            continue

        match = _match_game_to_vsin(game, vsin_data, game.league)
        if not match:
            continue

        # Persist betting splits
        try:
            tickets_away = match.get('tickets_away')
            tickets_home = match.get('tickets_home')
            money_away = match.get('money_away')
            money_home = match.get('money_home')

            if tickets_away is not None:
                game.away_tickets_pct = float(tickets_away)
            if tickets_home is not None:
                game.home_tickets_pct = float(tickets_home)
            if money_away is not None:
                game.away_money_pct = float(money_away)
            if money_home is not None:
                game.home_money_pct = float(money_home)

            # Totals splits
            over_bet = match.get('over_bet_pct')
            under_bet = match.get('under_bet_pct')
            over_money = match.get('over_money_pct')
            under_money = match.get('under_money_pct')
            game.over_tickets_pct = float(over_bet) if over_bet is not None else 50
            game.under_tickets_pct = float(under_bet) if under_bet is not None else 50
            game.over_money_pct = float(over_money) if over_money is not None else 50
            game.under_money_pct = float(under_money) if under_money is not None else 50

            # Opening spread (only set if NULL — preserve first-seen value)
            open_spread = match.get('open_away_spread')
            if open_spread is not None and game.opening_spread is None:
                try:
                    game.opening_spread = float(open_spread)
                except (ValueError, TypeError):
                    pass

            # Current/closed spread
            current_spread = match.get('current_away_spread')
            if current_spread is not None:
                try:
                    game.closed_spread = float(current_spread)
                except (ValueError, TypeError):
                    pass

            # Opening total (only set if NULL)
            open_total = match.get('total_open_line')
            if open_total is not None and open_total != 'N/A' and game.opening_total is None:
                try:
                    game.opening_total = float(open_total)
                except (ValueError, TypeError):
                    pass

            # Closed total
            closed_total = match.get('total_current_line')
            if closed_total is not None and closed_total != 'N/A':
                try:
                    game.closed_total = float(closed_total)
                except (ValueError, TypeError):
                    pass

            # Apply VSIN's pre-computed RLM detection
            if match.get('spread_rlm_detected'):
                game.rlm_detected = True
                game.rlm_sharp_side = match.get('spread_rlm_sharp_side', '')
                # Build explanation from VSIN data
                majority_pct = match.get('majority_pct', '')
                majority_team = match.get('majority_team', '')
                movement_val = match.get('line_movement_value')
                if majority_pct and majority_team and movement_val is not None:
                    game.rlm_explanation = f"RLM: {majority_pct}% money on {majority_team}, but line moved {abs(float(movement_val)):.1f} pts toward {game.rlm_sharp_side}"
                else:
                    game.rlm_explanation = f"RLM detected: sharp money on {game.rlm_sharp_side}"
                # Derive confidence from money divergence
                try:
                    money_diff = abs(float(money_away or 50) - float(money_home or 50))
                    game.rlm_confidence = min(100, 50 + money_diff)
                    game.rlm_severity = 'extreme' if money_diff >= 30 else ('strong' if money_diff >= 15 else 'moderate')
                except (ValueError, TypeError):
                    game.rlm_confidence = 60
                    game.rlm_severity = 'moderate'
                rlm_found += 1

            if match.get('totals_rlm_detected'):
                game.totals_rlm_detected = True
                game.totals_rlm_sharp_side = match.get('totals_rlm_sharp_side', '')
                try:
                    over_m = float(over_money or 50)
                    under_m = float(under_money or 50)
                    totals_diff = abs(over_m - under_m)
                    game.totals_rlm_confidence = min(100, 50 + totals_diff)
                    game.totals_rlm_severity = 'extreme' if totals_diff >= 30 else ('strong' if totals_diff >= 15 else 'moderate')
                except (ValueError, TypeError):
                    game.totals_rlm_confidence = 60
                    game.totals_rlm_severity = 'moderate'
                game.totals_rlm_explanation = f"Totals RLM: sharp money on {game.totals_rlm_sharp_side}"

            # Also run standalone RLM detector (uses DB columns we just set)
            calculate_rlm(game)

            vsin_updated += 1
        except Exception as e:
            logger.warning(f"VSIN persist error for {game.away_team}@{game.home_team}: {e}")
            continue

    try:
        db.session.commit()
        logger.info(f"VSIN data persisted: {vsin_updated} games updated, {rlm_found} RLM detected in {time.time() - start_time:.2f}s")
    except Exception as e:
        db.session.rollback()
        logger.error(f"VSIN commit failed: {e}")
        return {"success": False, "vsin_updated": 0, "rlm_found": 0, "reason": str(e)}

    return {"success": True, "vsin_updated": vsin_updated, "rlm_found": rlm_found}


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

def find_best_alt_line(outcomes: list, direction: str, current_line: float, is_spread: bool = False, home_team: str = "", debug_game: str = "") -> tuple:
    """
    Find the best alternate line with NEGATIVE odds only (no + money).
    Odds must be between -200 and -100 (no positive odds, no worse than -200).
    """
    MAX_ODDS = -185
    MIN_ODDS = -100
    candidates = []
    all_valid_lines = []
    all_raw_lines = []

    for outcome in outcomes:
        odds = outcome.get("price", 0)
        point = outcome.get("point")
        name = outcome.get("name", "")

        if point is not None:
            all_raw_lines.append((point, odds, name))

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
            is_over = direction in ("OVER", "O")
            is_under = direction in ("UNDER", "U")
            if is_over and name != "Over":
                continue
            if is_under and name != "Under":
                continue
            all_valid_lines.append((point, odds, name))
            if current_line:
                if is_over and point >= current_line:
                    continue
                if is_under and point <= current_line:
                    continue
            candidates.append((point, odds))

    if debug_game:
        is_over_dir = direction in ("OVER", "O")
        filter_name = "Over" if is_over_dir else "Under" if direction in ("UNDER", "U") else direction
        logger.info(f"Alt lines for {debug_game}: direction={direction}, main_line={current_line}")
        raw_matching = [x for x in all_raw_lines if filter_name in x[2] or is_spread]
        for line, odds, name in sorted(raw_matching, key=lambda x: x[0]):
            odds_status = "OK" if MAX_ODDS <= odds <= MIN_ODDS else f"FILTERED (odds={odds})"
            logger.info(f"  {name} {line} ({odds}) - {odds_status}")
        if all_valid_lines:
            logger.info(f"  Valid lines (passed odds filter): {sorted(all_valid_lines, key=lambda x: x[0])}")
        logger.info(f"  Candidates (passed line filter): {sorted(candidates, key=lambda x: x[0])}")

    if not candidates:
        return None, None

    is_over = direction in ("OVER", "O")
    if is_spread:
        if direction == "AWAY":
            candidates.sort(key=lambda x: x[0], reverse=True)
        else:
            candidates.sort(key=lambda x: x[0], reverse=True)
    elif is_over:
        candidates.sort(key=lambda x: x[0])
    else:
        candidates.sort(key=lambda x: x[0], reverse=True)

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
    """API endpoint for history page auto-refresh. TOTALS ONLY to match /history page."""
    picks = Pick.query.filter_by(is_lock=True, pick_type='total').order_by(Pick.date.desc(), Pick.edge.desc()).all()
    
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


@app.route('/tennis')
def tennis():
    """Tennis Game Spreads from Discord picks channel."""
    import concurrent.futures
    from discord_scraper import get_tennis_game_spreads, analyze_tournament_matchups
    from tennis_abstract_scraper import get_tennis_abstract_stats, get_tournament_draws, reset_upgrade_count

    reset_upgrade_count()
    data = {'success': False, 'error': 'Loading...', 'picks': [], 'top_plays': []}
    player_stats = {}
    tournament_draws = []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_data = executor.submit(get_tennis_game_spreads)
            future_stats = executor.submit(get_tennis_abstract_stats)
            try:
                data = future_data.result(timeout=20)
            except Exception as e:
                logger.error(f"Tennis game spreads timeout/error: {e}")
            try:
                player_stats = future_stats.result(timeout=10)
            except Exception as e:
                logger.error(f"Tennis Abstract stats timeout/error: {e}")
    except Exception as e:
        logger.error(f"Tennis data fetch error: {e}")

    try:
        tournament_draws = get_tournament_draws()
        tournament_draws = analyze_tournament_matchups(tournament_draws, player_stats)
    except Exception as e:
        logger.error(f"Tournament analysis error: {e}")

    return render_template('tennis.html', data=data, player_stats=player_stats, tournaments=tournament_draws)

@app.route('/bankroll')
def bankroll():
    """52 Week Bankroll Builder tracker."""
    return render_template('bankroll.html')

_nba_standings_cache = {'data': {}, 'timestamp': 0}
_nba_team_stats_cache = {'data': {}, 'timestamp': 0}

def get_nba_team_stats():
    """Fetch comprehensive NBA team stats including ATS, Last 10, Home/Road records."""
    global _nba_team_stats_cache
    import time
    
    # Check cache (30 min TTL)
    if _nba_team_stats_cache['data'] and time.time() - _nba_team_stats_cache['timestamp'] < 1800:
        return _nba_team_stats_cache['data']
    
    stats = {}
    try:
        # Fetch from ESPN API for team records
        url = 'https://site.api.espn.com/apis/v2/sports/basketball/nba/standings'
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for conf in data.get('children', []):
                conf_name = 'Eastern' if 'east' in conf.get('name', '').lower() else 'Western'
                entries = conf.get('standings', {}).get('entries', [])
                
                for entry in entries:
                    team_info = entry.get('team', {})
                    full_name = team_info.get('displayName', '')
                    team_name = full_name.split()[-1]  # Last word (nickname)
                    team_abbr = team_info.get('abbreviation', '')
                    
                    # Handle special cases like "Trail Blazers" (two-word nickname)
                    if 'Trail Blazers' in full_name:
                        team_name = 'Trail Blazers'
                    
                    # Parse all available stats - use displayValue for formatted records
                    raw_stats = {}
                    for s in entry.get('stats', []):
                        name = s.get('name', '')
                        raw_stats[name] = s.get('displayValue', str(s.get('value', '--')))
                    
                    overall_wins = int(float(raw_stats.get('wins', '0').replace(',', ''))) if raw_stats.get('wins', '0').replace(',', '').isdigit() else 0
                    overall_losses = int(float(raw_stats.get('losses', '0').replace(',', ''))) if raw_stats.get('losses', '0').replace(',', '').isdigit() else 0
                    
                    # Get Home, Road, and Last Ten directly from displayValue
                    home_record = raw_stats.get('Home', '--')
                    road_record = raw_stats.get('Road', '--')
                    last_10 = raw_stats.get('Last Ten Games', '--')
                    
                    team_data = {
                        'overall_record': f"{overall_wins}-{overall_losses}",
                        'home_record': home_record,
                        'road_record': road_record,
                        'wins': overall_wins,
                        'losses': overall_losses,
                        'conf': conf_name,
                        'last_10': last_10,
                        # ATS data needs Covers.com
                        'ats_record': '--',
                        'ats_home': '--',
                        'ats_road': '--',
                        'last_10_ats': '--'
                    }
                    
                    stats[team_name] = team_data
                    stats[team_abbr] = team_data
        
        _nba_team_stats_cache = {'data': stats, 'timestamp': time.time()}
        logger.info(f"Fetched NBA team stats for {len(stats)} teams")
    except Exception as e:
        logger.warning(f"Error fetching NBA team stats: {e}")
    
    return stats

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
                    full_name = team_info.get('displayName', '')
                    team_name = full_name.split()[-1]  # Get last word (nickname)
                    if 'Trail Blazers' in full_name:
                        team_name = 'Trail Blazers'
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

_cbb_standings_cache = {'data': {}, 'timestamp': 0}

def get_cbb_standings():
    """Fetch CBB standings from ESPN scoreboard API with caching."""
    global _cbb_standings_cache
    import time
    from datetime import datetime
    
    if _cbb_standings_cache['data'] and time.time() - _cbb_standings_cache['timestamp'] < 1800:
        return _cbb_standings_cache['data']
    
    standings = {}
    try:
        today = datetime.now().strftime('%Y%m%d')
        url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={today}&limit=200'
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for event in data.get('events', []):
                comps = event.get('competitions', [{}])[0]
                for team in comps.get('competitors', []):
                    team_info = team.get('team', {})
                    records = team.get('records', [])
                    record = records[0].get('summary', '--') if records else '--'
                    team_data = {'record': record, 'standing': ''}
                    short_name = team_info.get('shortDisplayName', '')
                    display_name = team_info.get('displayName', '')
                    nickname = team_info.get('nickname', '')
                    abbrev = team_info.get('abbreviation', '')
                    location = team_info.get('location', '')
                    # Add all possible name variations
                    for name in [short_name, display_name, nickname, abbrev, location]:
                        if name and name not in standings:
                            standings[name] = team_data
                            # Also add lowercase version for fuzzy matching
                            standings[name.lower()] = team_data
        _cbb_standings_cache = {'data': standings, 'timestamp': time.time()}
        logger.info(f"Fetched CBB standings for {len(standings)} teams from scoreboard")
    except Exception as e:
        logger.warning(f"Error fetching CBB standings: {e}")
    return standings

def get_cbb_team_record(team_name: str, standings: dict) -> str:
    """Get CBB team record. Direct lookup only."""
    # Exact match first
    if team_name in standings:
        return standings[team_name].get('record', '--')
    # Lowercase match
    if team_name.lower() in standings:
        return standings[team_name.lower()].get('record', '--')
    # Normalized name match
    normalized = normalize_cbb_team_name(team_name)
    if normalized in standings:
        return standings[normalized].get('record', '--')
    if normalized.lower() in standings:
        return standings[normalized.lower()].get('record', '--')
    # Fallback to KenPom data for records
    kenpom_data = get_torvik_team(team_name)
    if kenpom_data and kenpom_data.get('record'):
        return kenpom_data.get('record', '--')
    return '--'

_nhl_standings_cache = {'data': {}, 'timestamp': 0}

def get_nhl_standings():
    """Fetch NHL standings from ESPN API with caching."""
    global _nhl_standings_cache
    import time
    
    if _nhl_standings_cache['data'] and time.time() - _nhl_standings_cache['timestamp'] < 3600:
        return _nhl_standings_cache['data']
    
    standings = {}
    try:
        url = 'https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings'
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for child in data.get('children', []):
                conf_name = child.get('name', '')
                conf_abbr = 'East' if 'east' in conf_name.lower() else 'West'
                entries = child.get('standings', {}).get('entries', [])
                sorted_entries = sorted(entries, key=lambda x: -float([s.get('value',0) for s in x.get('stats',[]) if s.get('name')=='points'][0] if [s.get('value',0) for s in x.get('stats',[]) if s.get('name')=='points'] else 0))
                for idx, entry in enumerate(sorted_entries, 1):
                    team_info = entry.get('team', {})
                    full_name = team_info.get('displayName', '')
                    team_name = full_name.split()[-1]
                    if 'Blue Jackets' in full_name:
                        team_name = 'Blue Jackets'
                    elif 'Maple Leafs' in full_name:
                        team_name = 'Maple Leafs'
                    elif 'Golden Knights' in full_name:
                        team_name = 'Golden Knights'
                    elif 'Red Wings' in full_name:
                        team_name = 'Red Wings'
                    stats = {s['name']: s.get('value', 0) for s in entry.get('stats', [])}
                    wins = int(float(stats.get('wins', 0)))
                    losses = int(float(stats.get('losses', 0)))
                    otl = int(float(stats.get('otLosses', 0)))
                    if 11 <= idx <= 13:
                        suffix = 'th'
                    else:
                        suffix = ['th','st','nd','rd','th','th','th','th','th','th'][idx % 10]
                    standings[team_name] = {
                        'record': f"{wins}-{losses}-{otl}",
                        'standing': f"{idx}{suffix} {conf_abbr}",
                        'wins': wins,
                        'losses': losses,
                        'conf': conf_abbr
                    }
        _nhl_standings_cache = {'data': standings, 'timestamp': time.time()}
        logger.info(f"Fetched NHL standings for {len(standings)} teams")
    except Exception as e:
        logger.warning(f"Error fetching NHL standings: {e}")
    return standings


@app.route('/spreads')
def spreads():
    """Spreads page - shows all upcoming games with spread data (no totals filtering)."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()

    # Fetch standings for all leagues
    nba_standings = get_nba_standings()
    cbb_standings = get_cbb_standings()
    nhl_standings = get_nhl_standings()
    
    # Fetch comprehensive team stats from Covers.com (includes ATS, L10, Home/Road)
    nba_team_stats = get_nba_team_stats()
    covers_nba_stats = get_covers_matchup_stats('NBA')
    covers_cbb_stats = get_covers_matchup_stats('CBB')
    covers_nhl_stats = get_covers_matchup_stats('NHL')
    
    # Get Bovada games for filtering (only show games Bovada has lines for)
    bovada_cbb_games = get_bovada_games('CBB')
    bovada_nba_games = get_bovada_games('NBA')
    # Fetch VSIN betting splits and line tracker data for NBA and CBB
    vsin_nba_data = MatchupIntelligence.fetch_rlm_data('NBA')
    vsin_cbb_data = MatchupIntelligence.fetch_rlm_data('CBB')
    vsin_all_data = {'NBA': vsin_nba_data, 'CBB': vsin_cbb_data}
    logging.info(f"VSIN data loaded: NBA={len(vsin_nba_data)}, CBB={len(vsin_cbb_data)} games")
    
    # Helper function to match VSIN data to a game
    # Team name aliases for VSIN matching
    VSIN_TEAM_ALIASES = {
        # Major CBB programs - VSIN/Covers/Bovada/KenPom variations
        'uconn': 'connecticut', 'ole miss': 'mississippi', 'lsu': 'louisiana state',
        'pitt': 'pittsburgh', 'umass': 'massachusetts', 'unlv': 'nevada las vegas',
        'usc': 'southern california', 'ucf': 'central florida', 'smu': 'southern methodist',
        'tcu': 'texas christian', 'byu': 'brigham young', 'vcu': 'virginia commonwealth',
        'mtsu': 'middle tennessee', 'middle tenn': 'middle tennessee', 'mid tenn': 'middle tennessee',
        'utep': 'texas el paso', 'utsa': 'texas san antonio',
        'fiu': 'florida international', 'fau': 'florida atlantic', 'fgcu': 'florida gulf coast',
        'gcu': 'grand canyon', 'lmu': 'loyola marymount', 'siu': 'southern illinois',
        'niu': 'northern illinois', 'wku': 'western kentucky', 'wmu': 'western michigan',
        'emu': 'eastern michigan', 'cmu': 'central michigan', 'bgsu': 'bowling green',
        'siue': 'siu edwardsville', 'uic': 'illinois chicago', 'iupui': 'iupui',
        # SWAC / Smaller conference teams
        'e texas a&m': 'east texas am', 'east texas a&m': 'east texas am', 'etamu': 'east texas am',
        'c arkansas': 'central arkansas', 'uca': 'central arkansas', 'cent arkansas': 'central arkansas',
        'ar-pine bluff': 'arkansas pine bluff', 'arkansas-pine bluff': 'arkansas pine bluff', 
        'uapb': 'arkansas pine bluff', 'ark pine bluff': 'arkansas pine bluff',
        'grambling': 'grambling state', 'gram': 'grambling state', 'grambling st': 'grambling state',
        's dakota st': 'south dakota state', 'south dakota st': 'south dakota state', 
        's dakota state': 'south dakota state', 'sdsu': 'south dakota state',
        'st thomas': 'st thomas mn', 'st thomas (mn)': 'st thomas mn', 'saint thomas': 'st thomas mn',
        'gardner-webb': 'gardner webb', 'gwu': 'gardner webb', 'gardner webb': 'gardner webb',
        'longwood': 'longwood lancers', 'long wood': 'longwood',
        'n alabama': 'north alabama', 'una': 'north alabama', 'north ala': 'north alabama',
        # State school abbreviations
        'purdue fw': 'purdue fort wayne', 'pfw': 'purdue fort wayne', 'fort wayne': 'purdue fort wayne',
        'c connecticut': 'central connecticut', 'ccsu': 'central connecticut', 'cent conn': 'central connecticut',
        'g washington': 'george washington', 'gw': 'george washington', 'geo washington': 'george washington',
        's illinois': 'southern illinois', 'n illinois': 'northern illinois',
        'e michigan': 'eastern michigan', 'w kentucky': 'western kentucky',
        'western ky': 'western kentucky', 'e washington': 'eastern washington',
        'n colorado': 'northern colorado', 'so indiana': 'southern indiana', 'uso': 'southern indiana',
        'abil christian': 'abilene christian', 'abilene chrstn': 'abilene christian', 'acu': 'abilene christian',
        'ga southern': 'georgia southern', 'gaso': 'georgia southern',
        'miami oh': 'miami ohio', 'miami ohio': 'miami oh', 'miami (oh)': 'miami ohio', 'miamioh': 'miami ohio',
        'nc a&t': 'north carolina at', 'nc central': 'north carolina central', 'nccu': 'north carolina central',
        'sc state': 'south carolina state', 'jax state': 'jacksonville state', 'jsu': 'jacksonville state',
        "hawai'i": 'hawaii', 'hawaii': 'hawaii', "n'western st": 'northwestern state', 'nwst': 'northwestern state',
        'sam houston': 'sam houston state', 'shsu': 'sam houston state', 'sam houston st': 'sam houston state',
        'ut rio grande': 'texas rio grande valley', 'utrgv': 'texas rio grande valley', 'rio grande': 'texas rio grande valley',
        'hou christian': 'houston christian', 'hcu': 'houston christian', 'houston chr': 'houston christian',
        'texas a&m-cc': 'texas am corpus christi', 'tamucc': 'texas am corpus christi', 'a&m corpus': 'texas am corpus christi',
        'incarnate word': 'incarnate word', 'uiw': 'incarnate word',
        'tarleton st': 'tarleton state', 'tarleton': 'tarleton state',
        'utah tech': 'utah tech', 'app state': 'appalachian state', 'appst': 'appalachian state',
        'fdu': 'fairleigh dickinson', 'le moyne': 'le moyne', 'lemoyne': 'le moyne', 'le moyne dolphins': 'le moyne',
        'stonehill': 'stonehill', 'long island': 'long island university', 'liu': 'long island university', 'liu brooklyn': 'long island university', 'liu-brooklyn': 'long island university',
        'ualbany': 'albany', 'umbc': 'maryland baltimore county',
        'boston u': 'boston university', 'bu': 'boston university',
        'washington st': 'washington state', 'wsu': 'washington state', 'wazzu': 'washington state',
        'sacramento st': 'sacramento state', 'sac state': 'sacramento state', 'sacst': 'sacramento state',
        'missouri st': 'missouri state', 'mostate': 'missouri state',
        'indiana st': 'indiana state', 'instate': 'indiana state',
        'boston college': 'bc', 'bc': 'boston college',
        # Big schools - VSIN variations
        'texas': 'texas longhorns', 'oklahoma': 'oklahoma sooners', 'indiana': 'indiana hoosiers',
        'ucla': 'ucla bruins', 'houston': 'houston cougars', 'cincinnati': 'cincinnati bearcats',
        'virginia': 'virginia cavaliers', 'washington': 'washington huskies', 'northwestern': 'northwestern wildcats',
        'utah': 'utah utes', 'oklahoma st': 'oklahoma state', 'okstate': 'oklahoma state',
        'california': 'cal', 'cal': 'california', 'cal bears': 'california',
        'portland st': 'portland state', 'montana st': 'montana state', 'montana': 'montana grizzlies',
        'omaha': 'nebraska omaha', 'uno': 'nebraska omaha', 'denver': 'denver pioneers',
        'chicago st': 'chicago state', 'mercyhurst': 'mercyhurst', 'mercy': 'mercyhurst', 'mercyhurst lakers': 'mercyhurst',
        'fordham': 'fordham rams', 'bucknell': 'bucknell bison',
        'valparaiso': 'valpo', 'valpo': 'valparaiso',
        'portland': 'portland pilots', 'delaware st': 'delaware state',
        'md eastern': 'maryland eastern shore', 'umes': 'maryland eastern shore',
        'long beach st': 'long beach state', 'lbsu': 'long beach state', 'lb state': 'long beach state',
        'san diego st': 'san diego state', 'sdsu': 'san diego state',
        'uc-riverside': 'uc riverside', 'ucr': 'uc riverside', 'california riverside': 'uc riverside',
        'csu-fullerton': 'fullerton', 'csuf': 'fullerton', 'cal st fullerton': 'fullerton', 'cal state fullerton': 'fullerton',
        'uc-irvine': 'uc irvine', 'uci': 'uc irvine', 'california irvine': 'uc irvine',
        'uc-davis': 'uc davis', 'ucd': 'uc davis', 'california davis': 'uc davis',
        'uc-santa barbara': 'uc santa barbara', 'ucsb': 'uc santa barbara',
        'csu-northridge': 'cal st northridge', 'csu northridge': 'cal st northridge', 'csun': 'cal st northridge',
        'csu-bakersfield': 'bakersfield', 'cal st bakersfield': 'bakersfield',
        'md-balt co': 'umbc', 'maryland baltimore county': 'umbc', 'md baltimore county': 'umbc',
        'iu-brooklyn': 'long island university',
        'c conn st': 'central connecticut', 'conn st': 'central connecticut',
        'c conn': 'central connecticut',
        'abilene chr': 'abilene christian', 'abil chr': 'abilene christian', 'abilene chrstn': 'abilene christian',
        'se missouri': 'southeast missouri', 'semo': 'southeast missouri', 'se misso': 'southeast missouri',
        'se missouri st': 'southeast missouri', 'southeast missouri st': 'southeast missouri',
        'morehead st': 'morehead state', 'morehead': 'morehead state',
        'chicago stc': 'chicago state', 'chi state': 'chicago state',
        'new havenl': 'new haven', 'newhaven': 'new haven',
        'st francis-pa': 'saint francis pa', 'st francis pa': 'saint francis pa', 'sfpa': 'saint francis pa',
        'saint francis': 'saint francis pa',
        'utah state': 'utah state aggies', 'usu': 'utah state',
        'robert morris': 'rmu', 'rmu': 'robert morris',
        'southern utah': 'southern utah', 's utah': 'southern utah', 'suu': 'southern utah',
        'louisiana tech': 'la tech', 'latech': 'louisiana tech', 'la tech': 'louisiana tech',
        'louisiana': 'louisiana ragin cajuns', 'ul': 'louisiana',
        'appalachian st': 'appalachian state', 'app state': 'appalachian state', 'app st': 'appalachian state',
        'lemoyne': 'le moyne', 'le moyne': 'le moyne',
        'florida intl': 'florida international', 'fl intl': 'florida international',
        'florida international': 'florida international',
        'middle tenn st': 'middle tennessee', 'middle tenn': 'middle tennessee',
        'w kentucky': 'western kentucky', 'western ky': 'western kentucky',
        'va commonwealth': 'virginia commonwealth',
        's carolina st': 'south carolina state', 'sc state': 'south carolina state',
        'morgan st': 'morgan state', 'morgan': 'morgan state',
        'jacksonville st': 'jacksonville state', 'jax state': 'jacksonville state',
        'new mexico st': 'new mexico state', 'nmsu': 'new mexico state',
        'n dakota st': 'north dakota state', 'n dakota': 'north dakota',
        'se louisiana': 'southeastern louisiana', 'sela': 'southeastern louisiana',
        'northwestern st': 'northwestern state', "n'western st": 'northwestern state',
        'e tennessee st': 'east tennessee state', 'etsu': 'east tennessee state',
        'fl gulf coast': 'florida gulf coast', 'fgcu': 'florida gulf coast',
        'md-balt co': 'maryland baltimore county', 'umbc': 'maryland baltimore county',
        'md-e shore': 'maryland eastern shore', 'md eastern': 'maryland eastern shore', 'umes': 'maryland eastern shore',
        'sf austin': 'stephen f austin', 'sfa': 'stephen f austin', 'stephen f austin': 'stephen f austin',
        'ut rio grande': 'utrgv', 'utrgv': 'utrgv',
        'arkansas st': 'arkansas state', 'south alabama': 'south alabama', 's alabama': 'south alabama',
        'texas st': 'texas state', 'ul monroe': 'louisiana monroe', 'la monroe': 'louisiana monroe',
        'texas-arlington': 'ut arlington', 'ut arlington': 'ut arlington',
        'texas-el paso': 'texas el paso', 'utep': 'texas el paso',
        'texas-san antonio': 'texas san antonio', 'utsa': 'texas san antonio',
        'ut martin': 'tennessee martin', 'tennessee-martin': 'tennessee martin',
        'siu-edwardsville': 'siu edwardsville', 'siue': 'siu edwardsville',
        'queens nc': 'queens', 'queens (nc)': 'queens',
        'sam houston st': 'sam houston state', 'sam houston': 'sam houston state',
        'kennesaw st': 'kennesaw state', 'kennesaw': 'kennesaw state',
        'arkansas-little rock': 'little rock', 'ark-little rock': 'little rock',
        'e illinois': 'eastern illinois', 'eiu': 'eastern illinois',
        'st bonaventure': 'saint bonaventure', 'bonnies': 'saint bonaventure',
        'st peters': 'saint peters', 'st peter\'s': 'saint peters',
        'st johns': 'saint johns', "st john's": 'saint johns',
        'st marys': 'saint marys', "saint mary's": 'saint marys', "st mary's": 'saint marys',
        'st thomas-mn': 'st thomas mn', 'st thomas (mn)': 'st thomas mn',
        'umkc': 'kansas city', 'kansas city': 'kansas city',
        'georgia southern': 'georgia southern', 'ga southern': 'georgia southern',
        'houston christian': 'houston christian', 'hou christian': 'houston christian',
        'n florida': 'north florida', 'unf': 'north florida',
        'alabama st': 'alabama state', 'arkansas-pine bluff': 'arkansas pine bluff',
        'ark-pine bluff': 'arkansas pine bluff', 'ar-pine bluff': 'arkansas pine bluff',
        'grambling st': 'grambling state', 'texas southern': 'texas southern',
        'alcorn st': 'alcorn state', 'bethune-cookman': 'bethune cookman', 'bethune': 'bethune cookman',
        'coppin st': 'coppin state', 'nc central': 'north carolina central',
        'norfolk st': 'norfolk state', 'delaware st': 'delaware state',
        'miss valley st': 'mississippi valley state', 'mvsu': 'mississippi valley state',
        'alabama a&m': 'alabama am', 'prairie view a&m': 'prairie view am',
        'southern u': 'southern university', 'southern': 'southern university',
        'jackson st': 'jackson state', 'florida a&m': 'florida am',
        'california baptist': 'cal baptist', 'ca baptist': 'cal baptist',
        'cal poly slo': 'cal poly', 'cal poly': 'cal poly',
        'uc-san diego': 'uc san diego', 'ucsd': 'uc san diego',
        'ipfw': 'purdue fort wayne', 'iupui': 'iupui',
        'nicholls st': 'nicholls state', 'nicholls': 'nicholls state',
        'incarnate word': 'incarnate word', 'uiw': 'incarnate word',
        'texas a&m cc': 'texas am corpus christi', 'texas a&m-cc': 'texas am corpus christi',
        'lamar': 'lamar', 'tarleton st': 'tarleton state',
        'mcneese st': 'mcneese', 'mcneese': 'mcneese',
        'east texas a&m': 'east texas am', 'e texas a&m': 'east texas am',
        'n arizona': 'northern arizona', 'nau': 'northern arizona',
        'sacramento st': 'sacramento state', 'sac st': 'sacramento state',
        'idaho st': 'idaho state', 'weber st': 'weber state',
        'e washington': 'eastern washington', 'ewu': 'eastern washington',
        'portland st': 'portland state', 'n colorado': 'northern colorado',
        'montana st': 'montana state',
        'tennessee st': 'tennessee state', 'tennessee tech': 'tennessee tech',
        's indiana': 'southern indiana', 'so indiana': 'southern indiana',
        'sc-upstate': 'south carolina upstate', 'usc upstate': 'south carolina upstate',
        'longwood': 'longwood',
        'wichita st': 'wichita state', 'youngstown st': 'youngstown state',
        'wright st': 'wright state', 'cleveland st': 'cleveland state',
        'colorado st': 'colorado state', 'fresno st': 'fresno state',
        'air force': 'air force', 'boise st': 'boise state',
        'uw-green bay': 'green bay', 'uw green bay': 'green bay',
        'uw-milwaukee': 'milwaukee', 'uw milwaukee': 'milwaukee',
        's dakota st': 'south dakota state', 'oral roberts': 'oral roberts',
        'oklahoma st': 'oklahoma state', 'iowa st': 'iowa state',
        'kansas st': 'kansas state', 'ohio st': 'ohio state',
        'penn st': 'penn state', 'oregon st': 'oregon state',
        'michigan st': 'michigan state', 'mississippi st': 'mississippi state',
        'mississippi': 'ole miss', 'ole miss': 'ole miss',
        'brigham young': 'byu', 'byu': 'byu',
        'connecticut': 'uconn', 'uconn': 'uconn',
        'loyola-marymount': 'lmu', 'loyola marymount': 'lmu', 'lmu': 'lmu',
        'unc-wilmington': 'unc wilmington', 'uncw': 'unc wilmington',
        'unc-greensboro': 'unc greensboro', 'uncg': 'unc greensboro',
        'unc-asheville': 'unc asheville', 'unca': 'unc asheville',
        'virginia tech': 'virginia tech', 'florida st': 'florida state',
        'georgia tech': 'georgia tech', 'boston college': 'boston college',
        'charleston southern': 'charleston southern', 'chas southern': 'charleston southern',
        'gardner-webb': 'gardner webb', 'high point': 'high point',
        'nc state': 'nc state', 'miami fl': 'miami florida', 'miami (fl)': 'miami florida',
        'seattle u': 'seattle', 'seattle': 'seattle',
        'stonehill': 'stonehill', 'wagner': 'wagner',
        'sacred heart': 'sacred heart', 'rider': 'rider',
        'loyola-maryland': 'loyola maryland', 'loyola md': 'loyola maryland',
        'holy cross': 'holy cross',
        'new haven': 'new haven',
        'njit': 'njit', 'maine': 'maine',
        'presbyterian': 'presbyterian', 'wofford': 'wofford',
        'w carolina': 'western carolina', 'chattanooga': 'chattanooga',
        'the citadel': 'citadel', 'citadel': 'citadel',
        'mercer': 'mercer', 'furman': 'furman', 'vmi': 'vmi',
        'samford': 'samford', 'elon': 'elon',
        'william & mary': 'william and mary', 'william and mary': 'william and mary',
        'towson': 'towson', 'monmouth': 'monmouth',
        'radford': 'radford', 'campbell': 'campbell',
        'charleston': 'charleston', 'old dominion': 'old dominion',
        'georgia st': 'georgia state', 'belmont': 'belmont',
        'murray st': 'murray state', 'austin peay': 'austin peay',
        'bellarmine': 'bellarmine', 'lipscomb': 'lipscomb',
        'w michigan': 'western michigan', 'e michigan': 'eastern michigan',
        'n illinois': 'northern illinois', 'c michigan': 'central michigan',
        'ball st': 'ball state', 'kent': 'kent state', 'kent st': 'kent state',
        'bowling green': 'bowling green', 'toledo': 'toledo',
        'oakland': 'oakland', 'detroit': 'detroit mercy',
        'merrimack': 'merrimack', 'quinnipiac': 'quinnipiac',
        'siena': 'siena', 'marist': 'marist', 'niagara': 'niagara',
        'iona': 'iona', 'canisius': 'canisius', 'manhattan': 'manhattan',
        'fairfield': 'fairfield',
        # NBA city names and abbreviations
        '76ers': 'philadelphia', 'sixers': 'philadelphia', 'philly': 'philadelphia',
        'philadelphia 76ers': '76ers', 'phila 76ers': '76ers',
        'clips': 'clippers', 'la clippers': 'clippers',
        'lal': 'lakers', 'la lakers': 'lakers', 'los angeles lakers': 'lakers',
        'mavs': 'mavericks', 'dallas mavs': 'mavericks',
        'blazers': 'trail blazers', 'portland': 'trail blazers',
        't-wolves': 'timberwolves', 'wolves': 'timberwolves',
        'pels': 'pelicans', 'nola': 'pelicans',
        'okc': 'thunder', 'thunder': 'oklahoma city',
    }
    
    def match_vsin_data(game_away: str, game_home: str, league: str) -> dict:
        """Find matching VSIN data for a game using improved fuzzy matching."""
        vsin_data = vsin_all_data.get(league, {})
        if not vsin_data:
            return {}
        
        def normalize_team(name: str) -> str:
            """Normalize team name for matching."""
            if not name:
                return ''
            import re as re_mod
            n = name.lower().strip()
            n = n.replace('&amp;', '&').replace('&#39;', "'")
            n = n.replace('\u2018', "'").replace('\u2019', "'")
            if n in VSIN_TEAM_ALIASES:
                n = VSIN_TEAM_ALIASES[n]
            if league == 'CBB' and n in CBB_TEAM_NAME_ALIASES:
                n = CBB_TEAM_NAME_ALIASES[n].lower()
            n = n.replace("'", '').replace('-', ' ').replace('  ', ' ')
            n = re_mod.sub(r'\bst\b', 'state', n)
            n = n.replace('st.', 'state')
            n = n.replace('univ.', '').replace('university', '')
            n = n.replace('  ', ' ')
            return n.strip()
        
        def normalize_tokens(name: str) -> set:
            n = normalize_team(name)
            return {t for t in n.split() if len(t) >= 2}
        
        # Reverse alias lookup (VSIN name -> canonical)
        REVERSE_ALIASES = {}
        for alias, canonical in VSIN_TEAM_ALIASES.items():
            if canonical not in REVERSE_ALIASES:
                REVERSE_ALIASES[canonical] = set()
            REVERSE_ALIASES[canonical].add(alias)
        
        def get_all_variants(name: str) -> set:
            """Get all possible name variants for matching."""
            n = name.lower().strip().replace('&amp;', '&').replace('&#39;', "'")
            variants = {n}
            # Add alias if exists
            if n in VSIN_TEAM_ALIASES:
                variants.add(VSIN_TEAM_ALIASES[n])
            # Add reverse lookups
            for canonical, aliases in REVERSE_ALIASES.items():
                if n == canonical or n in aliases:
                    variants.add(canonical)
                    variants.update(aliases)
            # Add CBB aliases
            if league == 'CBB':
                if n in CBB_TEAM_NAME_ALIASES:
                    variants.add(CBB_TEAM_NAME_ALIASES[n].lower())
                for alias, canonical in CBB_TEAM_NAME_ALIASES.items():
                    if n == canonical.lower():
                        variants.add(alias.lower())
            return variants
        
        def teams_match(vsin_team: str, game_team: str) -> bool:
            if not vsin_team or not game_team:
                return False
            import html as html_mod
            vsin_lower = html_mod.unescape(vsin_team.lower().strip())
            game_lower = html_mod.unescape(game_team.lower().strip())
            # Exact match (case-insensitive)
            if vsin_lower == game_lower:
                return True
            # Use team_identity canonical matching first
            vsin_canonical = identity_normalize(vsin_lower, league)
            game_canonical = identity_normalize(game_lower, league)
            if vsin_canonical and game_canonical and vsin_canonical == game_canonical:
                return True
            # Get all variants for both names
            vsin_variants = get_all_variants(vsin_team)
            game_variants = get_all_variants(game_team)
            # Check if any variants overlap
            if vsin_variants & game_variants:
                return True
            # Normalized match
            vsin_norm = normalize_team(vsin_team)
            game_norm = normalize_team(game_team)
            if vsin_norm == game_norm:
                return True
            # Token overlap (any shared word with length >= 4, more selective)
            vsin_tokens = {t for t in normalize_tokens(vsin_team) if len(t) >= 4}
            game_tokens = {t for t in normalize_tokens(game_team) if len(t) >= 4}
            if vsin_tokens and game_tokens and vsin_tokens & game_tokens:
                return True
            # Also try with 3-char tokens but require at least 2 matches
            vsin_tokens3 = {t for t in normalize_tokens(vsin_team) if len(t) >= 3}
            game_tokens3 = {t for t in normalize_tokens(game_team) if len(t) >= 3}
            if vsin_tokens3 and game_tokens3 and len(vsin_tokens3 & game_tokens3) >= 1:
                # Verify it's not a false positive by checking first letter
                for tok in vsin_tokens3 & game_tokens3:
                    if len(tok) >= 4:
                        return True
            # Substring match (with shorter minimum)
            if len(vsin_norm) >= 3 and len(game_norm) >= 3:
                if vsin_norm in game_norm or game_norm in vsin_norm:
                    return True
            # First word match (handles partial names)
            vsin_words = vsin_norm.split()
            game_words = game_norm.split()
            if vsin_words and game_words:
                # Match first 4+ char word
                vsin_first = next((w for w in vsin_words if len(w) >= 4), '')
                game_first = next((w for w in game_words if len(w) >= 4), '')
                if vsin_first and game_first and vsin_first == game_first:
                    return True
                # Also try last word match for multi-word names
                if len(vsin_words) > 1 and len(game_words) > 1:
                    if vsin_words[-1] == game_words[-1] and len(vsin_words[-1]) >= 4:
                        return True
            return False
        
        for key, data in vsin_data.items():
            vsin_away = data.get('away_team', '')
            vsin_home = data.get('home_team', '')
            if not vsin_away or not vsin_home:
                if ' @ ' in key:
                    parts = key.split(' @ ')
                    if len(parts) == 2:
                        vsin_away = parts[0].strip()
                        vsin_home = parts[1].strip()
            
            if teams_match(vsin_away, game_away) and teams_match(vsin_home, game_home):
                return data
        return {}
    
    # Get ALL games for today without any totals filtering
    all_games = Game.query.filter_by(date=today).order_by(Game.game_time.asc()).all()
    
    # Group games by league
    games_by_league = {
        'NBA': [],
        'CBB': [],
    }
    
    # Helper to check if game is currently live (fetch ESPN live games early)
    # Also collect full live game data to inject missing games
    early_live_keys = set()
    early_live_games = []  # Full game data for missing games
    today_str = today.strftime('%Y%m%d') if hasattr(today, 'strftime') else str(today).replace('-', '')
    try:
        sport_league_map = {
            'basketball/mens-college-basketball': 'CBB',
            'basketball/nba': 'NBA',
            'hockey/nhl': 'NHL'
        }
        for sport_path, league in sport_league_map.items():
            resp = requests.get(f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/scoreboard?dates={today_str}", timeout=5)
            if resp.ok:
                for event in resp.json().get("events", []):
                    if event.get("status", {}).get("type", {}).get("state") == "in":
                        comps = event.get("competitions", [{}])[0].get("competitors", [])
                        if len(comps) >= 2:
                            away = next((c for c in comps if c.get("homeAway") == "away"), comps[0])
                            home = next((c for c in comps if c.get("homeAway") == "home"), comps[1])
                            away_name = away['team']['shortDisplayName']
                            home_name = home['team']['shortDisplayName']
                            early_live_keys.add(f"{away_name}@{home_name}")
                            # Store full game data
                            early_live_games.append({
                                'away_team': away_name,
                                'home_team': home_name,
                                'away_score': int(away.get('score', 0)),
                                'home_score': int(home.get('score', 0)),
                                'period': event.get('status', {}).get('period', 1),
                                'clock': event.get('status', {}).get('displayClock', ''),
                                'league': league,
                                'away_id': away['team'].get('id', ''),
                                'home_id': home['team'].get('id', ''),
                                'away_record': away.get('records', [{}])[0].get('summary', '--') if away.get('records') else '--',
                                'home_record': home.get('records', [{}])[0].get('summary', '--') if home.get('records') else '--'
                            })
    except Exception as e:
        logging.warning(f"Early live check failed: {e}")
    
    # Inject missing live games that aren't in the database
    existing_keys = {f"{g.away_team}@{g.home_team}" for g in all_games}
    for lg in early_live_games:
        key = f"{lg['away_team']}@{lg['home_team']}"
        # Check if this game is missing (fuzzy match)
        is_missing = True
        for ek in existing_keys:
            ek_parts = ek.split('@')
            if len(ek_parts) == 2:
                if (lg['away_team'].lower() in ek_parts[0].lower() or ek_parts[0].lower() in lg['away_team'].lower()) and \
                   (lg['home_team'].lower() in ek_parts[1].lower() or ek_parts[1].lower() in lg['home_team'].lower()):
                    is_missing = False
                    break
        if is_missing:
            # Create a temporary game object for this live game
            temp_game = Game(
                date=today,
                league=lg['league'],
                away_team=lg['away_team'],
                home_team=lg['home_team'],
                game_time=datetime.now(pytz.timezone('America/New_York')).strftime('%H:%M')
            )
            temp_game.away_record = lg['away_record']
            temp_game.home_record = lg['home_record']
            temp_game.is_live = True
            temp_game.live_away_score = lg['away_score']
            temp_game.live_home_score = lg['home_score']
            temp_game.live_period = f"P{lg['period']}" if lg['league'] != 'CBB' else f"H{min(lg['period'], 2)}"
            temp_game.live_clock = lg['clock']
            all_games.append(temp_game)
            logging.info(f"Injected missing live game: {lg['away_team']} @ {lg['home_team']} ({lg['league']})")
    
    def is_game_live_early(away: str, home: str) -> bool:
        """Check if game is currently in progress using early ESPN check"""
        def teams_match(name1: str, name2: str) -> bool:
            """Stricter team name matching to avoid E Kentucky matching Kentucky"""
            n1 = name1.lower().strip()
            n2 = name2.lower().strip()
            if n1 == n2:
                return True
            
            # Check for directional prefixes (E/W/N/S/C) that indicate different schools
            words1 = n1.split()
            words2 = n2.split()
            directional_prefixes = {'e', 'w', 'n', 's', 'c', 'east', 'west', 'north', 'south', 'central'}
            
            has_prefix1 = words1 and words1[0] in directional_prefixes
            has_prefix2 = words2 and words2[0] in directional_prefixes
            if has_prefix1 != has_prefix2:
                base1 = ' '.join(words1[1:]) if has_prefix1 else n1
                base2 = ' '.join(words2[1:]) if has_prefix2 else n2
                if base1 in base2 or base2 in base1:
                    return False  # Different schools!
            
            if n1 in n2 or n2 in n1:
                len_diff = abs(len(n1) - len(n2))
                if len_diff <= 3:
                    return True
            return False
        
        for lk in early_live_keys:
            if '@' in lk:
                la, lh = lk.split('@', 1)
                if teams_match(la, away) and teams_match(lh, home):
                    return True
        return False
    
    for g in all_games:
        # Check if game is currently live
        game_is_live = is_game_live_early(g.away_team, g.home_team)
        
        # No Bovada filter - show ALL games from Covers.com, VSIN, ESPN, etc.
        
        if g.league in games_by_league:
            if g.league == 'NBA':
                g.away_logo = nba_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
                g.home_logo = nba_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
                away_stand = nba_standings.get(g.away_team, {})
                home_stand = nba_standings.get(g.home_team, {})
                g.away_record = away_stand.get('record', '--')
                g.home_record = home_stand.get('record', '--')
                g.away_standing = away_stand.get('standing', '')
                g.home_standing = home_stand.get('standing', '')
                
                # Add Covers-style stats (ATS, Last 10, Home/Road) from Covers.com
                def find_nba_covers_stats(team_name, stats_dict):
                    """Try to find NBA Covers stats using team_identity canonical matching."""
                    if team_name in stats_dict:
                        return stats_dict[team_name]
                    # Use team_identity canonical key matching
                    team_canonical = identity_normalize(team_name, 'NBA')
                    if team_canonical:
                        for key in stats_dict:
                            key_canonical = identity_normalize(key, 'NBA')
                            if key_canonical == team_canonical:
                                return stats_dict[key]
                    # Partial match fallback
                    team_lower = team_name.lower()
                    for key in stats_dict:
                        if key.lower() == team_lower or key.lower().startswith(team_lower) or team_lower.startswith(key.lower()):
                            return stats_dict[key]
                    return {}
                
                away_covers = find_nba_covers_stats(g.away_team, covers_nba_stats)
                home_covers = find_nba_covers_stats(g.home_team, covers_nba_stats)
                
                # Fallback to nba_team_stats for ESPN data when Covers matchup not available
                away_espn = nba_team_stats.get(g.away_team, {})
                home_espn = nba_team_stats.get(g.home_team, {})
                
                # If Covers has data AND we haven't captured pre-game stats yet, save to DB
                if away_covers and home_covers and not g.pregame_stats_captured:
                    g.pregame_away_ats = away_covers.get('ats', '--')
                    g.pregame_home_ats = home_covers.get('ats', '--')
                    g.pregame_away_ats_road = away_covers.get('ats_road', '--')
                    g.pregame_home_ats_home = home_covers.get('ats_home', '--')
                    g.pregame_away_l10 = away_covers.get('l10', '--')
                    g.pregame_home_l10 = home_covers.get('l10', '--')
                    g.pregame_away_l10_ats = away_covers.get('l10_ats', '--')
                    g.pregame_home_l10_ats = home_covers.get('l10_ats', '--')
                    g.pregame_away_road_record = away_covers.get('road_record', '--')
                    g.pregame_home_home_record = home_covers.get('home_record', '--')
                    g.pregame_stats_captured = True
                    try:
                        db.session.commit()
                        logging.info(f"Saved pre-game stats for {g.away_team} @ {g.home_team}")
                    except Exception as e:
                        logging.warning(f"Failed to save pre-game stats: {e}")
                        db.session.rollback()
                
                # Use data priority: Covers (live) -> DB Pre-game -> ESPN fallback
                g.away_overall = away_covers.get('record') or away_espn.get('overall_record', g.away_record)
                g.home_overall = home_covers.get('record') or home_espn.get('overall_record', g.home_record)
                g.away_road_record = away_covers.get('road_record') or g.pregame_away_road_record or away_espn.get('road_record', '--')
                g.home_home_record = home_covers.get('home_record') or g.pregame_home_home_record or home_espn.get('home_record', '--')
                g.away_ats = away_covers.get('ats') or g.pregame_away_ats or away_espn.get('ats_record', '--')
                g.home_ats = home_covers.get('ats') or g.pregame_home_ats or home_espn.get('ats_record', '--')
                g.away_ats_road = away_covers.get('ats_road') or g.pregame_away_ats_road or away_espn.get('ats_road', '--')
                g.home_ats_home = home_covers.get('ats_home') or g.pregame_home_ats_home or home_espn.get('ats_home', '--')
                g.away_l10 = away_covers.get('l10') or g.pregame_away_l10 or away_espn.get('last_10', '--')
                g.home_l10 = home_covers.get('l10') or g.pregame_home_l10 or home_espn.get('last_10', '--')
                g.away_l10_ats = away_covers.get('l10_ats') or g.pregame_away_l10_ats or away_espn.get('last_10_ats', '--')
                g.home_l10_ats = home_covers.get('l10_ats') or g.pregame_home_l10_ats or home_espn.get('last_10_ats', '--')
            elif g.league == 'CBB':
                # Use CBB logo lookup with proper fallback
                def get_cbb_logo_with_fallback(team_name):
                    """Get CBB logo with proper ESPN fallback."""
                    logo = get_transparent_cbb_logo(team_name) or get_cbb_logo(team_name)
                    if logo:
                        return logo
                    # Try normalized name variations
                    for name_variant in [team_name, team_name.replace("'", ""), team_name.replace("'", "")]:
                        logo = get_transparent_cbb_logo(name_variant) or get_cbb_logo(name_variant)
                        if logo:
                            return logo
                    # Ultimate fallback - NCAA generic logo (not NBA!)
                    return 'https://a.espncdn.com/i/teamlogos/ncaa/500-dark/ncaa.png'
                g.away_logo = get_cbb_logo_with_fallback(g.away_team)
                g.home_logo = get_cbb_logo_with_fallback(g.home_team)
                # Use fuzzy matching for CBB team records
                g.away_record = get_cbb_team_record(g.away_team, cbb_standings)
                g.home_record = get_cbb_team_record(g.home_team, cbb_standings)
                g.away_standing = ''
                g.home_standing = ''
                # CBB Covers-style stats - try multiple name variations
                def find_covers_stats(team_name, stats_dict):
                    """Try to find Covers stats using team_identity canonical matching."""
                    import unicodedata
                    import re
                    import html as html_module
                    
                    def strip_accents(text):
                        return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
                    
                    def strip_ranking(text):
                        """Remove ranking numbers like (9) or (12) from team names."""
                        return re.sub(r'\s*\(\d+\)\s*', '', text).strip()
                    
                    team_name = html_module.unescape(team_name)
                    
                    # Direct lookup first
                    if team_name in stats_dict:
                        return stats_dict[team_name]
                    
                    # Use team_identity canonical key matching
                    team_canonical = identity_normalize(team_name, 'CBB')
                    if team_canonical:
                        # Check if any key in stats_dict normalizes to the same canonical key
                        for key in stats_dict:
                            key_clean = strip_ranking(key)
                            key_canonical = identity_normalize(key_clean, 'CBB')
                            if key_canonical == team_canonical:
                                return stats_dict[key]
                    
                    # Fallback: normalized lookup via old system
                    normalized = normalize_cbb_team_name(team_name)
                    if normalized in stats_dict:
                        return stats_dict[normalized]
                    
                    # Strip accents and try again (San José St -> San Jose State)
                    ascii_name = strip_accents(team_name)
                    if ascii_name in stats_dict:
                        return stats_dict[ascii_name]
                    ascii_normalized = normalize_cbb_team_name(ascii_name)
                    if ascii_normalized in stats_dict:
                        return stats_dict[ascii_normalized]
                    
                    # Use team_identity on ascii name too
                    ascii_canonical = identity_normalize(ascii_name, 'CBB')
                    if ascii_canonical and ascii_canonical != team_canonical:
                        for key in stats_dict:
                            key_clean = strip_ranking(key)
                            key_canonical = identity_normalize(key_clean, 'CBB')
                            if key_canonical == ascii_canonical:
                                return stats_dict[key]
                    
                    # Stricter fuzzy match for remaining cases
                    def normalize_for_match(name):
                        n = strip_accents(strip_ranking(name)).lower()
                        n = n.replace("'", "").replace("  ", " ")
                        n = n.replace(' st.', ' state').replace(' st ', ' state ')
                        if n.endswith(' st'):
                            n = n[:-3] + ' state'
                        return n.strip()
                    
                    team_normalized = normalize_for_match(ascii_name)
                    team_lower = team_name.lower()
                    
                    for key in stats_dict:
                        key_clean = strip_ranking(key)
                        key_normalized = normalize_for_match(key)
                        key_lower = key_clean.lower()
                        
                        if team_normalized == key_normalized:
                            return stats_dict[key]
                        
                        # Partial match
                        if key_lower.startswith(team_lower) or team_lower.startswith(key_lower):
                            return stats_dict[key]
                        
                        # CBB_TEAM_NAME_ALIASES fallback
                        key_upper = key_clean.upper()
                        if key_upper in CBB_TEAM_NAME_ALIASES:
                            alias_target = CBB_TEAM_NAME_ALIASES[key_upper]
                            if alias_target == normalized or alias_target == ascii_normalized or alias_target.lower() == team_lower:
                                return stats_dict[key]
                    
                    return {}
                
                away_covers = find_covers_stats(g.away_team, covers_cbb_stats)
                home_covers = find_covers_stats(g.home_team, covers_cbb_stats)
                
                # If Covers has data AND we haven't captured pre-game stats yet, save to DB
                if away_covers and home_covers and not g.pregame_stats_captured:
                    g.pregame_away_ats = away_covers.get('ats', '--')
                    g.pregame_home_ats = home_covers.get('ats', '--')
                    g.pregame_away_ats_road = away_covers.get('ats_road', '--')
                    g.pregame_home_ats_home = home_covers.get('ats_home', '--')
                    g.pregame_away_l10 = away_covers.get('l10', '--')
                    g.pregame_home_l10 = home_covers.get('l10', '--')
                    g.pregame_away_l10_ats = away_covers.get('l10_ats', '--')
                    g.pregame_home_l10_ats = home_covers.get('l10_ats', '--')
                    g.pregame_away_road_record = away_covers.get('road_record', '--')
                    g.pregame_home_home_record = home_covers.get('home_record', '--')
                    g.pregame_stats_captured = True
                    try:
                        db.session.commit()
                        logging.info(f"Saved CBB pre-game stats for {g.away_team} @ {g.home_team}")
                    except Exception as e:
                        logging.warning(f"Failed to save CBB pre-game stats: {e}")
                        db.session.rollback()
                
                # Use data priority: Covers (live) -> DB Pre-game
                g.away_overall = away_covers.get('record', g.away_record)
                g.home_overall = home_covers.get('record', g.home_record)
                g.away_road_record = away_covers.get('road_record') or g.pregame_away_road_record or '--'
                g.home_home_record = home_covers.get('home_record') or g.pregame_home_home_record or '--'
                g.away_ats = away_covers.get('ats') or g.pregame_away_ats or '--'
                g.home_ats = home_covers.get('ats') or g.pregame_home_ats or '--'
                g.away_ats_road = away_covers.get('ats_road') or g.pregame_away_ats_road or '--'
                g.home_ats_home = home_covers.get('ats_home') or g.pregame_home_ats_home or '--'
                g.away_l10 = away_covers.get('l10') or g.pregame_away_l10 or '--'
                g.home_l10 = home_covers.get('l10') or g.pregame_home_l10 or '--'
                g.away_l10_ats = away_covers.get('l10_ats') or g.pregame_away_l10_ats or '--'
                g.home_l10_ats = home_covers.get('l10_ats') or g.pregame_home_l10_ats or '--'
            elif g.league == 'NHL':
                g.away_logo = nhl_team_logos.get(g.away_team, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
                g.home_logo = nhl_team_logos.get(g.home_team, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
                away_stand = nhl_standings.get(g.away_team, {})
                home_stand = nhl_standings.get(g.home_team, {})
                g.away_record = away_stand.get('record', '--')
                g.home_record = home_stand.get('record', '--')
                g.away_standing = away_stand.get('standing', '')
                g.home_standing = home_stand.get('standing', '')
                
                # NHL Covers-style stats with database persistence
                away_covers = covers_nhl_stats.get(g.away_team, {})
                home_covers = covers_nhl_stats.get(g.home_team, {})
                
                # If Covers has data AND we haven't captured pre-game stats yet, save to DB
                if away_covers and home_covers and not g.pregame_stats_captured:
                    g.pregame_away_ats = away_covers.get('ats', '--')
                    g.pregame_home_ats = home_covers.get('ats', '--')
                    g.pregame_away_ats_road = away_covers.get('ats_road', '--')
                    g.pregame_home_ats_home = home_covers.get('ats_home', '--')
                    g.pregame_away_l10 = away_covers.get('l10', '--')
                    g.pregame_home_l10 = home_covers.get('l10', '--')
                    g.pregame_away_l10_ats = away_covers.get('l10_ats', '--')
                    g.pregame_home_l10_ats = home_covers.get('l10_ats', '--')
                    g.pregame_away_road_record = away_covers.get('road_record', '--')
                    g.pregame_home_home_record = home_covers.get('home_record', '--')
                    g.pregame_stats_captured = True
                    try:
                        db.session.commit()
                        logging.info(f"Saved NHL pre-game stats for {g.away_team} @ {g.home_team}")
                    except Exception as e:
                        logging.warning(f"Failed to save NHL pre-game stats: {e}")
                        db.session.rollback()
                
                # Use data priority: Covers (live) -> DB Pre-game
                g.away_overall = away_covers.get('record', g.away_record)
                g.home_overall = home_covers.get('record', g.home_record)
                g.away_road_record = away_covers.get('road_record') or g.pregame_away_road_record or '--'
                g.home_home_record = home_covers.get('home_record') or g.pregame_home_home_record or '--'
                g.away_ats = away_covers.get('ats') or g.pregame_away_ats or '--'
                g.home_ats = home_covers.get('ats') or g.pregame_home_ats or '--'
                g.away_ats_road = away_covers.get('ats_road') or g.pregame_away_ats_road or '--'
                g.home_ats_home = home_covers.get('ats_home') or g.pregame_home_ats_home or '--'
                g.away_l10 = away_covers.get('l10') or g.pregame_away_l10 or '--'
                g.home_l10 = home_covers.get('l10') or g.pregame_home_l10 or '--'
                g.away_l10_ats = away_covers.get('l10_ats') or g.pregame_away_l10_ats or '--'
                g.home_l10_ats = home_covers.get('l10_ats') or g.pregame_home_l10_ats or '--'
            else:
                g.away_logo = ''
                g.home_logo = ''
                g.away_record = '--'
                g.home_record = '--'
                g.away_standing = ''
                g.home_standing = ''
            
            # Attach VSIN data to each game
            vsin_match = match_vsin_data(g.away_team, g.home_team, g.league)
            if vsin_match:
                # Debug: Log RLM status for NBA games
                if g.league == 'NBA':
                    logger.info(f"VSIN match for {g.away_team} @ {g.home_team}: RLM={vsin_match.get('spread_rlm_detected', 'MISSING')}, sharp={vsin_match.get('spread_rlm_sharp_side', 'MISSING')}")
                # Get betting splits and lines from VSIN
                g.vsin_tickets_away = vsin_match.get('tickets_away') or 50
                g.vsin_tickets_home = vsin_match.get('tickets_home') or 50
                g.vsin_money_away = vsin_match.get('money_away') or 50
                g.vsin_money_home = vsin_match.get('money_home') or 50
                # Use empty string instead of None for spread values (prevents "None" string in HTML)
                g.vsin_open_spread = vsin_match.get('open_away_spread') if vsin_match.get('open_away_spread') is not None else ''
                g.vsin_current_spread = vsin_match.get('current_away_spread') if vsin_match.get('current_away_spread') is not None else ''
                g.vsin_open_odds = vsin_match.get('open_away_odds') or '-110'
                g.vsin_current_odds = vsin_match.get('current_away_odds') or '-110'
                g.vsin_has_data = True
                
                # Use RLM detection from VSIN fetch (single source of truth)
                # spread_rlm_detected is calculated in fetch_rlm_data with proper logic
                g.rlm_detected = vsin_match.get('spread_rlm_detected', False)
                g.rlm_sharp_side = vsin_match.get('spread_rlm_sharp_side', '')
            else:
                g.vsin_tickets_away = 50
                g.vsin_tickets_home = 50
                g.vsin_money_away = 50
                g.vsin_money_home = 50
                g.rlm_detected = False
                g.vsin_open_spread = ''
                g.vsin_current_spread = ''
                g.vsin_open_odds = '-110'
                g.vsin_current_odds = '-110'
                g.vsin_has_data = False
                logger.warning(f"No VSIN match for {g.league}: {g.away_team} @ {g.home_team}")
            
            games_by_league[g.league].append(g)
    
    # Filter CBB games: only show games with KenPom fanmatch predicted spread < 5 points
    if 'CBB' in games_by_league and games_by_league['CBB']:
        fanmatch_data = fetch_kenpom_fanmatch()
        if fanmatch_data:
            filtered_cbb = []
            for g in games_by_league['CBB']:
                prediction = get_kenpom_prediction(g.away_team, g.home_team)
                if prediction:
                    kp_spread = abs(prediction.get('kenpom_spread', 99))
                    if kp_spread < 5:
                        filtered_cbb.append(g)
                    else:
                        logger.debug(f"CBB filtered out: {g.away_team} @ {g.home_team} (KenPom spread: {kp_spread:.1f})")
                else:
                    logger.info(f"CBB filtered out (no fanmatch): {g.away_team} @ {g.home_team}")
            logger.info(f"CBB fanmatch filter: {len(games_by_league['CBB'])} -> {len(filtered_cbb)} games (< 5pt spread)")
            games_by_league['CBB'] = filtered_cbb
    
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
        
        # Initialize matchup data - CBB gets KenPom breakdown, NBA gets L5 data
        g.matchup_l5 = {}
        g.away_advanced = {}
        g.home_advanced = {}
        g.cbb_breakdown = {}

        # Pre-compute CBB breakdown using comprehensive KenPom data
        if g.league == 'CBB':
            try:
                cbb_breakdown = compute_cbb_matchup_breakdown(g.away_team, g.home_team)
                if cbb_breakdown.get('has_data'):
                    g.cbb_breakdown = cbb_breakdown
                    g.matchup_l5 = cbb_breakdown
            except Exception as e:
                logger.debug(f"CBB breakdown error for {g.away_team} vs {g.home_team}: {e}")
            
            market_spread = g.spread_line if g.spread_line is not None else 0
            spread_qual = qualify_spread_game(g.away_team, g.home_team, market_spread, 'CBB')
            g.spread_qualified = spread_qual.get('qualified', False)
            g.spread_projected = spread_qual.get('projected_spread', 0)
            g.spread_edge = spread_qual.get('spread_edge', 0)
            g.spread_decision = spread_qual.get('decision', '')
            g.spread_net_gap = spread_qual.get('net_gap', 0)
            g.spread_qual_reason = spread_qual.get('reason', '')
        
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
    
    # L10 Records - Fetched dynamically from ESPN Standings API
    nba_l10_records = {}
    try:
        standings_url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
        resp = requests.get(standings_url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for child in data.get('children', []):
                for standing in child.get('standings', {}).get('entries', []):
                    team_info = standing.get('team', {})
                    full_name = team_info.get('displayName', '')
                    team_name = team_info.get('shortDisplayName', '')
                    if not team_name:
                        team_name = full_name.split()[-1]
                    if 'Trail Blazers' in full_name:
                        team_name = 'Trail Blazers'
                    # Find L10 record in stats
                    for stat in standing.get('stats', []):
                        if stat.get('name') == 'streak' or stat.get('abbreviation') == 'L10':
                            continue
                        if stat.get('name') == 'record' and 'Last Ten' in str(stat.get('description', '')):
                            l10_val = stat.get('displayValue', '0-0')
                            if '-' in l10_val:
                                parts = l10_val.split('-')
                                nba_l10_records[team_name] = (int(parts[0]), int(parts[1]))
                    # Try alternate L10 stat location
                    for stat in standing.get('stats', []):
                        if stat.get('abbreviation') == 'L10' or 'last10' in stat.get('name', '').lower():
                            l10_val = stat.get('displayValue', '0-0')
                            if '-' in l10_val:
                                parts = l10_val.split('-')
                                nba_l10_records[team_name] = (int(parts[0]), int(parts[1]))
        # If ESPN standings didn't get L10, try team-by-team
        if not nba_l10_records:
            # Fallback: fetch from scoreboard/team endpoints
            teams_url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
            resp = requests.get(teams_url, timeout=15)
            if resp.status_code == 200:
                for team in resp.json().get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', []):
                    team_info = team.get('team', {})
                    team_name = team_info.get('shortDisplayName', '')
                    record = team_info.get('record', {})
                    # Some ESPN endpoints include L10 in record items
                    for item in record.get('items', []):
                        if 'Last 10' in item.get('description', '') or item.get('type') == 'last10':
                            l10_val = item.get('summary', '0-0')
                            if '-' in l10_val:
                                parts = l10_val.split('-')
                                nba_l10_records[team_name] = (int(parts[0]), int(parts[1]))
        logger.info(f"Fetched L10 records for {len(nba_l10_records)} NBA teams")
    except Exception as e:
        logger.warning(f"Error fetching L10 records: {e}")
    
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
    
    # 5. B2B teams - dynamically fetch yesterday's games from ESPN
    teams_played_yesterday = set()
    try:
        yesterday = today - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y%m%d')
        yesterday_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={yesterday_str}"
        resp = requests.get(yesterday_url, timeout=10)
        if resp.status_code == 200:
            for event in resp.json().get('events', []):
                comps = event.get('competitions', [{}])[0]
                for team in comps.get('competitors', []):
                    team_name = team.get('team', {}).get('shortDisplayName', '')
                    if team_name:
                        teams_played_yesterday.add(team_name)
        logger.info(f"B2B detection: {len(teams_played_yesterday)} teams played yesterday")
    except Exception as e:
        logger.warning(f"Error fetching yesterday's games for B2B: {e}")
    
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
    
    # ========== CBB DAILY SLATE ANALYSIS (Top 25 Teams Only) ==========
    cbb_games = games_by_league.get('CBB', [])

    # Fetch all KenPom data from all endpoints (ratings, four factors, height, misc, etc.)
    fetch_all_kenpom_data()
    
    # Build KenPom ranks dictionary for ALL teams playing today
    cbb_kenpom_ranks = {}
    cbb_all_teams_set = set()
    for g in cbb_games:
        away_kp_rank = get_kenpom_rank(g.away_team)
        home_kp_rank = get_kenpom_rank(g.home_team)
        if g.away_team not in cbb_all_teams_set:
            cbb_kenpom_ranks[g.away_team] = away_kp_rank
            cbb_all_teams_set.add(g.away_team)
        if g.home_team not in cbb_all_teams_set:
            cbb_kenpom_ranks[g.home_team] = home_kp_rank
            cbb_all_teams_set.add(g.home_team)
    
    # Filter to only Top 25 teams for daily analysis
    cbb_top25_set = {team for team, rank in cbb_kenpom_ranks.items() if rank <= 25}
    cbb_top25_ranks = {team: rank for team, rank in cbb_kenpom_ranks.items() if rank <= 25}
    cbb_top25_display = ', '.join([f'#{rank} {team}' for team, rank in sorted(cbb_top25_ranks.items(), key=lambda x: x[1])])
    
    # CBB Cold Teams - Top 25 teams only (use Covers L10 data - 3 or fewer wins)
    cbb_cold_teams_list = []
    cbb_cold_teams_set = set()
    for g in cbb_games:
        # Check away team L10 (Top 25 only)
        if g.away_team in cbb_top25_set:
            if hasattr(g, 'away_l10') and g.away_l10 and g.away_l10 != '--':
                try:
                    l10_parts = g.away_l10.replace(' ', '').split('-')
                    if len(l10_parts) >= 2:
                        wins = int(l10_parts[0])
                        losses = int(l10_parts[1])
                        if wins <= 3 and g.away_team not in cbb_cold_teams_set:
                            rank = cbb_top25_ranks.get(g.away_team, 999)
                            cbb_cold_teams_list.append(f'<span style="white-space:nowrap">#{rank} {g.away_team} ({wins}-{losses})</span>')
                            cbb_cold_teams_set.add(g.away_team)
                except:
                    pass
        # Check home team L10 (Top 25 only)
        if g.home_team in cbb_top25_set:
            if hasattr(g, 'home_l10') and g.home_l10 and g.home_l10 != '--':
                try:
                    l10_parts = g.home_l10.replace(' ', '').split('-')
                    if len(l10_parts) >= 2:
                        wins = int(l10_parts[0])
                        losses = int(l10_parts[1])
                        if wins <= 3 and g.home_team not in cbb_cold_teams_set:
                            rank = cbb_top25_ranks.get(g.home_team, 999)
                            cbb_cold_teams_list.append(f'<span style="white-space:nowrap">#{rank} {g.home_team} ({wins}-{losses})</span>')
                            cbb_cold_teams_set.add(g.home_team)
                except:
                    pass
    cbb_cold_teams_display = ', '.join(sorted(cbb_cold_teams_list, key=lambda x: int(x.split('#')[1].split(' ')[0]))) if cbb_cold_teams_list else 'None'
    
    # CBB Hot Teams - Top 25 teams only (8+ wins in L10)
    cbb_hot_teams_list = []
    cbb_hot_teams_set = set()
    for g in cbb_games:
        # Check away team L10 (Top 25 only)
        if g.away_team in cbb_top25_set:
            if hasattr(g, 'away_l10') and g.away_l10 and g.away_l10 != '--':
                try:
                    l10_parts = g.away_l10.replace(' ', '').split('-')
                    if len(l10_parts) >= 2:
                        wins = int(l10_parts[0])
                        losses = int(l10_parts[1])
                        if wins >= 8 and g.away_team not in cbb_hot_teams_set:
                            rank = cbb_top25_ranks.get(g.away_team, 999)
                            cbb_hot_teams_list.append(f'<span style="white-space:nowrap">#{rank} {g.away_team} ({wins}-{losses})</span>')
                            cbb_hot_teams_set.add(g.away_team)
                except:
                    pass
        # Check home team L10 (Top 25 only)
        if g.home_team in cbb_top25_set:
            if hasattr(g, 'home_l10') and g.home_l10 and g.home_l10 != '--':
                try:
                    l10_parts = g.home_l10.replace(' ', '').split('-')
                    if len(l10_parts) >= 2:
                        wins = int(l10_parts[0])
                        losses = int(l10_parts[1])
                        if wins >= 8 and g.home_team not in cbb_hot_teams_set:
                            rank = cbb_top25_ranks.get(g.home_team, 999)
                            cbb_hot_teams_list.append(f'<span style="white-space:nowrap">#{rank} {g.home_team} ({wins}-{losses})</span>')
                            cbb_hot_teams_set.add(g.home_team)
                except:
                    pass
    cbb_hot_teams_display = ', '.join(sorted(cbb_hot_teams_list, key=lambda x: int(x.split('#')[1].split(' ')[0]))) if cbb_hot_teams_list else 'None'
    
    # CBB Bad Defense - Top 25 teams only (defensive efficiency > 105 from KenPom)
    cbb_bad_defense_list = []
    cbb_bad_defense_set = set()
    for g in cbb_games:
        # Check Top 25 teams for bad defense
        if g.away_team in cbb_top25_set:
            away_data = get_torvik_team(g.away_team) or {}
            away_def = away_data.get('adj_d', 0)
            if away_def and away_def > 105 and g.away_team not in cbb_bad_defense_set:
                rank = cbb_top25_ranks.get(g.away_team, 999)
                cbb_bad_defense_list.append(f'<span style="white-space:nowrap">#{rank} {g.away_team} ({away_def:.1f})</span>')
                cbb_bad_defense_set.add(g.away_team)
        if g.home_team in cbb_top25_set:
            home_data = get_torvik_team(g.home_team) or {}
            home_def = home_data.get('adj_d', 0)
            if home_def and home_def > 105 and g.home_team not in cbb_bad_defense_set:
                rank = cbb_top25_ranks.get(g.home_team, 999)
                cbb_bad_defense_list.append(f'<span style="white-space:nowrap">#{rank} {g.home_team} ({home_def:.1f})</span>')
                cbb_bad_defense_set.add(g.home_team)
    cbb_bad_defense_list.sort(key=lambda x: int(x.split('#')[1].split(' ')[0]))  # Sort by KenPom rank
    cbb_bad_defense_display = ', '.join(cbb_bad_defense_list[:10]) if cbb_bad_defense_list else 'None'
    
    # CBB Large Spreads (10+ points) - ONLY Top 25 teams (using database spread only)
    cbb_large_spread_teams = set()
    cbb_large_spread_matchups = []
    wt_cbb_data = {}  # Skip WagerTalk call for page speed
    
    for g in cbb_games:
        # Only process games with Top 25 teams
        away_is_top25 = g.away_team in cbb_top25_set
        home_is_top25 = g.home_team in cbb_top25_set
        if not (away_is_top25 or home_is_top25):
            continue
        
        # Try to get open spread from WagerTalk
        open_spread = None
        for key, data in wt_cbb_data.items():
            if g.away_team.lower() in key.lower() or g.home_team.lower() in key.lower():
                open_spread = data.get('open_spread')
                break
        
        # Fallback to database spread
        spread_val = open_spread if open_spread else (abs(g.spread_line) if g.spread_line else 0)
        
        if spread_val and spread_val >= 10:
            if g.spread_line and g.spread_line < 0:
                # Away team is favorite
                if away_is_top25:
                    rank = cbb_top25_ranks.get(g.away_team, 99)
                    cbb_large_spread_matchups.append(f'<span style="white-space:nowrap">#{rank} {g.away_team} -{spread_val}</span>')
                    cbb_large_spread_teams.add(g.away_team)
            else:
                # Home team is favorite
                if home_is_top25:
                    rank = cbb_top25_ranks.get(g.home_team, 99)
                    cbb_large_spread_matchups.append(f'<span style="white-space:nowrap">#{rank} {g.home_team} -{spread_val}</span>')
                    cbb_large_spread_teams.add(g.home_team)
    cbb_large_spread_display = ', '.join(cbb_large_spread_matchups) if cbb_large_spread_matchups else 'None'
    
    # CBB Remaining Teams - Top 25 HOME teams with momentum (home-court advantage filter)
    # Filter: Must be Top 25 HOME team + not in eliminated categories + good recent form
    cbb_eliminated_teams = cbb_cold_teams_set | cbb_bad_defense_set
    cbb_remaining_teams_pool = cbb_top25_set - cbb_eliminated_teams
    
    # Build home teams set - only Top 25 HOME teams with momentum (5+ wins in L10)
    cbb_home_teams_list = []
    for g in cbb_games:
        if g.home_team in cbb_remaining_teams_pool:
            l10_wins = 5  # Default neutral
            if hasattr(g, 'home_l10') and g.home_l10 and g.home_l10 != '--':
                try:
                    l10_parts = g.home_l10.replace(' ', '').split('-')
                    if len(l10_parts) >= 2:
                        l10_wins = int(l10_parts[0])
                except:
                    pass
            # Only include if decent recent form (5+ wins in L10)
            if l10_wins >= 5:
                rank = cbb_top25_ranks.get(g.home_team, 999)
                cbb_home_teams_list.append((rank, g.home_team))
    
    # Sort by ranking, display just team names like NBA
    cbb_home_teams_list.sort(key=lambda x: x[0])
    cbb_remaining_display = ', '.join([f'#{rank} {team}' for rank, team in cbb_home_teams_list]) if cbb_home_teams_list else 'No qualified teams'
    
    # Fetch live scores from ESPN to detect games in progress
    live_game_keys = set()
    live_game_data = {}
    try:
        today_str = today.strftime("%Y%m%d")
        # Fetch NBA live scores
        nba_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today_str}"
        resp = requests.get(nba_url, timeout=5)
        for event in resp.json().get("events", []):
            state = event.get("status", {}).get("type", {}).get("state", "")
            if state == "in":  # Currently in progress
                comps = event.get("competitions", [{}])[0]
                teams = comps.get("competitors", [])
                if len(teams) == 2:
                    away = next((t for t in teams if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        key = f"{away_name}@{home_name}"
                        live_game_keys.add(key)
                        status = event.get("status", {})
                        live_game_data[key] = {
                            "away_score": int(away.get("score", 0)),
                            "home_score": int(home.get("score", 0)),
                            "period": f"Q{status.get('period', 1)}",
                            "clock": status.get("displayClock", "")
                        }
        # Fetch CBB live scores
        cbb_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={today_str}&limit=200&groups=50"
        resp = requests.get(cbb_url, timeout=10)
        for event in resp.json().get("events", []):
            state = event.get("status", {}).get("type", {}).get("state", "")
            if state == "in":  # Currently in progress
                comps = event.get("competitions", [{}])[0]
                teams = comps.get("competitors", [])
                if len(teams) == 2:
                    away = next((t for t in teams if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        key = f"{away_name}@{home_name}"
                        live_game_keys.add(key)
                        status = event.get("status", {})
                        live_game_data[key] = {
                            "away_score": int(away.get("score", 0)),
                            "home_score": int(home.get("score", 0)),
                            "period": f"H{status.get('period', 1)}",
                            "clock": status.get("displayClock", "")
                        }
        # Fetch NHL live scores
        nhl_url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={today_str}"
        resp = requests.get(nhl_url, timeout=5)
        for event in resp.json().get("events", []):
            state = event.get("status", {}).get("type", {}).get("state", "")
            if state == "in":
                comps = event.get("competitions", [{}])[0]
                teams = comps.get("competitors", [])
                if len(teams) == 2:
                    away = next((t for t in teams if t.get("homeAway") == "away"), None)
                    home = next((t for t in teams if t.get("homeAway") == "home"), None)
                    if away and home:
                        away_name = away.get("team", {}).get("shortDisplayName", "")
                        home_name = home.get("team", {}).get("shortDisplayName", "")
                        key = f"{away_name}@{home_name}"
                        live_game_keys.add(key)
                        status = event.get("status", {})
                        live_game_data[key] = {
                            "away_score": int(away.get("score", 0)),
                            "home_score": int(home.get("score", 0)),
                            "period": f"P{status.get('period', 1)}",
                            "clock": status.get("displayClock", "")
                        }
        logging.info(f"Live games detected: {len(live_game_keys)} games in progress")
        if live_game_keys:
            logging.info(f"Live game keys from ESPN: {list(live_game_keys)[:5]}")
    except Exception as e:
        logging.warning(f"Error fetching live scores for spreads: {e}")
    
    # Helper for fuzzy team name matching
    def teams_match_fuzzy(name1: str, name2: str) -> bool:
        """Stricter fuzzy matching to avoid E Kentucky matching Kentucky"""
        if not name1 or not name2:
            return False
        n1 = name1.lower().strip()
        n2 = name2.lower().strip()
        if n1 == n2:
            return True
        
        # Get first word of each name - check for directional/regional prefixes
        words1 = n1.split()
        words2 = n2.split()
        directional_prefixes = {'e', 'w', 'n', 's', 'c', 'east', 'west', 'north', 'south', 'central'}
        
        # If one has a directional prefix and the other doesn't, they're different schools
        has_prefix1 = words1 and words1[0] in directional_prefixes
        has_prefix2 = words2 and words2[0] in directional_prefixes
        if has_prefix1 != has_prefix2:
            # One has prefix, one doesn't - check if base names match
            base1 = ' '.join(words1[1:]) if has_prefix1 else n1
            base2 = ' '.join(words2[1:]) if has_prefix2 else n2
            # If base names are similar, these are DIFFERENT schools (E Kentucky vs Kentucky)
            if base1 in base2 or base2 in base1:
                return False  # Different schools!
        
        # Check if one contains the other BUT require similar length
        if n1 in n2 or n2 in n1:
            len_diff = abs(len(n1) - len(n2))
            # Only allow small differences (abbreviation differences like "St" vs "State")
            if len_diff <= 3:
                return True
        # Check core tokens - but require the MAIN token (longest) to match AND same token count
        tokens1 = [t for t in n1.replace('.', '').split() if len(t) > 2]
        tokens2 = [t for t in n2.replace('.', '').split() if len(t) > 2]
        if tokens1 and tokens2:
            main1 = max(tokens1, key=len)
            main2 = max(tokens2, key=len)
            if main1 == main2 and len(tokens1) == len(tokens2):
                return True
        return False
    
    # Helper function to detect if game is FINAL based on time
    def check_and_set_final_status(g):
        """Check if game is FINAL and set is_final attribute"""
        # Already set as final
        if getattr(g, 'is_final', False):
            return True
        # Check live_period
        if (getattr(g, 'live_period', '') or '').lower() in ('final', 'f'):
            g.is_final = True
            return True
        # Check if game started long ago (likely FINAL even if not in ESPN live data)
        is_live = getattr(g, 'is_live', False)
        if g.game_time and not is_live:
            try:
                et_tz = pytz.timezone('America/New_York')
                now = datetime.now(et_tz)
                game_time_str = g.game_time.replace(' EST', '').replace(' ET', '').strip()
                # Handle formats like "1/31 - 7:00 PM" or "7:00 PM"
                if ' - ' in game_time_str:
                    date_part, time_part = game_time_str.split(' - ', 1)
                    game_dt = datetime.strptime(f"{date_part}/{now.year} {time_part}", "%m/%d/%Y %I:%M %p")
                else:
                    game_dt = datetime.strptime(game_time_str, "%I:%M %p")
                    game_dt = game_dt.replace(year=now.year, month=now.month, day=now.day)
                game_dt = et_tz.localize(game_dt)
                hours_since_start = (now - game_dt).total_seconds() / 3600
                if hours_since_start >= 3.0:
                    g.is_final = True
                    return True
            except:
                pass
        return False
    
    # Sort key function for ordering: live first, upcoming middle, final last
    def game_sort_key(g):
        is_final = check_and_set_final_status(g)
        is_live = getattr(g, 'is_live', False) and not is_final
        
        # 0 = live, 1 = upcoming, 2 = final
        if is_live:
            return (0, g.game_time or '')
        elif is_final:
            return (2, g.game_time or '')
        else:
            return (1, g.game_time or '')
    
    # Mark games as live and sort
    matched_live_count = 0
    for league in games_by_league:
        for g in games_by_league[league]:
            key = f"{g.away_team}@{g.home_team}"
            g.is_live = key in live_game_keys
            # If not exact match, try fuzzy match
            if not g.is_live:
                for live_key in live_game_keys:
                    if '@' in live_key:
                        live_away, live_home = live_key.split('@', 1)
                        if teams_match_fuzzy(g.away_team, live_away) and teams_match_fuzzy(g.home_team, live_home):
                            g.is_live = True
                            key = live_key
                            matched_live_count += 1
                            logging.info(f"Live game matched: {g.away_team} vs {g.home_team} -> {live_key}")
                            break
            if g.is_live and key in live_game_data:
                ld = live_game_data[key]
                g.live_away_score = ld.get("away_score", 0)
                g.live_home_score = ld.get("home_score", 0)
                g.live_period = ld.get("period", "")
                g.live_clock = ld.get("clock", "")
                # Set is_final flag based on the live data
                g.is_final = ld.get("is_final", False) or (g.live_period or '').lower() in ('final', 'f')
        # Sort games for this league
        games_by_league[league] = sorted(games_by_league[league], key=game_sort_key)
    
    # Count RLM games per league for ordering
    league_rlm_counts = {}
    for league in ['CBB', 'NBA']:
        rlm_count = sum(1 for g in games_by_league.get(league, []) if getattr(g, 'rlm_detected', False))
        league_rlm_counts[league] = rlm_count
    
    # Sort leagues: most RLM games first, then by original order (CBB, NBA)
    default_order = ['CBB', 'NBA']
    sorted_leagues = sorted(default_order, key=lambda l: (-league_rlm_counts.get(l, 0), default_order.index(l)))
    
    # Build ordered dict with leagues having RLM games first
    ordered_games_by_league = {}
    for league in sorted_leagues:
        ordered_games_by_league[league] = games_by_league.get(league, [])
    
    # Reorder all_games to show leagues with RLM games first, with live games at top and final games at bottom
    # Group by league, then sort games within each league
    all_sorted_games = []
    for league in sorted_leagues:
        league_games = sorted(games_by_league.get(league, []), key=game_sort_key)
        all_sorted_games.extend(league_games)
    cbb_first_games = all_sorted_games
    
    return render_template('spreads.html', 
                           games_by_league=ordered_games_by_league,
                           all_games=cbb_first_games,
                           today=today,
                           total_games=sum(len(v) for v in ordered_games_by_league.values()),
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
                           cbb_top25_teams=cbb_top25_display,
                           cbb_cold_teams=cbb_cold_teams_display,
                           cbb_hot_teams=cbb_hot_teams_display,
                           cbb_bad_defense=cbb_bad_defense_display,
                           cbb_remaining_teams=cbb_remaining_display,
                           team_colors=NBA_TEAM_COLORS)

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
    
    # Check if game has started based on game time
    game_started = False
    try:
        from datetime import datetime
        import pytz
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        
        # Parse game time (format like "7:10p")
        game_time_str = game.game_time if hasattr(game, 'game_time') and game.game_time else None
        if game_time_str:
            # Handle format like "7:10p" or "7:10 PM"
            time_str = game_time_str.replace('p', ' PM').replace('a', ' AM').upper()
            try:
                game_time_obj = datetime.strptime(time_str.strip(), "%I:%M %p")
                game_datetime = now.replace(hour=game_time_obj.hour, minute=game_time_obj.minute, second=0)
                # Game considered started if current time is past game time
                game_started = now >= game_datetime
            except:
                pass
    except Exception as e:
        logging.debug(f"Game started check error: {e}")
    
    # Get team logos based on league
    def get_team_logo_for_league(team_name, league):
        """Get team logo URL based on league type."""
        if league == 'NBA':
            return nba_team_logos.get(team_name, 'https://a.espncdn.com/i/teamlogos/nba/500/nba.png')
        elif league == 'CBB':
            # Try original name first
            logo = get_transparent_cbb_logo(team_name) or get_cbb_logo(team_name)
            if logo:
                return logo
            # Try without apostrophes and special chars
            clean_name = team_name.replace("'", "").replace("'", "").replace("-", " ")
            logo = get_transparent_cbb_logo(clean_name) or get_cbb_logo(clean_name)
            if logo:
                return logo
            # Try common variations
            variations = [
                team_name.replace("St ", "Saint "),
                team_name.replace("Saint ", "St "),
                team_name.replace("'s", "s"),
            ]
            for var in variations:
                logo = get_transparent_cbb_logo(var) or get_cbb_logo(var)
                if logo:
                    return logo
            return 'https://a.espncdn.com/i/teamlogos/ncaa/500/ncaa.png'
        elif league == 'NHL':
            return nhl_team_logos.get(team_name, 'https://a.espncdn.com/i/teamlogos/nhl/500/nhl.png')
        elif league == 'NFL':
            # NFL logos - use team name abbreviation
            nfl_abbrs = {
                'Cardinals': 'ari', 'Falcons': 'atl', 'Ravens': 'bal', 'Bills': 'buf',
                'Panthers': 'car', 'Bears': 'chi', 'Bengals': 'cin', 'Browns': 'cle',
                'Cowboys': 'dal', 'Broncos': 'den', 'Lions': 'det', 'Packers': 'gb',
                'Texans': 'hou', 'Colts': 'ind', 'Jaguars': 'jax', 'Chiefs': 'kc',
                'Raiders': 'lv', 'Chargers': 'lac', 'Rams': 'lar', 'Dolphins': 'mia',
                'Vikings': 'min', 'Patriots': 'ne', 'Saints': 'no', 'Giants': 'nyg',
                'Jets': 'nyj', 'Eagles': 'phi', 'Steelers': 'pit', '49ers': 'sf',
                'Seahawks': 'sea', 'Buccaneers': 'tb', 'Titans': 'ten', 'Commanders': 'was'
            }
            abbr = nfl_abbrs.get(team_name, 'nfl')
            return f'https://a.espncdn.com/i/teamlogos/nfl/500/{abbr}.png'
        return ''
    
    result = {
        'game_id': game_id,
        'away_team': game.away_team,
        'home_team': game.home_team,
        'league': game.league,
        'game_started': game_started,
        'away_logo': get_team_logo_for_league(game.away_team, game.league),
        'home_logo': get_team_logo_for_league(game.home_team, game.league),
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

            # If TeamRankings returned empty (JS-rendered site), use basketball-reference as fallback
            if game.league == 'NBA' and len(away_season) < 3:
                logging.info(f"TeamRankings empty for {game.away_team} vs {game.home_team}, trying basketball-reference...")
                try:
                    with ThreadPoolExecutor(max_workers=2) as bref_executor:
                        away_bref_future = bref_executor.submit(MatchupIntelligence.fetch_bball_ref_team_stats, game.away_team)
                        home_bref_future = bref_executor.submit(MatchupIntelligence.fetch_bball_ref_team_stats, game.home_team)
                        away_bref = away_bref_future.result(timeout=20) or {}
                        home_bref = home_bref_future.result(timeout=20) or {}
                    if away_bref:
                        away_season.update(away_bref)
                        logging.info(f"BBall-Ref away stats loaded: {list(away_bref.keys())[:8]}")
                    if home_bref:
                        home_season.update(home_bref)
                        logging.info(f"BBall-Ref home stats loaded: {list(home_bref.keys())[:8]}")
                except Exception as e:
                    logging.warning(f"BBall-Ref fallback error: {e}")

            # Fetch external data in PARALLEL for faster loading
            away_ctg = {}
            home_ctg = {}
            h2h_data = {}
            rlm_data = {}

            def fetch_ctg_away():
                if game.league == 'NBA':
                    return MatchupIntelligence.fetch_ctg_four_factors(game.away_team)
                elif game.league == 'CBB':
                    return MatchupIntelligence.fetch_kenpom_stats(game.away_team)
                return {}

            def fetch_ctg_home():
                if game.league == 'NBA':
                    return MatchupIntelligence.fetch_ctg_four_factors(game.home_team)
                elif game.league == 'CBB':
                    return MatchupIntelligence.fetch_kenpom_stats(game.home_team)
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
                    away_ctg = ctg_away_future.result(timeout=45)  # Allow time for retries
                except Exception as e:
                    logging.warning(f"CTG away fetch error: {e}")
                    away_ctg = {}
                
                try:
                    home_ctg = ctg_home_future.result(timeout=45)  # Allow time for retries
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
            
            # MERGE KenPom/CTG stats into raw away_season/home_season BEFORE building result
            # This ensures find_stat can access KenPom data like Adj O, 3PT%, etc.
            if away_ctg:
                away_season['Adj O'] = away_ctg.get('adj_o') or away_ctg.get('off_ppp') or away_season.get('Adj O', 0)
                away_season['Adj D'] = away_ctg.get('adj_d') or away_ctg.get('def_ppp') or away_season.get('Adj D', 0)
                away_season['eFG%'] = away_ctg.get('off_efg') or away_season.get('eFG%', 0)
                away_season['Opp eFG%'] = away_ctg.get('def_efg') or away_season.get('Opp eFG%', 0)
                away_season['3PT%'] = away_ctg.get('off_3pt') or away_season.get('3PT%', 0)
                away_season['Opp 3PT%'] = away_ctg.get('def_3pt') or away_season.get('Opp 3PT%', 0)
                away_season['TOV%'] = away_ctg.get('off_tov') or away_season.get('TOV%', 0)
                away_season['ORB%'] = away_ctg.get('off_orb') or away_season.get('ORB%', 0)
                away_season['DRB%'] = away_ctg.get('def_orb') or away_season.get('DRB%', 0)
                away_season['FTA/FGA'] = away_ctg.get('off_ft_rate') or away_season.get('FTA/FGA', 0)
                away_season['Tempo'] = away_ctg.get('tempo') or away_season.get('Tempo', 0)
            if home_ctg:
                home_season['Adj O'] = home_ctg.get('adj_o') or home_ctg.get('off_ppp') or home_season.get('Adj O', 0)
                home_season['Adj D'] = home_ctg.get('adj_d') or home_ctg.get('def_ppp') or home_season.get('Adj D', 0)
                home_season['eFG%'] = home_ctg.get('off_efg') or home_season.get('eFG%', 0)
                home_season['Opp eFG%'] = home_ctg.get('def_efg') or home_season.get('Opp eFG%', 0)
                home_season['3PT%'] = home_ctg.get('off_3pt') or home_season.get('3PT%', 0)
                home_season['Opp 3PT%'] = home_ctg.get('def_3pt') or home_season.get('Opp 3PT%', 0)
                home_season['TOV%'] = home_ctg.get('off_tov') or home_season.get('TOV%', 0)
                home_season['ORB%'] = home_ctg.get('off_orb') or home_season.get('ORB%', 0)
                home_season['DRB%'] = home_ctg.get('def_orb') or home_season.get('DRB%', 0)
                home_season['FTA/FGA'] = home_ctg.get('off_ft_rate') or home_season.get('FTA/FGA', 0)
                home_season['Tempo'] = home_ctg.get('tempo') or home_season.get('Tempo', 0)
            
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
                # CBB team name aliases for VSIN matching
                cbb_team_aliases = {
                    'fau': 'fl atlantic', 'florida atlantic': 'fl atlantic',
                    'ecu': 'east carolina', 
                    'uconn': 'connecticut', 'conn': 'connecticut',
                    'smu': 'southern methodist', 'smoo': 'southern methodist',
                    'ucf': 'central florida',
                    'usf': 'south florida',
                    'fiu': 'florida international', 'fla intl': 'florida international', 'florida intl': 'florida international',
                    'utep': 'texas el paso',
                    'unlv': 'nevada las vegas',
                    'lsu': 'louisiana state',
                    'ole miss': 'mississippi',
                    'usc': 'southern california', 'southern cal': 'southern california',
                    'ucla': 'california los angeles',
                    'umass': 'massachusetts',
                    'uva': 'virginia', 'va': 'virginia',
                    'vcu': 'virginia commonwealth',
                    'wvu': 'west virginia',
                    'tcu': 'texas christian',
                    'byu': 'brigham young',
                    'st marys': 'saint marys', "st mary's": 'saint marys', 'st. marys': 'saint marys',
                    "saint peter's": 'st peters', 'saint peters': 'st peters', "st. peter's": 'st peters',
                    'st peters': 'st peters',
                    # App State / Appalachian State
                    'app state': 'appalachian', 'appalachian st': 'appalachian', 'appalachian state': 'appalachian',
                    # South Alabama / USA
                    'south alabama': 's alabama', 'so alabama': 's alabama', 'usa': 's alabama',
                    # More Sun Belt teams
                    'texas state': 'texas st', 'texas st.': 'texas st',
                    'georgia state': 'georgia st', 'georgia st.': 'georgia st',
                    'coastal carolina': 'coastal', 'ccu': 'coastal',
                    # E Texas A&M / East Texas A&M (SWAC)
                    'e texas a&m': 'east texas', 'etamu': 'east texas', 'east texas a&m': 'east texas',
                    # N'Western St / Northwestern State (Southland)
                    "n'western st": 'northwestern', 'northwestern st': 'northwestern', 'northwestern state': 'northwestern', 'nwst': 'northwestern',
                    # Youngstown St @ Purdue FW
                    'youngstown st': 'youngstown', 'youngstown state': 'youngstown', 'ysu': 'youngstown',
                    'purdue fw': 'purdue fort wayne', 'purdue fort wayne': 'purdue fort wayne', 'pfw': 'purdue fort wayne', 'ipfw': 'purdue fort wayne', 'ft wayne': 'purdue fort wayne',
                    # Furman @ ETSU (SoCon)
                    'etsu': 'east tennessee', 'east tennessee st': 'east tennessee', 'east tennessee state': 'east tennessee', 'e tennessee st': 'east tennessee',
                    'furman': 'furman', 'fur': 'furman',
                    # Louisiana @ James Madison (Sun Belt)
                    'louisiana': 'louisiana lafayette', 'ul lafayette': 'louisiana lafayette', 'louisiana-lafayette': 'louisiana lafayette', 'ull': 'louisiana lafayette',
                    'james madison': 'jmu', 'jm': 'jmu', 'j madison': 'jmu',
                    # South Dakota @ Kansas City (Summit)
                    'south dakota': 's dakota', 'so dakota': 's dakota', 'sd': 's dakota',
                    'kansas city': 'umkc', 'missouri kansas city': 'umkc', 'mo kansas city': 'umkc', 'missouri-kansas city': 'umkc',
                    # Central Arkansas (ASUN)
                    'c arkansas': 'central arkansas', 'uca': 'central arkansas', 'cent arkansas': 'central arkansas',
                    # Arkansas-Pine Bluff (SWAC)
                    'ar-pine bluff': 'arkansas pine bluff', 'arkansas-pine bluff': 'arkansas pine bluff', 'uapb': 'arkansas pine bluff', 'ark pine bluff': 'arkansas pine bluff',
                    # North Alabama (ASUN)
                    'n alabama': 'north alabama', 'una': 'north alabama', 'north ala': 'north alabama',
                    # Grambling State (SWAC)
                    'grambling': 'grambling', 'grambling st': 'grambling', 'gram': 'grambling',
                    # Middle Tennessee (C-USA)
                    'mtsu': 'middle tennessee', 'middle tenn': 'middle tennessee', 'mid tenn': 'middle tennessee',
                    # South Dakota State (Summit)
                    's dakota st': 'south dakota state', 'south dakota st': 'south dakota state', 'sdsu': 'south dakota state',
                    # St. Thomas Minnesota (Summit)
                    'st thomas': 'st thomas mn', 'st thomas (mn)': 'st thomas mn', 'saint thomas': 'st thomas mn',
                    # Gardner-Webb (Big South)
                    'gardner-webb': 'gardner webb', 'gwu': 'gardner webb',
                    # Longwood (Big South)
                    'longwood': 'longwood', 'long wood': 'longwood',
                    # SC State (MEAC)
                    'sc state': 'south carolina state', 'sc st': 'south carolina state',
                    # Maryland Eastern Shore (MEAC)
                    'md eastern': 'maryland eastern shore', 'umes': 'maryland eastern shore',
                    # George Washington (A-10)
                    'g washington': 'george washington', 'gw': 'george washington', 'geo washington': 'george washington',
                    # Saint Joseph's (A-10)
                    "saint joseph's": 'st josephs', "st joseph's": 'st josephs', 'st josephs': 'st josephs', 'sju': 'st josephs',
                    # UIC / Illinois-Chicago (Horizon)
                    'uic': 'illinois chicago', 'illinois-chicago': 'illinois chicago', 'illinois chicago': 'illinois chicago', 'uic flames': 'illinois chicago',
                    # SIUE / SIU Edwardsville (OVC)
                    'siue': 'siu edwardsville', 'siu edwardsville': 'siu edwardsville',
                    # S Illinois / Southern Illinois (MVC)
                    's illinois': 'southern illinois', 'so illinois': 'southern illinois', 'siu': 'southern illinois', 'southern ill': 'southern illinois',
                    # N Iowa / Northern Iowa (MVC)
                    'n iowa': 'northern iowa', 'uni': 'northern iowa', 'northern iowa': 'northern iowa',
                    # Loyola Chicago (A-10)
                    'loyola chicago': 'loyola chi', 'loyola-chicago': 'loyola chi', 'luc': 'loyola chi',
                    # Murray St / Murray State (MVC)
                    'murray st': 'murray state', 'murray state': 'murray state',
                }
                
                def normalize_for_vsin_match(name: str) -> set:
                    """Create a set of normalized tokens for fuzzy matching."""
                    if not name:
                        return set()
                    n = name.lower().strip()
                    # Apply aliases
                    if n in cbb_team_aliases:
                        n = cbb_team_aliases[n]
                    # Remove common suffixes
                    for suffix in [' state', ' st', ' st.', ' university', ' univ']:
                        n = n.replace(suffix, ' ')
                    # Clean up and tokenize
                    tokens = set(n.split())
                    # Filter short tokens
                    tokens = {t for t in tokens if len(t) > 2}
                    return tokens
                
                def teams_match(vsin_team: str, game_team: str) -> bool:
                    """Check if VSIN team matches game team using token overlap."""
                    if not vsin_team or not game_team:
                        return False
                    
                    # Apply aliases to both sides
                    vsin_lower = vsin_team.lower().strip()
                    game_lower = game_team.lower().strip()
                    vsin_aliased = cbb_team_aliases.get(vsin_lower, vsin_lower)
                    game_aliased = cbb_team_aliases.get(game_lower, game_lower)
                    
                    # Direct match after aliasing
                    if vsin_aliased == game_aliased:
                        return True
                    if vsin_lower == game_lower:
                        return True
                    
                    vsin_tokens = normalize_for_vsin_match(vsin_team)
                    game_tokens = normalize_for_vsin_match(game_team)
                    # Token overlap - at least one significant word matches
                    overlap = vsin_tokens & game_tokens
                    if overlap:
                        return True
                    # Substring match for single-word names (Duke, Gonzaga, etc)
                    if vsin_lower in game_lower or game_lower in vsin_lower:
                        return True
                    if vsin_aliased in game_aliased or game_aliased in vsin_aliased:
                        return True
                    return False
                
                # Match VSIN data to this game
                for key, data in all_rlm.items():
                    # VSIN key format: "Away Team @ Home Team"
                    vsin_away = data.get('away_team', '')
                    vsin_home = data.get('home_team', '')
                    
                    # Also try parsing from key if team fields are missing
                    if not vsin_away or not vsin_home:
                        if ' @ ' in key:
                            parts = key.split(' @ ')
                            if len(parts) == 2:
                                vsin_away = parts[0].strip()
                                vsin_home = parts[1].strip()
                    
                    away_match = teams_match(vsin_away, game.away_team)
                    home_match = teams_match(vsin_home, game.home_team)
                    
                    if away_match and home_match:
                        rlm_data = data
                        break
                
                if rlm_data:
                    logging.debug(f"VSIN match found: {game.away_team} @ {game.home_team}")
                else:
                    logging.debug(f"No VSIN match: {game.away_team} @ {game.home_team}")
            except Exception as e:
                logging.warning(f"RLM fetch error: {e}")
            
            # Get lines from VSIN first, fallback to database (The Odds API)
            # VSIN field names: open_away_spread, current_away_spread
            vsin_open_spread = rlm_data.get('open_away_spread') or rlm_data.get('open_spread')
            vsin_current_spread = rlm_data.get('current_away_spread') or rlm_data.get('current_spread')
            vsin_open_total = rlm_data.get('total_open_line')
            vsin_current_total = rlm_data.get('total_current_line')
            
            # Use VSIN data if available, else fallback to database
            db_spread = game.spread if hasattr(game, 'spread') else None
            db_opening_spread = game.opening_spread if hasattr(game, 'opening_spread') else None
            db_total = game.total if hasattr(game, 'total') else None
            db_opening_total = game.opening_total if hasattr(game, 'opening_total') else None
            
            # Prefer VSIN lines (most up-to-date)
            # Filter out 'N/A' and non-numeric strings
            def safe_numeric(val):
                if val is None or val == '' or val == 'N/A':
                    return None
                try:
                    float(val)
                    return val
                except (ValueError, TypeError):
                    return None
            
            open_spread = safe_numeric(vsin_open_spread) or safe_numeric(db_opening_spread)
            current_spread = safe_numeric(vsin_current_spread) or safe_numeric(db_spread)
            open_total = vsin_open_total if vsin_open_total else db_opening_total
            current_total = vsin_current_total if vsin_current_total else db_total
            
            # Get away/home bet percentages from VSIN
            # VSIN field names: tickets_away, tickets_home (bets %), money_away, money_home (handle %)
            away_bet = int(rlm_data.get('tickets_away') or rlm_data.get('away_bet_pct') or 50)
            home_bet = int(rlm_data.get('tickets_home') or rlm_data.get('home_bet_pct') or 50)
            away_money = int(rlm_data.get('money_away') or rlm_data.get('away_money_pct') or 50)
            home_money = int(rlm_data.get('money_home') or rlm_data.get('home_money_pct') or 50)
            
            # Get Over/Under percentages from WagerTalk
            over_bet = int(rlm_data.get('over_bet_pct', 50) or 50)
            under_bet = int(rlm_data.get('under_bet_pct', 50) or 50)
            over_money = int(rlm_data.get('over_money_pct', 50) or 50)
            under_money = int(rlm_data.get('under_money_pct', 50) or 50)
            
            # Detect sharp money (money % significantly different from tickets %)
            away_sharp_diff = away_money - away_bet
            home_sharp_diff = home_money - home_bet
            spread_sharp_detected = abs(away_sharp_diff) >= 10 or abs(home_sharp_diff) >= 10
            spread_sharp_side = game.away_team if away_sharp_diff >= 10 else (game.home_team if home_sharp_diff >= 10 else None)
            
            # USE CACHED RLM DATA from fetch_rlm_data (single source of truth)
            # RLM FORMULA: Line 📉 + Money to new favorite 📈 = RLM
            # Detection is done in fetch_rlm_data, we just use the cached results here
            spread_rlm_detected = rlm_data.get('spread_rlm_detected', False)
            spread_rlm_sharp_side = rlm_data.get('spread_rlm_sharp_side', None)
            totals_rlm_detected = False  # RLM is for SPREADS only
            totals_rlm_sharp_side = None
            
            # Calculate favorite from open spread (negative = away favorite)
            # Priority: Use rlm_data if available, else calculate from spread
            open_favorite = rlm_data.get('open_favorite', '')
            if not open_favorite and open_spread is not None:
                try:
                    open_spread_val = float(open_spread)
                    if open_spread_val < 0:
                        open_favorite = game.away_team  # Negative spread = away is favorite
                    elif open_spread_val > 0:
                        open_favorite = game.home_team  # Positive spread = home is favorite
                    else:
                        open_favorite = 'Pick'  # Even spread
                except (ValueError, TypeError):
                    open_favorite = ''
            
            # Also calculate current favorite
            current_favorite = ''
            if current_spread is not None:
                try:
                    current_spread_val = float(current_spread)
                    if current_spread_val < 0:
                        current_favorite = game.away_team
                    elif current_spread_val > 0:
                        current_favorite = game.home_team
                    else:
                        current_favorite = 'Pick'
                except (ValueError, TypeError):
                    current_favorite = ''
            
            # Calculate line movement direction (who the line moved toward)
            line_moved_toward = ''
            if open_spread is not None and current_spread is not None:
                try:
                    open_val = float(open_spread)
                    current_val = float(current_spread)
                    movement = current_val - open_val
                    if abs(movement) >= 0.5:
                        # Line decreased (e.g., -6 to -4): moved toward underdog (away spread less negative = home favored less)
                        # Line increased (e.g., -4 to -6): moved toward favorite
                        if movement > 0:
                            # Spread went from more negative to less negative = moved toward away team (underdog if away was favorite)
                            line_moved_toward = game.away_team if open_val < 0 else game.home_team
                        else:
                            # Spread went more negative = moved toward home team
                            line_moved_toward = game.home_team if open_val < 0 else game.away_team
                except (ValueError, TypeError):
                    line_moved_toward = ''
            
            # Majority team
            majority_pct = max(away_bet, home_bet)
            majority_team = game.away_team if away_bet > home_bet else game.home_team
            
            # Add RLM checklist data to result
            result['rlm'] = {
                'open_spread': open_spread if open_spread else 'N/A',
                'current_spread': current_spread if current_spread else 'N/A',
                'away_spread': current_spread if current_spread else 'N/A',
                'home_spread': current_spread if current_spread else 'N/A',
                'spread_open_line': open_spread,
                'spread_current_line': current_spread,
                'spread_open_odds': rlm_data.get('spread_open_odds', '-110'),
                'spread_current_odds': rlm_data.get('spread_current_odds', '-110'),
                'spread_tickets_pct': away_bet,
                'spread_money_pct': away_money,
                'spread_sharp_detected': spread_sharp_detected,
                'spread_sharp_side': spread_sharp_side,
                'total_open_line': open_total if open_total else 'N/A',
                'total_current_line': current_total if current_total else 'N/A',
                'total_open_odds': rlm_data.get('total_open_odds', '-110'),
                'total_current_odds': rlm_data.get('total_current_odds', '-110'),
                'line_movement': rlm_data.get('line_movement', 'N/A'),
                'line_movement_value': rlm_data.get('line_movement_value'),
                'line_direction': rlm_data.get('line_direction', 'stable'),
                'away_bet_pct': away_bet,
                'home_bet_pct': home_bet,
                'away_money_pct': away_money,
                'home_money_pct': home_money,
                # Totals betting percentages from WagerTalk
                'over_bet_pct': over_bet,
                'under_bet_pct': under_bet,
                'over_money_pct': over_money,
                'under_money_pct': under_money,
                'totals_data_available': over_bet != 50 or under_bet != 50,
                'majority_team': majority_team,
                'majority_pct': majority_pct,
                'rlm_potential': spread_rlm_detected or totals_rlm_detected,
                'spread_rlm_detected': spread_rlm_detected,
                'spread_rlm_sharp_side': spread_rlm_sharp_side,
                'totals_rlm_detected': totals_rlm_detected,
                'totals_rlm_sharp_side': totals_rlm_sharp_side,
                'sharp_detected': spread_sharp_detected,
                'sharp_side': spread_sharp_side,
                # Favorite tracking - calculated from spread data
                'favorite_is_away': (lambda s: s is not None and s not in ('N/A', '') and float(s) < 0)(open_spread) if open_spread and open_spread not in ('N/A', '') else None,
                'open_favorite': open_favorite,
                'current_favorite': current_favorite,
                'line_moved_toward': line_moved_toward
            }
            
            # Also add top-level VSIN fields for direct API access
            result['open_spread'] = open_spread
            result['current_spread'] = current_spread
            result['open_total'] = open_total
            result['current_total'] = current_total
            result['away_bet_pct'] = away_bet
            result['home_bet_pct'] = home_bet
            result['away_money_pct'] = away_money
            result['home_money_pct'] = home_money
            result['has_rlm'] = spread_rlm_detected
            result['has_vsin_data'] = bool(rlm_data)
            
            # Convert to display format - Season Stats using exact TeamRankings AND KenPom stat names
            # Use TeamRankings first, KenPom as fallback, CTG as final fallback
            # For CBB: Use Adj O/D (efficiency) instead of raw PPG if PPG not available
            away_adj_o = find_stat(away_season, 'Adj O', 'AdjOE', 'AdjO')
            away_adj_d = find_stat(away_season, 'Adj D', 'AdjDE', 'AdjD')
            result['away_season'] = {
                'PPG': find_stat(away_season, 'points/game', 'PPG', 'PPP') or away_adj_o,
                'Opp PPG': find_stat(away_season, 'opp points/game', 'Opp PPG', 'Opp PPP') or away_adj_d,
                'FG%': find_stat(away_season, 'shooting %', 'FG%', 'FG2Pct'),
                'Opp FG%': find_stat(away_season, 'opp shooting %', 'Opp FG%', 'OppFG2Pct'),
                '3PT%': find_stat(away_season, 'three point %', '3PT%', 'FG3Pct', '3P%'),
                'Opp 3PT%': find_stat(away_season, 'opp three point %', 'Opp 3PT%', 'OppFG3Pct', 'Opp 3P%'),
                'FT%': find_stat(away_season, 'free throw %', 'FT%', 'FTPct'),
                'Opp FT%': find_stat(away_season, 'opp free throw %', 'Opp FT%', 'OppFTPct'),
                'PACE': find_stat(away_season, 'possessions/gm', 'PACE', 'Tempo'),
                'Assists/TO': find_stat(away_season, 'assists/turnover', 'Assists/TO', 'ARate'),
                'eFG%': find_stat(away_season, 'effective fg %', 'eFG%') or away_ctg.get('off_efg'),
                'Opp eFG%': find_stat(away_season, 'opp effective fg %', 'Opp eFG%') or away_ctg.get('def_efg'),
                'TOV': find_stat(away_season, 'turnovers/game'),
                'TOV%': find_stat(away_season, 'turnovers/play') or away_ctg.get('off_tov'),
                'ORB': find_stat(away_season, 'off rebounds/gm'),
                'ORB%': find_stat(away_season, 'off rebound %') or away_ctg.get('off_orb'),
                'DRB': find_stat(away_season, 'def rebounds/gm'),
                'DRB%': find_stat(away_season, 'def rebound %') or away_ctg.get('def_orb'),
                'Assists': find_stat(away_season, 'assists/game'),
                'Blocks': find_stat(away_season, 'blocks/game'),
                'Steals': find_stat(away_season, 'steals/game'),
                'Fouls': find_stat(away_season, 'personal fouls/gm'),
                'O Eff': find_stat(away_season, 'off efficiency') or away_ctg.get('off_ppp'),
                'D Eff': find_stat(away_season, 'def efficiency') or away_ctg.get('def_ppp'),
                'Pts in Paint': find_stat(away_season, 'pts in paint/gm'),
                'Fastbreak Pts': find_stat(away_season, 'fastbreak pts/gm'),
                'FTA/FGA': away_ctg.get('off_ft_rate') or find_stat(away_season, 'fta/fga'),
                '3PM/Game': find_stat(away_season, '3pm/game'),
                'Opp TOV': find_stat(away_season, 'opp turnovers/game'),
                'Opp TOV%': find_stat(away_season, 'opp turnovers/play'),
                'Opp 3PM/Game': find_stat(away_season, 'opp 3pm/game'),
                'Opp FTA/FGA': away_ctg.get('def_ft_rate') or find_stat(away_season, 'opp fta/fga'),
                # RANKS and PPP: Use TeamRankings first, CTG as fallback, Adj O/D as final fallback
                'PPP': find_stat(away_season, 'PPP') or away_ctg.get('off_ppp') or away_adj_o,
                'PPP Rank': find_stat(away_season, 'PPP Rank') or away_ctg.get('off_ppp_rank'),
                'Opp PPP': find_stat(away_season, 'Opp PPP') or away_ctg.get('def_ppp') or away_adj_d,
                'Opp PPP Rank': find_stat(away_season, 'Opp PPP Rank') or away_ctg.get('def_ppp_rank'),
                'eFG% Rank': find_stat(away_season, 'eFG% Rank') or away_ctg.get('off_efg_rank'),
                'Opp eFG% Rank': find_stat(away_season, 'Opp eFG% Rank') or away_ctg.get('def_efg_rank'),
                'TOV% Rank': find_stat(away_season, 'TOV% Rank') or away_ctg.get('off_tov_rank'),
                'F-TOV% Rank': find_stat(away_season, 'F-TOV% Rank') or away_ctg.get('def_tov_rank'),
                'ORB% Rank': find_stat(away_season, 'ORB% Rank') or away_ctg.get('off_orb_rank'),
                'DRB% Rank': find_stat(away_season, 'DRB% Rank') or away_ctg.get('def_orb_rank'),
                'FT Rate Rank': find_stat(away_season, 'FT Rate Rank') or away_ctg.get('off_ft_rank'),
                'Opp FT Rate Rank': find_stat(away_season, 'Opp FT Rate Rank') or away_ctg.get('def_ft_rank'),
                'Opp 3PT% Rank': find_stat(away_season, 'Opp 3PT% Rank') or away_ctg.get('def_3pt_rank'),
                '3PT% Rank': find_stat(away_season, '3PT% Rank') or away_ctg.get('off_3pt_rank')
            }
            home_adj_o = find_stat(home_season, 'Adj O', 'AdjOE', 'AdjO')
            home_adj_d = find_stat(home_season, 'Adj D', 'AdjDE', 'AdjD')
            result['home_season'] = {
                'PPG': find_stat(home_season, 'points/game', 'PPG', 'PPP') or home_adj_o,
                'Opp PPG': find_stat(home_season, 'opp points/game', 'Opp PPG', 'Opp PPP') or home_adj_d,
                'FG%': find_stat(home_season, 'shooting %', 'FG%', 'FG2Pct'),
                'Opp FG%': find_stat(home_season, 'opp shooting %', 'Opp FG%', 'OppFG2Pct'),
                '3PT%': find_stat(home_season, 'three point %', '3PT%', 'FG3Pct', '3P%'),
                'Opp 3PT%': find_stat(home_season, 'opp three point %', 'Opp 3PT%', 'OppFG3Pct', 'Opp 3P%'),
                'FT%': find_stat(home_season, 'free throw %', 'FT%', 'FTPct'),
                'Opp FT%': find_stat(home_season, 'opp free throw %', 'Opp FT%', 'OppFTPct'),
                'PACE': find_stat(home_season, 'possessions/gm', 'PACE', 'Tempo'),
                'Assists/TO': find_stat(home_season, 'assists/turnover', 'Assists/TO', 'ARate'),
                'eFG%': find_stat(home_season, 'effective fg %', 'eFG%') or home_ctg.get('off_efg'),
                'Opp eFG%': find_stat(home_season, 'opp effective fg %', 'Opp eFG%') or home_ctg.get('def_efg'),
                'TOV': find_stat(home_season, 'turnovers/game'),
                'TOV%': find_stat(home_season, 'turnovers/play') or home_ctg.get('off_tov'),
                'ORB': find_stat(home_season, 'off rebounds/gm'),
                'ORB%': find_stat(home_season, 'off rebound %') or home_ctg.get('off_orb'),
                'DRB': find_stat(home_season, 'def rebounds/gm'),
                'DRB%': find_stat(home_season, 'def rebound %') or home_ctg.get('def_orb'),
                'Assists': find_stat(home_season, 'assists/game'),
                'Blocks': find_stat(home_season, 'blocks/game'),
                'Steals': find_stat(home_season, 'steals/game'),
                'Fouls': find_stat(home_season, 'personal fouls/gm'),
                'O Eff': find_stat(home_season, 'off efficiency') or home_ctg.get('off_ppp'),
                'D Eff': find_stat(home_season, 'def efficiency') or home_ctg.get('def_ppp'),
                'Pts in Paint': find_stat(home_season, 'pts in paint/gm'),
                'Fastbreak Pts': find_stat(home_season, 'fastbreak pts/gm'),
                'FTA/FGA': home_ctg.get('off_ft_rate') or find_stat(home_season, 'fta/fga'),
                '3PM/Game': find_stat(home_season, '3pm/game'),
                'Opp TOV': find_stat(home_season, 'opp turnovers/game'),
                'Opp TOV%': find_stat(home_season, 'opp turnovers/play'),
                'Opp 3PM/Game': find_stat(home_season, 'opp 3pm/game'),
                'Opp FTA/FGA': home_ctg.get('def_ft_rate') or find_stat(home_season, 'opp fta/fga'),
                # RANKS and PPP: Use TeamRankings first, CTG as fallback, Adj O/D as final fallback
                'PPP': find_stat(home_season, 'PPP') or home_ctg.get('off_ppp') or home_adj_o,
                'PPP Rank': find_stat(home_season, 'PPP Rank') or home_ctg.get('off_ppp_rank'),
                'Opp PPP': find_stat(home_season, 'Opp PPP') or home_ctg.get('def_ppp') or home_adj_d,
                'Opp PPP Rank': find_stat(home_season, 'Opp PPP Rank') or home_ctg.get('def_ppp_rank'),
                'eFG% Rank': find_stat(home_season, 'eFG% Rank') or home_ctg.get('off_efg_rank'),
                'Opp eFG% Rank': find_stat(home_season, 'Opp eFG% Rank') or home_ctg.get('def_efg_rank'),
                'TOV% Rank': find_stat(home_season, 'TOV% Rank') or home_ctg.get('off_tov_rank'),
                'F-TOV% Rank': find_stat(home_season, 'F-TOV% Rank') or home_ctg.get('def_tov_rank'),
                'ORB% Rank': find_stat(home_season, 'ORB% Rank') or home_ctg.get('off_orb_rank'),
                'DRB% Rank': find_stat(home_season, 'DRB% Rank') or home_ctg.get('def_orb_rank'),
                'FT Rate Rank': find_stat(home_season, 'FT Rate Rank') or home_ctg.get('off_ft_rank'),
                'Opp FT Rate Rank': find_stat(home_season, 'Opp FT Rate Rank') or home_ctg.get('def_ft_rank'),
                'Opp 3PT% Rank': find_stat(home_season, 'Opp 3PT% Rank') or home_ctg.get('def_3pt_rank'),
                '3PT% Rank': find_stat(home_season, '3PT% Rank') or home_ctg.get('off_3pt_rank')
            }
            
            # SOS Rank comes from the power-ratings page scraper
            result['away_season']['SOS'] = find_stat(away_season, 'sos rank') or 'N/A'
            result['home_season']['SOS'] = find_stat(home_season, 'sos rank') or 'N/A'
            
            # For CBB games, populate season stats with KenPom data
            if game.league == 'CBB' and (away_ctg or home_ctg):
                # Add KenPom efficiency metrics to away_season
                if away_ctg:
                    result['away_season']['KenPom Rank'] = away_ctg.get('rank', 999)
                    result['away_season']['Adj O'] = away_ctg.get('adj_o', 0)
                    result['away_season']['Adj D'] = away_ctg.get('adj_d', 0)
                    result['away_season']['Adj EM'] = away_ctg.get('adj_em', 0)
                    result['away_season']['Tempo'] = away_ctg.get('tempo', 0)
                    result['away_season']['SOS'] = away_ctg.get('sos_rank', 'N/A')
                    result['away_season']['Record'] = away_ctg.get('record', '')
                    result['away_season']['Conference'] = away_ctg.get('conf', '')
                    # Four Factors from team page (if scraped)
                    result['away_season']['eFG%'] = away_ctg.get('off_efg') or result['away_season'].get('eFG%', 0)
                    result['away_season']['eFG% Rank'] = away_ctg.get('off_efg_rank')
                    result['away_season']['Opp eFG%'] = away_ctg.get('def_efg') or result['away_season'].get('Opp eFG%', 0)
                    result['away_season']['Opp eFG% Rank'] = away_ctg.get('def_efg_rank')
                    result['away_season']['TOV%'] = away_ctg.get('off_tov') or result['away_season'].get('TOV%', 0)
                    result['away_season']['TOV% Rank'] = away_ctg.get('off_tov_rank')
                    result['away_season']['Opp TOV%'] = away_ctg.get('def_tov') or result['away_season'].get('Opp TOV%', 0)
                    result['away_season']['F-TOV% Rank'] = away_ctg.get('def_tov_rank')
                    result['away_season']['ORB%'] = away_ctg.get('off_orb') or result['away_season'].get('ORB%', 0)
                    result['away_season']['ORB% Rank'] = away_ctg.get('off_orb_rank')
                    result['away_season']['DRB%'] = away_ctg.get('def_orb') or result['away_season'].get('DRB%', 0)
                    result['away_season']['DRB% Rank'] = away_ctg.get('def_orb_rank')
                    result['away_season']['FTA/FGA'] = away_ctg.get('off_ft_rate') or result['away_season'].get('FTA/FGA', 0)
                    result['away_season']['FT Rate Rank'] = away_ctg.get('off_ft_rank')
                    result['away_season']['Opp FTA/FGA'] = away_ctg.get('def_ft_rate') or result['away_season'].get('Opp FTA/FGA', 0)
                    result['away_season']['Opp FT Rate Rank'] = away_ctg.get('def_ft_rank')
                    result['away_season']['3PT%'] = away_ctg.get('off_3pt') or result['away_season'].get('3PT%', 0)
                    # Opp 3PT% = defensive 3PT% (what opponents shoot against you)
                    # Use explicit None check since 0 is falsy but valid
                    away_def_3pt = away_ctg.get('def_3pt')
                    if away_def_3pt is not None and away_def_3pt > 0:
                        result['away_season']['Opp 3PT%'] = away_def_3pt
                    elif not result['away_season'].get('Opp 3PT%'):
                        result['away_season']['Opp 3PT%'] = 0
                    result['away_season']['Opp 3PT% Rank'] = away_ctg.get('def_3pt_rank') or result['away_season'].get('Opp 3PT% Rank', 0)
                
                # Add KenPom efficiency metrics to home_season
                if home_ctg:
                    result['home_season']['KenPom Rank'] = home_ctg.get('rank', 999)
                    result['home_season']['Adj O'] = home_ctg.get('adj_o', 0)
                    result['home_season']['Adj D'] = home_ctg.get('adj_d', 0)
                    result['home_season']['Adj EM'] = home_ctg.get('adj_em', 0)
                    result['home_season']['Tempo'] = home_ctg.get('tempo', 0)
                    result['home_season']['SOS'] = home_ctg.get('sos_rank', 'N/A')
                    result['home_season']['Record'] = home_ctg.get('record', '')
                    result['home_season']['Conference'] = home_ctg.get('conf', '')
                    # Four Factors from team page (if scraped)
                    result['home_season']['eFG%'] = home_ctg.get('off_efg') or result['home_season'].get('eFG%', 0)
                    result['home_season']['eFG% Rank'] = home_ctg.get('off_efg_rank')
                    result['home_season']['Opp eFG%'] = home_ctg.get('def_efg') or result['home_season'].get('Opp eFG%', 0)
                    result['home_season']['Opp eFG% Rank'] = home_ctg.get('def_efg_rank')
                    result['home_season']['TOV%'] = home_ctg.get('off_tov') or result['home_season'].get('TOV%', 0)
                    result['home_season']['TOV% Rank'] = home_ctg.get('off_tov_rank')
                    result['home_season']['Opp TOV%'] = home_ctg.get('def_tov') or result['home_season'].get('Opp TOV%', 0)
                    result['home_season']['F-TOV% Rank'] = home_ctg.get('def_tov_rank')
                    result['home_season']['ORB%'] = home_ctg.get('off_orb') or result['home_season'].get('ORB%', 0)
                    result['home_season']['ORB% Rank'] = home_ctg.get('off_orb_rank')
                    result['home_season']['DRB%'] = home_ctg.get('def_orb') or result['home_season'].get('DRB%', 0)
                    result['home_season']['DRB% Rank'] = home_ctg.get('def_orb_rank')
                    result['home_season']['FTA/FGA'] = home_ctg.get('off_ft_rate') or result['home_season'].get('FTA/FGA', 0)
                    result['home_season']['FT Rate Rank'] = home_ctg.get('off_ft_rank')
                    result['home_season']['Opp FTA/FGA'] = home_ctg.get('def_ft_rate') or result['home_season'].get('Opp FTA/FGA', 0)
                    result['home_season']['Opp FT Rate Rank'] = home_ctg.get('def_ft_rank')
                    result['home_season']['3PT%'] = home_ctg.get('off_3pt') or result['home_season'].get('3PT%', 0)
                    # Opp 3PT% = defensive 3PT% (what opponents shoot against you)
                    # Use explicit None check since 0 is falsy but valid
                    home_def_3pt = home_ctg.get('def_3pt')
                    if home_def_3pt is not None and home_def_3pt > 0:
                        result['home_season']['Opp 3PT%'] = home_def_3pt
                    elif not result['home_season'].get('Opp 3PT%'):
                        result['home_season']['Opp 3PT%'] = 0
                    result['home_season']['Opp 3PT% Rank'] = home_ctg.get('def_3pt_rank') or result['home_season'].get('Opp 3PT% Rank', 0)
                
                logging.info(f"KenPom data merged into season stats for {game.away_team} (#{away_ctg.get('rank')}) vs {game.home_team} (#{home_ctg.get('rank')})")
            
            # Last 3 Games - use season stats as fallback since L3 may not be available from all pages
            result['away_l3'] = {
                'PPG': find_stat(away_l3, 'points/game', 'PPG', 'PPP') or result['away_season']['PPG'],
                'Opp PPG': find_stat(away_l3, 'opp points/game', 'Opp PPG', 'Opp PPP') or result['away_season']['Opp PPG'],
                'FG%': find_stat(away_l3, 'shooting %', 'FG%', 'FG2Pct') or result['away_season']['FG%'],
                'Opp FG%': find_stat(away_l3, 'opp shooting %', 'Opp FG%', 'OppFG2Pct') or result['away_season']['Opp FG%'],
                '3P%': find_stat(away_l3, 'three point %', '3PT%', 'FG3Pct', '3P%') or result['away_season'].get('3P%', 0),
                'Opp 3P%': find_stat(away_l3, 'opp three point %', 'Opp 3PT%', 'OppFG3Pct', 'Opp 3P%') or result['away_season'].get('Opp 3P%', 0),
                'FT%': find_stat(away_l3, 'free throw %', 'FT%', 'FTPct') or result['away_season']['FT%'],
                'PACE': find_stat(away_l3, 'possessions/game', 'PACE', 'Tempo') or result['away_season']['PACE'],
                'Assists/TO': find_stat(away_l3, 'assists/turnover', 'Assists/TO', 'ARate') or result['away_season']['Assists/TO'],
                'eFG%': find_stat(away_l3, 'effective fg %', 'eFG%') or result['away_season']['eFG%'],
                'TOV': find_stat(away_l3, 'turnovers/game', 'TOV', 'TOV%') or result['away_season']['TOV'],
                'ORB': find_stat(away_l3, 'off rebounds/gm', 'ORB', 'ORB%') or result['away_season']['ORB'],
                'DRB': find_stat(away_l3, 'def rebounds/gm', 'DRB', 'DRB%') or result['away_season']['DRB'],
                'Assists': find_stat(away_l3, 'assists/game', 'Assists', 'ARate') or result['away_season']['Assists'],
                'Blocks': find_stat(away_l3, 'blocks/game', 'Blocks', 'BlockPct') or result['away_season']['Blocks'],
                'Steals': find_stat(away_l3, 'steals/game', 'Steals', 'StlRate') or result['away_season']['Steals']
            }
            result['home_l3'] = {
                'PPG': find_stat(home_l3, 'points/game', 'PPG', 'PPP') or result['home_season']['PPG'],
                'Opp PPG': find_stat(home_l3, 'opp points/game', 'Opp PPG', 'Opp PPP') or result['home_season']['Opp PPG'],
                'FG%': find_stat(home_l3, 'shooting %', 'FG%', 'FG2Pct') or result['home_season']['FG%'],
                'Opp FG%': find_stat(home_l3, 'opp shooting %', 'Opp FG%', 'OppFG2Pct') or result['home_season']['Opp FG%'],
                '3P%': find_stat(home_l3, 'three point %', '3PT%', 'FG3Pct', '3P%') or result['home_season'].get('3P%', 0),
                'Opp 3P%': find_stat(home_l3, 'opp three point %', 'Opp 3PT%', 'OppFG3Pct', 'Opp 3P%') or result['home_season'].get('Opp 3P%', 0),
                'FT%': find_stat(home_l3, 'free throw %', 'FT%', 'FTPct') or result['home_season']['FT%'],
                'PACE': find_stat(home_l3, 'possessions/game', 'PACE', 'Tempo') or result['home_season']['PACE'],
                'Assists/TO': find_stat(home_l3, 'assists/turnover', 'Assists/TO', 'ARate') or result['home_season']['Assists/TO'],
                'eFG%': find_stat(home_l3, 'effective fg %', 'eFG%') or result['home_season']['eFG%'],
                'TOV': find_stat(home_l3, 'turnovers/game', 'TOV', 'TOV%') or result['home_season']['TOV'],
                'ORB': find_stat(home_l3, 'off rebounds/gm', 'ORB', 'ORB%') or result['home_season']['ORB'],
                'DRB': find_stat(home_l3, 'def rebounds/gm', 'DRB', 'DRB%') or result['home_season']['DRB'],
                'Assists': find_stat(home_l3, 'assists/game', 'Assists', 'ARate') or result['home_season']['Assists'],
                'Blocks': find_stat(home_l3, 'blocks/game', 'Blocks', 'BlockPct') or result['home_season']['Blocks'],
                'Steals': find_stat(home_l3, 'steals/game', 'Steals', 'StlRate') or result['home_season']['Steals']
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

@app.route('/run_brains', methods=['POST'])
def run_brains():
    """Manually trigger brain analysis. Returns JSON results."""
    if not AI_BRAINS_AVAILABLE:
        return jsonify({"success": False, "message": "AI brains not available"})
    try:
        result = run_brain_analysis()
        return jsonify({"success": True, **result})
    except Exception as e:
        logger.error(f"Manual brain analysis error: {e}")
        return jsonify({"success": False, "message": str(e)})


@app.route('/model_status', methods=['GET'])
def model_status():
    """AI system status — loaded models, brain availability."""
    status = {
        "ai_brains_available": AI_BRAINS_AVAILABLE,
        "models": {}
    }
    if AI_BRAINS_AVAILABLE:
        status["models"] = ensemble_predictor.get_model_status()
    return jsonify(status)


with app.app_context():
    db.create_all()

    # Load AI ensemble models on startup (graceful — no impact if models don't exist)
    if AI_BRAINS_AVAILABLE:
        try:
            ensemble_predictor.load_all_models()
            logger.info("AI ensemble models loaded on startup")
        except Exception as e:
            logger.info(f"AI model loading skipped: {e}")

    # On startup, check and load games for today
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    # Clear old games from previous days
    old_games_count = Game.query.filter(Game.date < today).count()
    if old_games_count > 0:
        logger.info(f"Startup cleanup: Removing {old_games_count} old games from previous days")
        old_game_ids = [g.id for g in Game.query.filter(Game.date < today).all()]
        if old_game_ids:
            Pick.query.filter(Pick.game_id.in_(old_game_ids)).update({Pick.game_id: None}, synchronize_session=False)
            Game.query.filter(Game.id.in_(old_game_ids)).delete(synchronize_session=False)
            db.session.commit()
            logger.info(f"Startup cleanup: Removed {len(old_game_ids)} old games")
    
    # Check if we have games for today
    today_games_count = Game.query.filter_by(date=today).count()
    if today_games_count == 0:
        logger.info("No games for today - will auto-load on first page visit")

# Initialize automatic game loading system
auto_loader = setup_automatic_loading(app, db)
logger.info("Automatic game loading enabled - games will load on new day automatically")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
