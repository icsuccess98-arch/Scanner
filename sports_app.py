import os
import logging
import time
from datetime import datetime, date, timedelta
from typing import Tuple, Optional
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase, validates
import requests
import pytz

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

def fetch_team_injuries(team_name: str, league: str) -> dict:
    """
    Fetch injury data from ESPN for key player availability.
    Returns: {"has_key_injuries": bool, "injured_starters": int, "details": list}
    """
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
            return {"has_key_injuries": False, "injured_starters": 0, "details": []}
        
        team_id = get_espn_team_id(team_name, league)
        if not team_id:
            return {"has_key_injuries": False, "injured_starters": 0, "details": []}
        
        url = f"https://sports.core.api.espn.com/v2/sports/{sport}/teams/{team_id}/injuries?limit=50"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return {"has_key_injuries": False, "injured_starters": 0, "details": []}
        
        data = resp.json()
        items = data.get("items", [])
        
        injured_starters = 0
        injury_details = []
        
        for item in items:
            if "$ref" in item:
                try:
                    injury_resp = requests.get(item["$ref"], timeout=5)
                    if injury_resp.status_code == 200:
                        injury_data = injury_resp.json()
                        status = injury_data.get("status", "").lower()
                        if status in ["out", "doubtful"]:
                            injured_starters += 1
                            injury_details.append({
                                "status": status,
                                "type": injury_data.get("type", {}).get("description", "Unknown")
                            })
                except:
                    continue
        
        result = {
            "has_key_injuries": injured_starters >= 2,
            "injured_starters": injured_starters,
            "details": injury_details[:5]
        }
        injury_cache[cache_key] = result
        return result
    except Exception as e:
        logger.debug(f"Injury fetch error for {team_name}: {e}")
        return {"has_key_injuries": False, "injured_starters": 0, "details": []}

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
    - Spread line from home perspective (negative = home favored)
    - Line moves MORE NEGATIVE (e.g., -7 → -9) = sharps on HOME
    - Line moves MORE POSITIVE (e.g., -7 → -5) = sharps on AWAY
    
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
    
    # SPREADS: line is from home perspective (negative = home favored)
    # Movement negative = line going more toward home (sharps on home)
    # Movement positive = line going more toward away (sharps on away)
    elif direction == "HOME":
        if movement <= -1.5:  # Line moved -7 → -9, sharps on HOME
            return {"sharp_aligned": True, "sharp_against": False, "movement": movement, "signal": "SHARP_AGREES"}
        elif movement >= 1.5:  # Line moved -7 → -5, sharps on AWAY
            return {"sharp_aligned": False, "sharp_against": True, "movement": movement, "signal": "SHARP_DISAGREES"}
    elif direction == "AWAY":
        if movement >= 1.5:  # Line moved -7 → -5, sharps on AWAY
            return {"sharp_aligned": True, "sharp_against": False, "movement": movement, "signal": "SHARP_AGREES"}
        elif movement <= -1.5:  # Line moved -7 → -9, sharps on HOME
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
    # Historical percentages (last 10 games)
    away_ou_pct = db.Column(db.Float)  # Away team's O/U hit rate
    home_ou_pct = db.Column(db.Float)  # Home team's O/U hit rate
    away_spread_pct = db.Column(db.Float)  # Away team's spread cover rate
    home_spread_pct = db.Column(db.Float)  # Home team's spread cover rate
    h2h_ou_pct = db.Column(db.Float)  # Head-to-head O/U hit rate
    h2h_spread_pct = db.Column(db.Float)  # Head-to-head spread cover rate
    history_qualified = db.Column(db.Boolean, default=None)  # NULL = not checked, True/False = checked
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
    
    Spread line is from home team perspective (negative = home favored)
    E.g., Home -13 means home is favored by 13, must win by 14+ to cover
    
    Convert to home margin frame:
    line_margin = -spread_line (what the line implies home margin should be)
    E.g., spread -13 -> line_margin = 13 (line says home wins by 13)
    
    projected_margin = expected_home - expected_away (positive = home wins by X)
    
    Direction Rules:
    - Take HOME if: projected_margin >= line_margin + threshold (we think home wins by MORE than the line)
    - Take AWAY if: projected_margin <= line_margin - threshold (we think home wins by LESS than the line)
    
    Example: Home expected to win by 3, spread is Home -13
    line_margin = 13, projected_margin = 3
    We expect home to win by 3, but line says home wins by 13
    Difference = 10 points of value on AWAY side
    projected_margin (3) <= line_margin (13) - threshold (8) = 5? Yes!
    Bet AWAY +13 (they lose by 3 but cover +13)
    """
    threshold = THRESHOLDS.get(league, 8.0)
    projected_margin = expected_home - expected_away
    line_margin = -spread_line
    edge = abs(projected_margin - line_margin)
    
    if projected_margin >= line_margin + threshold:
        return True, "HOME", edge
    elif projected_margin <= line_margin - threshold:
        return True, "AWAY", edge
    return False, None, edge

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

def calculate_ou_hit_rate(games: list, direction: str, threshold: float = 0) -> float:
    """
    Calculate what percentage of games would have hit the O/U.
    For now, uses average total as the "line" proxy since we don't have historical lines.
    """
    if len(games) < 5:
        return 0.0
    
    totals = [g["total"] for g in games]
    avg_total = sum(totals) / len(totals)
    
    hits = 0
    for g in games:
        if direction == "O" and g["total"] > avg_total:
            hits += 1
        elif direction == "U" and g["total"] < avg_total:
            hits += 1
    
    return (hits / len(games)) * 100

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

def calculate_spread_cover_rate(games: list, spread_direction: str = None, avg_spread: float = None) -> float:
    """
    Calculate what percentage of games the team would have covered the spread.
    Uses average margin as proxy for typical spread, then checks if team beat that margin.
    
    For underdogs (getting points): Cover if loss is within spread or win
    For favorites (giving points): Cover if win margin exceeds spread
    """
    if len(games) < 5:
        return 0.0
    
    margins = [g["margin"] for g in games]
    avg_margin = sum(margins) / len(margins)
    
    covers = 0
    for g in games:
        if avg_margin < 0:
            if g["margin"] >= avg_margin:
                covers += 1
        else:
            if g["margin"] >= avg_margin:
                covers += 1
    
    return (covers / len(games)) * 100

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
    
    For spreads, we use average margin as a proxy:
    - If picking HOME favorite: Home avg margin should exceed 85% of spread
    - If picking AWAY underdog: Must have positive avg margin (winning on average)
    """
    try:
        away_games = fetch_team_last_10_games(game.away_team, game.league)
        home_games = fetch_team_last_10_games(game.home_team, game.league)
        
        if len(away_games) < 5 or len(home_games) < 5:
            logger.info(f"Insufficient history for {game.away_team} @ {game.home_team}: {len(away_games)}/{len(home_games)} games")
            game.history_qualified = False
            return False
        
        direction = game.direction or "O"
        game.away_ou_pct = calculate_ou_hit_rate(away_games, direction)
        game.home_ou_pct = calculate_ou_hit_rate(home_games, direction)
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
        totals_qualified = max_ou_pct >= 60
        
        if h2h_games >= 3:
            h2h_qualified = (game.h2h_ou_pct or 0) >= 60
            totals_qualified = totals_qualified and h2h_qualified
        
        if totals_qualified and injury_concern:
            logger.info(f"{game.away_team} @ {game.home_team}: Totals DISQUALIFIED due to injury concern")
            totals_qualified = False
        
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
        
        spread_qualified = False
        if game.spread_is_qualified and game.spread_line is not None:
            spread_line = game.spread_line
            
            if game.spread_direction == "HOME":
                spread_qualified = home_avg_margin >= abs(spread_line) * 0.85
                
                if spread_qualified and home_form_trending == "DOWN":
                    logger.info(f"{game.away_team} @ {game.home_team}: HOME spread marginal due to declining form")
                    if home_recent_margin < abs(spread_line) * 0.5:
                        spread_qualified = False
                        logger.info(f"{game.away_team} @ {game.home_team}: HOME spread DISQUALIFIED - recent form too weak")
                        
            elif game.spread_direction == "AWAY":
                spread_qualified = away_avg_margin > 0
                
                if spread_qualified and away_form_trending == "DOWN":
                    logger.info(f"{game.away_team} @ {game.home_team}: AWAY spread marginal due to declining form")
                    if away_recent_margin < 0:
                        spread_qualified = False
                        logger.info(f"{game.away_team} @ {game.home_team}: AWAY spread DISQUALIFIED - recent form shows losses")
            else:
                spread_qualified = True
            
            if spread_qualified and injury_concern:
                logger.info(f"{game.away_team} @ {game.home_team}: Spread DISQUALIFIED due to injury concern")
                spread_qualified = False
            
            if spread_qualified and spread_sharp_against:
                logger.info(f"{game.away_team} @ {game.home_team}: Spread DISQUALIFIED due to sharp money against")
                spread_qualified = False
            
            logger.info(f"{game.away_team} @ {game.home_team}: Margins Away={away_avg_margin:.1f}(recent:{away_recent_margin:.1f})/Home={home_avg_margin:.1f}(recent:{home_recent_margin:.1f}), Spread={spread_line}, spread_qualified={spread_qualified}")
        
        game.history_qualified = totals_qualified or spread_qualified
        
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
        Game.query.filter(Game.id.in_(old_game_ids)).delete(synchronize_session=False)
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
    
    # TOTALS: Must pass historical threshold (if history fetched)
    # If history_qualified is None (not checked), do NOT show the game
    # If history_qualified is True, show the game
    # If history_qualified is False, hide the game
    qualified = [g for g in edge_qualified if g.history_qualified == True]
    
    # SPREADS: Also require history_qualified (margin validation done during fetch)
    # Must have history_qualified = True to be shown
    spread_qualified = [g for g in edge_spread_qualified if g.history_qualified == True]
    spread_qualified.sort(key=lambda x: x.spread_edge or 0, reverse=True)
    
    # SUPERMAX = single best edge across both totals and spreads
    supermax_lock = None
    supermax_type = None  # 'totals' or 'spread'
    supermax_edge = 0
    
    # Check best totals pick
    if qualified:
        best_totals = qualified[0]
        if best_totals.edge and best_totals.edge > supermax_edge:
            supermax_lock = best_totals
            supermax_type = 'totals'
            supermax_edge = best_totals.edge
    
    # Check best spread pick
    if spread_qualified:
        best_spread = spread_qualified[0]
        if best_spread.spread_edge and best_spread.spread_edge > supermax_edge:
            supermax_lock = best_spread
            supermax_type = 'spread'
            supermax_edge = best_spread.spread_edge
    
    # Combined qualified: games that qualify for EITHER totals OR spreads AND pass historical threshold
    all_qualified_games = [g for g in all_games if 
        ((g.is_qualified or g.spread_is_qualified) and 
         g.history_qualified == True)]
    
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
        league_totals_qualified = [g for g in league_games if g.is_qualified and g.history_qualified == True]
        league_spread_qualified = [g for g in league_games if g.spread_is_qualified and g.history_qualified == True]
        league_any_qualified = [g for g in league_games if 
            (g.is_qualified or g.spread_is_qualified) and g.history_qualified == True]
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
        # alt_spread_line is already the picked team's perspective
        # spread_line is home-team perspective (need to flip for away picks)
        if g.alt_spread_line:
            display_line = g.alt_spread_line if g.spread_direction == 'HOME' else g.alt_spread_line
            vegas_line = g.spread_line if g.spread_direction == 'HOME' else -g.spread_line
        elif g.spread_line:
            display_line = g.spread_line if g.spread_direction == 'HOME' else -g.spread_line
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
                        
                        # Extract moneylines from h2h for spread validation
                        away_ml = None
                        home_ml = None
                        if "h2h" in bovada_markets:
                            h2h_market = bovada_markets["h2h"]
                            h2h_outcomes = h2h_market.get("outcomes", [])
                            for h2h_out in h2h_outcomes:
                                if teams_match(h2h_out.get("name", ""), away_team):
                                    away_ml = h2h_out.get("price")
                                elif teams_match(h2h_out.get("name", ""), home_team):
                                    home_ml = h2h_out.get("price")
                        
                        # Process SPREADS with validation
                        if "spreads" in bovada_markets:
                            spreads_market = bovada_markets["spreads"]
                            outcomes = spreads_market.get("outcomes", [])
                            for outcome in outcomes:
                                if teams_match(outcome.get("name", ""), home_team):
                                    spread_line = outcome.get("point")
                                    home_spread_odds = outcome.get("price")
                                    if spread_line is not None:
                                        # Validate spread sign against moneylines (auto-correct if needed)
                                        if away_ml and home_ml:
                                            spread_line, was_corrected = SpreadValidator.validate_and_correct_spread(
                                                spread_line, away_ml, home_ml,
                                                game.away_team, game.home_team
                                            )
                                        game.spread_line = spread_line
                                        # Store opening spread line for CLV tracking
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
                                            
                                            # Get odds based on spread direction
                                            bovada_spread_odds = None
                                            pinnacle_spread_odds = None
                                            if spread_dir == "HOME":
                                                bovada_spread_odds = home_spread_odds
                                            elif spread_dir == "AWAY":
                                                away_outcome = next((o for o in outcomes if teams_match(o.get("name", ""), away_team)), None)
                                                bovada_spread_odds = away_outcome.get("price") if away_outcome else None
                                            
                                            # Get Pinnacle odds for same direction
                                            if pinnacle_markets.get("spreads") and spread_dir:
                                                pinn_outcomes = pinnacle_markets["spreads"].get("outcomes", [])
                                                pick_team = home_team if spread_dir == "HOME" else away_team
                                                pinn_outcome = next((o for o in pinn_outcomes if teams_match(o.get("name", ""), pick_team)), None)
                                                if pinn_outcome:
                                                    pinnacle_spread_odds = pinn_outcome.get("price")
                                            
                                            # Store and calculate EV
                                            game.bovada_spread_odds = bovada_spread_odds
                                            game.pinnacle_spread_odds = pinnacle_spread_odds
                                            if bovada_spread_odds and pinnacle_spread_odds:
                                                game.spread_ev = calculate_ev(bovada_spread_odds, pinnacle_spread_odds)
                                            
                                            # EV is informational only - Bovada is primary
                                            # Qualification based on edge threshold only
                                            game.spread_is_qualified = spread_qual
                                                
                                        spreads_updated += 1
                                    break
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
    """Internal function to fetch historical data for qualified games."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    games = Game.query.filter_by(date=today).filter(
        db.or_(Game.is_qualified == True, Game.spread_is_qualified == True)
    ).all()
    
    history_updated = 0
    history_qualified = 0
    
    for game in games:
        try:
            if update_game_historical_data(game):
                history_qualified += 1
            history_updated += 1
        except Exception as e:
            logger.error(f"Error updating history for {game.away_team} @ {game.home_team}: {e}")
    
    db.session.commit()
    
    return {
        "games_checked": history_updated,
        "history_qualified": history_qualified
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
            if r['alt_spread']:
                game.alt_spread_line, game.alt_spread_odds = r['alt_spread']
                alt_lines_found += 1
    
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
    
    # Only get games that pass both edge threshold AND historical qualification
    all_qualified = Game.query.filter_by(date=today, is_qualified=True, history_qualified=True).order_by(Game.edge.desc()).all()
    games = [g for g in all_qualified if not (g.game_time and 'final' in g.game_time.lower())]
    
    spread_qualified = Game.query.filter_by(date=today, spread_is_qualified=True, history_qualified=True).order_by(Game.spread_edge.desc()).all()
    spread_games = [g for g in spread_qualified if not (g.game_time and 'final' in g.game_time.lower())]
    
    if not games and not spread_games:
        return jsonify({"success": False, "message": "No qualified picks to post"})
    
    # Build combined picks list sorted by edge with away favorite priority
    combined = []
    for g in games:
        line_val = g.alt_total_line if g.alt_total_line else g.line
        # Away team is favorite when spread_line > 0 (home is underdog)
        away_favorite = (g.spread_line or 0) > 0
        combined.append({
            'game': g,
            'edge': g.edge or 0,
            'pick_type': 'total',
            'pick_str': f"{g.direction}{line_val}",
            'line_val': line_val,
            'away_favorite': away_favorite
        })
    for g in spread_games:
        if g.spread_direction == 'HOME':
            spread_val = g.alt_spread_line if g.alt_spread_line else g.spread_line
            pick_str = f"{g.home_team} {spread_val:+.1f}" if spread_val else g.home_team
        else:
            spread_val = abs(g.alt_spread_line) if g.alt_spread_line else abs(g.spread_line) if g.spread_line else 0
            pick_str = f"{g.away_team} +{spread_val:.1f}" if spread_val else g.away_team
        away_favorite = (g.spread_line or 0) > 0
        combined.append({
            'game': g,
            'edge': g.spread_edge or 0,
            'pick_type': 'spread',
            'pick_str': pick_str,
            'line_val': spread_val,
            'away_favorite': away_favorite
        })
    # Sort by: 1) edge (desc), 2) away_favorite priority (True first)
    combined.sort(key=lambda x: (x['edge'], 1 if x['away_favorite'] else 0), reverse=True)
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
        
        # Only save the Supermax/Lock of the Day to history (not all picks)
        sm_game = supermax['game']
        matchup = f"{sm_game.away_team} @ {sm_game.home_team}"
        
        # Check if this supermax already saved today
        existing_pick = Pick.query.filter_by(date=today, matchup=matchup, pick_type=supermax['pick_type']).first()
        if not existing_pick:
            if supermax['pick_type'] == 'total':
                line_val = sm_game.alt_total_line if sm_game.alt_total_line else sm_game.line
                pick_str = f"{sm_game.direction}{line_val}"
                edge_val = sm_game.edge
            else:
                if sm_game.spread_direction == 'HOME':
                    line_val = sm_game.alt_spread_line if sm_game.alt_spread_line else sm_game.spread_line
                    pick_str = f"{sm_game.home_team} {line_val:+.1f}" if line_val else sm_game.home_team
                else:
                    line_val = sm_game.alt_spread_line if sm_game.alt_spread_line else -sm_game.spread_line if sm_game.spread_line else None
                    pick_str = f"{sm_game.away_team} {line_val:+.1f}" if line_val else sm_game.away_team
                edge_val = sm_game.spread_edge
            
            game_start_dt = parse_game_time_to_datetime(sm_game.game_time, today)
            if game_start_dt and game_start_dt.tzinfo:
                game_start_dt = game_start_dt.replace(tzinfo=None)
            pick = Pick(
                game_id=sm_game.id,
                date=today,
                league=sm_game.league,
                matchup=matchup,
                pick=pick_str,
                edge=edge_val,
                is_lock=True,
                posted_to_discord=True,
                pick_type=supermax['pick_type'],
                line_value=line_val,
                game_start=game_start_dt
            )
            db.session.add(pick)
            db.session.commit()
        
        return jsonify({"success": True, "status": status_code, "picks_count": 1})
    
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
    
    # Get qualified games for this window (must pass historical qualification)
    all_qualified = Game.query.filter_by(date=today, is_qualified=True, history_qualified=True).order_by(Game.edge.desc()).all()
    spread_qualified = Game.query.filter_by(date=today, spread_is_qualified=True, history_qualified=True).order_by(Game.spread_edge.desc()).all()
    
    # Filter by window and exclude finished games
    window_games = [g for g in all_qualified if get_game_window(g.game_time) == window 
                    and not (g.game_time and 'final' in g.game_time.lower())]
    window_spreads = [g for g in spread_qualified if get_game_window(g.game_time) == window
                      and not (g.game_time and 'final' in g.game_time.lower())]
    
    if not window_games and not window_spreads:
        return jsonify({"success": False, "message": f"No qualified picks for {window} window"})
    
    # Build combined picks and find best (with away favorite priority)
    combined = []
    for g in window_games:
        line_val = g.alt_total_line if g.alt_total_line else g.line
        away_favorite = (g.spread_line or 0) > 0
        combined.append({'game': g, 'edge': g.edge or 0, 'pick_type': 'total', 'line_val': line_val, 'away_favorite': away_favorite})
    for g in window_spreads:
        spread_val = g.alt_spread_line if g.alt_spread_line else abs(g.spread_line) if g.spread_line else 0
        away_favorite = (g.spread_line or 0) > 0
        combined.append({'game': g, 'edge': g.spread_edge or 0, 'pick_type': 'spread', 'line_val': spread_val, 'away_favorite': away_favorite})
    
    # Sort by: 1) edge (desc), 2) away_favorite priority (True first)
    combined.sort(key=lambda x: (x['edge'], 1 if x['away_favorite'] else 0), reverse=True)
    supermax = combined[0]
    sm_game = supermax['game']
    
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
    
    all_picks = Pick.query.filter_by(is_lock=True).order_by(Pick.date.desc(), Pick.edge.desc()).all()
    
    # Separate upcoming (pending, game not started) from resulted picks
    upcoming_picks = []
    past_picks = []
    
    for p in all_picks:
        if p.result is None:
            # Check if game has started yet
            if p.game_start:
                game_start_et = p.game_start.replace(tzinfo=pytz.UTC).astimezone(et) if p.game_start.tzinfo else et.localize(p.game_start)
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
        Game.query.filter(Game.id.in_(game_ids)).delete(synchronize_session=False)

@app.route('/clear_games', methods=['POST'])
def clear_games():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    game_ids = [g.id for g in Game.query.filter_by(date=today).all()]
    safe_delete_games(game_ids)
    db.session.commit()
    return redirect(url_for('dashboard'))

with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
