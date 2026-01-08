import os
import logging
import time
from datetime import datetime, date
from typing import Tuple, Optional
from functools import wraps
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
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or "sports-model-secret-key"
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
db.init_app(app)

last_game_count = {}

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
    posted_to_discord = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    pick_type = db.Column(db.String(10), default="total")
    line_value = db.Column(db.Float)
    
    __table_args__ = (
        db.Index('idx_pick_result', 'result'),
        db.Index('idx_pick_date_league', 'date', 'league'),
        db.Index('idx_pick_type', 'pick_type'),
    )

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
    
    old_game_ids = [g.id for g in Game.query.filter(Game.date < today).all()]
    if old_game_ids:
        Pick.query.filter(Pick.game_id.in_(old_game_ids)).update({Pick.game_id: None}, synchronize_session=False)
        Game.query.filter(Game.id.in_(old_game_ids)).delete(synchronize_session=False)
        db.session.commit()
    
    all_games_db = Game.query.filter_by(date=today).order_by(Game.edge.desc()).all()
    all_games = [g for g in all_games_db if is_game_upcoming(g)]
    qualified = [g for g in all_games if g.is_qualified]
    lock = qualified[0] if qualified else None
    
    spread_qualified = [g for g in all_games if g.spread_is_qualified]
    spread_qualified.sort(key=lambda x: x.spread_edge or 0, reverse=True)
    spread_lock = spread_qualified[0] if spread_qualified else None
    
    if show_only_qualified:
        games = qualified
    else:
        games = all_games
    
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
        'spread_best_edge': spread_lock.spread_edge if spread_lock else 0,
        'spread_home_count': len([g for g in spread_qualified if g.spread_direction == 'HOME']),
        'spread_away_count': len([g for g in spread_qualified if g.spread_direction == 'AWAY']),
        'spread_top_picks': spread_qualified[:5]
    }
    
    for league in ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']:
        league_games = [g for g in all_games if g.league == league]
        league_qualified = [g for g in league_games if g.is_qualified]
        analytics['league_breakdown'][league] = {
            'total': len(league_games),
            'qualified': len(league_qualified)
        }
    
    edge_sum = 0
    for g in qualified:
        if g.edge:
            edge_sum += g.edge
            if g.edge > analytics['best_edge']:
                analytics['best_edge'] = g.edge
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
    
    if qualified:
        analytics['avg_edge'] = edge_sum / len(qualified)
        analytics['top_picks'] = qualified[:5]
    
    global last_game_count
    last_game_count['count'] = len(all_games)
    last_game_count['qualified'] = len(qualified)
    
    return render_template('dashboard.html', games=games, qualified=qualified, lock=lock, 
                          today=today, thresholds=THRESHOLDS, total_games=len(all_games),
                          show_only_qualified=show_only_qualified, analytics=analytics,
                          spread_qualified=spread_qualified, spread_lock=spread_lock)

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
    
    return jsonify({"live_scores": live_scores, "count": len(live_scores)})

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

def fetch_cbb_team_stats(team_id):
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}"
        resp = requests.get(url, timeout=10)
        items = resp.json().get("team", {}).get("record", {}).get("items", [])
        ppg = opp_ppg = None
        for item in items:
            if item.get("type") == "total":
                for stat in item.get("stats", []):
                    if stat.get("name") == "avgPointsFor": ppg = stat.get("value")
                    if stat.get("name") == "avgPointsAgainst": opp_ppg = stat.get("value")
        return ppg, opp_ppg
    except Exception:
        return None, None

def fetch_cfb_team_stats(team_id):
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}"
        resp = requests.get(url, timeout=10)
        items = resp.json().get("team", {}).get("record", {}).get("items", [])
        ppg = opp_ppg = None
        for item in items:
            if item.get("type") == "total":
                for stat in item.get("stats", []):
                    if stat.get("name") == "avgPointsFor": ppg = stat.get("value")
                    if stat.get("name") == "avgPointsAgainst": opp_ppg = stat.get("value")
        return ppg, opp_ppg
    except Exception:
        return None, None

def fetch_nfl_team_stats(team_id):
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}"
        resp = requests.get(url, timeout=10)
        items = resp.json().get("team", {}).get("record", {}).get("items", [])
        ppg = opp_ppg = None
        for item in items:
            if item.get("type") == "total":
                for stat in item.get("stats", []):
                    if stat.get("name") == "avgPointsFor": ppg = stat.get("value")
                    if stat.get("name") == "avgPointsAgainst": opp_ppg = stat.get("value")
        return ppg, opp_ppg
    except Exception:
        return None, None

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

@app.route('/fetch_games', methods=['POST'])
def fetch_games():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%Y%m%d")
    
    nba_stats = get_nba_stats()
    nhl_stats = get_nhl_stats()
    games_added = 0
    leagues_cleared = []
    
    try:
        Game.query.filter_by(date=today, league="NBA").delete()
        leagues_cleared.append("NBA")
        
        nba_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today_str}"
        resp = requests.get(nba_url, timeout=30)
        events = resp.json().get("events", [])
        nba_today_count = 0
        
        for event in events:
            if not validate_espn_event_date(event, today, et):
                continue
            nba_today_count += 1
            
            comps = event.get("competitions", [{}])[0]
            teams = comps.get("competitors", [])
            if len(teams) == 2:
                away = next((t for t in teams if t.get("homeAway") == "away"), None)
                home = next((t for t in teams if t.get("homeAway") == "home"), None)
                if away and home:
                    away_name = away.get("team", {}).get("shortDisplayName", "")
                    home_name = home.get("team", {}).get("shortDisplayName", "")
                    game_time = event.get("status", {}).get("type", {}).get("shortDetail", "")
                    
                    away_s = find_team_stats(away_name, nba_stats)
                    home_s = find_team_stats(home_name, nba_stats)
                    
                    game = Game(
                        date=today, league="NBA", away_team=away_name, home_team=home_name,
                        game_time=game_time,
                        away_ppg=away_s["ppg"] if away_s else None,
                        away_opp_ppg=away_s["opp_ppg"] if away_s else None,
                        home_ppg=home_s["ppg"] if home_s else None,
                        home_opp_ppg=home_s["opp_ppg"] if home_s else None
                    )
                    db.session.add(game)
                    games_added += 1
        
        if nba_today_count == 0:
            print(f"NBA: No games found for today ({today})")
    except Exception as e:
        print(f"NBA games error: {e}")
    
    try:
        Game.query.filter_by(date=today, league="NHL").delete()
        leagues_cleared.append("NHL")
        
        nhl_url = f"https://api-web.nhle.com/v1/schedule/{today.strftime('%Y-%m-%d')}"
        resp = requests.get(nhl_url, timeout=30)
        game_weeks = resp.json().get("gameWeek", [])
        nhl_today_count = 0
        
        for gw in game_weeks:
            if gw.get("date") == today.strftime("%Y-%m-%d"):
                for game_data in gw.get("games", []):
                    away_name = game_data.get("awayTeam", {}).get("placeName", {}).get("default", "")
                    home_name = game_data.get("homeTeam", {}).get("placeName", {}).get("default", "")
                    start_time = game_data.get("startTimeUTC", "")
                    
                    if away_name and home_name:
                        nhl_today_count += 1
                        away_s = find_team_stats(away_name, nhl_stats)
                        home_s = find_team_stats(home_name, nhl_stats)
                        
                        nhl_game_time = ""
                        if start_time:
                            try:
                                utc_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                                et_dt = utc_dt.astimezone(et)
                                nhl_game_time = et_dt.strftime("%-m/%-d - %-I:%M %p EST")
                            except Exception:
                                nhl_game_time = start_time[:10]
                        
                        game = Game(
                            date=today, league="NHL", away_team=away_name, home_team=home_name,
                            game_time=nhl_game_time,
                            away_ppg=away_s["ppg"] if away_s else None,
                            away_opp_ppg=away_s["opp_ppg"] if away_s else None,
                            home_ppg=home_s["ppg"] if home_s else None,
                            home_opp_ppg=home_s["opp_ppg"] if home_s else None
                        )
                        db.session.add(game)
                        games_added += 1
        
        if nhl_today_count == 0:
            print(f"NHL: No games found for today ({today})")
    except Exception as e:
        print(f"NHL games error: {e}")
    
    try:
        Game.query.filter_by(date=today, league="CBB").delete()
        leagues_cleared.append("CBB")
        
        cbb_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?dates={today_str}&limit=500&groups=50"
        resp = requests.get(cbb_url, timeout=60)
        events = resp.json().get("events", [])
        cbb_today_count = 0
        
        for event in events:
            if not validate_espn_event_date(event, today, et):
                continue
            cbb_today_count += 1
            
            comps = event.get("competitions", [{}])[0]
            teams = comps.get("competitors", [])
            if len(teams) == 2:
                away = next((t for t in teams if t.get("homeAway") == "away"), None)
                home = next((t for t in teams if t.get("homeAway") == "home"), None)
                if away and home:
                    away_name = away.get("team", {}).get("shortDisplayName", "")
                    home_name = home.get("team", {}).get("shortDisplayName", "")
                    away_id = away.get("team", {}).get("id")
                    home_id = home.get("team", {}).get("id")
                    game_time = event.get("status", {}).get("type", {}).get("shortDetail", "")
                    
                    away_ppg, away_opp = fetch_cbb_team_stats(away_id)
                    home_ppg, home_opp = fetch_cbb_team_stats(home_id)
                    
                    game = Game(
                        date=today, league="CBB", away_team=away_name, home_team=home_name,
                        game_time=game_time,
                        away_ppg=away_ppg, away_opp_ppg=away_opp,
                        home_ppg=home_ppg, home_opp_ppg=home_opp
                    )
                    db.session.add(game)
                    games_added += 1
        
        if cbb_today_count == 0:
            print(f"CBB: No games found for today ({today})")
    except Exception as e:
        print(f"CBB games error: {e}")
    
    try:
        Game.query.filter_by(date=today, league="CFB").delete()
        leagues_cleared.append("CFB")
        
        cfb_url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates={today_str}&limit=100"
        resp = requests.get(cfb_url, timeout=30)
        events = resp.json().get("events", [])
        cfb_today_count = 0
        
        for event in events:
            if not validate_espn_event_date(event, today, et):
                continue
            cfb_today_count += 1
            
            comps = event.get("competitions", [{}])[0]
            teams = comps.get("competitors", [])
            if len(teams) == 2:
                away = next((t for t in teams if t.get("homeAway") == "away"), None)
                home = next((t for t in teams if t.get("homeAway") == "home"), None)
                if away and home:
                    away_name = away.get("team", {}).get("shortDisplayName", "")
                    home_name = home.get("team", {}).get("shortDisplayName", "")
                    away_id = away.get("team", {}).get("id")
                    home_id = home.get("team", {}).get("id")
                    game_time = event.get("status", {}).get("type", {}).get("shortDetail", "")
                    
                    away_ppg, away_opp = fetch_cfb_team_stats(away_id)
                    home_ppg, home_opp = fetch_cfb_team_stats(home_id)
                    
                    game = Game(
                        date=today, league="CFB", away_team=away_name, home_team=home_name,
                        game_time=game_time,
                        away_ppg=away_ppg, away_opp_ppg=away_opp,
                        home_ppg=home_ppg, home_opp_ppg=home_opp
                    )
                    db.session.add(game)
                    games_added += 1
        
        if cfb_today_count == 0:
            print(f"CFB: No games found for today ({today})")
    except Exception as e:
        print(f"CFB games error: {e}")
    
    try:
        Game.query.filter_by(date=today, league="NFL").delete()
        leagues_cleared.append("NFL")
        
        nfl_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates={today_str}"
        resp = requests.get(nfl_url, timeout=30)
        events = resp.json().get("events", [])
        nfl_today_count = 0
        
        for event in events:
            if not validate_espn_event_date(event, today, et):
                continue
            nfl_today_count += 1
            
            comps = event.get("competitions", [{}])[0]
            teams = comps.get("competitors", [])
            if len(teams) == 2:
                away = next((t for t in teams if t.get("homeAway") == "away"), None)
                home = next((t for t in teams if t.get("homeAway") == "home"), None)
                if away and home:
                    away_name = away.get("team", {}).get("shortDisplayName", "")
                    home_name = home.get("team", {}).get("shortDisplayName", "")
                    away_id = away.get("team", {}).get("id")
                    home_id = home.get("team", {}).get("id")
                    game_time = event.get("status", {}).get("type", {}).get("shortDetail", "")
                    
                    away_ppg, away_opp = fetch_nfl_team_stats(away_id)
                    home_ppg, home_opp = fetch_nfl_team_stats(home_id)
                    
                    game = Game(
                        date=today, league="NFL", away_team=away_name, home_team=home_name,
                        game_time=game_time,
                        away_ppg=away_ppg, away_opp_ppg=away_opp,
                        home_ppg=home_ppg, home_opp_ppg=home_opp
                    )
                    db.session.add(game)
                    games_added += 1
        
        if nfl_today_count == 0:
            print(f"NFL: No games found for today ({today})")
    except Exception as e:
        print(f"NFL games error: {e}")
    
    db.session.commit()
    print(f"Fetch complete: {games_added} games added, leagues cleared: {leagues_cleared}")
    return jsonify({"success": True, "games_added": games_added, "leagues_cleared": leagues_cleared})

@app.route('/fetch_stats', methods=['POST'])
def fetch_stats():
    nba_stats = get_nba_stats()
    nhl_stats = get_nhl_stats()
    return jsonify({"success": True, "counts": {"nba": len(nba_stats), "nhl": len(nhl_stats)}})

@app.route('/fetch_odds', methods=['POST'])
def fetch_odds():
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        return jsonify({"success": False, "message": "ODDS_API_KEY not configured. Get a free key at the-odds-api.com"})
    
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
    
    for league, sport_key in sport_map.items():
        try:
            url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/"
            params = {
                "apiKey": api_key,
                "regions": "us",
                "markets": "totals,spreads",
                "oddsFormat": "american",
                "bookmakers": "bovada,fanduel"
            }
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                continue
            
            events = resp.json()
            
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
                        fanduel_book = next((b for b in bookmakers if b.get("key") == "fanduel"), None)
                        book = bovada_book or fanduel_book
                        if book:
                            markets = book.get("markets", [])
                            for market in markets:
                                if market.get("key") == "totals":
                                    outcomes = market.get("outcomes", [])
                                    for outcome in outcomes:
                                        if outcome.get("name") == "Over":
                                            line = outcome.get("point")
                                            if line is not None:
                                                game.line = line
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
                                                    game.is_qualified = qualified
                                                    game.direction = direction
                                                    game.edge = edge
                                                lines_updated += 1
                                            break
                                elif market.get("key") == "spreads":
                                    outcomes = market.get("outcomes", [])
                                    for outcome in outcomes:
                                        if teams_match(outcome.get("name", ""), home_team):
                                            spread_line = outcome.get("point")
                                            if spread_line is not None:
                                                game.spread_line = spread_line
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
                                                    game.spread_is_qualified = spread_qual
                                                    game.spread_direction = spread_dir
                                                    game.spread_edge = spread_edge
                                                spreads_updated += 1
                                            break
        except Exception as e:
            print(f"Odds fetch error for {league}: {e}")
    
    db.session.commit()
    return jsonify({"success": True, "lines_updated": lines_updated, "spreads_updated": spreads_updated})

def find_best_alt_line(outcomes: list, direction: str, current_line: float, is_spread: bool = False, home_team: str = "") -> tuple:
    """
    Find the best alternate line with odds not exceeding -180.
    For totals: direction is OVER/UNDER
    For spreads: direction is HOME/AWAY, need to find team-specific line
    
    Returns (best_line, best_odds) or (None, None) if no valid line found.
    """
    MAX_ODDS = -180
    best_line = None
    best_odds = None
    best_value = None
    
    for outcome in outcomes:
        odds = outcome.get("price", 0)
        point = outcome.get("point")
        name = outcome.get("name", "")
        
        if point is None or odds < MAX_ODDS:
            continue
        
        if is_spread:
            is_home_outcome = teams_match(name, home_team)
            if direction == "HOME" and not is_home_outcome:
                continue
            if direction == "AWAY" and is_home_outcome:
                continue
            value = abs(point - current_line) if current_line else abs(point)
        else:
            if direction == "OVER" and name != "Over":
                continue
            if direction == "UNDER" and name != "Under":
                continue
            if direction == "OVER":
                value = current_line - point if current_line else -point
            else:
                value = point - current_line if current_line else point
        
        if value > 0 and (best_value is None or value > best_value):
            best_value = value
            best_line = point
            best_odds = odds
    
    return best_line, best_odds

@app.route('/fetch_alt_lines', methods=['POST'])
def fetch_alt_lines():
    """Fetch alternate lines for qualified games to find better value."""
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        return jsonify({"success": False, "message": "ODDS_API_KEY not configured"})
    
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
    
    all_qualified = list(set(qualified_totals + qualified_spreads))
    
    alt_lines_found = 0
    
    for game in all_qualified:
        if not game.event_id or not game.sport_key:
            continue
            
        try:
            url = f"https://api.the-odds-api.com/v4/sports/{game.sport_key}/events/{game.event_id}/odds"
            params = {
                "apiKey": api_key,
                "regions": "us",
                "markets": "alternate_totals,alternate_spreads",
                "oddsFormat": "american",
                "bookmakers": "fanduel,draftkings"
            }
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"Alt lines API error for {game.away_team}@{game.home_team}: {resp.status_code}")
                continue
            
            data = resp.json()
            bookmakers = data.get("bookmakers", [])
            
            book = next((b for b in bookmakers if b.get("key") in ["fanduel", "draftkings"]), None)
            if not book:
                continue
            
            markets = book.get("markets", [])
            home_team = data.get("home_team", game.home_team)
            
            for market in markets:
                market_key = market.get("key")
                outcomes = market.get("outcomes", [])
                
                if market_key == "alternate_totals" and game.is_qualified and game.direction:
                    alt_line, alt_odds = find_best_alt_line(
                        outcomes, game.direction, game.line, is_spread=False
                    )
                    if alt_line is not None:
                        game.alt_total_line = alt_line
                        game.alt_total_odds = alt_odds
                        alt_lines_found += 1
                
                elif market_key == "alternate_spreads" and game.spread_is_qualified and game.spread_direction:
                    alt_line, alt_odds = find_best_alt_line(
                        outcomes, game.spread_direction, game.spread_line, 
                        is_spread=True, home_team=home_team
                    )
                    if alt_line is not None:
                        game.alt_spread_line = alt_line
                        game.alt_spread_odds = alt_odds
                        alt_lines_found += 1
                        
        except Exception as e:
            print(f"Alt lines error for {game.away_team}@{game.home_team}: {e}")
    
    db.session.commit()
    return jsonify({"success": True, "alt_lines_found": alt_lines_found, "games_checked": len(all_qualified)})

@app.route('/post_discord', methods=['POST'])
def post_discord():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%B %d, %Y")
    
    all_qualified = Game.query.filter_by(date=today, is_qualified=True).order_by(Game.edge.desc()).all()
    games = [g for g in all_qualified if not (g.game_time and 'final' in g.game_time.lower())][:5]
    
    spread_qualified = Game.query.filter_by(date=today, spread_is_qualified=True).order_by(Game.spread_edge.desc()).all()
    spread_games = [g for g in spread_qualified if not (g.game_time and 'final' in g.game_time.lower())][:3]
    
    if not games and not spread_games:
        return jsonify({"success": False, "message": "No qualified picks to post"})
    
    top_picks = games[:3]
    
    msg = f"🎯 PICKS OF THE DAY - {today_str}\n\n"
    
    if top_picks:
        msg += "📊 **TOTALS**\n"
        for league in ["NBA", "CBB", "NFL", "CFB", "NHL"]:
            league_games = [g for g in top_picks if g.league == league]
            if league_games:
                emoji = {"NBA": "🏀", "CBB": "🏀", "NFL": "🏈", "CFB": "🏈", "NHL": "🏒"}.get(league, "🎯")
                msg += f"{emoji} {league}\n"
                for g in league_games:
                    msg += f"{g.away_team}/{g.home_team} ({g.game_time})\n"
                    msg += f"Game Total {g.direction}{g.line}\n\n"
        
        lock = top_picks[0]
        msg += f"🔒 Totals Lock:\n"
        msg += f"{lock.away_team}/{lock.home_team} ({lock.game_time})\n"
        msg += f"Game Total {lock.direction}{lock.line}\n\n"
    
    if spread_games:
        msg += "📈 **SPREADS**\n"
        for g in spread_games:
            emoji = {"NBA": "🏀", "CBB": "🏀", "NFL": "🏈", "CFB": "🏈", "NHL": "🏒"}.get(g.league, "🎯")
            if g.spread_direction == 'HOME':
                pick_str = f"{g.home_team} {g.spread_line:+.1f}" if g.spread_line else g.home_team
            else:
                pick_str = f"{g.away_team} {-g.spread_line:+.1f}" if g.spread_line else g.away_team
            msg += f"{emoji} {g.away_team}/{g.home_team}\n"
            msg += f"{pick_str}\n\n"
        
        spread_lock = spread_games[0]
        if spread_lock.spread_direction == 'HOME':
            lock_pick = f"{spread_lock.home_team} {spread_lock.spread_line:+.1f}" if spread_lock.spread_line else spread_lock.home_team
        else:
            lock_pick = f"{spread_lock.away_team} {-spread_lock.spread_line:+.1f}" if spread_lock.spread_line else spread_lock.away_team
        msg += f"📈 Spread Lock:\n"
        msg += f"{spread_lock.away_team}/{spread_lock.home_team}\n"
        msg += f"{lock_pick}\n"
    
    webhook = os.environ.get("SPORTS_DISCORD_WEBHOOK")
    if webhook:
        resp = requests.post(webhook, json={"content": msg})
        
        for i, g in enumerate(top_picks):
            matchup = f"{g.away_team} @ {g.home_team}"
            existing_pick = Pick.query.filter_by(date=today, matchup=matchup, pick_type="total").first()
            if not existing_pick:
                pick = Pick(
                    game_id=g.id,
                    date=today,
                    league=g.league,
                    matchup=matchup,
                    pick=f"{g.direction}{g.line}",
                    edge=g.edge,
                    is_lock=(i == 0),
                    posted_to_discord=True,
                    pick_type="total",
                    line_value=g.line
                )
                db.session.add(pick)
        
        for i, g in enumerate(spread_games):
            matchup = f"{g.away_team} @ {g.home_team}"
            existing_pick = Pick.query.filter_by(date=today, matchup=matchup, pick_type="spread").first()
            if not existing_pick:
                if g.spread_direction == 'HOME':
                    pick_str = f"{g.home_team} {g.spread_line:+.1f}" if g.spread_line else g.home_team
                else:
                    pick_str = f"{g.away_team} {-g.spread_line:+.1f}" if g.spread_line else g.away_team
                pick = Pick(
                    game_id=g.id,
                    date=today,
                    league=g.league,
                    matchup=matchup,
                    pick=pick_str,
                    edge=g.spread_edge,
                    is_lock=(i == 0),
                    posted_to_discord=True,
                    pick_type="spread",
                    line_value=g.spread_line
                )
                db.session.add(pick)
        
        db.session.commit()
        
        return jsonify({"success": True, "status": resp.status_code, "picks_count": len(top_picks) + len(spread_games)})
    
    return jsonify({"success": False, "message": "Discord webhook not configured"})

@app.route('/check_results', methods=['POST'])
def check_results():
    updated = check_pick_results()
    return jsonify({"success": True, "results_updated": updated})

@app.route('/history')
def history():
    """Display pick history with win/loss stats."""
    check_pick_results()
    
    picks = Pick.query.order_by(Pick.date.desc(), Pick.edge.desc()).all()
    
    wins = len([p for p in picks if p.result == 'W'])
    losses = len([p for p in picks if p.result == 'L'])
    
    return render_template('history.html', picks=picks, wins=wins, losses=losses)

@app.route('/update_result/<int:pick_id>', methods=['POST'])
def update_result(pick_id):
    pick = Pick.query.get_or_404(pick_id)
    data = request.get_json()
    pick.result = data.get('result')
    if data.get('actual_total'):
        pick.actual_total = float(data['actual_total'])
    db.session.commit()
    return jsonify({"success": True})

@app.route('/clear_games', methods=['POST'])
def clear_games():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    Game.query.filter_by(date=today).delete()
    db.session.commit()
    return redirect(url_for('dashboard'))

with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
