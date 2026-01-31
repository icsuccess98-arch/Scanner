"""
CENTRALIZED CONSTANTS MODULE
=============================

Single source of truth for all configuration values, thresholds,
team mappings, and constants used across the sports betting application.
"""

from typing import Dict, Set

# ============================================================
# SEASON CONSTANTS - Update at start of each season
# ============================================================

CURRENT_NBA_SEASON = '2025-26'  # Format: YYYY-YY for nba_api
CURRENT_NHL_SEASON_ID = '20252026'  # Format: YYYYYYYY for NHL API
CURRENT_CBB_SEASON = 2026  # Year for CBB (e.g., 2026 for 2025-26 season)
CURRENT_NFL_SEASON = 2025  # Year for NFL season


# ============================================================
# VALIDATION BOUNDS
# ============================================================

VALID_LEAGUES: Set[str] = {'NBA', 'CBB', 'NFL', 'CFB', 'NHL'}

# Maximum values for validation
MAX_LINE_VALUE = 500.0
MIN_LINE_VALUE = -100.0
MAX_PPG_VALUE = 200.0
MAX_TEAM_NAME_LENGTH = 100
MAX_SPREAD_VALUE = 50.0
MIN_SPREAD_VALUE = -50.0


# ============================================================
# CACHE TTLs (seconds)
# ============================================================

class CacheTTL:
    """Cache time-to-live values in seconds."""
    DASHBOARD = 30
    LIVE_SCORES = 15
    WAGERTALK = 60
    CTG = 14400  # 4 hours
    TEAM_STATS = 86400  # 24 hours
    PRE_GAME = 86400  # 24 hours
    SCHEDULE = 43200  # 12 hours
    OPENING_LINE = 86400  # 24 hours
    HISTORICAL = 21600  # 6 hours
    TORVIK = 86400  # 24 hours (daily)
    COVERS = 300  # 5 minutes
    ESPN = 60  # 1 minute


# ============================================================
# RATE LIMITS (requests per second by domain)
# ============================================================

class RateLimits:
    """Rate limits for external APIs (requests per second)."""
    ESPN = 5.0
    ODDS_API = 0.02  # ~500/month = 0.02/sec
    COVERS = 1.0
    NHLE = 3.0
    WAGERTALK = 0.5  # Playwright heavy
    BART_TORVIK = 1.0
    TEAM_RANKINGS = 1.0
    DEFAULT = 2.0


# ============================================================
# EXECUTOR SETTINGS
# ============================================================

EXECUTOR_TIMEOUT = 30  # Default timeout for ThreadPoolExecutor futures
MAX_WORKERS_DEFAULT = 5
MAX_WORKERS_BATCH = 10


# ============================================================
# EDGE THRESHOLDS BY LEAGUE
# ============================================================

EDGE_THRESHOLDS: Dict[str, Dict[str, float]] = {
    'NBA': {
        'total_edge': 8.0,
        'spread_edge': 3.5,
        'min_ev': 3.0,
        'max_spread': 12.0,
        'rlm_threshold': 10.0,
    },
    'CBB': {
        'total_edge': 8.0,
        'spread_edge': 3.5,
        'min_ev': 3.0,
        'max_spread': 15.0,
        'rlm_threshold': 10.0,
    },
    'NHL': {
        'total_edge': 0.5,
        'spread_edge': 0.5,
        'min_ev': 2.5,
        'max_spread': 2.5,
        'rlm_threshold': 5.0,
    },
    'NFL': {
        'total_edge': 3.0,
        'spread_edge': 2.5,
        'min_ev': 3.0,
        'max_spread': 14.0,
        'rlm_threshold': 8.0,
    },
    'CFB': {
        'total_edge': 3.5,
        'spread_edge': 3.0,
        'min_ev': 3.0,
        'max_spread': 35.0,
        'rlm_threshold': 8.0,
    },
}


def get_threshold(league: str, key: str, default: float = 0.0) -> float:
    """Get a threshold value for a league."""
    return EDGE_THRESHOLDS.get(league, {}).get(key, default)


# ============================================================
# TOTAL VALUE RANGES BY LEAGUE
# ============================================================

TOTAL_RANGES: Dict[str, tuple] = {
    'NBA': (150, 300),
    'CBB': (100, 200),
    'NFL': (25, 75),
    'CFB': (25, 100),
    'NHL': (3, 10),
}


# ============================================================
# NBA TEAM DATA
# ============================================================

NBA_TEAM_COLORS: Dict[str, str] = {
    'Hawks': '#E03A3E', 'Celtics': '#007A33', 'Nets': '#000000',
    'Hornets': '#1D1160', 'Bulls': '#CE1141', 'Cavaliers': '#860038',
    'Mavericks': '#00538C', 'Nuggets': '#0E2240', 'Pistons': '#C8102E',
    'Warriors': '#1D428A', 'Rockets': '#CE1141', 'Pacers': '#002D62',
    'Clippers': '#C8102E', 'Lakers': '#552583', 'Grizzlies': '#5D76A9',
    'Heat': '#98002E', 'Bucks': '#00471B', 'Timberwolves': '#0C2340',
    'Pelicans': '#0C2340', 'Knicks': '#F58426', 'Thunder': '#007AC1',
    'Magic': '#0077C0', '76ers': '#006BB6', 'Suns': '#1D1160',
    'Trail Blazers': '#E03A3E', 'Kings': '#5A2D81', 'Spurs': '#C4CED4',
    'Raptors': '#CE1141', 'Jazz': '#002B5C', 'Wizards': '#002B5C',
}

NBA_ABBREVIATIONS: Dict[str, str] = {
    'LAL': 'Lakers', 'BOS': 'Celtics', 'NYK': 'Knicks', 'NY': 'Knicks',
    'PHI': '76ers', 'MIA': 'Heat', 'CHI': 'Bulls', 'DET': 'Pistons',
    'CLE': 'Cavaliers', 'TOR': 'Raptors', 'ORL': 'Magic', 'SAC': 'Kings',
    'WAS': 'Wizards', 'DEN': 'Nuggets', 'NO': 'Pelicans', 'NOP': 'Pelicans',
    'MEM': 'Grizzlies', 'GS': 'Warriors', 'GSW': 'Warriors',
    'LAC': 'Clippers', 'PHX': 'Suns', 'POR': 'Trail Blazers',
    'ATL': 'Hawks', 'BKN': 'Nets', 'CHA': 'Hornets', 'DAL': 'Mavericks',
    'HOU': 'Rockets', 'IND': 'Pacers', 'MIL': 'Bucks', 'MIN': 'Timberwolves',
    'OKC': 'Thunder', 'SA': 'Spurs', 'SAS': 'Spurs', 'UTA': 'Jazz',
}

NBA_CITY_TO_NICKNAME: Dict[str, str] = {
    'Atlanta': 'Hawks', 'Boston': 'Celtics', 'Brooklyn': 'Nets',
    'Charlotte': 'Hornets', 'Chicago': 'Bulls', 'Cleveland': 'Cavaliers',
    'Dallas': 'Mavericks', 'Denver': 'Nuggets', 'Detroit': 'Pistons',
    'Golden State': 'Warriors', 'Houston': 'Rockets', 'Indiana': 'Pacers',
    'LA Clippers': 'Clippers', 'LA Lakers': 'Lakers', 'Los Angeles Lakers': 'Lakers',
    'Los Angeles Clippers': 'Clippers', 'Memphis': 'Grizzlies', 'Miami': 'Heat',
    'Milwaukee': 'Bucks', 'Minnesota': 'Timberwolves', 'New Orleans': 'Pelicans',
    'New York': 'Knicks', 'Oklahoma City': 'Thunder', 'Orlando': 'Magic',
    'Philadelphia': '76ers', 'Phoenix': 'Suns', 'Portland': 'Trail Blazers',
    'Sacramento': 'Kings', 'San Antonio': 'Spurs', 'Toronto': 'Raptors',
    'Utah': 'Jazz', 'Washington': 'Wizards',
}


# ============================================================
# NHL TEAM DATA
# ============================================================

NHL_TEAM_COLORS: Dict[str, str] = {
    'Ducks': '#F47A38', 'Coyotes': '#8C2633', 'Bruins': '#FFB81C',
    'Sabres': '#002654', 'Flames': '#C8102E', 'Hurricanes': '#CC0000',
    'Blackhawks': '#CF0A2C', 'Avalanche': '#6F263D', 'Blue Jackets': '#002654',
    'Stars': '#006847', 'Red Wings': '#CE1126', 'Oilers': '#041E42',
    'Panthers': '#041E42', 'Kings': '#111111', 'Wild': '#154734',
    'Canadiens': '#AF1E2D', 'Predators': '#FFB81C', 'Devils': '#CE1126',
    'Islanders': '#00539B', 'Rangers': '#0038A8', 'Senators': '#C52032',
    'Flyers': '#F74902', 'Penguins': '#FCB514', 'Sharks': '#006D75',
    'Kraken': '#001628', 'Blues': '#002F87', 'Lightning': '#002868',
    'Maple Leafs': '#00205B', 'Golden Knights': '#B4975A', 'Canucks': '#00205B',
    'Capitals': '#C8102E', 'Jets': '#041E42', 'Utah Hockey Club': '#6CADE5',
}

NHL_ABBREVIATIONS: Dict[str, str] = {
    'ANA': 'Ducks', 'ARI': 'Coyotes', 'BOS': 'Bruins', 'BUF': 'Sabres',
    'CGY': 'Flames', 'CAR': 'Hurricanes', 'CHI': 'Blackhawks', 'COL': 'Avalanche',
    'CBJ': 'Blue Jackets', 'DAL': 'Stars', 'DET': 'Red Wings', 'EDM': 'Oilers',
    'FLA': 'Panthers', 'LA': 'Kings', 'LAK': 'Kings', 'MIN': 'Wild',
    'MTL': 'Canadiens', 'NSH': 'Predators', 'NJ': 'Devils', 'NJD': 'Devils',
    'NYI': 'Islanders', 'NYR': 'Rangers', 'OTT': 'Senators', 'PHI': 'Flyers',
    'PIT': 'Penguins', 'SJ': 'Sharks', 'SJS': 'Sharks', 'SEA': 'Kraken',
    'STL': 'Blues', 'TB': 'Lightning', 'TBL': 'Lightning', 'TOR': 'Maple Leafs',
    'VAN': 'Canucks', 'VGK': 'Golden Knights', 'WSH': 'Capitals', 'WPG': 'Jets',
    'UTA': 'Utah Hockey Club',
}


# ============================================================
# API ENDPOINTS
# ============================================================

class APIEndpoints:
    """External API base URLs."""
    ESPN_SITE = 'https://site.api.espn.com'
    ESPN_CORE = 'http://sports.core.api.espn.com'
    NHL_API = 'https://api-web.nhle.com'
    ODDS_API = 'https://api.the-odds-api.com/v4'
    COVERS = 'https://www.covers.com'
    WAGERTALK = 'https://www.wagertalk.com'
    BART_TORVIK = 'https://barttorvik.com'
    TEAM_RANKINGS = 'https://www.teamrankings.com'


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

LOG_FORMAT_DEFAULT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_team_nickname(identifier: str, league: str = 'NBA') -> str:
    """
    Get team nickname from abbreviation or city name.

    Args:
        identifier: Team abbreviation or city name
        league: League context

    Returns:
        Team nickname or original identifier if not found
    """
    if league == 'NBA':
        if identifier in NBA_ABBREVIATIONS:
            return NBA_ABBREVIATIONS[identifier]
        if identifier in NBA_CITY_TO_NICKNAME:
            return NBA_CITY_TO_NICKNAME[identifier]
    elif league == 'NHL':
        if identifier in NHL_ABBREVIATIONS:
            return NHL_ABBREVIATIONS[identifier]

    return identifier


def get_team_color(team_name: str, league: str = 'NBA') -> str:
    """
    Get team primary color.

    Args:
        team_name: Team nickname
        league: League context

    Returns:
        Hex color code or default gray
    """
    if league == 'NBA':
        return NBA_TEAM_COLORS.get(team_name, '#666666')
    elif league == 'NHL':
        return NHL_TEAM_COLORS.get(team_name, '#666666')
    return '#666666'


def is_valid_league(league: str) -> bool:
    """Check if league is valid."""
    return league.upper() in VALID_LEAGUES
