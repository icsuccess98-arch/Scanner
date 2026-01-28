"""
CONFIGURATION FILE
==================

Customize the betting engine behavior by adjusting these parameters.
All settings can be overridden when initializing the analyzer.
"""

# ============================================================
# MARKET ANALYSIS SETTINGS
# ============================================================

# Sharp money detection threshold (percentage points)
# Higher = more conservative (fewer sharp flags)
# Lower = more aggressive (more sharp flags)
# Recommended range: 10-20
SHARP_THRESHOLD = 15.0

# High handle threshold (percentage)
# Games with this % or more on one side are flagged
HIGH_HANDLE_THRESHOLD = 80.0

# RLM minimum line movement to consider (points)
# Smaller movements are considered noise
RLM_MIN_MOVEMENT = 0.5


# ============================================================
# ELIMINATION FILTER SETTINGS
# ============================================================

# Large spread threshold (points)
# Games with spreads larger than this are filtered out
LARGE_SPREAD_THRESHOLD = 10.0

# Bad record thresholds (wins, losses)
# Games where either team has one of these records are filtered
BAD_RECORD_THRESHOLDS = [
    (0, 5), (0, 10), (0, 12),
    (1, 10), (1, 12), (1, 15),
    (2, 10), (2, 12), (2, 15),
    (3, 15), (3, 18)
]

# Bad defense rank threshold (last 5 games)
# Teams ranked worse than this are filtered
# NBA has 30 teams, so 25+ = bottom tier
BAD_DEFENSE_RANK_THRESHOLD = 25

# Back-to-back game handling
# Options: 'filter', 'flag', 'ignore'
# - 'filter': Automatically filter out B2B games
# - 'flag': Flag but don't filter (included in analysis with warning)
# - 'ignore': Don't consider B2B status
B2B_HANDLING = 'flag'


# ============================================================
# SPREAD PROJECTION SETTINGS
# ============================================================

# League average pace (possessions per game)
# NBA average is typically 99-101
LEAGUE_AVG_PACE = 100.0

# Defensive efficiency blend weight
# How much to weight defensive efficiency vs raw PPG allowed
# 0.0 = use only PPG, 1.0 = use only defensive efficiency
# Recommended: 0.4 (60% PPG, 40% efficiency)
DEF_EFFICIENCY_WEIGHT = 0.4

# Points per possession values
POINTS_PER_TURNOVER = 1.05
POINTS_PER_OFFENSIVE_REBOUND = 1.1

# Shooting efficiency adjustment cap (percentage)
# Caps the efficiency adjustment to avoid hot shooting noise
# 0.04 = ±4% maximum adjustment
EFFICIENCY_ADJ_CAP = 0.04

# Ball control adjustment cap (points)
# Maximum points to add/subtract from assist/turnover ratio
BALL_CONTROL_ADJ_CAP = 1.0


# ============================================================
# BET DECISION SETTINGS
# ============================================================

# Minimum spread value to consider betting (points)
# Your true spread must differ from Vegas by at least this much
MIN_SPREAD_VALUE = 2.0

# Minimum stat edge differential
# Team must have at least this many more stat edges than opponent
MIN_STAT_EDGE_DIFFERENTIAL = 2

# Confidence level requirements
CONFIDENCE_REQUIREMENTS = {
    'High': {
        'min_stat_edge_diff': 3,
        'min_spread_value': 2.0,
        'require_sharp_confirmation': True,
        'allow_rlm': False
    },
    'Medium': {
        'min_stat_edge_diff': 2,
        'min_spread_value': 1.5,
        'require_sharp_confirmation': False,
        'allow_rlm': False
    },
    'Low': {
        'min_stat_edge_diff': 1,
        'min_spread_value': 1.0,
        'require_sharp_confirmation': False,
        'allow_rlm': True
    }
}


# ============================================================
# WEB SCRAPING SETTINGS
# ============================================================

# Request timeout (seconds)
REQUEST_TIMEOUT = 30

# Rate limiting (seconds between requests)
RATE_LIMIT_DELAY = 2.0

# User agent for web requests
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# Maximum retries for failed requests
MAX_RETRIES = 3

# API keys (set these as environment variables for security)
# Example: export NBA_API_KEY="your_key_here"
import os

NBA_API_KEY = os.getenv('NBA_API_KEY', '')
COVERS_API_KEY = os.getenv('COVERS_API_KEY', '')
VSIN_API_KEY = os.getenv('VSIN_API_KEY', '')
CLEANING_GLASS_API_KEY = os.getenv('CLEANING_GLASS_API_KEY', '')


# ============================================================
# DATA SOURCE URLS
# ============================================================

DATA_SOURCES = {
    'covers': 'https://www.covers.com',
    'vsin': 'https://www.vsin.com',
    'scores_and_odds': 'https://www.scoresandodds.com',
    'nba_official': 'https://www.nba.com',
    'cleaning_the_glass': 'https://cleaningtheglass.com'
}


# ============================================================
# LOGGING SETTINGS
# ============================================================

# Log level: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
LOG_LEVEL = 'INFO'

# Log file path
LOG_FILE = 'betting_engine.log'

# Log format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Console logging enabled
CONSOLE_LOGGING = True


# ============================================================
# DATABASE SETTINGS (if using persistence)
# ============================================================

# Database connection string
# Format: postgresql://user:password@localhost:5432/dbname
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///betting_engine.db')

# Enable bet tracking
ENABLE_BET_TRACKING = True

# Enable backtest logging
ENABLE_BACKTEST_LOGGING = True


# ============================================================
# ALERT SETTINGS
# ============================================================

# Enable alerts for high-confidence bets
ENABLE_ALERTS = False

# Alert methods: 'telegram', 'discord', 'email', 'sms'
ALERT_METHODS = ['telegram']

# Telegram settings
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')

# Discord settings
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')

# Email settings
EMAIL_FROM = os.getenv('EMAIL_FROM', '')
EMAIL_TO = os.getenv('EMAIL_TO', '')
SMTP_SERVER = os.getenv('SMTP_SERVER', '')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USERNAME = os.getenv('SMTP_USERNAME', '')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')


# ============================================================
# BANKROLL MANAGEMENT SETTINGS
# ============================================================

# Default unit size (% of bankroll)
DEFAULT_UNIT_SIZE = 0.02  # 2% of bankroll

# Unit sizing by confidence
UNIT_SIZING = {
    'High': 3.0,      # 3 units
    'Medium': 2.0,    # 2 units
    'Low': 0.0        # Don't bet
}

# Maximum units per day
MAX_UNITS_PER_DAY = 10.0

# Kelly Criterion enabled
# If True, calculates optimal bet size using Kelly formula
USE_KELLY_CRITERION = False

# Kelly fraction (if using Kelly)
# 1.0 = full Kelly, 0.5 = half Kelly (more conservative)
KELLY_FRACTION = 0.5


# ============================================================
# BACKTESTING SETTINGS
# ============================================================

# Historical data date range
BACKTEST_START_DATE = '2023-10-01'  # Start of 2023-24 season
BACKTEST_END_DATE = '2024-04-30'    # End of regular season

# Closing Line Value (CLV) tracking
TRACK_CLV = True

# Juice/Vig assumptions for profit calculations
STANDARD_VIG = -110  # American odds


# ============================================================
# ADVANCED SETTINGS
# ============================================================

# Statistical splits to use
# Options: 'season', 'last_10', 'last_5', 'home_away', 'conference'
STAT_SPLITS = ['season', 'last_10', 'home_away']

# Stat split weights (must sum to 1.0)
STAT_SPLIT_WEIGHTS = {
    'season': 0.5,
    'last_10': 0.2,
    'last_5': 0.1,
    'home_away': 0.2
}

# Injury adjustment
# Automatically adjust projections for key player injuries
ENABLE_INJURY_ADJUSTMENT = False

# Weather adjustment (for NFL - not applicable to NBA)
ENABLE_WEATHER_ADJUSTMENT = False

# Rest advantage threshold (days)
# If team has this many more rest days than opponent, flag it
REST_ADVANTAGE_THRESHOLD = 2


# ============================================================
# PRESET CONFIGURATIONS
# ============================================================

PRESETS = {
    'conservative': {
        'SHARP_THRESHOLD': 20.0,
        'MIN_SPREAD_VALUE': 3.0,
        'MIN_STAT_EDGE_DIFFERENTIAL': 3,
        'LARGE_SPREAD_THRESHOLD': 8.0,
        'B2B_HANDLING': 'filter'
    },
    
    'balanced': {
        'SHARP_THRESHOLD': 15.0,
        'MIN_SPREAD_VALUE': 2.0,
        'MIN_STAT_EDGE_DIFFERENTIAL': 2,
        'LARGE_SPREAD_THRESHOLD': 10.0,
        'B2B_HANDLING': 'flag'
    },
    
    'aggressive': {
        'SHARP_THRESHOLD': 10.0,
        'MIN_SPREAD_VALUE': 1.5,
        'MIN_STAT_EDGE_DIFFERENTIAL': 1,
        'LARGE_SPREAD_THRESHOLD': 12.0,
        'B2B_HANDLING': 'ignore'
    }
}


# ============================================================
# USAGE
# ============================================================

def load_preset(preset_name):
    """
    Load a preset configuration.
    
    Args:
        preset_name: 'conservative', 'balanced', or 'aggressive'
        
    Example:
        config.load_preset('conservative')
    """
    if preset_name not in PRESETS:
        raise ValueError(f"Unknown preset: {preset_name}")
    
    preset = PRESETS[preset_name]
    globals().update(preset)
    
    return f"Loaded preset: {preset_name}"


def apply_config(analyzer):
    """
    Apply current config to an analyzer instance.
    
    Example:
        analyzer = ComprehensiveGameAnalyzer()
        config.apply_config(analyzer)
    """
    analyzer.market_engine.sharp_threshold = SHARP_THRESHOLD
    analyzer.spread_engine.LEAGUE_AVG_PACE = LEAGUE_AVG_PACE
    
    return "Configuration applied"


# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

def validate_config():
    """
    Validate configuration settings.
    Returns list of warnings if any settings are out of recommended range.
    """
    warnings = []
    
    if SHARP_THRESHOLD < 5.0:
        warnings.append("SHARP_THRESHOLD very low - may flag noise as sharp action")
    elif SHARP_THRESHOLD > 25.0:
        warnings.append("SHARP_THRESHOLD very high - may miss sharp action")
    
    if MIN_SPREAD_VALUE < 1.0:
        warnings.append("MIN_SPREAD_VALUE very low - may bet on small edges")
    
    if sum(STAT_SPLIT_WEIGHTS.values()) != 1.0:
        warnings.append("STAT_SPLIT_WEIGHTS must sum to 1.0")
    
    if DEFAULT_UNIT_SIZE > 0.05:
        warnings.append("DEFAULT_UNIT_SIZE >5% of bankroll - very aggressive")
    
    return warnings


if __name__ == "__main__":
    print("=" * 80)
    print("CURRENT CONFIGURATION")
    print("=" * 80)
    print()
    print(f"Sharp Threshold: {SHARP_THRESHOLD}")
    print(f"Min Spread Value: {MIN_SPREAD_VALUE}")
    print(f"Min Stat Edge Differential: {MIN_STAT_EDGE_DIFFERENTIAL}")
    print(f"Large Spread Threshold: {LARGE_SPREAD_THRESHOLD}")
    print(f"B2B Handling: {B2B_HANDLING}")
    print()
    
    warnings = validate_config()
    if warnings:
        print("⚠️ CONFIGURATION WARNINGS:")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print("✓ Configuration validated successfully")
