"""
CENTRALIZED UTILITIES MODULE
============================

Shared utility functions used across the sports betting application.
Consolidates duplicated patterns from sports_app.py, enhanced_scraping.py,
and wagertalk_scraper.py.
"""

import logging
import re
from typing import Optional, Tuple, List, Any, Dict

logger = logging.getLogger(__name__)


# ============================================================
# TEAM EXTRACTION UTILITIES
# ============================================================

def extract_teams_from_competitors(teams_data: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Extract away and home team data from ESPN-style competitors list.

    Replaces the repeated pattern:
        away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
        home = next((t for t in teams_data if t.get("homeAway") == "home"), None)

    Args:
        teams_data: List of competitor dictionaries with 'homeAway' key

    Returns:
        Tuple of (away_team, home_team) dictionaries, either may be None
    """
    away = next((t for t in teams_data if t.get("homeAway") == "away"), None)
    home = next((t for t in teams_data if t.get("homeAway") == "home"), None)
    return away, home


def get_team_name_from_competitor(competitor: Optional[Dict], key: str = 'displayName') -> str:
    """
    Safely extract team name from ESPN competitor data.

    Args:
        competitor: Competitor dictionary with nested 'team' data
        key: The key to extract ('displayName', 'shortDisplayName', 'abbreviation')

    Returns:
        Team name string, or empty string if not found
    """
    if not competitor:
        return ''
    team = competitor.get('team', {})
    if isinstance(team, dict):
        return team.get(key, '')
    return ''


# ============================================================
# VALIDATION UTILITIES
# ============================================================

def validate_percentage(value: Any, field_name: str = 'percentage') -> Optional[float]:
    """
    Validate percentage is in valid range (0-100).

    Args:
        value: Value to validate
        field_name: Name for logging purposes

    Returns:
        Float value if valid, None if invalid
    """
    if value is None:
        return None
    try:
        val = float(value)
        if 0 <= val <= 100:
            return val
        logger.debug(f"Invalid {field_name}: {val} (outside 0-100 range)")
        return None
    except (ValueError, TypeError):
        return None


def validate_spread(value: Any, league: str = 'NBA') -> Optional[float]:
    """
    Validate spread is in reasonable range based on league.

    Args:
        value: Spread value to validate
        league: League for range determination

    Returns:
        Float value if valid, None if invalid
    """
    if value is None:
        return None
    try:
        val = float(value)
        # Reasonable spread ranges by league
        max_spread = {
            'NBA': 25.0,
            'CBB': 35.0,
            'NFL': 20.0,
            'CFB': 50.0,
            'NHL': 3.5,
        }.get(league, 50.0)

        if abs(val) <= max_spread:
            return val
        logger.debug(f"Invalid spread for {league}: {val} (outside +/-{max_spread} range)")
        return None
    except (ValueError, TypeError):
        return None


def validate_total(value: Any, league: str = 'NBA') -> Optional[float]:
    """
    Validate total is in reasonable range based on league.

    Args:
        value: Total value to validate
        league: League for range determination

    Returns:
        Float value if valid, None if invalid
    """
    if value is None:
        return None
    try:
        val = float(value)
        # Reasonable total ranges by league
        ranges = {
            'NBA': (150, 300),
            'CBB': (100, 200),
            'NFL': (25, 75),
            'CFB': (25, 100),
            'NHL': (3, 10),
        }
        min_val, max_val = ranges.get(league, (0, 500))
        if min_val <= val <= max_val:
            return val
        logger.debug(f"Invalid total for {league}: {val} (outside {min_val}-{max_val} range)")
        return None
    except (ValueError, TypeError):
        return None


def validate_float_param(
    value: Any,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    allow_none: bool = True
) -> Tuple[Optional[float], Optional[str]]:
    """
    Validate a float parameter with optional bounds.

    Args:
        value: Value to validate
        min_val: Minimum allowed value (inclusive)
        max_val: Maximum allowed value (inclusive)
        allow_none: Whether None/empty is acceptable

    Returns:
        Tuple of (validated_value, error_message)
    """
    if value is None or value == '':
        if allow_none:
            return None, None
        return None, "Value is required"
    try:
        float_val = float(value)
        if min_val is not None and float_val < min_val:
            return None, f"Value must be >= {min_val}"
        if max_val is not None and float_val > max_val:
            return None, f"Value must be <= {max_val}"
        return float_val, None
    except (ValueError, TypeError):
        return None, "Invalid number format"


def validate_string_param(
    value: Any,
    max_length: Optional[int] = None,
    allowed_values: Optional[set] = None,
    allow_empty: bool = False
) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate a string parameter.

    Args:
        value: Value to validate
        max_length: Maximum string length
        allowed_values: Set of allowed values (if specified)
        allow_empty: Whether empty string is acceptable

    Returns:
        Tuple of (validated_value, error_message)
    """
    if value is None:
        if allow_empty:
            return '', None
        return None, "Value is required"

    str_val = str(value).strip()

    if not str_val and not allow_empty:
        return None, "Value cannot be empty"

    if max_length is not None and len(str_val) > max_length:
        return None, f"Value exceeds max length of {max_length}"

    if allowed_values is not None and str_val not in allowed_values:
        return None, f"Value must be one of: {', '.join(sorted(allowed_values))}"

    return str_val, None


# ============================================================
# SAFE MATH UTILITIES
# ============================================================

def safe_divide(numerator: Any, denominator: Any, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default on error.

    Args:
        numerator: The dividend
        denominator: The divisor
        default: Value to return on error

    Returns:
        Result of division or default value
    """
    try:
        if denominator is None or denominator == 0:
            return default
        return float(numerator) / float(denominator)
    except (ValueError, TypeError, ZeroDivisionError):
        return default


def safe_average(values: List[Any], default: float = 0.0) -> float:
    """
    Safely compute average of a list of values.

    Args:
        values: List of numeric values
        default: Value to return if list is empty or invalid

    Returns:
        Average value or default
    """
    if not values:
        return default
    try:
        valid_values = [float(v) for v in values if v is not None]
        if not valid_values:
            return default
        return sum(valid_values) / len(valid_values)
    except (ValueError, TypeError):
        return default


def safe_min(values: List[Any], default: float = 0.0) -> float:
    """Safely get minimum of a list."""
    try:
        valid_values = [float(v) for v in values if v is not None]
        return min(valid_values) if valid_values else default
    except (ValueError, TypeError):
        return default


def safe_max(values: List[Any], default: float = 0.0) -> float:
    """Safely get maximum of a list."""
    try:
        valid_values = [float(v) for v in values if v is not None]
        return max(valid_values) if valid_values else default
    except (ValueError, TypeError):
        return default


# ============================================================
# TEAM NAME NORMALIZATION
# ============================================================

# Common team name variations
TEAM_NAME_ALIASES = {
    # NBA
    'sixers': '76ers',
    'philly': '76ers',
    'philadelphia': '76ers',
    'blazers': 'Trail Blazers',
    'portland': 'Trail Blazers',
    'clips': 'Clippers',
    'la clippers': 'Clippers',
    'la lakers': 'Lakers',
    'los angeles lakers': 'Lakers',
    'los angeles clippers': 'Clippers',
    'okc': 'Thunder',
    'gsw': 'Warriors',
    'golden state': 'Warriors',
    'nyk': 'Knicks',
    'new york': 'Knicks',
    # NHL
    'habs': 'Canadiens',
    'pens': 'Penguins',
    'caps': 'Capitals',
    'bolts': 'Lightning',
    'wings': 'Red Wings',
    'leafs': 'Maple Leafs',
    'sens': 'Senators',
    'nucks': 'Canucks',
    'hawks': 'Blackhawks',
    'preds': 'Predators',
    'avs': 'Avalanche',
    'canes': 'Hurricanes',
    'jackets': 'Blue Jackets',
    'knights': 'Golden Knights',
    'kraken': 'Kraken',
}


def normalize_team_name(name: str, league: str = 'NBA') -> str:
    """
    Normalize team name to standard format.

    Args:
        name: Raw team name
        league: League for context

    Returns:
        Normalized team name
    """
    if not name:
        return ''

    # Clean up the name
    name_lower = name.lower().strip()

    # Remove common prefixes/suffixes
    name_lower = re.sub(r'^(the\s+)', '', name_lower)
    name_lower = re.sub(r'\s+(basketball|hockey|football)$', '', name_lower)

    # Check aliases
    if name_lower in TEAM_NAME_ALIASES:
        return TEAM_NAME_ALIASES[name_lower]

    # Capitalize properly
    return name.strip().title()


# ============================================================
# URL AND STRING UTILITIES
# ============================================================

def sanitize_url_for_logging(url: str) -> str:
    """
    Remove sensitive parameters from URL for safe logging.

    Args:
        url: URL that may contain sensitive params

    Returns:
        URL with sensitive params redacted
    """
    sensitive_params = ['apiKey', 'api_key', 'appid', 'key', 'token', 'secret', 'password']
    result = url
    for param in sensitive_params:
        result = re.sub(
            rf'({param}=)[^&]+',
            r'\1[REDACTED]',
            result,
            flags=re.IGNORECASE
        )
    return result


def sql_escape(value: Any) -> Optional[str]:
    """
    Safely escape a string for SQL insertion (PostgreSQL standard).

    Note: Prefer parameterized queries over this function.

    Args:
        value: Value to escape

    Returns:
        Escaped string or None
    """
    if value is None:
        return None
    # Standard SQL escaping: replace ' with ''
    return str(value).replace("'", "''")


# ============================================================
# PARSING UTILITIES
# ============================================================

def parse_spread_string(spread_str: str) -> Optional[float]:
    """
    Parse spread from various string formats.

    Handles: '-3.5', '+7', '-3½', 'PK', 'EVEN'

    Args:
        spread_str: Spread string to parse

    Returns:
        Float spread value or None
    """
    if not spread_str:
        return None

    spread_str = spread_str.strip().upper()

    # Handle pick/even
    if spread_str in ('PK', 'PICK', 'EVEN', 'EV'):
        return 0.0

    try:
        # Replace half symbol
        spread_str = spread_str.replace('½', '.5')
        spread_str = spread_str.replace('1/2', '.5')

        # Extract numeric portion
        match = re.search(r'([+-]?\d+\.?\d*)', spread_str)
        if match:
            return float(match.group(1))
    except (ValueError, AttributeError):
        pass

    return None


def parse_total_string(total_str: str) -> Optional[float]:
    """
    Parse total from various string formats.

    Handles: '220.5', '220½', 'O220.5', 'U215'

    Args:
        total_str: Total string to parse

    Returns:
        Float total value or None
    """
    if not total_str:
        return None

    total_str = total_str.strip().upper()

    try:
        # Replace half symbol
        total_str = total_str.replace('½', '.5')
        total_str = total_str.replace('1/2', '.5')

        # Remove O/U prefix
        total_str = re.sub(r'^[OU]', '', total_str)

        # Extract numeric portion
        match = re.search(r'(\d+\.?\d*)', total_str)
        if match:
            return float(match.group(1))
    except (ValueError, AttributeError):
        pass

    return None


# ============================================================
# API RESPONSE UTILITIES
# ============================================================

def api_response(
    success: bool,
    data: Any = None,
    error: Optional[str] = None,
    **extra
) -> Dict[str, Any]:
    """
    Create a standardized API response dictionary.

    All API endpoints should use this for consistent response format.

    Args:
        success: Whether the operation succeeded
        data: Response data (optional)
        error: Error message if failed (optional)
        **extra: Additional fields to include

    Returns:
        Standardized response dictionary
    """
    from datetime import datetime
    response = {
        'success': success,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    if data is not None:
        response['data'] = data
    if error is not None:
        response['error'] = error
    response.update(extra)
    return response


# ============================================================
# RETRY UTILITIES
# ============================================================

import time
from functools import wraps


def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying failed operations with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exception types to catch and retry

    Returns:
        Decorated function with retry logic

    Example:
        @retry_on_failure(max_retries=3, delay=1.0, exceptions=(requests.RequestException,))
        def fetch_data():
            return requests.get(url)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = delay * (backoff ** attempt)
                        logger.debug(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                            f"after {sleep_time:.1f}s: {e}"
                        )
                        time.sleep(sleep_time)
            # All retries exhausted
            logger.warning(f"{func.__name__} failed after {max_retries} attempts: {last_exception}")
            raise last_exception
        return wrapper
    return decorator


def validate_odds_value(
    value: Any,
    min_val: float,
    max_val: float,
    field_name: str = 'value'
) -> Optional[float]:
    """
    Validate odds-related value is in acceptable range.

    Args:
        value: Value to validate
        min_val: Minimum acceptable value
        max_val: Maximum acceptable value
        field_name: Name for logging purposes

    Returns:
        Float value if valid, None if invalid
    """
    if value is None:
        return None
    try:
        val = float(value)
        if min_val <= val <= max_val:
            return val
        logger.debug(f"Invalid {field_name}: {val} (outside {min_val}-{max_val} range)")
        return None
    except (ValueError, TypeError):
        return None
