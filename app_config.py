"""
CONFIGURATION - SINGLE SOURCE OF TRUTH
All thresholds, settings, and constants in one place.
"""

THRESHOLDS = {
    'NBA': {
        'total_edge': 8.0,
        'spread_edge': 3.5,
        'min_ev': 3.0,
        'max_spread': 10.0,
        'min_handle_avoid': 80,
        'bad_defense_threshold': 27,
        'sharp_threshold': 10.0,
        'rlm_movement_threshold': 0.5,
        'min_historical_rate': 0.85,
        'min_sample_size': 10,
    },
    'CBB': {
        'total_edge': 8.0,
        'spread_edge': 3.5,
        'min_ev': 3.0,
        'max_spread': 10.0,
        'min_handle_avoid': 80,
        'bad_defense_threshold': 300,
        'sharp_threshold': 10.0,
        'rlm_movement_threshold': 0.5,
        'min_historical_rate': 0.85,
        'min_sample_size': 10,
    },
    'NHL': {
        'total_edge': 0.5,
        'spread_edge': 0.5,
        'min_ev': 2.5,
        'max_spread': 2.0,
        'min_handle_avoid': 80,
        'bad_defense_threshold': 27,
        'sharp_threshold': 10.0,
        'rlm_movement_threshold': 0.5,
        'min_historical_rate': 0.85,
        'min_sample_size': 10,
    }
}

CACHE_TTL = {
    'dashboard': 30,
    'team_stats': 3600,
    'historical': 21600,
    'wagertalk': 180,
}

def get_threshold(league, key, default=None):
    return THRESHOLDS.get(league, {}).get(key, default)
