"""
Feature engineering for the 4-Brain AI Ensemble system.
Extracts features from Game objects for ML models and brain analysis.
No imports from sports_app.py to avoid circular dependencies.
"""
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Feature groups for brain routing
STATISTICIAN_FEATURES = [
    'away_ppg', 'home_ppg', 'away_opp_ppg', 'home_opp_ppg',
    'projected_total', 'projected_margin', 'expected_away', 'expected_home',
    'torvik_away_adj_o', 'torvik_away_adj_d', 'torvik_home_adj_o', 'torvik_home_adj_d',
    'torvik_tempo', 'kenpom_away_efg', 'kenpom_home_efg',
    'kenpom_away_to', 'kenpom_home_to', 'kenpom_away_or', 'kenpom_home_or',
    'kenpom_away_ft_rate', 'kenpom_home_ft_rate',
    'kenpom_away_3pt', 'kenpom_home_3pt', 'kenpom_away_2pt', 'kenpom_home_2pt',
    'kenpom_away_d_efg', 'kenpom_home_d_efg',
    'kenpom_away_height', 'kenpom_home_height', 'kenpom_away_exp', 'kenpom_home_exp',
    'kenpom_away_sos', 'kenpom_home_sos',
    'elo_away', 'elo_home', 'elo_diff',
    'ppg_total', 'ppg_diff',
]

SHARP_FEATURES = [
    'line', 'spread_line', 'opening_spread', 'opening_total',
    'closed_spread', 'closed_total',
    'away_tickets_pct', 'home_tickets_pct', 'away_money_pct', 'home_money_pct',
    'over_tickets_pct', 'under_tickets_pct', 'over_money_pct', 'under_money_pct',
    'rlm_detected', 'rlm_confidence', 'totals_rlm_detected', 'totals_rlm_confidence',
    'true_edge', 'vig_percentage', 'fair_probability', 'kelly_fraction',
    'total_ev', 'spread_ev',
    'spread_line_movement', 'total_line_movement',
    'money_ticket_divergence_away', 'money_ticket_divergence_home',
    'is_qualified', 'direction', 'spread_direction',
    'away_is_favorite',
]

SCOUT_FEATURES = [
    'days_rest_away', 'days_rest_home',
    'is_back_to_back_away', 'is_back_to_back_home',
    'travel_distance', 'situational_adjustment',
    'rest_advantage',
    'is_home_b2b_opponent_rested',
    'def_mismatch', 'def_rank_away', 'def_rank_home',
    'away_is_favorite',
]

HISTORIAN_FEATURES = [
    'away_ou_pct', 'home_ou_pct', 'h2h_ou_pct',
    'away_spread_pct', 'home_spread_pct', 'h2h_spread_pct',
    'history_sample_size',
    'ou_l5', 'ou_l10', 'ou_l20', 'ou_hit_rate', 'ou_hit_rate_qualified',
]

# All direct attributes to extract from Game object
_DIRECT_ATTRS = [
    'away_ppg', 'home_ppg', 'away_opp_ppg', 'home_opp_ppg',
    'projected_total', 'projected_margin', 'expected_away', 'expected_home',
    'line', 'spread_line', 'opening_spread', 'opening_total',
    'closed_spread', 'closed_total',
    'away_tickets_pct', 'home_tickets_pct', 'away_money_pct', 'home_money_pct',
    'over_tickets_pct', 'under_tickets_pct', 'over_money_pct', 'under_money_pct',
    'true_edge', 'vig_percentage', 'fair_probability', 'kelly_fraction',
    'total_ev', 'spread_ev',
    'days_rest_away', 'days_rest_home', 'travel_distance', 'situational_adjustment',
    'away_ou_pct', 'home_ou_pct', 'h2h_ou_pct',
    'away_spread_pct', 'home_spread_pct', 'h2h_spread_pct',
    'history_sample_size', 'rlm_confidence', 'totals_rlm_confidence',
    # KenPom (CBB only - None for other leagues)
    'torvik_tempo', 'torvik_away_adj_o', 'torvik_away_adj_d',
    'torvik_home_adj_o', 'torvik_home_adj_d',
    'kenpom_away_efg', 'kenpom_home_efg', 'kenpom_away_to', 'kenpom_home_to',
    'kenpom_away_or', 'kenpom_home_or', 'kenpom_away_ft_rate', 'kenpom_home_ft_rate',
    'kenpom_away_3pt', 'kenpom_home_3pt', 'kenpom_away_2pt', 'kenpom_home_2pt',
    'kenpom_away_d_efg', 'kenpom_home_d_efg', 'kenpom_away_d_to', 'kenpom_home_d_to',
    'kenpom_away_height', 'kenpom_home_height',
    'kenpom_away_exp', 'kenpom_home_exp',
    'kenpom_away_sos', 'kenpom_home_sos',
    'edge', 'spread_edge',
    'ou_l5', 'ou_l10', 'ou_l20', 'ou_hit_rate',
    'def_rank_away', 'def_rank_home',
]

# PREDICTION FEATURES: Available at prediction time (no closing line data)
# These features should be used for live predictions and model training
PREDICTION_FEATURES = [
    'away_ppg', 'home_ppg', 'away_opp_ppg', 'home_opp_ppg',
    'elo_away', 'elo_home', 'elo_diff',
    'days_rest_away', 'days_rest_home', 'is_back_to_back_away', 'is_back_to_back_home',
    'travel_distance', 'rest_advantage',
    'away_tickets_pct', 'home_tickets_pct', 'away_money_pct', 'home_money_pct',
    'over_money_pct', 'under_money_pct',
    'away_ou_pct', 'home_ou_pct', 'h2h_ou_pct',
    'away_spread_pct', 'home_spread_pct', 'h2h_spread_pct',
    'ppg_total', 'ppg_diff',
    # KenPom (CBB) - will be NaN for other leagues, handled by fillna
    'torvik_tempo', 'torvik_away_adj_o', 'torvik_away_adj_d',
    'torvik_home_adj_o', 'torvik_home_adj_d',
    'kenpom_away_efg', 'kenpom_home_efg',
    'kenpom_away_to', 'kenpom_home_to',
    'kenpom_away_or', 'kenpom_home_or',
    'kenpom_away_3pt', 'kenpom_home_3pt',
    'kenpom_away_d_efg', 'kenpom_home_d_efg',
    'kenpom_away_height', 'kenpom_home_height',
    'kenpom_away_exp', 'kenpom_home_exp',
    'kenpom_away_sos', 'kenpom_home_sos',
    # NBA pace features (for pace-adjusted projections)
    'away_pace', 'home_pace', 'away_off_eff', 'home_off_eff', 'away_def_eff', 'home_def_eff',
]

# POST-CLOSE FEATURES: Include closing line derived features for analysis (not live predictions)
# These features create data leakage if used for live predictions since they depend on final market state
POST_CLOSE_FEATURES = PREDICTION_FEATURES + [
    'rlm_detected', 'totals_rlm_detected',  # RLM detection happens post-close
    'spread_line_movement', 'total_line_movement',  # Final line movement
    'money_ticket_divergence_away', 'money_ticket_divergence_home',  # Final divergence
    'closed_spread', 'closed_total',  # Closing lines
    'opening_spread', 'opening_total',  # Opening lines (for CLV calculation)
    'early_spread_movement', 'late_spread_movement',  # Time-weighted movements
    'early_total_movement', 'late_total_movement',  # Time-weighted movements  
    'pinnacle_spread', 'pinnacle_total',  # Pinnacle lines for validation
    'draftkings_spread', 'fanduel_spread', 'betmgm_spread', 'caesars_spread',  # Multi-book data
    'draftkings_total', 'fanduel_total', 'betmgm_total', 'caesars_total',  # Multi-book data
]

# Backward compatibility: Default to PREDICTION_FEATURES to avoid leakage
ML_FEATURE_COLS = PREDICTION_FEATURES


@dataclass
class FeatureVector:
    """Complete feature vector for a single game."""
    game_id: int
    league: str
    features: Dict[str, Optional[float]]
    statistician_features: Dict[str, Optional[float]] = field(default_factory=dict)
    sharp_features: Dict[str, Optional[float]] = field(default_factory=dict)
    scout_features: Dict[str, Optional[float]] = field(default_factory=dict)
    historian_features: Dict[str, Optional[float]] = field(default_factory=dict)
    feature_completeness: float = 0.0
    ml_ready: bool = False


def _safe_float(val) -> Optional[float]:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_subtract(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Subtract b from a, returning None if either is None."""
    if a is not None and b is not None:
        return a - b
    return None


def extract_features(game, elo_away: float = 1500.0, elo_home: float = 1500.0) -> FeatureVector:
    """
    Extract all features from a Game ORM object.

    Args:
        game: Game SQLAlchemy model instance
        elo_away: Current Elo rating for away team
        elo_home: Current Elo rating for home team

    Returns:
        FeatureVector with all feature groups populated
    """
    features = {}

    # Direct attribute extraction
    for attr in _DIRECT_ATTRS:
        features[attr] = _safe_float(getattr(game, attr, None))

    # Booleans to float
    features['rlm_detected'] = 1.0 if getattr(game, 'rlm_detected', False) else 0.0
    features['totals_rlm_detected'] = 1.0 if getattr(game, 'totals_rlm_detected', False) else 0.0
    features['is_back_to_back_away'] = 1.0 if getattr(game, 'is_back_to_back_away', False) else 0.0
    features['is_back_to_back_home'] = 1.0 if getattr(game, 'is_back_to_back_home', False) else 0.0

    # App-specific boolean/string attrs
    features['is_qualified'] = 1.0 if getattr(game, 'is_qualified', False) else 0.0
    features['ou_hit_rate_qualified'] = 1.0 if getattr(game, 'ou_hit_rate_qualified', False) else 0.0
    features['def_mismatch'] = 1.0 if getattr(game, 'def_mismatch', False) else 0.0
    features['away_is_favorite'] = 1.0 if getattr(game, 'away_is_favorite', False) else 0.0
    # direction: 'O'/'U'/None -> 1.0/−1.0/0.0
    _dir = getattr(game, 'direction', None)
    features['direction'] = 1.0 if _dir == 'O' else (-1.0 if _dir == 'U' else 0.0)
    _sdir = getattr(game, 'spread_direction', None)
    features['spread_direction'] = 1.0 if _sdir == 'HOME' else (-1.0 if _sdir == 'AWAY' else 0.0)

    # Elo features (injected from caller)
    features['elo_away'] = elo_away
    features['elo_home'] = elo_home
    features['elo_diff'] = elo_home - elo_away  # positive = home favored

    # Derived features
    features['spread_line_movement'] = _safe_subtract(
        features.get('closed_spread'), features.get('opening_spread'))
    features['total_line_movement'] = _safe_subtract(
        features.get('closed_total'), features.get('opening_total'))
    features['money_ticket_divergence_away'] = _safe_subtract(
        features.get('away_money_pct'), features.get('away_tickets_pct'))
    features['money_ticket_divergence_home'] = _safe_subtract(
        features.get('home_money_pct'), features.get('home_tickets_pct'))
    features['rest_advantage'] = _safe_subtract(
        features.get('days_rest_home'), features.get('days_rest_away'))

    # B2B mismatch detection
    b2b_home = features.get('is_back_to_back_home', 0)
    rest_away = features.get('days_rest_away')
    features['is_home_b2b_opponent_rested'] = (
        1.0 if (b2b_home == 1.0 and rest_away is not None and rest_away >= 2) else 0.0
    )

    # PPG-derived
    away_ppg = features.get('away_ppg')
    home_ppg = features.get('home_ppg')
    if away_ppg is not None and home_ppg is not None:
        features['ppg_total'] = away_ppg + home_ppg
        features['ppg_diff'] = home_ppg - away_ppg
    else:
        features['ppg_total'] = None
        features['ppg_diff'] = None

    # Split into brain groups
    stat_feats = {k: features.get(k) for k in STATISTICIAN_FEATURES if k in features}
    sharp_feats = {k: features.get(k) for k in SHARP_FEATURES if k in features}
    scout_feats = {k: features.get(k) for k in SCOUT_FEATURES if k in features}
    hist_feats = {k: features.get(k) for k in HISTORIAN_FEATURES if k in features}

    # Completeness
    total = len(features)
    non_null = sum(1 for v in features.values() if v is not None)
    completeness = non_null / total if total > 0 else 0.0

    # ML readiness: need at minimum PPG data + a line
    ml_ready = all(features.get(k) is not None for k in
                   ['away_ppg', 'home_ppg', 'away_opp_ppg', 'home_opp_ppg'])

    return FeatureVector(
        game_id=game.id,
        league=getattr(game, 'league', 'UNKNOWN'),
        features=features,
        statistician_features=stat_feats,
        sharp_features=sharp_feats,
        scout_features=scout_feats,
        historian_features=hist_feats,
        feature_completeness=round(completeness, 3),
        ml_ready=ml_ready
    )


def get_ml_features(feature_vector: FeatureVector) -> Dict[str, float]:
    """
    Extract the ML-ready feature subset from a FeatureVector.
    Returns only the columns used for XGB/GLM training, with None replaced by 0.
    """
    return {col: (feature_vector.features.get(col) or 0.0) for col in ML_FEATURE_COLS}
