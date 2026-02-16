"""
ML Models for the 4-Brain Ensemble system.
XGBoost, Elastic Net, Elo ratings, and ensemble logic.
All model classes have graceful degradation when dependencies are missing.
"""
import os
import logging
import pickle
import statistics as stats_module
from datetime import datetime, date
from typing import Optional, Dict, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Safe imports with fallback
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("pandas not installed - training disabled")

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    logger.warning("xgboost not installed - XGBoost models disabled")

try:
    from sklearn.linear_model import ElasticNet
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not installed - Elastic Net models disabled")

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

# =====================================================================
# ENSEMBLE WEIGHTS (configurable per league)
# =====================================================================
DEFAULT_ENSEMBLE_WEIGHTS = {
    'xgb': 0.35, 'glm': 0.20, 'elo': 0.20, 'ppg': 0.25
}

LEAGUE_ENSEMBLE_WEIGHTS = {
    'NBA': {'xgb': 0.35, 'glm': 0.20, 'elo': 0.20, 'ppg': 0.25},
    'CBB': {'xgb': 0.30, 'glm': 0.20, 'elo': 0.15, 'ppg': 0.35},
    'NFL': {'xgb': 0.40, 'glm': 0.20, 'elo': 0.25, 'ppg': 0.15},
    'CFB': {'xgb': 0.35, 'glm': 0.20, 'elo': 0.25, 'ppg': 0.20},
    'NHL': {'xgb': 0.35, 'glm': 0.20, 'elo': 0.20, 'ppg': 0.25},
    'MLB': {'xgb': 0.30, 'glm': 0.20, 'elo': 0.25, 'ppg': 0.25},
}


@dataclass
class EnsemblePrediction:
    """Output of the ensemble model for a single game."""
    spread_pred: Optional[float] = None
    total_pred: Optional[float] = None
    win_prob: Optional[float] = None
    # Component predictions
    xgb_spread: Optional[float] = None
    xgb_total: Optional[float] = None
    xgb_win_prob: Optional[float] = None
    glm_spread: Optional[float] = None
    glm_total: Optional[float] = None
    glm_win_prob: Optional[float] = None
    elo_spread: Optional[float] = None
    elo_total: Optional[float] = None
    elo_win_prob: Optional[float] = None
    ppg_spread: Optional[float] = None
    ppg_total: Optional[float] = None
    # Metadata
    confidence: float = 0.0
    models_used: List[str] = field(default_factory=list)
    degraded: bool = False


# =====================================================================
# ELO RATING SYSTEM
# =====================================================================
class EloSystem:
    """
    Elo rating system per team per league.
    Uses DB-backed EloRating table via deferred imports.
    """
    BASE_K = 20.0
    HOME_ADVANTAGE = {
        'NBA': 3.0, 'CBB': 3.5, 'NFL': 2.5, 'CFB': 3.0, 'NHL': 2.0, 'MLB': 1.5
    }
    # Elo points per game point (calibration factor)
    ELO_PER_POINT = 25.0

    @classmethod
    def get_rating(cls, team: str, league: str, db_session) -> float:
        """Get current Elo rating, returning 1500 default if not found."""
        from sports_app import EloRating
        elo = db_session.query(EloRating).filter_by(
            team=team, league=league
        ).order_by(EloRating.last_updated.desc().nullslast()).first()
        return elo.rating if elo else 1500.0

    @classmethod
    def get_ratings_batch(cls, teams: list, league: str, db_session) -> Dict[str, float]:
        """Get ratings for multiple teams at once (single query)."""
        from sports_app import EloRating
        elos = db_session.query(EloRating).filter(
            EloRating.team.in_(teams),
            EloRating.league == league
        ).all()
        result = {e.team: e.rating for e in elos}
        for team in teams:
            if team not in result:
                result[team] = 1500.0
        return result

    @classmethod
    def expected_score(cls, rating_a: float, rating_b: float) -> float:
        """Expected score for team A vs team B (0.0 to 1.0)."""
        return 1.0 / (1.0 + 10 ** ((rating_b - rating_a) / 400))

    @classmethod
    def predict_spread(cls, away_rating: float, home_rating: float, league: str) -> float:
        """
        Predict spread from Elo ratings.
        Returns negative value = home favored (standard spread convention).
        """
        home_adv_elo = cls.HOME_ADVANTAGE.get(league, 3.0) * cls.ELO_PER_POINT
        elo_diff = (home_rating + home_adv_elo) - away_rating
        spread = -(elo_diff / cls.ELO_PER_POINT)
        return round(spread, 1)

    @classmethod
    def predict_win_prob(cls, away_rating: float, home_rating: float, league: str) -> float:
        """Home win probability from Elo."""
        home_adv_elo = cls.HOME_ADVANTAGE.get(league, 3.0) * cls.ELO_PER_POINT
        return cls.expected_score(home_rating + home_adv_elo, away_rating)

    @classmethod
    def update_after_game(cls, away_team: str, home_team: str,
                          away_score: float, home_score: float,
                          league: str, game_date: date, db_session):
        """Update Elo ratings after a completed game."""
        from sports_app import EloRating

        away_elo = cls.get_rating(away_team, league, db_session)
        home_elo = cls.get_rating(home_team, league, db_session)
        home_adv_elo = cls.HOME_ADVANTAGE.get(league, 3.0) * cls.ELO_PER_POINT

        # Actual result (from home perspective)
        if home_score > away_score:
            actual_home = 1.0
        elif home_score < away_score:
            actual_home = 0.0
        else:
            actual_home = 0.5

        expected_home = cls.expected_score(home_elo + home_adv_elo, away_elo)

        # Margin-of-victory multiplier (sqrt scaling, capped)
        margin = abs(home_score - away_score)
        mov_mult = min(2.5, max(1.0, (margin ** 0.5) * 0.5))

        k = cls.BASE_K * mov_mult

        new_home = home_elo + k * (actual_home - expected_home)
        new_away = away_elo + k * ((1 - actual_home) - (1 - expected_home))

        # Determine season string
        year = game_date.year
        month = game_date.month
        if month >= 7:
            season = f"{year}-{year + 1}"
        else:
            season = f"{year - 1}-{year}"

        # Upsert both teams
        for team, new_rating in [(home_team, new_home), (away_team, new_away)]:
            existing = db_session.query(EloRating).filter_by(
                team=team, league=league, season=season).first()
            if existing:
                existing.rating = round(new_rating, 1)
                existing.games_played += 1
                existing.last_updated = game_date
                if new_rating > (existing.peak_rating or 1500):
                    existing.peak_rating = round(new_rating, 1)
            else:
                new_elo = EloRating(
                    team=team, league=league,
                    rating=round(new_rating, 1),
                    games_played=1, last_updated=game_date,
                    season=season, peak_rating=round(new_rating, 1)
                )
                db_session.add(new_elo)

    @classmethod
    def get_all_ratings(cls, league: str, db_session) -> List[Dict]:
        """Get all team ratings for a league, sorted by rating desc."""
        from sports_app import EloRating
        elos = db_session.query(EloRating).filter_by(
            league=league
        ).order_by(EloRating.rating.desc()).all()
        return [
            {'team': e.team, 'rating': e.rating, 'games': e.games_played,
             'peak': e.peak_rating, 'updated': str(e.last_updated) if e.last_updated else None}
            for e in elos
        ]


# =====================================================================
# XGBOOST MODEL WRAPPER
# =====================================================================
class XGBModel:
    """XGBoost model wrapper for spread/total/win prediction."""

    @staticmethod
    def available() -> bool:
        return XGB_AVAILABLE and PANDAS_AVAILABLE

    def __init__(self, league: str, target: str):
        self.league = league
        self.target = target  # 'spread', 'total', or 'win_prob'
        self.model = None
        self.feature_names = None
        self.trained = False

    def train(self, X: 'pd.DataFrame', y: 'pd.Series') -> Dict:
        """Train XGBoost model. Returns training metadata dict."""
        if not self.available():
            return {'error': 'xgboost not available'}

        if len(X) < 10:
            return {'error': f'Too few samples: {len(X)}'}

        is_classifier = (self.target == 'win_prob')
        params = {
            'max_depth': 4,
            'learning_rate': 0.05,
            'n_estimators': 200,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 5,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
        }

        if is_classifier:
            params['objective'] = 'binary:logistic'
            params['eval_metric'] = 'logloss'
        else:
            params['objective'] = 'reg:squarederror'

        self.feature_names = list(X.columns)
        X_filled = X.fillna(0)

        if is_classifier:
            self.model = xgb.XGBClassifier(**params)
        else:
            self.model = xgb.XGBRegressor(**params)

        self.model.fit(X_filled, y)
        self.trained = True

        # Feature importance
        importance = dict(zip(self.feature_names,
                              self.model.feature_importances_.tolist()))
        top_features = dict(sorted(importance.items(),
                                   key=lambda x: x[1], reverse=True)[:20])

        # Cross-val score
        cv_scores = cross_val_score(
            self.model, X_filled, y,
            cv=min(5, max(2, len(X) // 20)),
            scoring='neg_mean_absolute_error' if not is_classifier else 'accuracy'
        )

        result = {
            'samples': len(X),
            'features': len(self.feature_names),
            'feature_importance': top_features,
            'cv_mean': float(abs(cv_scores.mean())),
            'cv_std': float(cv_scores.std()),
        }

        logger.info(f"XGB {self.league}/{self.target}: trained on {len(X)} samples, "
                     f"CV={result['cv_mean']:.3f} +/- {result['cv_std']:.3f}")
        return result

    def predict(self, features: Dict[str, float]) -> Optional[float]:
        """Predict from a feature dict. Returns None if not trained."""
        if not self.trained or not self.model or not self.feature_names:
            return None
        try:
            row = {fn: features.get(fn, 0) for fn in self.feature_names}
            X = pd.DataFrame([row])
            if self.target == 'win_prob':
                pred = self.model.predict_proba(X)[0][1]
            else:
                pred = self.model.predict(X)[0]
            return round(float(pred), 3)
        except Exception as e:
            logger.error(f"XGB predict error ({self.league}/{self.target}): {e}")
            return None

    def save(self) -> str:
        """Save model to disk. Returns file path."""
        os.makedirs(MODEL_DIR, exist_ok=True)
        path = os.path.join(MODEL_DIR, f'xgb_{self.league}_{self.target}.pkl')
        with open(path, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'features': self.feature_names,
                'league': self.league,
                'target': self.target,
                'saved_at': datetime.utcnow().isoformat()
            }, f)
        logger.info(f"Saved XGB model: {path}")
        return path

    def load(self) -> bool:
        """Load model from disk. Returns True if successful."""
        path = os.path.join(MODEL_DIR, f'xgb_{self.league}_{self.target}.pkl')
        if not os.path.exists(path):
            return False
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
            self.model = data['model']
            self.feature_names = data['features']
            self.trained = True
            logger.info(f"Loaded XGB model: {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load XGB model {path}: {e}")
            return False


# =====================================================================
# ELASTIC NET MODEL WRAPPER
# =====================================================================
class GLMModel:
    """Elastic Net (GLM) model - linear baseline for ensemble."""

    @staticmethod
    def available() -> bool:
        return SKLEARN_AVAILABLE and PANDAS_AVAILABLE

    def __init__(self, league: str, target: str):
        self.league = league
        self.target = target
        self.model = None
        self.scaler = None
        self.feature_names = None
        self.trained = False

    def train(self, X: 'pd.DataFrame', y: 'pd.Series') -> Dict:
        """Train Elastic Net model. Returns training metadata."""
        if not self.available():
            return {'error': 'scikit-learn not available'}

        if len(X) < 10:
            return {'error': f'Too few samples: {len(X)}'}

        self.feature_names = list(X.columns)
        X_filled = X.fillna(0)

        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X_filled)

        self.model = ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=2000, random_state=42)
        self.model.fit(X_scaled, y)
        self.trained = True

        # Coefficient importance
        coefs = dict(zip(self.feature_names,
                         [round(abs(float(c)), 4) for c in self.model.coef_]))
        top_coefs = dict(sorted(coefs.items(),
                                key=lambda x: x[1], reverse=True)[:20])

        # Cross-val score
        cv_scores = cross_val_score(
            ElasticNet(alpha=0.1, l1_ratio=0.5, max_iter=2000),
            X_scaled, y,
            cv=min(5, max(2, len(X) // 20)),
            scoring='neg_mean_absolute_error'
        )

        result = {
            'samples': len(X),
            'features': len(self.feature_names),
            'top_coefficients': top_coefs,
            'cv_mae': float(abs(cv_scores.mean())),
            'cv_std': float(cv_scores.std()),
        }

        logger.info(f"GLM {self.league}/{self.target}: trained on {len(X)} samples, "
                     f"CV MAE={result['cv_mae']:.3f}")
        return result

    def predict(self, features: Dict[str, float]) -> Optional[float]:
        """Predict from a feature dict. Returns None if not trained."""
        if not self.trained or not self.model or not self.feature_names:
            return None
        try:
            row = {fn: features.get(fn, 0) for fn in self.feature_names}
            X = pd.DataFrame([row])
            X_scaled = self.scaler.transform(X)
            pred = self.model.predict(X_scaled)[0]
            return round(float(pred), 3)
        except Exception as e:
            logger.error(f"GLM predict error ({self.league}/{self.target}): {e}")
            return None

    def save(self) -> str:
        """Save model + scaler to disk."""
        os.makedirs(MODEL_DIR, exist_ok=True)
        path = os.path.join(MODEL_DIR, f'glm_{self.league}_{self.target}.pkl')
        with open(path, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'scaler': self.scaler,
                'features': self.feature_names,
                'league': self.league,
                'target': self.target,
                'saved_at': datetime.utcnow().isoformat()
            }, f)
        logger.info(f"Saved GLM model: {path}")
        return path

    def load(self) -> bool:
        """Load model from disk. Returns True if successful."""
        path = os.path.join(MODEL_DIR, f'glm_{self.league}_{self.target}.pkl')
        if not os.path.exists(path):
            return False
        try:
            with open(path, 'rb') as f:
                data = pickle.load(f)
            self.model = data['model']
            self.scaler = data['scaler']
            self.feature_names = data['features']
            self.trained = True
            logger.info(f"Loaded GLM model: {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load GLM model {path}: {e}")
            return False


# =====================================================================
# ENSEMBLE ORCHESTRATOR
# =====================================================================
def _weighted_avg(preds: Dict[str, Optional[float]],
                  weights: Dict[str, float]) -> Optional[float]:
    """Weighted average, skipping None values and renormalizing."""
    valid = [(preds[k], weights.get(k, 0)) for k in preds
             if preds.get(k) is not None and weights.get(k, 0) > 0]
    if not valid:
        return None
    total_w = sum(w for _, w in valid)
    if total_w == 0:
        return None
    return round(sum(v * w for v, w in valid) / total_w, 2)


class EnsemblePredictor:
    """
    Orchestrates XGB + GLM + Elo + PPG into weighted ensemble.
    Handles graceful degradation when models are unavailable.
    """

    def __init__(self):
        self.xgb_models: Dict[str, Dict[str, XGBModel]] = {}
        self.glm_models: Dict[str, Dict[str, GLMModel]] = {}
        self._loaded = False

    def load_all_models(self):
        """Attempt to load all saved models from disk."""
        leagues = ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']
        targets = ['spread', 'total', 'win_prob']

        for league in leagues:
            self.xgb_models[league] = {}
            self.glm_models[league] = {}

            for target in targets:
                if XGB_AVAILABLE:
                    xgb_m = XGBModel(league, target)
                    if xgb_m.load():
                        self.xgb_models[league][target] = xgb_m

                if SKLEARN_AVAILABLE:
                    glm_m = GLMModel(league, target)
                    if glm_m.load():
                        self.glm_models[league][target] = glm_m

        self._loaded = True
        xgb_count = sum(len(v) for v in self.xgb_models.values())
        glm_count = sum(len(v) for v in self.glm_models.values())
        logger.info(f"Ensemble loaded: {xgb_count} XGB models, {glm_count} GLM models")

    def predict(self, features: Dict[str, float], league: str,
                elo_away: float, elo_home: float,
                ppg_total: Optional[float] = None,
                ppg_margin: Optional[float] = None) -> EnsemblePrediction:
        """
        Generate ensemble prediction with graceful degradation.
        If a model is unavailable, its weight is redistributed to others.
        """
        weights = dict(LEAGUE_ENSEMBLE_WEIGHTS.get(league, DEFAULT_ENSEMBLE_WEIGHTS))
        pred = EnsemblePrediction()

        # XGBoost predictions
        xgb_league = self.xgb_models.get(league, {})
        if 'spread' in xgb_league:
            pred.xgb_spread = xgb_league['spread'].predict(features)
        if 'total' in xgb_league:
            pred.xgb_total = xgb_league['total'].predict(features)
        if 'win_prob' in xgb_league:
            pred.xgb_win_prob = xgb_league['win_prob'].predict(features)

        has_xgb = pred.xgb_spread is not None or pred.xgb_total is not None
        if has_xgb:
            pred.models_used.append('xgb')

        # GLM predictions
        glm_league = self.glm_models.get(league, {})
        if 'spread' in glm_league:
            pred.glm_spread = glm_league['spread'].predict(features)
        if 'total' in glm_league:
            pred.glm_total = glm_league['total'].predict(features)
        if 'win_prob' in glm_league:
            pred.glm_win_prob = glm_league['win_prob'].predict(features)

        has_glm = pred.glm_spread is not None or pred.glm_total is not None
        if has_glm:
            pred.models_used.append('glm')

        # Elo predictions (always available)
        pred.elo_spread = EloSystem.predict_spread(elo_away, elo_home, league)
        pred.elo_win_prob = round(EloSystem.predict_win_prob(elo_away, elo_home, league), 3)
        pred.elo_total = None  # Elo doesn't predict totals
        pred.models_used.append('elo')

        # PPG baseline (always available if stats exist)
        pred.ppg_spread = round(ppg_margin, 1) if ppg_margin is not None else None
        pred.ppg_total = round(ppg_total, 1) if ppg_total is not None else None
        if ppg_total is not None:
            pred.models_used.append('ppg')

        # Determine available models and redistribute weights
        available = {}
        if has_xgb:
            available['xgb'] = weights['xgb']
        if has_glm:
            available['glm'] = weights['glm']
        available['elo'] = weights['elo']
        if ppg_total is not None:
            available['ppg'] = weights['ppg']

        if not available:
            pred.degraded = True
            return pred

        # Normalize weights to sum to 1.0
        total_w = sum(available.values())
        norm_weights = {k: v / total_w for k, v in available.items()}

        if len(available) < 4:
            pred.degraded = True

        # Weighted ensemble for spread
        spread_preds = {
            'xgb': pred.xgb_spread, 'glm': pred.glm_spread,
            'elo': pred.elo_spread, 'ppg': pred.ppg_spread
        }
        pred.spread_pred = _weighted_avg(spread_preds, norm_weights)

        # Weighted ensemble for total (exclude Elo - it doesn't predict totals)
        total_preds = {
            'xgb': pred.xgb_total, 'glm': pred.glm_total, 'ppg': pred.ppg_total
        }
        total_weights = {k: v for k, v in norm_weights.items() if k != 'elo'}
        tw_sum = sum(total_weights.values())
        if tw_sum > 0:
            total_weights = {k: v / tw_sum for k, v in total_weights.items()}
        pred.total_pred = _weighted_avg(total_preds, total_weights)

        # Win probability ensemble
        wp_preds = {
            'xgb': pred.xgb_win_prob, 'glm': pred.glm_win_prob,
            'elo': pred.elo_win_prob
        }
        pred.win_prob = _weighted_avg(wp_preds, norm_weights)

        # Confidence: lower std deviation across models = higher confidence
        spread_vals = [v for v in [pred.xgb_spread, pred.glm_spread,
                                    pred.elo_spread, pred.ppg_spread]
                       if v is not None]
        if len(spread_vals) >= 2:
            std = stats_module.stdev(spread_vals)
            pred.confidence = round(max(0, min(95, 95 - std * 5)), 1)
        elif len(spread_vals) == 1:
            pred.confidence = 30.0
        else:
            pred.confidence = 0.0

        return pred

    def get_model_status(self) -> Dict:
        """Return status of all loaded models."""
        status = {
            'loaded': self._loaded,
            'xgb_available': XGB_AVAILABLE,
            'glm_available': SKLEARN_AVAILABLE,
            'models': {}
        }
        for league in ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']:
            xgb_targets = list(self.xgb_models.get(league, {}).keys())
            glm_targets = list(self.glm_models.get(league, {}).keys())
            if xgb_targets or glm_targets:
                status['models'][league] = {
                    'xgb': xgb_targets,
                    'glm': glm_targets
                }
        return status


# Singleton instance
ensemble_predictor = EnsemblePredictor()
