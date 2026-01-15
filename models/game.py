from datetime import datetime
from sqlalchemy.orm import validates
from models.database import db


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
    away_ou_pct = db.Column(db.Float)
    home_ou_pct = db.Column(db.Float)
    away_spread_pct = db.Column(db.Float)
    home_spread_pct = db.Column(db.Float)
    h2h_ou_pct = db.Column(db.Float)
    h2h_spread_pct = db.Column(db.Float)
    history_qualified = db.Column(db.Boolean, default=None)
    spread_history_qualified = db.Column(db.Boolean, default=None)
    history_sample_size = db.Column(db.Integer)
    bovada_total_odds = db.Column(db.Integer)
    pinnacle_total_odds = db.Column(db.Integer)
    bovada_spread_odds = db.Column(db.Integer)
    pinnacle_spread_odds = db.Column(db.Integer)
    total_ev = db.Column(db.Float)
    spread_ev = db.Column(db.Float)
    nba_1h_ml_odds = db.Column(db.Integer)
    nba_1h_ml_qualified = db.Column(db.Boolean, default=False)
    nba_1h_away_win_pct = db.Column(db.Float)
    nba_1h_h2h_win_pct = db.Column(db.Float)
    nba_1h_history_qualified = db.Column(db.Boolean, default=None)
    true_edge = db.Column(db.Float)
    true_line = db.Column(db.Float)
    vig_percentage = db.Column(db.Float)
    market_shade = db.Column(db.String(10))
    kelly_fraction = db.Column(db.Float)
    recommended_bet_size = db.Column(db.Float)
    clv_predicted = db.Column(db.Float)
    sharp_money_side = db.Column(db.String(10))
    fair_probability = db.Column(db.Float)
    probability_edge = db.Column(db.Float)
    days_rest_away = db.Column(db.Integer)
    days_rest_home = db.Column(db.Integer)
    is_back_to_back_away = db.Column(db.Boolean, default=False)
    is_back_to_back_home = db.Column(db.Boolean, default=False)
    travel_distance = db.Column(db.Float)
    situational_adjustment = db.Column(db.Float, default=0.0)
    steam_alert = db.Column(db.Boolean, default=False)
    pinnacle_edge = db.Column(db.Float)
    fair_line = db.Column(db.Float)
    market_balance = db.Column(db.String(20))

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
