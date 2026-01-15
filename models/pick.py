from datetime import datetime
from models.database import db


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
    game_start = db.Column(db.DateTime)
    opening_line = db.Column(db.Float)
    closing_line = db.Column(db.Float)
    clv = db.Column(db.Float)
    line_moved_favor = db.Column(db.Boolean)
    bet_line = db.Column(db.Float)
    true_edge = db.Column(db.Float)
    kelly_fraction = db.Column(db.Float)
    expected_ev = db.Column(db.Float)
    injury_source = db.Column(db.String(20))
    away_injury_impact = db.Column(db.Float, default=0.0)
    home_injury_impact = db.Column(db.Float, default=0.0)
    lineup_confirmed = db.Column(db.Boolean, default=False)
    injury_check_timestamp = db.Column(db.DateTime)

    __table_args__ = (
        db.Index('idx_pick_result', 'result'),
        db.Index('idx_pick_date_league', 'date', 'league'),
        db.Index('idx_pick_type', 'pick_type'),
        db.Index('idx_date_result', 'date', 'result'),
        db.Index('idx_is_lock_date', 'is_lock', 'date'),
    )
