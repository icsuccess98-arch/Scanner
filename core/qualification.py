import logging
from dataclasses import dataclass
from typing import Optional, List
from core.edge import SharpThresholds

logger = logging.getLogger(__name__)


@dataclass
class EdgeResult:
    qualified: bool
    direction: Optional[str]
    true_edge: float
    raw_edge: float
    fair_line: float
    posted_line: float
    vig_pct: float
    market_balance: str
    confidence: str


@dataclass
class QualificationResult:
    qualified: bool
    confidence: str
    bet_size_pct: float
    true_edge: float
    ev_pct: Optional[float]
    reasons_pass: List[str]
    reasons_fail: List[str]
    recommendation: str


class ProfessionalQualifier:
    MIN_EV = 1.0
    MIN_KELLY = 0.02
    MIN_WIN_RATE = 0.58
    MIN_SAMPLE_SIZE = 10
    MAX_VIG = 8.0
    FRACTIONAL_KELLY = 0.25
    MAX_BET_SIZE = 0.10

    @classmethod
    def qualify_pick(cls, edge_result: EdgeResult, ev_data: Optional[dict],
                     historical_win_rate: float, sample_size: int,
                     vig_pct: float, league: str) -> QualificationResult:
        reasons_pass = []
        reasons_fail = []

        min_edge = SharpThresholds.MIN_TRUE_EDGE.get(league, 3.5)

        if edge_result.qualified:
            reasons_pass.append(f"True Edge: {edge_result.true_edge:.1f} >= {min_edge:.1f}")
        else:
            reasons_fail.append(f"True Edge: {edge_result.true_edge:.1f} < {min_edge:.1f}")

        if ev_data is None:
            reasons_pass.append("EV: No Pinnacle data (allowed)")
            ev_pct = None
        elif ev_data['ev_pct'] >= cls.MIN_EV:
            reasons_pass.append(f"EV: +{ev_data['ev_pct']:.2f}% >= +{cls.MIN_EV:.1f}%")
            ev_pct = ev_data['ev_pct']
        else:
            reasons_fail.append(f"EV: {ev_data['ev_pct']:+.2f}% < +{cls.MIN_EV:.1f}%")
            ev_pct = ev_data['ev_pct']

        kelly = ev_data['kelly'] if ev_data else 0
        if kelly >= cls.MIN_KELLY:
            reasons_pass.append(f"Kelly: {kelly*100:.2f}% >= {cls.MIN_KELLY*100:.1f}%")
        else:
            reasons_fail.append(f"Kelly: {kelly*100:.2f}% < {cls.MIN_KELLY*100:.1f}%")

        if historical_win_rate >= cls.MIN_WIN_RATE:
            reasons_pass.append(f"History: {historical_win_rate:.1%} >= {cls.MIN_WIN_RATE:.1%}")
        else:
            reasons_fail.append(f"History: {historical_win_rate:.1%} < {cls.MIN_WIN_RATE:.1%}")

        if sample_size >= cls.MIN_SAMPLE_SIZE:
            reasons_pass.append(f"Sample: {sample_size} games >= {cls.MIN_SAMPLE_SIZE}")
        else:
            reasons_fail.append(f"Sample: {sample_size} games < {cls.MIN_SAMPLE_SIZE}")

        if vig_pct <= cls.MAX_VIG:
            reasons_pass.append(f"Vig: {vig_pct:.1f}% <= {cls.MAX_VIG:.1f}%")
        else:
            reasons_fail.append(f"Vig: {vig_pct:.1f}% > {cls.MAX_VIG:.1f}%")

        qualified = len(reasons_fail) == 0

        if qualified and kelly > 0:
            bet_size = min(kelly * cls.FRACTIONAL_KELLY, cls.MAX_BET_SIZE)
        else:
            bet_size = 0.0

        if not qualified:
            confidence = 'NONE'
            recommendation = 'PASS'
        elif (edge_result.confidence == 'ELITE' and ev_pct and ev_pct >= 5.0 and historical_win_rate >= 0.70):
            confidence = 'ELITE'
            recommendation = 'MAX_BET'
        elif (edge_result.confidence in ['ELITE', 'HIGH'] and ev_pct and ev_pct >= 3.0 and historical_win_rate >= 0.65):
            confidence = 'HIGH'
            recommendation = 'STANDARD_BET'
        elif historical_win_rate >= 0.60:
            confidence = 'STANDARD'
            recommendation = 'STANDARD_BET'
        else:
            confidence = 'LOW'
            recommendation = 'SMALL_BET'

        return QualificationResult(
            qualified=qualified,
            confidence=confidence,
            bet_size_pct=round(bet_size * 100, 2),
            true_edge=edge_result.true_edge,
            ev_pct=ev_pct,
            reasons_pass=reasons_pass,
            reasons_fail=reasons_fail,
            recommendation=recommendation
        )


def validate_game_data(game) -> dict:
    errors = []
    warnings = []

    if game.projected_total:
        league_ranges = {
            'NBA': (180, 260), 'CBB': (120, 180), 'NFL': (30, 70),
            'CFB': (35, 85), 'NHL': (4, 9)
        }
        min_total, max_total = league_ranges.get(game.league, (0, 999))
        if not (min_total <= game.projected_total <= max_total):
            errors.append(f"Projection {game.projected_total} outside range [{min_total}-{max_total}]")

    if game.away_ppg and game.away_ppg < 0:
        errors.append(f"Invalid away_ppg: {game.away_ppg}")
    if game.home_ppg and game.home_ppg < 0:
        errors.append(f"Invalid home_ppg: {game.home_ppg}")
    if game.true_edge and game.true_edge > 20:
        warnings.append(f"Very large true edge: {game.true_edge}")
    if game.vig_percentage and game.vig_percentage > 10:
        warnings.append(f"Very high vig: {game.vig_percentage}%")
    if game.kelly_fraction and game.kelly_fraction > 0.15:
        warnings.append(f"Very high Kelly: {game.kelly_fraction*100}%")

    return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings}


def log_qualification_decision(game, qual_result: QualificationResult):
    log_data = {
        'game': f"{game.away_team} @ {game.home_team}",
        'league': game.league,
        'qualified': qual_result.qualified,
        'confidence': qual_result.confidence,
        'true_edge': qual_result.true_edge,
        'ev_pct': qual_result.ev_pct,
        'kelly_pct': qual_result.bet_size_pct,
        'recommendation': qual_result.recommendation
    }

    if qual_result.qualified:
        logger.info(f"QUALIFIED: {log_data['game']} - {log_data['confidence']} "
                    f"(Edge:{log_data['true_edge']}, EV:{log_data['ev_pct']}%)")
    else:
        logger.debug(f"REJECTED: {log_data['game']} - {qual_result.reasons_fail[:2]}")
