"""
4-Brain AI Analysis System — Seven Thirty's Locks

The 4 Brains (all anchored to the LOCKED PPG formula):
  - Model Purist (statistician): IS the formula. Pure PPG projection output.
  - Conservative (historian): Formula direction gated behind 5 qualification filters.
  - Aggressive (scout): Formula direction + situational factors (B2B, travel, defense).
  - Sharp (sharp): Formula direction validated against market signals (RLM, EV, divergence).

Target Leagues: NBA, CBB, NFL, CFB, NHL

Confidence Scale (5.0 - 8.5):
  5.0-5.9 = PASS (50-55%)
  6.0-6.4 = LEAN (55-57%)
  6.5-6.9 = LEAN/BET (57-60%)
  7.0-7.5 = BET (60-63%)
  7.5-8.5 = STRONG BET (63-68%)

Consensus: 3/4 brains must agree on side for a pick to qualify.

No imports from sports_app.py to avoid circular dependencies.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

# =====================================================================
# APP CONSTANTS (mirrored from sports_app.py to avoid circular import)
# =====================================================================
EDGE_THRESHOLDS = {
    "NBA": 8.0, "CBB": 8.0, "NFL": 3.5, "CFB": 3.5, "NHL": 0.5, "MLB": 1.5
}
SPREAD_THRESHOLDS = {
    "NBA": 8.0, "CBB": 4.0, "NFL": 3.5, "CFB": 3.5, "NHL": 0.5
}
HOME_COURT_ADV_CBB = 3.5
DEFAULT_TEMPO = 68.0

MIN_TRUE_EDGE = {
    "NBA": 3.5, "CBB": 4.0, "NFL": 2.0, "CFB": 2.5, "NHL": 0.3, "MLB": 0.8
}
APP_CONFIDENCE_TIERS = [
    (12.0, "SUPERMAX"),
    (10.0, "HIGH"),
    (8.0, "STANDARD"),
    (0.0, "LOW"),
]
MIN_EV = 1.0
MIN_WIN_RATE = 0.58
MIN_SAMPLE_SIZE = 10
MAX_VIG = 8.0
RLM_MONEY_THRESHOLD = 60
SHARP_DIVERGENCE_MIN = 10
TOTAL_EV_MULTIPLIER = 0.5
SPREAD_EV_MULTIPLIER = 0.4
OU_L5_MIN = 100.0
OU_L10_MIN = 90.0
OU_L20_MIN = 95.0
OU_SAMPLE_MIN = 20

# =====================================================================
# CONFIDENCE SCALE: 5.0 - 8.5
# Maps brain's internal signal strength to a probability-based rating
# =====================================================================
CONF_FLOOR = 5.0
CONF_CEIL = 8.5

# Action labels by confidence
CONFIDENCE_ACTIONS = [
    (7.5, "STRONG BET"),
    (7.0, "BET"),
    (6.5, "LEAN/BET"),
    (6.0, "LEAN"),
    (0.0, "PASS"),
]

CONSENSUS_MIN = 3  # 3/4 brains must agree


def _conf_action(conf: float) -> str:
    """Map confidence rating to action label."""
    for threshold, label in CONFIDENCE_ACTIONS:
        if conf >= threshold:
            return label
    return "PASS"


def _raw_to_conf(raw: float, max_raw: float = 100.0) -> float:
    """Convert 0-100 raw confidence to 5.0-8.5 scale."""
    if raw <= 0:
        return CONF_FLOOR
    pct = min(raw / max_raw, 1.0)
    return round(CONF_FLOOR + pct * (CONF_CEIL - CONF_FLOOR), 1)


def _get_app_tier(true_edge: float) -> str:
    """Map true_edge to app's confidence tier."""
    for threshold, tier in APP_CONFIDENCE_TIERS:
        if true_edge >= threshold:
            return tier
    return "LOW"


def _safe(val, default=0.0):
    return val if val is not None else default


def _get_bet_type(game):
    """Determine the bet type from the game's direction fields.

    Returns 'totals' (OVER/UNDER), 'spread' (HOME/AWAY), or None.
    All brains must evaluate the SAME bet type for consensus to be meaningful.
    """
    app_dir = getattr(game, 'direction', None)         # 'O' or 'U'
    app_spread = getattr(game, 'spread_direction', None)  # 'HOME' or 'AWAY'
    if app_dir:
        return 'totals'
    elif app_spread:
        return 'spread'
    return None


# =====================================================================
# LOCKED FORMULA — called by ALL 4 brains
# =====================================================================
def _run_formula(features, league: str, bet_type: str) -> Optional[Dict]:
    """
    The LOCKED projection formula. Every brain calls this as its anchor.

    Totals (all leagues): PPG-only, no KenPom
        exp_away = (away_ppg + home_opp_ppg) / 2
        exp_home = (home_ppg + away_opp_ppg) / 2
        proj_total = exp_away + exp_home

    Spread NBA/NFL/CFB/NHL: PPG margin
        Same exp_away/exp_home, proj_margin = exp_home - exp_away

    Spread CBB: KenPom model
        exp_away = ((away_adj_o + home_adj_d) / 2) * tempo_factor
        exp_home = ((home_adj_o + away_adj_d) / 2) * tempo_factor
        proj_spread = (exp_away - exp_home) + 3.5 (home court)

    Returns dict with keys:
        side, expected_away, expected_home, projected_value, market_line,
        edge, threshold, diff
    Or None if insufficient data.
    """
    sf = features.statistician_features
    shf = features.sharp_features

    if bet_type == 'totals':
        # --- TOTALS: Enhanced with pace data for NBA ---
        away_ppg = sf.get('away_ppg')
        home_ppg = sf.get('home_ppg')
        away_opp = sf.get('away_opp_ppg')
        home_opp = sf.get('home_opp_ppg')
        market_line = shf.get('line')

        if any(v is None or v <= 0 for v in [away_ppg, home_ppg, away_opp, home_opp]):
            return None
        if market_line is None or market_line <= 0:
            return None

        # NBA PACE ADJUSTMENT: Use pace data when available for more accurate projections
        if league == 'NBA':
            away_pace = sf.get('away_pace')  # Possessions per game
            home_pace = sf.get('home_pace') 
            away_off_eff = sf.get('away_off_eff')  # Offensive efficiency (pts per 100 possessions)
            home_off_eff = sf.get('home_off_eff')
            away_def_eff = sf.get('away_def_eff')  # Defensive efficiency (pts allowed per 100 possessions)
            home_def_eff = sf.get('home_def_eff')
            
            # Use pace-adjusted projection if pace data is available
            if all(v is not None and v > 0 for v in [away_pace, home_pace, away_off_eff, home_off_eff, away_def_eff, home_def_eff]):
                logger.debug(f"Using pace-adjusted NBA totals projection")
                
                # Project game pace: average of both teams' pace
                game_pace = (away_pace + home_pace) / 2
                
                # Project efficiency for each team vs opponent
                # Away team offense vs home team defense
                exp_away_eff = (away_off_eff + home_def_eff) / 2
                # Home team offense vs away team defense  
                exp_home_eff = (home_off_eff + away_def_eff) / 2
                
                # Convert to expected points: (efficiency / 100) * pace
                exp_away = (exp_away_eff / 100) * game_pace
                exp_home = (exp_home_eff / 100) * game_pace
                proj_total = exp_away + exp_home
                
                logger.debug(f"Pace projection: Away {exp_away:.1f}, Home {exp_home:.1f}, Total {proj_total:.1f}")
            else:
                # Fallback to PPG-based projection when pace data unavailable
                logger.debug(f"Falling back to PPG-based NBA totals projection (pace data incomplete)")
                exp_away = (away_ppg + home_opp) / 2
                exp_home = (home_ppg + away_opp) / 2
                proj_total = exp_away + exp_home
        else:
            # Non-NBA leagues: Use traditional PPG approach
            exp_away = (away_ppg + home_opp) / 2
            exp_home = (home_ppg + away_opp) / 2
            proj_total = exp_away + exp_home

        threshold = EDGE_THRESHOLDS.get(league, 8.0)
        diff = proj_total - market_line

        if proj_total >= market_line + threshold:
            side = 'OVER'
        elif market_line >= proj_total + threshold:
            side = 'UNDER'
        else:
            side = None

        return {
            'side': side,
            'expected_away': round(exp_away, 1),
            'expected_home': round(exp_home, 1),
            'projected_value': round(proj_total, 1),
            'market_line': market_line,
            'edge': round(abs(diff), 1),
            'threshold': threshold,
            'diff': round(diff, 1),
        }

    elif bet_type == 'spread':
        spread_line = shf.get('spread_line')
        if spread_line is None:
            return None

        if league == 'CBB':
            # --- CBB SPREAD: KenPom model ---
            away_adj_o = sf.get('torvik_away_adj_o')
            away_adj_d = sf.get('torvik_away_adj_d')
            home_adj_o = sf.get('torvik_home_adj_o')
            home_adj_d = sf.get('torvik_home_adj_d')
            tempo = sf.get('torvik_tempo')

            if any(v is None for v in [away_adj_o, away_adj_d, home_adj_o, home_adj_d]):
                return None

            game_tempo = tempo if tempo else DEFAULT_TEMPO
            tempo_factor = game_tempo / DEFAULT_TEMPO

            exp_away = ((away_adj_o + home_adj_d) / 2) * tempo_factor
            exp_home = ((home_adj_o + away_adj_d) / 2) * tempo_factor
            proj_spread = (exp_away - exp_home) + HOME_COURT_ADV_CBB
            threshold = SPREAD_THRESHOLDS['CBB']  # 4.0

            diff = proj_spread - abs(spread_line)

            # Net gap validation: away_adj_em - home_adj_em must align
            away_adj_em = away_adj_o - away_adj_d
            home_adj_em = home_adj_o - home_adj_d
            net_gap = away_adj_em - home_adj_em

            if abs(diff) < threshold:
                side = None
            elif proj_spread > 0 and net_gap <= 0:
                side = None  # Net rating conflicts
            elif proj_spread < 0 and net_gap >= 0:
                side = None  # Net rating conflicts
            elif proj_spread > 0:
                side = 'AWAY'  # Away team projected stronger
            else:
                side = 'HOME'  # Home team projected stronger

            return {
                'side': side,
                'expected_away': round(exp_away, 1),
                'expected_home': round(exp_home, 1),
                'projected_value': round(proj_spread, 1),
                'market_line': spread_line,
                'edge': round(abs(diff), 1),
                'threshold': threshold,
                'diff': round(diff, 1),
                'net_gap': round(net_gap, 1),
            }

        else:
            # --- NBA/NFL/CFB/NHL SPREAD: PPG margin ---
            away_ppg = sf.get('away_ppg')
            home_ppg = sf.get('home_ppg')
            away_opp = sf.get('away_opp_ppg')
            home_opp = sf.get('home_opp_ppg')

            if any(v is None or v <= 0 for v in [away_ppg, home_ppg, away_opp, home_opp]):
                return None

            exp_away = (away_ppg + home_opp) / 2
            exp_home = (home_ppg + away_opp) / 2
            proj_margin = exp_home - exp_away
            threshold = SPREAD_THRESHOLDS.get(league, 8.0)
            diff = proj_margin - spread_line

            if proj_margin >= spread_line + threshold:
                side = 'HOME'
            elif spread_line >= proj_margin + threshold:
                side = 'AWAY'
            else:
                side = None

            return {
                'side': side,
                'expected_away': round(exp_away, 1),
                'expected_home': round(exp_home, 1),
                'projected_value': round(proj_margin, 1),
                'market_line': spread_line,
                'edge': round(abs(diff), 1),
                'threshold': threshold,
                'diff': round(diff, 1),
            }

    return None


@dataclass
class BrainOutput:
    """Output from a single brain's analysis."""
    side: Optional[str] = None   # 'HOME', 'AWAY', 'OVER', 'UNDER', or None
    confidence: float = 5.0      # 5.0 - 8.5 scale
    reasoning: str = ""
    factors: List[str] = field(default_factory=list)
    action: str = "PASS"         # PASS / LEAN / LEAN-BET / BET / STRONG BET


@dataclass
class MasterVerdict:
    """Synthesized output from the Master Brain."""
    verdict: Optional[str] = None
    confidence: float = 5.0
    agreement_level: str = 'NONE'   # CONSENSUS, STRONG, SPLIT, NONE
    agreement_count: int = 0
    summary: str = ""
    action: str = "PASS"
    qualified: bool = False  # True only if 3/4 brains agree
    # Individual brains (stored to DB as statistician/sharp/scout/historian)
    statistician: BrainOutput = field(default_factory=BrainOutput)
    sharp: BrainOutput = field(default_factory=BrainOutput)
    scout: BrainOutput = field(default_factory=BrainOutput)
    historian: BrainOutput = field(default_factory=BrainOutput)


# =====================================================================
# BRAIN 1: MODEL PURIST  (stored in DB as "statistician")
# "The Calculator" — IS the formula. No ensemble, no Elo, no extras.
# =====================================================================
class ModelPuristBrain:

    @staticmethod
    def analyze(game, features, ensemble_pred) -> BrainOutput:
        try:
            league = features.league
            bet_type = _get_bet_type(game)

            if bet_type is None:
                return BrainOutput(reasoning="Model Purist: No bet direction set", factors=["No direction"])

            result = _run_formula(features, league, bet_type)

            if result is None:
                return BrainOutput(
                    reasoning="Model Purist: Insufficient data — no play",
                    factors=["Insufficient data — no play"]
                )

            side = result['side']
            edge = result['edge']
            threshold = result['threshold']

            # Confidence from edge magnitude only
            if edge > 0 and threshold > 0:
                raw = min(90, 30 * (edge / threshold))
            else:
                raw = 0.0

            # Build factors showing full formula chain
            factors = []
            if bet_type == 'totals':
                factors.append(
                    f"Exp A: {result['expected_away']:.1f}, "
                    f"Exp H: {result['expected_home']:.1f}")
                factors.append(
                    f"Proj Total: {result['projected_value']:.1f} vs "
                    f"Line: {result['market_line']:.1f}")
                factors.append(
                    f"Diff: {result['diff']:+.1f}, "
                    f"Threshold: {threshold:.1f}")
                verdict = side if side else "NO BET"
                factors.append(f"VERDICT: {verdict}")
            else:
                factors.append(
                    f"Exp A: {result['expected_away']:.1f}, "
                    f"Exp H: {result['expected_home']:.1f}")
                label = "Proj Spread" if league == 'CBB' else "Proj Margin"
                factors.append(
                    f"{label}: {result['projected_value']:+.1f} vs "
                    f"Line: {result['market_line']:+.1f}")
                factors.append(
                    f"Edge: {edge:.1f}, "
                    f"Threshold: {threshold:.1f}")
                if league == 'CBB' and 'net_gap' in result:
                    factors.append(f"Net Gap: {result['net_gap']:+.1f}")
                verdict = side if side else "NO BET"
                factors.append(f"VERDICT: {verdict}")

            conf = _raw_to_conf(min(95, max(0, raw)))
            action = _conf_action(conf)
            reasoning = _build_reasoning("Model Purist", factors)
            return BrainOutput(side=side, confidence=conf, reasoning=reasoning, factors=factors, action=action)
        except Exception as e:
            logger.error(f"ModelPurist error: {e}")
            return BrainOutput(reasoning=f"Error: {e}")


# =====================================================================
# BRAIN 2: CONSERVATIVE  (stored in DB as "historian")
# "The Gatekeeper" — Formula direction gated behind 5 qualification
# filters. ALL must pass to agree; any fail = abstain.
# =====================================================================
class ConservativeBrain:

    @staticmethod
    def analyze(game, features, ensemble_pred) -> BrainOutput:
        try:
            factors = []
            league = features.league
            hf = features.historian_features
            shf = features.sharp_features
            bet_type = _get_bet_type(game)

            if bet_type is None:
                return BrainOutput(reasoning="Conservative: No bet direction set", factors=["No direction"])

            # Step 1: Run the formula — if it says NO BET, we say NO BET
            result = _run_formula(features, league, bet_type)
            if result is None:
                return BrainOutput(
                    reasoning="Conservative: Formula has insufficient data — no play",
                    factors=["Formula: insufficient data — no play"]
                )

            formula_side = result['side']
            if formula_side is None:
                factors.append(f"Formula: NO BET (edge {result['edge']:.1f} < threshold {result['threshold']:.1f})")
                return BrainOutput(
                    reasoning=_build_reasoning("Conservative", factors),
                    factors=factors
                )

            factors.append(f"Formula: {formula_side} (edge {result['edge']:.1f})")

            # Step 2: Run 5 ProfessionalQualifier filters
            filters_pass = 0
            filters_total = 5  # Always check all 5

            # Filter 1: True edge >= MIN_TRUE_EDGE
            true_edge = shf.get('true_edge') or features.statistician_features.get('true_edge')
            min_te = MIN_TRUE_EDGE.get(league, 3.5)
            if true_edge is not None and true_edge >= min_te:
                filters_pass += 1
                factors.append(f"True edge {true_edge:.1f} >= {min_te} PASS")
            else:
                te_str = f"{true_edge:.1f}" if true_edge is not None else "N/A"
                factors.append(f"True edge {te_str} < {min_te} FAIL")

            # Filter 2: EV >= 1.0%
            if bet_type == 'totals':
                ev_val = shf.get('total_ev')
            else:
                ev_val = shf.get('spread_ev')
            if ev_val is None:
                ev_val = shf.get('total_ev') or shf.get('spread_ev')
            if ev_val is not None and ev_val >= MIN_EV:
                filters_pass += 1
                factors.append(f"EV +{ev_val:.1f}% >= +{MIN_EV}% PASS")
            elif ev_val is None:
                # No Pinnacle data = pass (matches app behavior)
                filters_pass += 1
                factors.append("EV: no Pinnacle data (pass)")
            else:
                factors.append(f"EV {ev_val:+.1f}% < +{MIN_EV}% FAIL")

            # Filter 3: Vig <= 8.0%
            vig = shf.get('vig_percentage')
            if vig is not None and vig <= MAX_VIG:
                filters_pass += 1
                factors.append(f"Vig {vig:.1f}% <= {MAX_VIG}% PASS")
            elif vig is None:
                filters_pass += 1
                factors.append("Vig: no data (pass)")
            else:
                factors.append(f"Vig {vig:.1f}% > {MAX_VIG}% FAIL")

            # Filter 4: Historical win rate >= 58%
            away_spread = hf.get('away_spread_pct')
            home_spread = hf.get('home_spread_pct')
            if away_spread is not None and home_spread is not None:
                best_rate = max(away_spread, home_spread) / 100
                if best_rate >= MIN_WIN_RATE:
                    filters_pass += 1
                    factors.append(f"History {best_rate:.0%} >= {MIN_WIN_RATE:.0%} PASS")
                else:
                    factors.append(f"History {best_rate:.0%} < {MIN_WIN_RATE:.0%} FAIL")
            else:
                filters_pass += 1
                factors.append("History: no data (pass)")

            # Filter 5: Sample size >= 10
            sample = hf.get('history_sample_size')
            if sample is not None and sample >= MIN_SAMPLE_SIZE:
                filters_pass += 1
                factors.append(f"Sample {sample:.0f} >= {MIN_SAMPLE_SIZE} PASS")
            elif sample is None:
                filters_pass += 1
                factors.append("Sample: no data (pass)")
            else:
                factors.append(f"Sample {sample:.0f} < {MIN_SAMPLE_SIZE} FAIL")

            # Step 3: Bet-type specific checks (informational, tracked as bonus filters)
            bonus_pass = True
            if bet_type == 'totals':
                ou_l5 = hf.get('ou_l5')
                ou_l10 = hf.get('ou_l10')
                ou_l20 = hf.get('ou_l20')
                ou_qualified = hf.get('ou_hit_rate_qualified')

                if ou_l5 is not None:
                    strict_pass = (
                        _safe(ou_l5) >= OU_L5_MIN and
                        _safe(ou_l10) >= OU_L10_MIN and
                        _safe(ou_l20) >= OU_L20_MIN
                    )
                    if strict_pass or ou_qualified == 1.0:
                        factors.append(
                            f"O/U strict: L5={_safe(ou_l5):.0f}% L10={_safe(ou_l10):.0f}% "
                            f"L20={_safe(ou_l20):.0f}% PASS")
                    else:
                        bonus_pass = False
                        factors.append(
                            f"O/U: L5={_safe(ou_l5):.0f}% L10={_safe(ou_l10):.0f}% "
                            f"L20={_safe(ou_l20):.0f}% FAIL (need 100/90/95)")

                h2h_ou = hf.get('h2h_ou_pct')
                if h2h_ou is not None and h2h_ou > 0:
                    game_direction = getattr(game, 'direction', None)
                    if (game_direction == 'O' and h2h_ou >= 50) or (game_direction == 'U' and h2h_ou <= 50):
                        factors.append(f"H2H O/U: {h2h_ou:.0f}% overs (aligns)")
                    else:
                        bonus_pass = False
                        factors.append(f"H2H O/U: {h2h_ou:.0f}% overs (conflicts)")
            else:
                h2h_spread = hf.get('h2h_spread_pct')
                if h2h_spread is not None and h2h_spread > 0:
                    factors.append(f"H2H spread cover: {h2h_spread:.0f}%")

            # Step 4: ALL filters must pass to agree with formula direction
            all_pass = (filters_pass == filters_total) and bonus_pass
            factors.insert(0, f"Filters: {filters_pass}/{filters_total}" + (" ALL PASS" if all_pass else " FAIL"))

            if all_pass:
                side = formula_side
                raw = min(90, 30 * (result['edge'] / result['threshold']))
                # Bonus for passing all filters
                raw = min(95, raw + 10)
            else:
                side = None
                raw = 0.0

            conf = _raw_to_conf(min(95, max(0, raw)))
            action = _conf_action(conf)
            reasoning = _build_reasoning("Conservative", factors)
            return BrainOutput(side=side, confidence=conf, reasoning=reasoning, factors=factors, action=action)
        except Exception as e:
            logger.error(f"Conservative error: {e}")
            return BrainOutput(reasoning=f"Error: {e}")


# =====================================================================
# BRAIN 3: AGGRESSIVE  (stored in DB as "scout")
# "The Situational Expert" — Gets formula direction, evaluates
# situational factors for alignment or contradiction.
# =====================================================================
class AggressiveBrain:

    @staticmethod
    def analyze(game, features, ensemble_pred) -> BrainOutput:
        try:
            factors = []
            league = features.league
            sf = features.scout_features
            bet_type = _get_bet_type(game)

            if bet_type is None:
                return BrainOutput(reasoning="Aggressive: No bet direction set", factors=["No direction"])

            # Step 1: Get formula direction as baseline
            result = _run_formula(features, league, bet_type)
            formula_side = result['side'] if result else None
            formula_edge = result['edge'] if result else 0.0
            formula_threshold = result['threshold'] if result else 1.0

            if result:
                factors.append(f"Formula: {formula_side or 'NO BET'} (edge {formula_edge:.1f})")
            else:
                factors.append("Formula: insufficient data")

            # Step 2: Evaluate situational factors
            b2b_away = _safe(sf.get('is_back_to_back_away'))
            b2b_home = _safe(sf.get('is_back_to_back_home'))
            def_mismatch = sf.get('def_mismatch')
            def_rank_away = sf.get('def_rank_away')
            def_rank_home = sf.get('def_rank_home')
            rest_adv = sf.get('rest_advantage')
            travel = _safe(sf.get('travel_distance'))
            away_is_fav = sf.get('away_is_favorite')

            sit_side = None
            sit_raw = 0.0

            if bet_type == 'totals':
                # B2B fatigue -> UNDER
                if b2b_away == 1.0 or b2b_home == 1.0:
                    boost = {'NBA': 20, 'NHL': 18, 'CBB': 12}.get(league, 12)
                    tired = "Away" if b2b_away == 1.0 else "Home"
                    label = "goalie fatigue" if league == 'NHL' else "tired legs"
                    sit_side = 'UNDER'
                    sit_raw += boost
                    factors.append(f"B2B FADE: {tired} {label} -> UNDER (+{boost})")

                # Defense mismatch
                if def_mismatch == 1.0 and league in ('NBA', 'CBB'):
                    if def_rank_away and def_rank_away >= 25:
                        sit_side = 'OVER'
                        sit_raw += 22
                        factors.append(f"DEF MISMATCH: bad D (rank #{int(def_rank_away)}) -> OVER")
                    elif def_rank_home and def_rank_home >= 25:
                        sit_side = 'OVER'
                        sit_raw += 22
                        factors.append(f"DEF MISMATCH: bad D (rank #{int(def_rank_home)}) -> OVER")
                    elif def_rank_away and def_rank_away <= 5:
                        sit_side = 'UNDER'
                        sit_raw += 18
                        factors.append(f"DEF MISMATCH: elite D (rank #{int(def_rank_away)}) -> UNDER")
                    elif def_rank_home and def_rank_home <= 5:
                        sit_side = 'UNDER'
                        sit_raw += 18
                        factors.append(f"DEF MISMATCH: elite D (rank #{int(def_rank_home)}) -> UNDER")

                # Travel fatigue -> UNDER
                if travel > 2000:
                    if sit_side is None:
                        sit_side = 'UNDER'
                    sit_raw += min(10, travel / 300)
                    factors.append(f"Travel: {travel:.0f}mi fatigue -> UNDER")

            else:
                # B2B -> against tired team
                if b2b_away == 1.0 and b2b_home != 1.0:
                    boost = {'NBA': 30, 'NHL': 25, 'CBB': 15}.get(league, 15)
                    sit_side = 'HOME'
                    sit_raw += boost
                    factors.append(f"B2B FADE: Away tired -> HOME (+{boost})")
                elif b2b_home == 1.0 and b2b_away != 1.0:
                    boost = {'NBA': 30, 'NHL': 25, 'CBB': 15}.get(league, 15)
                    sit_side = 'AWAY'
                    sit_raw += boost
                    factors.append(f"B2B FADE: Home tired -> AWAY (+{boost})")

                # Rest advantage -> rested side
                if rest_adv is not None and abs(rest_adv) >= 2:
                    rest_side = 'HOME' if rest_adv > 0 else 'AWAY'
                    if sit_side is None:
                        sit_side = rest_side
                    sit_raw += min(15, abs(rest_adv) * 5)
                    factors.append(f"Rest edge: {'Home' if rest_adv > 0 else 'Away'} +{abs(rest_adv):.0f} days")

                # Away favorite -> AWAY
                if away_is_fav == 1.0:
                    if sit_side is None:
                        sit_side = 'AWAY'
                    sit_raw += 12
                    factors.append("Away favorite -> AWAY")

                # Travel fatigue -> HOME
                if travel > 2000:
                    if sit_side is None:
                        sit_side = 'HOME'
                    sit_raw += min(10, travel / 300)
                    factors.append(f"Travel: {travel:.0f}mi fatigue -> HOME")

            # Step 3: Resolution logic
            side = None
            raw = 0.0

            if formula_side and sit_side:
                if formula_side == sit_side:
                    # ALIGN: agree with high confidence
                    side = formula_side
                    raw = min(90, 30 * (formula_edge / formula_threshold)) + sit_raw * 0.3
                    factors.append(f"ALIGN: situations confirm formula -> {side}")
                elif sit_raw >= 25:
                    # CONTRADICT + strong situations -> side with situations
                    side = sit_side
                    raw = sit_raw * 0.7
                    factors.append(f"OVERRIDE: strong situations ({sit_raw:.0f}) contradict formula -> {side}")
                else:
                    # CONTRADICT + weak -> abstain
                    side = None
                    raw = 0.0
                    factors.append(f"CONFLICT: situations ({sit_side}) vs formula ({formula_side}), weak -> abstain")
            elif formula_side and not sit_side:
                # No situations, formula has direction -> defer at low confidence
                side = formula_side
                raw = min(40, 15 * (formula_edge / formula_threshold))
                factors.append(f"No situations -> defer to formula ({side}, low conf)")
            elif not formula_side and sit_side and sit_raw >= 25:
                # No formula edge, strong situations -> side with situations
                side = sit_side
                raw = sit_raw * 0.6
                factors.append(f"No formula edge, strong situations -> {side}")
            else:
                factors.append("No strong signal from formula or situations")

            if not factors:
                factors.append("No situational edges found")

            conf = _raw_to_conf(min(95, max(0, raw)))
            action = _conf_action(conf)
            reasoning = _build_reasoning("Aggressive", factors)
            return BrainOutput(side=side, confidence=conf, reasoning=reasoning, factors=factors, action=action)
        except Exception as e:
            logger.error(f"Aggressive error: {e}")
            return BrainOutput(reasoning=f"Error: {e}")


# =====================================================================
# BRAIN 4: SHARP  (stored in DB as "sharp")
# "The Market Reader" — Gets formula direction, validates against
# market signals (RLM, EV, divergence, line movement).
# =====================================================================
class SharpBrain:

    @staticmethod
    def analyze(game, features, ensemble_pred) -> BrainOutput:
        try:
            factors = []
            league = features.league
            sf = features.sharp_features
            bet_type = _get_bet_type(game)

            if bet_type is None:
                return BrainOutput(reasoning="Sharp: No bet direction set", factors=["No direction"])

            # Step 1: Get formula direction as baseline
            result = _run_formula(features, league, bet_type)
            formula_side = result['side'] if result else None
            formula_edge = result['edge'] if result else 0.0
            formula_threshold = result['threshold'] if result else 1.0

            if result:
                factors.append(f"Formula: {formula_side or 'NO BET'} (edge {formula_edge:.1f})")
            else:
                factors.append("Formula: insufficient data")

            # Step 2: Enhanced market signals with advanced RLM detection
            market_side = None
            market_raw = 0.0

            # PINNACLE VALIDATION: Weight Pinnacle as truth
            pinnacle_spread = sf.get('pinnacle_spread')
            pinnacle_total = sf.get('pinnacle_total')
            model_spread = result.get('projected_value') if result and bet_type == 'spread' else None
            model_total = result.get('projected_value') if result and bet_type == 'totals' else None
            
            pinnacle_conflict = False
            if bet_type == 'spread' and pinnacle_spread is not None and model_spread is not None:
                spread_diff = abs(model_spread - pinnacle_spread)
                if spread_diff > 1.0:
                    pinnacle_conflict = True
                    factors.append(f"⚠️ PINNACLE CONFLICT: Model {model_spread:+.1f} vs Pinnacle {pinnacle_spread:+.1f} (diff: {spread_diff:.1f})")
                    market_raw *= 0.3  # Heavy penalty for disagreeing with Pinnacle
            elif bet_type == 'totals' and pinnacle_total is not None and model_total is not None:
                total_diff = abs(model_total - pinnacle_total)
                if total_diff > 1.0:
                    pinnacle_conflict = True
                    factors.append(f"⚠️ PINNACLE CONFLICT: Model {model_total:.1f} vs Pinnacle {pinnacle_total:.1f} (diff: {total_diff:.1f})")
                    market_raw *= 0.3  # Heavy penalty for disagreeing with Pinnacle

            if bet_type == 'totals':
                # Enhanced Totals RLM with time weighting and steam detection
                if sf.get('totals_rlm_detected') == 1.0:
                    totals_side = getattr(game, 'totals_rlm_sharp_side', None)
                    if totals_side:
                        mapped = totals_side.upper() if totals_side.upper() in ('OVER', 'UNDER') else None
                        if mapped:
                            market_side = mapped
                            base_rlm = 25
                            
                            # Time-weighted RLM: Early moves weighted more heavily
                            early_line_movement = sf.get('early_total_movement')  # Line movement in first 2 hours
                            late_line_movement = sf.get('late_total_movement')    # Line movement in final 2 hours
                            
                            if early_line_movement is not None and abs(early_line_movement) >= 1.0:
                                base_rlm += 15  # Early moves indicate sharp money
                                factors.append(f"EARLY STEAM: {abs(early_line_movement):.1f}pts total movement")
                            
                            if late_line_movement is not None and abs(late_line_movement) >= 1.5:
                                base_rlm += 10  # Late steam also significant
                                factors.append(f"LATE STEAM: {abs(late_line_movement):.1f}pts total movement")
                            
                            market_raw += base_rlm
                            factors.append(f"Totals RLM: sharp on {mapped}")

                # Multi-book steam detection for totals
                steam_count = 0
                draftkings_total = sf.get('draftkings_total')
                fanduel_total = sf.get('fanduel_total') 
                betmgm_total = sf.get('betmgm_total')
                caesars_total = sf.get('caesars_total')
                
                if all(v is not None for v in [draftkings_total, fanduel_total, betmgm_total]):
                    # Check if 3+ books moved same direction
                    opening_total = sf.get('opening_total')
                    if opening_total is not None:
                        moves = [
                            draftkings_total - opening_total,
                            fanduel_total - opening_total,
                            betmgm_total - opening_total
                        ]
                        if caesars_total is not None:
                            moves.append(caesars_total - opening_total)
                        
                        # Count books moving same direction with magnitude > 0.5
                        over_moves = sum(1 for move in moves if move > 0.5)
                        under_moves = sum(1 for move in moves if move < -0.5)
                        
                        if over_moves >= 3:
                            steam_count = over_moves
                            if market_side is None:
                                market_side = 'OVER'
                            market_raw += 20
                            factors.append(f"MULTI-BOOK STEAM: {over_moves} books moved OVER")
                        elif under_moves >= 3:
                            steam_count = under_moves  
                            if market_side is None:
                                market_side = 'UNDER'
                            market_raw += 20
                            factors.append(f"MULTI-BOOK STEAM: {under_moves} books moved UNDER")

                # Enhanced O/U divergence analysis
                over_money = sf.get('over_money_pct')
                over_tickets = sf.get('over_tickets_pct')
                if over_money is not None and over_tickets is not None:
                    div = abs(over_money - over_tickets)
                    if div >= SHARP_DIVERGENCE_MIN:
                        sharp_ou = 'OVER' if over_money > over_tickets else 'UNDER'
                        if market_side is None:
                            market_side = sharp_ou
                        
                        # Enhanced weighting based on Pinnacle alignment
                        div_bonus = min(20, div * 1.2)
                        if not pinnacle_conflict:
                            div_bonus *= 1.3  # Boost when aligned with Pinnacle
                        market_raw += div_bonus
                        
                        factors.append(
                            f"O/U sharp split: money {over_money:.0f}% vs tickets "
                            f"{over_tickets:.0f}% -> {sharp_ou} ({'Pinnacle aligned' if not pinnacle_conflict else 'Pinnacle conflict'})")

            else:
                # Enhanced Spread RLM with advanced detection
                if sf.get('rlm_detected') == 1.0:
                    rlm_side = getattr(game, 'rlm_sharp_side', None)
                    if rlm_side:
                        if rlm_side == getattr(game, 'home_team', ''):
                            mapped = 'HOME'
                        elif rlm_side == getattr(game, 'away_team', ''):
                            mapped = 'AWAY'
                        else:
                            mapped = rlm_side
                        market_side = mapped
                        rlm_conf = _safe(sf.get('rlm_confidence'), 50)
                        base_rlm = min(35, rlm_conf * 0.5)
                        
                        # Time-weighted spread movement
                        early_spread_movement = sf.get('early_spread_movement')
                        late_spread_movement = sf.get('late_spread_movement')
                        
                        if early_spread_movement is not None and abs(early_spread_movement) >= 1.0:
                            base_rlm += 15
                            factors.append(f"EARLY SHARP: {abs(early_spread_movement):.1f}pts spread movement")
                        
                        if late_spread_movement is not None and abs(late_spread_movement) >= 1.5:
                            base_rlm += 10  
                            factors.append(f"LATE SHARP: {abs(late_spread_movement):.1f}pts spread movement")
                        
                        # Pinnacle vs recreational book comparison
                        if not pinnacle_conflict:
                            base_rlm *= 1.4  # Boost when aligned with Pinnacle
                            
                        market_raw += base_rlm
                        rlm_expl = getattr(game, 'rlm_explanation', f"Sharp on {rlm_side}")
                        factors.append(f"RLM: {rlm_expl} ({'Pinnacle aligned' if not pinnacle_conflict else 'Pinnacle conflict'})")

                # Multi-book steam detection for spreads
                draftkings_spread = sf.get('draftkings_spread')
                fanduel_spread = sf.get('fanduel_spread')
                betmgm_spread = sf.get('betmgm_spread')
                caesars_spread = sf.get('caesars_spread')
                
                if all(v is not None for v in [draftkings_spread, fanduel_spread, betmgm_spread]):
                    opening_spread = sf.get('opening_spread')
                    if opening_spread is not None:
                        moves = [
                            draftkings_spread - opening_spread,
                            fanduel_spread - opening_spread,
                            betmgm_spread - opening_spread
                        ]
                        if caesars_spread is not None:
                            moves.append(caesars_spread - opening_spread)
                        
                        # Count books moving same direction with magnitude > 0.5
                        toward_home = sum(1 for move in moves if move > 0.5)  
                        toward_away = sum(1 for move in moves if move < -0.5)
                        
                        if toward_home >= 3:
                            if market_side is None:
                                market_side = 'HOME'
                            market_raw += 25
                            factors.append(f"MULTI-BOOK STEAM: {toward_home} books moved toward HOME")
                        elif toward_away >= 3:
                            if market_side is None:
                                market_side = 'AWAY'
                            market_raw += 25  
                            factors.append(f"MULTI-BOOK STEAM: {toward_away} books moved toward AWAY")

                # Enhanced money/ticket divergence
                div_away = sf.get('money_ticket_divergence_away')
                div_home = sf.get('money_ticket_divergence_home')
                if div_away is not None and div_home is not None:
                    max_div = max(abs(_safe(div_away)), abs(_safe(div_home)))
                    if max_div >= SHARP_DIVERGENCE_MIN:
                        sharp_team = 'AWAY' if _safe(div_away) > _safe(div_home) else 'HOME'
                        if market_side is None:
                            market_side = sharp_team
                        
                        div_bonus = min(20, max_div * 1.2)
                        if not pinnacle_conflict:
                            div_bonus *= 1.2  # Boost when aligned with Pinnacle
                        market_raw += div_bonus
                        
                        factors.append(
                            f"Sharp split: Away {_safe(div_away):+.0f}% "
                            f"Home {_safe(div_home):+.0f}% -> {sharp_team} ({'Pinnacle aligned' if not pinnacle_conflict else 'Pinnacle conflict'})")

                # Enhanced public fade logic
                away_pct = _safe(sf.get('away_tickets_pct'))
                home_pct = _safe(sf.get('home_tickets_pct'))
                if max(away_pct, home_pct) >= 75:
                    heavy = 'AWAY' if away_pct > home_pct else 'HOME'
                    fade = 'HOME' if heavy == 'AWAY' else 'AWAY'
                    factors.append(f"Public: {max(away_pct, home_pct):.0f}% on {heavy} (fade {fade})")
                    if market_side is None and not pinnacle_conflict:  # Only fade if not conflicting with Pinnacle
                        market_side = fade
                        market_raw += 6

                # Kelly with Pinnacle weighting  
                kelly = sf.get('kelly_fraction')
                if kelly is not None and kelly > 0.02:
                    kelly_bonus = min(8, kelly * 100)
                    if not pinnacle_conflict:
                        kelly_bonus *= 1.2
                    market_raw += kelly_bonus
                    factors.append(f"Kelly {kelly:.1%} bankroll ({'Pinnacle aligned' if not pinnacle_conflict else 'Pinnacle conflict'})")

            # Common EV analysis for both bet types
            ev_val = sf.get('total_ev') if bet_type == 'totals' else sf.get('spread_ev')
            if ev_val is not None:
                if ev_val >= MIN_EV:
                    ev_bonus = min(15, ev_val * 2.5)
                    if not pinnacle_conflict:
                        ev_bonus *= 1.3  # Boost when aligned with Pinnacle
                    market_raw += ev_bonus
                    factors.append(f"+EV ({bet_type}): {ev_val:.1f}% ({'Pinnacle aligned' if not pinnacle_conflict else 'Pinnacle conflict'})")
                elif ev_val < 0:
                    market_raw *= 0.7
                    factors.append(f"Negative {bet_type} EV: {ev_val:+.1f}%")

            # Step 3: Resolution logic (same pattern as Aggressive)
            side = None
            raw = 0.0

            if formula_side and market_side:
                if formula_side == market_side:
                    # CONFIRM: market validates formula -> agree with confidence boost
                    side = formula_side
                    raw = min(90, 30 * (formula_edge / formula_threshold)) + market_raw * 0.4
                    factors.append(f"CONFIRM: market validates formula -> {side}")
                elif market_raw >= 25:
                    # CONTRADICT + strong market -> side with market
                    side = market_side
                    raw = market_raw * 0.7
                    factors.append(f"OVERRIDE: strong market ({market_raw:.0f}) contradicts formula -> {side}")
                else:
                    # CONTRADICT + weak -> abstain
                    side = None
                    raw = 0.0
                    factors.append(f"CONFLICT: market ({market_side}) vs formula ({formula_side}), weak -> abstain")
            elif formula_side and not market_side:
                # No market data -> defer to formula at low confidence
                side = formula_side
                raw = min(40, 15 * (formula_edge / formula_threshold))
                factors.append(f"No market signal -> defer to formula ({side}, low conf)")
            elif not formula_side and market_side and market_raw >= 25:
                # No formula edge, strong market -> side with market
                side = market_side
                raw = market_raw * 0.6
                factors.append(f"No formula edge, strong market -> {side}")
            else:
                factors.append("No strong signal from formula or market")

            if not factors:
                factors.append("Insufficient market data")

            conf = _raw_to_conf(min(95, max(0, raw)))
            action = _conf_action(conf)
            reasoning = _build_reasoning("Sharp", factors)
            return BrainOutput(side=side, confidence=conf, reasoning=reasoning, factors=factors, action=action)
        except Exception as e:
            logger.error(f"Sharp error: {e}")
            return BrainOutput(reasoning=f"Error: {e}")


# =====================================================================
# MASTER BRAIN: CONSENSUS SYNTHESIZER
# 3/4 brains must agree on side for a pick to qualify
# =====================================================================
class MasterBrain:

    DEFAULT_WEIGHTS = {
        'purist': 0.30,
        'conservative': 0.25,
        'aggressive': 0.20,
        'sharp': 0.25,
    }

    LEAGUE_WEIGHTS = {
        'NBA': {'purist': 0.30, 'conservative': 0.20, 'aggressive': 0.25, 'sharp': 0.25},
        'CBB': {'purist': 0.35, 'conservative': 0.30, 'aggressive': 0.10, 'sharp': 0.25},
        'NHL': {'purist': 0.25, 'conservative': 0.20, 'aggressive': 0.25, 'sharp': 0.30},
        'MLB': {'purist': 0.25, 'conservative': 0.25, 'aggressive': 0.25, 'sharp': 0.25},
        'NFL': {'purist': 0.25, 'conservative': 0.20, 'aggressive': 0.30, 'sharp': 0.25},
        'CFB': {'purist': 0.30, 'conservative': 0.25, 'aggressive': 0.20, 'sharp': 0.25},
    }

    @classmethod
    def synthesize(cls, purist: BrainOutput, conservative: BrainOutput,
                   aggressive: BrainOutput, sharp: BrainOutput,
                   game, league: str = 'NBA',
                   brain_weights: dict = None) -> MasterVerdict:
        base_weights = cls.LEAGUE_WEIGHTS.get(league, cls.DEFAULT_WEIGHTS)

        # Apply accuracy-based scaling if brain_weights provided
        # brain_weights maps: {statistician: acc, sharp: acc, scout: acc, historian: acc}
        # DB names -> brain names: statistician=purist, historian=conservative, scout=aggressive
        if brain_weights and brain_weights.get('total_graded', 0) >= 20:
            accuracy_map = {
                'purist': brain_weights.get('statistician', 0.50),
                'conservative': brain_weights.get('historian', 0.50),
                'aggressive': brain_weights.get('scout', 0.50),
                'sharp': brain_weights.get('sharp', 0.50),
            }
            weights = {}
            for name, base_w in base_weights.items():
                acc = accuracy_map.get(name, 0.50)
                scaled = base_w * (acc / 0.50)
                weights[name] = max(0.5 * base_w, min(2.0 * base_w, scaled))
            # Normalize weights to sum to 1.0
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        else:
            weights = base_weights

        brains = {
            'purist': (purist, weights['purist']),
            'conservative': (conservative, weights['conservative']),
            'aggressive': (aggressive, weights['aggressive']),
            'sharp': (sharp, weights['sharp']),
        }

        # Group by side
        sides = {}
        brains_with_opinion = 0
        for name, (brain, weight) in brains.items():
            if brain.side:
                brains_with_opinion += 1
                sides.setdefault(brain.side, []).append((name, brain.confidence, weight))

        if not sides:
            return MasterVerdict(
                summary="No brains have an opinion.",
                statistician=purist, sharp=sharp, scout=aggressive, historian=conservative
            )

        # Best side by weighted confidence
        side_scores = {}
        for side_name, supporters in sides.items():
            score = sum(conf * w for _, conf, w in supporters)
            side_scores[side_name] = score

        best_side = max(side_scores, key=side_scores.get)
        agree_count = len(sides.get(best_side, []))

        # CONSENSUS CHECK: 3/4 must agree
        qualified = agree_count >= CONSENSUS_MIN

        # Agreement level
        if agree_count >= 4:
            agreement = 'CONSENSUS'
        elif agree_count >= 3:
            agreement = 'STRONG'
        elif agree_count == 2:
            agreement = 'SPLIT'
        else:
            agreement = 'NONE'

        # Composite confidence (weighted average of agreeing brains)
        agreeing = sides.get(best_side, [])
        total_weight = sum(w for _, _, w in agreeing)
        if total_weight > 0:
            composite = sum(conf * w for _, conf, w in agreeing) / total_weight
        else:
            composite = CONF_FLOOR

        # Agreement boost/penalty
        if agreement == 'CONSENSUS':
            composite = min(CONF_CEIL, composite + 0.3)
        elif agreement == 'STRONG':
            composite = min(CONF_CEIL, composite + 0.15)
        elif agreement == 'SPLIT':
            composite = max(CONF_FLOOR, composite - 0.3)

        # If not qualified (< 3/4 agree), cap at LEAN max
        if not qualified:
            composite = min(6.4, composite)

        composite = round(min(CONF_CEIL, max(CONF_FLOOR, composite)), 1)
        action = _conf_action(composite)

        # Build summary
        brain_labels = {'purist': 'Purist', 'conservative': 'Conserv',
                        'aggressive': 'Aggro', 'sharp': 'Sharp'}
        brain_objs = {'purist': purist, 'conservative': conservative,
                      'aggressive': aggressive, 'sharp': sharp}

        agree_names = [brain_labels[n] for n, _, _ in agreeing]
        dissent_info = []
        for side_name, supporters in sides.items():
            if side_name != best_side:
                for name, _, _ in supporters:
                    dissent_info.append(f"{brain_labels[name]}->{side_name}")

        parts = []
        parts.append(f"{agree_count}/4 {agreement} {best_side}")
        parts.append(f"Agree: {', '.join(agree_names)}")
        if dissent_info:
            parts.append(f"Dissent: {', '.join(dissent_info)}")
        parts.append(f"{composite} ({action})")

        summary = " | ".join(parts)

        return MasterVerdict(
            verdict=best_side if qualified else None,
            confidence=composite,
            agreement_level=agreement,
            agreement_count=agree_count,
            summary=summary,
            action=action,
            qualified=qualified,
            # Map to DB column names: statistician=purist, historian=conservative, scout=aggressive
            statistician=purist,
            sharp=sharp,
            scout=aggressive,
            historian=conservative
        )


# =====================================================================
# PUBLIC API
# =====================================================================
def analyze_game(game, features, ensemble_pred, brain_weights=None) -> MasterVerdict:
    """Run all 4 brains and return master verdict."""
    league = getattr(game, 'league', features.league if hasattr(features, 'league') else 'NBA')

    purist = ModelPuristBrain.analyze(game, features, ensemble_pred)
    conservative = ConservativeBrain.analyze(game, features, ensemble_pred)
    aggressive = AggressiveBrain.analyze(game, features, ensemble_pred)
    sharp = SharpBrain.analyze(game, features, ensemble_pred)

    verdict = MasterBrain.synthesize(purist, conservative, aggressive, sharp, game, league,
                                     brain_weights=brain_weights)

    logger.info(
        f"Brain: {getattr(game, 'away_team', '?')} @ {getattr(game, 'home_team', '?')} -> "
        f"{verdict.agreement_level} {verdict.verdict} {verdict.confidence} ({verdict.action})"
        f" [{verdict.agreement_count}/4]")

    return verdict


def _build_reasoning(prefix: str, factors: List[str]) -> str:
    if not factors:
        return f"{prefix}: Insufficient data"
    return f"{prefix}: {'; '.join(factors[:3])}"
