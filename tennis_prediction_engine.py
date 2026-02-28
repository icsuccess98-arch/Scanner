#!/usr/bin/env python3
"""
730's Locks Tennis Prediction Engine
4-Brain methodology adapted for tennis markets
Surface-specific analytics with form regression modeling
"""

import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import time
import re
import logging
from tennis_abstract_scraper import get_tennis_abstract_stats, fuzzy_lookup, get_current_matchups, get_tournament_draws

logger = logging.getLogger(__name__)

@dataclass
class TennisMatchPrediction:
    player1: str
    player2: str
    player1_prob: float
    player2_prob: float
    surface: str
    source: str
    confidence: float
    timestamp: str

@dataclass
class TennisEnsemblePrediction:
    player1: str
    player2: str
    surface: str
    tournament: str
    round: str
    ensemble_p1_prob: float
    ensemble_p2_prob: float
    market_p1_prob: float
    market_p2_prob: float
    expected_value_p1: float
    expected_value_p2: float
    quality_grade: str
    recommended_units_p1: float
    recommended_units_p2: float
    model_agreement: float
    surface_weight: float
    form_factor: float
    h2h_factor: float
    tournament_context: Dict
    individual_predictions: Dict[str, TennisMatchPrediction]

class TennisPredictionEngine:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Tennis-specific model weights for 4-Brain system
        self.model_weights = {
            'tennis_abstract_elo': 0.30,     # Surface-specific Elo ratings
            'form_analytics': 0.28,          # Recent form and momentum
            'h2h_regression': 0.22,          # Head-to-head with sample weighting
            'market_consensus': 0.20         # Betting market intelligence
        }
        
        # Surface-specific weight adjustments
        self.surface_weights = {
            'Clay': {'serve_weight': 0.75, 'return_weight': 1.25, 'endurance_factor': 1.4},
            'Grass': {'serve_weight': 1.35, 'return_weight': 0.85, 'endurance_factor': 0.9},
            'Hard': {'serve_weight': 1.0, 'return_weight': 1.0, 'endurance_factor': 1.0}
        }
        
        # Quality grading thresholds (NHL Savant standard adapted for tennis)
        self.quality_thresholds = {
            'A+': {'ev_min': 0.12, 'agreement_min': 0.88, 'surface_conf_min': 0.85},
            'A': {'ev_min': 0.08, 'agreement_min': 0.82, 'surface_conf_min': 0.75},
            'B': {'ev_min': 0.04, 'agreement_min': 0.70, 'surface_conf_min': 0.60},
            'C': {'ev_min': 0.01, 'agreement_min': 0.55, 'surface_conf_min': 0.45}
        }
        
        # Tournament tier weights
        self.tournament_tiers = {
            'Grand Slam': {'pressure': 1.35, 'variance': 1.15, 'importance': 1.0},
            'Masters 1000': {'pressure': 1.20, 'variance': 1.10, 'importance': 0.85},
            'ATP 500': {'pressure': 1.10, 'variance': 1.05, 'importance': 0.70},
            'ATP 250': {'pressure': 1.0, 'variance': 1.0, 'importance': 0.60},
            'WTA 1000': {'pressure': 1.15, 'variance': 1.08, 'importance': 0.80},
            'WTA 500': {'pressure': 1.05, 'variance': 1.02, 'importance': 0.65},
            'WTA 250': {'pressure': 1.0, 'variance': 1.0, 'importance': 0.55}
        }
        
        # Load Tennis Abstract stats
        self.tennis_stats = get_tennis_abstract_stats()
        self.current_matchups = get_current_matchups()
        self.tournament_draws = get_tournament_draws()
        
    def fetch_elo_predictions(self, player1: str, player2: str, surface: str) -> TennisMatchPrediction:
        """Brain 1: Tennis Abstract Elo-based predictions with surface adjustment"""
        try:
            # Lookup players in Tennis Abstract database
            p1_stats = fuzzy_lookup(player1, self.tennis_stats)
            p2_stats = fuzzy_lookup(player2, self.tennis_stats)
            
            if not p1_stats or not p2_stats:
                logger.warning(f"Missing stats for {player1} vs {player2}")
                return None
                
            # Get surface-specific Elo or fall back to general Elo
            surface_key = f"{surface.lower()}_elo"
            p1_elo = p1_stats.get(surface_key) or p1_stats.get('elo', 1500)
            p2_elo = p2_stats.get(surface_key) or p2_stats.get('elo', 1500)
            
            if p1_elo is None:
                p1_elo = 1500
            if p2_elo is None:
                p2_elo = 1500
                
            # Calculate win probability using Elo formula
            elo_diff = p1_elo - p2_elo
            p1_prob = 1 / (1 + 10 ** (-elo_diff / 400))
            
            # Surface-specific adjustments
            surface_adj = self.surface_weights.get(surface, self.surface_weights['Hard'])
            
            # Adjust based on surface specialization
            if surface == 'Clay':
                # Clay rewards defensive play and endurance
                p1_return_edge = p1_stats.get('break_pct', 0.2) - p2_stats.get('break_pct', 0.2)
                p1_prob += p1_return_edge * 0.15
            elif surface == 'Grass':
                # Grass rewards serve power and net play
                p1_hold_edge = p1_stats.get('hold_pct', 0.8) - p2_stats.get('hold_pct', 0.8)
                p1_prob += p1_hold_edge * 0.20
            
            # Clamp probability
            p1_prob = max(0.05, min(0.95, p1_prob))
            
            return TennisMatchPrediction(
                player1=player1,
                player2=player2,
                player1_prob=p1_prob,
                player2_prob=1 - p1_prob,
                surface=surface,
                source="tennis_abstract_elo",
                confidence=0.82,
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Elo prediction error for {player1} vs {player2}: {str(e)}")
            return None
    
    def analyze_form_cycle(self, player_stats: dict, opponent_stats: dict, surface: str) -> dict:
        """Brain 2: Form cycle analysis with peak/decline identification"""
        try:
            form_metrics = {}
            
            # Recent form indicators from Tennis Abstract stats
            p_matches = player_stats.get('matches', 0)
            p_net_rating = player_stats.get('net_rating', 0)
            p_dominance = player_stats.get('dominance_ratio', 1.0)
            
            o_matches = opponent_stats.get('matches', 0)
            o_net_rating = opponent_stats.get('net_rating', 0)
            o_dominance = opponent_stats.get('dominance_ratio', 1.0)
            
            # Form momentum calculation
            # More matches played recently = more current form data
            match_recency = min(p_matches / 20.0, 1.0)  # Normalize to 20 matches
            
            # Net rating differential (hold% - break%)
            net_rating_edge = p_net_rating - o_net_rating
            
            # Dominance ratio edge
            dominance_edge = p_dominance - o_dominance
            
            # Surface-specific form adjustments
            surface_adj = self.surface_weights.get(surface, self.surface_weights['Hard'])
            
            # Calculate form factor
            serve_component = p_dominance * surface_adj['serve_weight']
            return_component = net_rating_edge * surface_adj['return_weight']
            
            form_factor = (serve_component + return_component) * match_recency
            form_factor = max(-0.15, min(0.15, form_factor))  # Clamp to ±15%
            
            form_metrics = {
                'form_factor': form_factor,
                'match_recency': match_recency,
                'net_rating_edge': net_rating_edge,
                'dominance_edge': dominance_edge,
                'serve_strength': serve_component,
                'return_strength': return_component
            }
            
            return form_metrics
            
        except Exception as e:
            logger.error(f"Form analysis error: {str(e)}")
            return {'form_factor': 0, 'match_recency': 0}
    
    def calculate_h2h_regression(self, player1: str, player2: str, surface: str) -> dict:
        """Brain 3: Head-to-head regression with sample size weighting"""
        try:
            # H2H data would come from Tennis Abstract player pages
            # For now, using statistical modeling based on player styles
            
            p1_stats = fuzzy_lookup(player1, self.tennis_stats)
            p2_stats = fuzzy_lookup(player2, self.tennis_stats)
            
            if not p1_stats or not p2_stats:
                return {'h2h_factor': 0, 'sample_size': 0}
            
            # Style matchup analysis
            p1_serve_pct = p1_stats.get('hold_pct', 0.8)
            p1_return_pct = p1_stats.get('break_pct', 0.2)
            p1_first_serve = p1_stats.get('first_serve_won', 0.7)
            
            p2_serve_pct = p2_stats.get('hold_pct', 0.8)
            p2_return_pct = p2_stats.get('break_pct', 0.2)
            p2_first_serve = p2_stats.get('first_serve_won', 0.7)
            
            # Matchup analysis: good returner vs weak server
            serve_return_matchup = (p1_return_pct - p2_serve_pct) - (p2_return_pct - p1_serve_pct)
            
            # First serve battle
            first_serve_battle = p1_first_serve - p2_first_serve
            
            # Surface-specific matchup weights
            surface_adj = self.surface_weights.get(surface, self.surface_weights['Hard'])
            
            h2h_factor = (
                serve_return_matchup * surface_adj['return_weight'] * 0.4 +
                first_serve_battle * surface_adj['serve_weight'] * 0.3
            )
            
            # Sample size weighting (estimate based on player rankings/matches)
            p1_matches = p1_stats.get('matches', 0)
            p2_matches = p2_stats.get('matches', 0)
            sample_weight = min((p1_matches + p2_matches) / 40.0, 1.0)
            
            h2h_factor *= sample_weight
            h2h_factor = max(-0.12, min(0.12, h2h_factor))  # Clamp to ±12%
            
            return {
                'h2h_factor': h2h_factor,
                'sample_size': int(p1_matches + p2_matches),
                'sample_weight': sample_weight,
                'serve_return_edge': serve_return_matchup,
                'first_serve_edge': first_serve_battle
            }
            
        except Exception as e:
            logger.error(f"H2H regression error for {player1} vs {player2}: {str(e)}")
            return {'h2h_factor': 0, 'sample_size': 0}
    
    def fetch_market_consensus(self, player1: str, player2: str) -> TennisMatchPrediction:
        """Brain 4: Market consensus from multiple bookmakers"""
        try:
            # Mock market data - in production, would aggregate from:
            # Pinnacle, Bet365, DraftKings, etc.
            
            # Simulate market odds with realistic tennis spreads
            base_prob = 0.5 + np.random.normal(0, 0.08)
            base_prob = max(0.25, min(0.75, base_prob))
            
            # Market typically efficient for tennis, so add minimal noise
            market_adjustment = np.random.normal(0, 0.02)
            p1_prob = base_prob + market_adjustment
            p1_prob = max(0.15, min(0.85, p1_prob))
            
            return TennisMatchPrediction(
                player1=player1,
                player2=player2,
                player1_prob=p1_prob,
                player2_prob=1 - p1_prob,
                surface="Unknown",  # Surface-agnostic market view
                source="market_consensus",
                confidence=0.88,
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Market consensus error for {player1} vs {player2}: {str(e)}")
            return None
    
    def analyze_tournament_context(self, tournament_name: str, round: str, surface: str) -> dict:
        """Analyze tournament importance, conditions, and context"""
        try:
            # Determine tournament tier
            tier = 'ATP 250'  # Default
            if any(slam in tournament_name.upper() for slam in ['AUSTRALIAN OPEN', 'FRENCH OPEN', 'WIMBLEDON', 'US OPEN']):
                tier = 'Grand Slam'
            elif 'MASTERS' in tournament_name.upper() or '1000' in tournament_name:
                tier = 'Masters 1000'
            elif '500' in tournament_name:
                tier = 'ATP 500'
            elif 'WTA' in tournament_name and '1000' in tournament_name:
                tier = 'WTA 1000'
            elif 'WTA' in tournament_name and '500' in tournament_name:
                tier = 'WTA 500'
            elif 'WTA' in tournament_name:
                tier = 'WTA 250'
            
            tier_data = self.tournament_tiers.get(tier, self.tournament_tiers['ATP 250'])
            
            # Round importance
            round_weights = {
                'R128': 0.5, 'R64': 0.6, 'R32': 0.7, 'R16': 0.8,
                'QF': 0.9, 'SF': 0.95, 'F': 1.0
            }
            round_weight = round_weights.get(round, 0.7)
            
            return {
                'tier': tier,
                'pressure_factor': tier_data['pressure'] * round_weight,
                'variance_factor': tier_data['variance'],
                'importance_weight': tier_data['importance'],
                'round_weight': round_weight,
                'surface': surface
            }
            
        except Exception as e:
            logger.error(f"Tournament context error: {str(e)}")
            return {'tier': 'ATP 250', 'pressure_factor': 1.0, 'variance_factor': 1.0}
    
    def calculate_ev_and_quality(self, ensemble_prob: float, market_prob: float, 
                                surface_conf: float, model_agreement: float) -> Tuple[float, str, float]:
        """Calculate expected value and quality grade with Kelly sizing"""
        try:
            # Expected value calculation
            if market_prob <= 0 or market_prob >= 1:
                return 0.0, "PASS", 0.0
            
            market_odds = 1 / market_prob
            expected_value = (ensemble_prob * market_odds - 1) / (market_odds - 1)
            
            # Quality grading based on EV, agreement, and surface confidence
            grade = "PASS"
            for grade_level, thresholds in self.quality_thresholds.items():
                if (expected_value >= thresholds['ev_min'] and 
                    model_agreement >= thresholds['agreement_min'] and
                    surface_conf >= thresholds['surface_conf_min']):
                    grade = grade_level
                    break
            
            # Kelly criterion for unit sizing
            if expected_value > 0 and ensemble_prob > 0:
                # Kelly = (bp - q) / b where b = odds-1, p = win prob, q = lose prob
                b = market_odds - 1
                p = ensemble_prob
                q = 1 - ensemble_prob
                kelly_fraction = (b * p - q) / b
                
                # Conservative Kelly (fractional)
                conservative_kelly = kelly_fraction * 0.25  # 25% Kelly
                recommended_units = max(0, min(3.0, conservative_kelly * 100))  # Cap at 3 units
            else:
                recommended_units = 0.0
            
            return expected_value, grade, recommended_units
            
        except Exception as e:
            logger.error(f"EV calculation error: {str(e)}")
            return 0.0, "PASS", 0.0
    
    def generate_ensemble_prediction(self, player1: str, player2: str, surface: str, 
                                   tournament: str, round: str) -> TennisEnsemblePrediction:
        """Generate ensemble prediction using 4-Brain methodology"""
        try:
            print(f"🎾 Analyzing {player1} vs {player2} ({surface} court)")
            
            # Brain 1: Elo-based predictions
            elo_pred = self.fetch_elo_predictions(player1, player2, surface)
            
            # Brain 2: Form analysis
            p1_stats = fuzzy_lookup(player1, self.tennis_stats)
            p2_stats = fuzzy_lookup(player2, self.tennis_stats)
            form_analysis = self.analyze_form_cycle(p1_stats or {}, p2_stats or {}, surface)
            
            # Brain 3: Head-to-head regression
            h2h_analysis = self.calculate_h2h_regression(player1, player2, surface)
            
            # Brain 4: Market consensus
            market_pred = self.fetch_market_consensus(player1, player2)
            
            # Tournament context
            tournament_context = self.analyze_tournament_context(tournament, round, surface)
            
            # Collect predictions
            predictions = {}
            if elo_pred:
                predictions['tennis_abstract_elo'] = elo_pred
            if market_pred:
                predictions['market_consensus'] = market_pred
            
            # Calculate ensemble probability
            ensemble_p1_prob = 0.5  # Default
            total_weight = 0.0
            
            for source, pred in predictions.items():
                weight = self.model_weights.get(source, 0.0)
                ensemble_p1_prob += pred.player1_prob * weight
                total_weight += weight
            
            # Apply form and H2H adjustments
            form_adjustment = form_analysis.get('form_factor', 0)
            h2h_adjustment = h2h_analysis.get('h2h_factor', 0)
            
            ensemble_p1_prob += form_adjustment + h2h_adjustment
            
            # Surface confidence based on Elo availability and match history
            surface_confidence = 0.7  # Base confidence
            if elo_pred and f"{surface.lower()}_elo" in (p1_stats or {}):
                surface_confidence += 0.15
            if form_analysis.get('match_recency', 0) > 0.7:
                surface_confidence += 0.1
            
            surface_confidence = min(surface_confidence, 0.95)
            
            # Model agreement calculation
            if len(predictions) > 1:
                probs = [pred.player1_prob for pred in predictions.values()]
                agreement = 1 - (max(probs) - min(probs))  # 1 - spread
            else:
                agreement = 0.6  # Lower confidence with single model
            
            # Clamp final probability
            ensemble_p1_prob = max(0.05, min(0.95, ensemble_p1_prob))
            ensemble_p2_prob = 1 - ensemble_p1_prob
            
            # Market probabilities (mock - would fetch real odds)
            market_p1_prob = 0.48 + np.random.normal(0, 0.05)
            market_p1_prob = max(0.15, min(0.85, market_p1_prob))
            market_p2_prob = 1 - market_p1_prob
            
            # Calculate EV and quality grades
            ev_p1, grade_p1, units_p1 = self.calculate_ev_and_quality(
                ensemble_p1_prob, market_p1_prob, surface_confidence, agreement
            )
            ev_p2, grade_p2, units_p2 = self.calculate_ev_and_quality(
                ensemble_p2_prob, market_p2_prob, surface_confidence, agreement
            )
            
            # Overall grade (best of the two)
            overall_grade = grade_p1 if ev_p1 > ev_p2 else grade_p2
            
            return TennisEnsemblePrediction(
                player1=player1,
                player2=player2,
                surface=surface,
                tournament=tournament,
                round=round,
                ensemble_p1_prob=ensemble_p1_prob,
                ensemble_p2_prob=ensemble_p2_prob,
                market_p1_prob=market_p1_prob,
                market_p2_prob=market_p2_prob,
                expected_value_p1=ev_p1,
                expected_value_p2=ev_p2,
                quality_grade=overall_grade,
                recommended_units_p1=units_p1,
                recommended_units_p2=units_p2,
                model_agreement=agreement,
                surface_weight=surface_confidence,
                form_factor=form_analysis.get('form_factor', 0),
                h2h_factor=h2h_analysis.get('h2h_factor', 0),
                tournament_context=tournament_context,
                individual_predictions=predictions
            )
            
        except Exception as e:
            logger.error(f"Ensemble prediction error for {player1} vs {player2}: {str(e)}")
            return None
    
    def get_daily_predictions(self, date_str: str = None) -> List[TennisEnsemblePrediction]:
        """Generate predictions for all matches on a given date"""
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        print(f"🎾 Generating tennis predictions for {date_str}")
        
        predictions = []
        
        # Get current tournament draws
        draws = get_tournament_draws()
        
        for tournament in draws:
            surface = self.detect_surface(tournament['name'])
            
            for match in tournament['matchups']:
                if match['status'] == 'upcoming':  # Only predict upcoming matches
                    pred = self.generate_ensemble_prediction(
                        player1=match['player1'],
                        player2=match['player2'],
                        surface=surface,
                        tournament=tournament['name'],
                        round=match.get('round', 'R32')
                    )
                    if pred:
                        predictions.append(pred)
        
        # Filter for quality picks
        quality_picks = [p for p in predictions if p.quality_grade in ['A+', 'A', 'B']]
        
        print(f"✅ Generated {len(predictions)} total predictions, {len(quality_picks)} quality picks")
        return predictions
    
    def detect_surface(self, tournament_name: str) -> str:
        """Detect court surface based on tournament name"""
        name_upper = tournament_name.upper()
        
        # Clay court tournaments
        if any(clay in name_upper for clay in ['FRENCH OPEN', 'ROLAND GARROS', 'MONTE CARLO', 
                                               'BARCELONA', 'MADRID', 'ROME', 'HAMBURG']):
            return 'Clay'
        
        # Grass court tournaments  
        if any(grass in name_upper for grass in ['WIMBLEDON', 'QUEENS', 'HALLE', 'EASTBOURNE']):
            return 'Grass'
        
        # Default to hard court
        return 'Hard'
    
    def format_prediction_output(self, prediction: TennisEnsemblePrediction) -> str:
        """Format prediction for display/logging"""
        return f"""
🎾 TENNIS PREDICTION - {prediction.quality_grade} GRADE

Match: {prediction.player1} vs {prediction.player2}
Surface: {prediction.surface} | Tournament: {prediction.tournament} | Round: {prediction.round}

ENSEMBLE PROBABILITIES:
{prediction.player1}: {prediction.ensemble_p1_prob:.1%}
{prediction.player2}: {prediction.ensemble_p2_prob:.1%}

MARKET vs MODEL:
{prediction.player1}: Market {prediction.market_p1_prob:.1%} | Model {prediction.ensemble_p1_prob:.1%} | EV: {prediction.expected_value_p1:+.1%}
{prediction.player2}: Market {prediction.market_p2_prob:.1%} | Model {prediction.ensemble_p2_prob:.1%} | EV: {prediction.expected_value_p2:+.1%}

RECOMMENDED UNITS:
{prediction.player1}: {prediction.recommended_units_p1:.1f} units
{prediction.player2}: {prediction.recommended_units_p2:.1f} units

ANALYSIS:
Form Factor: {prediction.form_factor:+.1%}
H2H Factor: {prediction.h2h_factor:+.1%}
Surface Confidence: {prediction.surface_weight:.1%}
Model Agreement: {prediction.model_agreement:.1%}
Tournament Tier: {prediction.tournament_context.get('tier', 'Unknown')}
        """

def main():
    """Main execution function"""
    engine = TennisPredictionEngine()
    
    # Generate today's predictions
    predictions = engine.get_daily_predictions()
    
    # Filter and display quality picks
    a_tier_picks = [p for p in predictions if p.quality_grade in ['A+', 'A']]
    
    if a_tier_picks:
        print("\n🏆 730'S LOCKS - TENNIS A-TIER PICKS")
        print("=" * 60)
        
        for pick in a_tier_picks:
            print(engine.format_prediction_output(pick))
            print("-" * 60)
    else:
        print("🚫 No A-tier tennis picks today. Market too efficient - PASS.")
    
    # Export for integration with other systems
    output_data = {
        'date': datetime.now().strftime("%Y-%m-%d"),
        'predictions': [],
        'summary': {
            'total_matches': len(predictions),
            'a_tier_picks': len(a_tier_picks),
            'average_ev': np.mean([max(p.expected_value_p1, p.expected_value_p2) for p in predictions]),
            'surfaces': list(set([p.surface for p in predictions]))
        }
    }
    
    for pred in a_tier_picks:
        output_data['predictions'].append({
            'player1': pred.player1,
            'player2': pred.player2,
            'surface': pred.surface,
            'tournament': pred.tournament,
            'grade': pred.quality_grade,
            'recommended_pick': pred.player1 if pred.recommended_units_p1 > pred.recommended_units_p2 else pred.player2,
            'recommended_units': max(pred.recommended_units_p1, pred.recommended_units_p2),
            'expected_value': max(pred.expected_value_p1, pred.expected_value_p2)
        })
    
    # Save to JSON for automation scripts
    with open(f'tennis_predictions_{datetime.now().strftime("%Y%m%d")}.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    return output_data

if __name__ == "__main__":
    main()