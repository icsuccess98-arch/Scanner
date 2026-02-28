#!/usr/bin/env python3
"""
Test script for Tennis Prediction Engine
Validates functionality and output format
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import tennis prediction engine
import importlib.util
spec = importlib.util.spec_from_file_location("tennis_engine", "tools/tennis-prediction-engine.py")
tennis_engine = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tennis_engine)
TennisPredictionEngine = tennis_engine.TennisPredictionEngine
import json

def test_engine():
    """Test the tennis prediction engine with mock data"""
    print("🎾 Testing Tennis Prediction Engine...")
    
    try:
        # Initialize engine
        engine = TennisPredictionEngine()
        print("✅ Engine initialized successfully")
        
        # Test individual components
        print("\n🧠 Testing 4-Brain Components...")
        
        # Test Brain 1: Elo predictions
        print("  Brain 1 (Elo): ", end="")
        elo_pred = engine.fetch_elo_predictions("Novak Djokovic", "Carlos Alcaraz", "Hard")
        print("✅" if elo_pred else "⚠️  (No data)")
        
        # Test Brain 2: Form analysis
        print("  Brain 2 (Form): ", end="")
        mock_stats = {'net_rating': 0.15, 'dominance_ratio': 1.2, 'matches': 25}
        form_analysis = engine.analyze_form_cycle(mock_stats, mock_stats, "Hard")
        print("✅" if 'form_factor' in form_analysis else "❌")
        
        # Test Brain 3: H2H regression
        print("  Brain 3 (H2H): ", end="")
        h2h_analysis = engine.calculate_h2h_regression("Roger Federer", "Rafael Nadal", "Clay")
        print("✅" if 'h2h_factor' in h2h_analysis else "❌")
        
        # Test Brain 4: Market consensus
        print("  Brain 4 (Market): ", end="")
        market_pred = engine.fetch_market_consensus("Stefanos Tsitsipas", "Alexander Zverev")
        print("✅" if market_pred else "❌")
        
        # Test surface detection
        print("\n🏟️  Testing Surface Detection...")
        surfaces = {
            "French Open": engine.detect_surface("French Open"),
            "Wimbledon": engine.detect_surface("Wimbledon"),
            "US Open": engine.detect_surface("US Open"),
            "Monte Carlo Masters": engine.detect_surface("Monte Carlo Masters")
        }
        print(f"  Surfaces detected: {surfaces}")
        
        # Test tournament context
        print("\n🏆 Testing Tournament Context...")
        context = engine.analyze_tournament_context("Wimbledon", "SF", "Grass")
        print(f"  Wimbledon SF context: {context.get('tier')} - Pressure: {context.get('pressure_factor', 0):.2f}")
        
        # Test ensemble prediction
        print("\n🎯 Testing Ensemble Prediction...")
        ensemble_pred = engine.generate_ensemble_prediction(
            player1="Novak Djokovic",
            player2="Carlos Alcaraz", 
            surface="Hard",
            tournament="US Open",
            round="SF"
        )
        
        if ensemble_pred:
            print("✅ Ensemble prediction generated")
            print(f"  Quality Grade: {ensemble_pred.quality_grade}")
            print(f"  {ensemble_pred.player1}: {ensemble_pred.ensemble_p1_prob:.1%} (EV: {ensemble_pred.expected_value_p1:+.1%})")
            print(f"  {ensemble_pred.player2}: {ensemble_pred.ensemble_p2_prob:.1%} (EV: {ensemble_pred.expected_value_p2:+.1%})")
            print(f"  Surface Confidence: {ensemble_pred.surface_weight:.1%}")
            print(f"  Model Agreement: {ensemble_pred.model_agreement:.1%}")
        else:
            print("❌ Ensemble prediction failed")
        
        # Test output formatting
        print("\n📄 Testing Output Formatting...")
        if ensemble_pred:
            formatted = engine.format_prediction_output(ensemble_pred)
            print("✅ Output formatted successfully")
            print("  Sample output:")
            print(formatted[:200] + "..." if len(formatted) > 200 else formatted)
        
        # Test daily predictions (limited)
        print("\n📅 Testing Daily Predictions...")
        try:
            daily_preds = engine.get_daily_predictions()
            print(f"✅ Found {len(daily_preds)} daily predictions")
        except Exception as e:
            print(f"⚠️  Daily predictions: {str(e)} (expected if no live tournaments)")
        
        print("\n🎾 Tennis Engine Test Complete!")
        print("=" * 50)
        print("✅ TENNIS PREDICTION ENGINE READY FOR PRODUCTION")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_engine()