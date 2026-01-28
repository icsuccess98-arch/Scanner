"""
QUICK START EXAMPLE
===================

This script demonstrates how to use the NBA Betting Engine with sample data.
Replace the sample data with real data from your sources (Covers, NBA.com, etc.)
"""

from nba_betting_engine import (
    ComprehensiveGameAnalyzer,
    StatEngine,
    SpreadProjectionEngine,
    MarketEngine,
    EliminationFilters
)
import json


def example_pistons_vs_nuggets():
    """
    Complete example: Detroit Pistons @ Denver Nuggets
    
    This shows how to structure your data and get a complete analysis.
    """
    
    print("=" * 80)
    print("NBA BETTING ENGINE - QUICK START EXAMPLE")
    print("=" * 80)
    print()
    
    # Initialize the analyzer
    analyzer = ComprehensiveGameAnalyzer(sharp_threshold=15.0)
    
    # Sample game data (you would fetch this from your data sources)
    game_data = {
        # Basic game info
        'away_team': 'Detroit Pistons',
        'home_team': 'Denver Nuggets',
        
        # Stat comparison data (15 key metrics)
        'away_stats': {
            'ORB%': 28.5,           # Pistons offensive rebounding %
            'DRB%': 72.1,           # Pistons defensive rebounding %
            'TOV%': 12.8,           # Pistons turnover %
            'Forced TOV%': 14.2,    # Pistons forced turnovers %
            'OFF_Efficiency': 112.4, # Pistons offensive rating
            'DEF_Efficiency': 115.2, # Pistons defensive rating
            'eFG%': 53.1,           # Pistons effective FG%
            'Opp_eFG%': 55.8,       # Pistons opponent eFG%
            '3PM_Game': 12.3,       # Pistons 3PM per game
            'Opp_3PM_Game': 13.1,   # Pistons opponent 3PM
            'FT_Rate': 22.4,        # Pistons FT rate
            'Opp_FT_Rate': 24.1,    # Pistons opponent FT rate
            'SOS_Rank': 12,         # Pistons strength of schedule rank
            'H2H_L10': 4,           # Pistons H2H wins last 10
            'Rest_Days': 1          # Pistons rest days
        },
        'home_stats': {
            'ORB%': 26.8,
            'DRB%': 73.5,
            'TOV%': 13.5,
            'Forced TOV%': 13.8,
            'OFF_Efficiency': 116.7,
            'DEF_Efficiency': 112.1,
            'eFG%': 56.2,
            'Opp_eFG%': 52.4,
            '3PM_Game': 13.8,
            'Opp_3PM_Game': 11.9,
            'FT_Rate': 25.3,
            'Opp_FT_Rate': 21.8,
            'SOS_Rank': 8,
            'H2H_L10': 6,
            'Rest_Days': 2
        },
        
        # Spread projection data (SEPARATE from stats - for point differential only)
        'away_spread_data': {
            'ppg': 117.4,           # Pistons avg points scored
            'opp_ppg': 110.1,       # Pistons avg points allowed
            'def_eff': 115.2,       # Pistons defensive efficiency
            'pace': 98.5,           # Pistons pace (possessions/game)
            'off_to': 12.8,         # Pistons offensive turnovers/game
            'def_to': 14.2,         # Pistons defensive turnovers forced
            'orb': 10.5,            # Pistons offensive rebounds/game
            'drb': 32.8,            # Pistons defensive rebounds/game
            'fta': 22.4,            # Pistons FTA per game
            'ft_pct': 78.5,         # Pistons FT%
            'ast_to': 1.65,         # Pistons assist/TO ratio
            'fg_edge': -2.7,        # Pistons FG% - Opp FG% allowed
            'tp_edge': -0.8,        # Pistons 3P% - Opp 3P% allowed
            'ft_edge': -2.1         # Pistons FT% - Opp FT% allowed
        },
        'home_spread_data': {
            'ppg': 120.7,
            'opp_ppg': 116.2,
            'def_eff': 112.1,
            'pace': 101.2,
            'off_to': 13.5,
            'def_to': 13.8,
            'orb': 9.8,
            'drb': 34.1,
            'fta': 25.3,
            'ft_pct': 81.2,
            'ast_to': 1.82,
            'fg_edge': 3.8,
            'tp_edge': 1.9,
            'ft_edge': 3.5
        },
        
        # Market data (from Covers, ScoresAndOdds, etc.)
        'away_money': 45.0,         # 45% of money on Pistons
        'away_tickets': 52.0,       # 52% of tickets on Pistons
        'home_money': 55.0,         # 55% of money on Nuggets
        'home_tickets': 48.0,       # 48% of tickets on Nuggets
        'opening_spread': -4.5,     # Nuggets opened -4.5
        'current_spread': -3.0,     # Nuggets currently -3.0
        
        # Team records and context
        'away_record': (15, 18),    # Pistons 15-18
        'home_record': (22, 15),    # Nuggets 22-15
        'away_def_rank_l5': 18,     # Pistons defense rank last 5 games
        'home_def_rank_l5': 12,     # Nuggets defense rank last 5 games
        'away_is_b2b': False,       # Pistons not on back-to-back
        'home_is_b2b': False        # Nuggets not on back-to-back
    }
    
    # Run complete analysis
    print("Analyzing game...")
    analysis = analyzer.analyze_game(game_data)
    
    # Generate human-readable report
    print("\n")
    report = analyzer.generate_report(analysis)
    print(report)
    
    # Show structured decision data
    print("\n\n")
    print("=" * 80)
    print("STRUCTURED DECISION DATA (for app integration)")
    print("=" * 80)
    print(json.dumps(analysis['bet_recommendation'], indent=2))
    
    return analysis


def example_individual_components():
    """
    Example showing how to use individual components separately.
    """
    
    print("\n\n")
    print("=" * 80)
    print("INDIVIDUAL COMPONENT EXAMPLES")
    print("=" * 80)
    print()
    
    # Example 1: Just stat comparison
    print("1. STAT COMPARISON ONLY")
    print("-" * 40)
    
    stat_engine = StatEngine()
    
    away_stats = {
        'ORB%': 28.5,
        'DRB%': 72.1,
        'TOV%': 12.8,
        'Forced TOV%': 14.2,
        'OFF_Efficiency': 112.4,
        'DEF_Efficiency': 115.2,
        'eFG%': 53.1,
        'Opp_eFG%': 55.8,
        '3PM_Game': 12.3,
        'Opp_3PM_Game': 13.1,
        'FT_Rate': 22.4,
        'Opp_FT_Rate': 24.1,
        'SOS_Rank': 12,
        'H2H_L10': 4,
        'Rest_Days': 1
    }
    
    home_stats = {
        'ORB%': 26.8,
        'DRB%': 73.5,
        'TOV%': 13.5,
        'Forced TOV%': 13.8,
        'OFF_Efficiency': 116.7,
        'DEF_Efficiency': 112.1,
        'eFG%': 56.2,
        'Opp_eFG%': 52.4,
        '3PM_Game': 13.8,
        'Opp_3PM_Game': 11.9,
        'FT_Rate': 25.3,
        'Opp_FT_Rate': 21.8,
        'SOS_Rank': 8,
        'H2H_L10': 6,
        'Rest_Days': 2
    }
    
    away_edges, home_edges, comparisons = stat_engine.calculate_stat_edges(
        away_stats, home_stats
    )
    
    print(f"Away edges: {away_edges}")
    print(f"Home edges: {home_edges}")
    print(f"Winner: {'Away' if away_edges > home_edges else 'Home' if home_edges > away_edges else 'Tie'}")
    print()
    
    # Example 2: Just spread projection
    print("2. SPREAD PROJECTION ONLY")
    print("-" * 40)
    
    spread_engine = SpreadProjectionEngine()
    
    away_data = {
        'ppg': 117.4,
        'opp_ppg': 110.1,
        'def_eff': 115.2,
        'pace': 98.5,
        'off_to': 12.8,
        'def_to': 14.2,
        'orb': 10.5,
        'drb': 32.8,
        'fta': 22.4,
        'ft_pct': 78.5,
        'ast_to': 1.65,
        'fg_edge': -2.7,
        'tp_edge': -0.8,
        'ft_edge': -2.1
    }
    
    home_data = {
        'ppg': 120.7,
        'opp_ppg': 116.2,
        'def_eff': 112.1,
        'pace': 101.2,
        'off_to': 13.5,
        'def_to': 13.8,
        'orb': 9.8,
        'drb': 34.1,
        'fta': 25.3,
        'ft_pct': 81.2,
        'ast_to': 1.82,
        'fg_edge': 3.8,
        'tp_edge': 1.9,
        'ft_edge': 3.5
    }
    
    projection = spread_engine.true_spread(away_data, home_data)
    
    print(f"Away projected points: {projection['away_proj_pts']}")
    print(f"Home projected points: {projection['home_proj_pts']}")
    print(f"True spread: {projection['true_spread']:.1f}")
    print(f"(Negative = away underdog, Positive = away favored)")
    print()
    
    # Example 3: Just market analysis
    print("3. MARKET ANALYSIS ONLY")
    print("-" * 40)
    
    market_engine = MarketEngine(sharp_threshold=15.0)
    
    market_summary = market_engine.market_summary(
        away_money=45.0,
        away_tkt=52.0,
        home_money=55.0,
        home_tkt=48.0,
        opening_spread=-4.5,
        current_spread=-3.0
    )
    
    print(f"Sharp side: {market_summary['sharp_info']['sharp_side']}")
    print(f"Away diff: {market_summary['sharp_info']['away_diff']:.1f} (money - tickets)")
    print(f"Home diff: {market_summary['sharp_info']['home_diff']:.1f} (money - tickets)")
    print(f"RLM detected: {market_summary['rlm_info']['rlm_detected']}")
    print(f"RLM flag: {market_summary['rlm_info']['rlm_flag']}")
    print()
    
    # Example 4: Just elimination filters
    print("4. ELIMINATION FILTERS ONLY")
    print("-" * 40)
    
    filter_data = {
        'market_summary': market_summary,
        'spread': -3.0,
        'away_record': (15, 18),
        'home_record': (22, 15),
        'away_def_rank_l5': 18,
        'home_def_rank_l5': 12,
        'away_is_b2b': False,
        'home_is_b2b': False
    }
    
    filter_result = EliminationFilters.run_all_filters(filter_data)
    
    print(f"Should avoid: {filter_result['should_avoid']}")
    if filter_result['reasons']:
        print("Reasons:")
        for reason in filter_result['reasons']:
            print(f"  - {reason}")
    else:
        print("✓ Game passes all filters")
    print()


def example_daily_slate():
    """
    Example showing how to analyze multiple games in a daily slate.
    """
    
    print("\n\n")
    print("=" * 80)
    print("DAILY SLATE ANALYSIS")
    print("=" * 80)
    print()
    
    analyzer = ComprehensiveGameAnalyzer(sharp_threshold=15.0)
    
    # Sample slate of games (you would fetch this from your data source)
    games = [
        {
            'away_team': 'Detroit Pistons',
            'home_team': 'Denver Nuggets',
            'current_spread': -3.0,
            # ... (rest of game data)
        },
        # Add more games...
    ]
    
    print("Analyzing today's slate...")
    print()
    
    recommendations = []
    
    for game in games:
        # For this example, we'll just use the one game we have data for
        game_data = {
            'away_team': 'Detroit Pistons',
            'home_team': 'Denver Nuggets',
            'away_stats': {
                'ORB%': 28.5, 'DRB%': 72.1, 'TOV%': 12.8, 'Forced TOV%': 14.2,
                'OFF_Efficiency': 112.4, 'DEF_Efficiency': 115.2, 'eFG%': 53.1,
                'Opp_eFG%': 55.8, '3PM_Game': 12.3, 'Opp_3PM_Game': 13.1,
                'FT_Rate': 22.4, 'Opp_FT_Rate': 24.1, 'SOS_Rank': 12,
                'H2H_L10': 4, 'Rest_Days': 1
            },
            'home_stats': {
                'ORB%': 26.8, 'DRB%': 73.5, 'TOV%': 13.5, 'Forced TOV%': 13.8,
                'OFF_Efficiency': 116.7, 'DEF_Efficiency': 112.1, 'eFG%': 56.2,
                'Opp_eFG%': 52.4, '3PM_Game': 13.8, 'Opp_3PM_Game': 11.9,
                'FT_Rate': 25.3, 'Opp_FT_Rate': 21.8, 'SOS_Rank': 8,
                'H2H_L10': 6, 'Rest_Days': 2
            },
            'away_spread_data': {
                'ppg': 117.4, 'opp_ppg': 110.1, 'def_eff': 115.2, 'pace': 98.5,
                'off_to': 12.8, 'def_to': 14.2, 'orb': 10.5, 'drb': 32.8,
                'fta': 22.4, 'ft_pct': 78.5, 'ast_to': 1.65, 'fg_edge': -2.7,
                'tp_edge': -0.8, 'ft_edge': -2.1
            },
            'home_spread_data': {
                'ppg': 120.7, 'opp_ppg': 116.2, 'def_eff': 112.1, 'pace': 101.2,
                'off_to': 13.5, 'def_to': 13.8, 'orb': 9.8, 'drb': 34.1,
                'fta': 25.3, 'ft_pct': 81.2, 'ast_to': 1.82, 'fg_edge': 3.8,
                'tp_edge': 1.9, 'ft_edge': 3.5
            },
            'away_money': 45.0, 'away_tickets': 52.0,
            'home_money': 55.0, 'home_tickets': 48.0,
            'opening_spread': -4.5, 'current_spread': -3.0,
            'away_record': (15, 18), 'home_record': (22, 15),
            'away_def_rank_l5': 18, 'home_def_rank_l5': 12,
            'away_is_b2b': False, 'home_is_b2b': False
        }
        
        analysis = analyzer.analyze_game(game_data)
        
        recommendations.append({
            'game': analysis['game'],
            'decision': analysis['bet_recommendation']['decision'],
            'confidence': analysis['bet_recommendation']['confidence'],
            'reasons': analysis['bet_recommendation']['reasons'][:2]  # Top 2 reasons
        })
        
        break  # Only one game in this example
    
    # Display summary
    print("📋 TODAY'S RECOMMENDATIONS")
    print("-" * 80)
    
    for rec in recommendations:
        print(f"\n{rec['game']}")
        print(f"  Decision: {rec['decision']}")
        print(f"  Confidence: {rec['confidence']}")
        if rec['reasons']:
            print(f"  Key factors: {rec['reasons'][0]}")


if __name__ == "__main__":
    # Run all examples
    
    # 1. Full game analysis
    analysis = example_pistons_vs_nuggets()
    
    # 2. Individual components
    example_individual_components()
    
    # 3. Daily slate
    example_daily_slate()
    
    print("\n\n")
    print("=" * 80)
    print("EXAMPLES COMPLETE")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Replace sample data with real data from your sources")
    print("2. Implement web scraping for automated data collection")
    print("3. Set up database for tracking and backtesting")
    print("4. Configure alerts for high-confidence bets")
    print()
    print("See README.md for complete documentation.")
