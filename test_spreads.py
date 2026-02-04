#!/usr/bin/env python3
"""Comprehensive test script for the spreads page functionality"""

import sys
import os
sys.path.insert(0, '/home/runner/workspace')

from datetime import datetime, timedelta
from sports_app import app, db, Game, MatchupIntelligence
from sqlalchemy import func

def print_section(title):
    print("\n" + "="*80)
    print(f" {title}")
    print("="*80 + "\n")

def test_database_games():
    """Test 1 & 4: Check database for today's games and their stats"""
    print_section("TEST 1 & 4: Database Games Check")
    
    with app.app_context():
        today = datetime.now().date()
        
        # Get all games for today
        games = db.session.query(Game).filter(
            Game.date >= today,
            Game.date < today + timedelta(days=1)
        ).all()
        
        total_games = len(games)
        print(f"Total games for today ({today}): {total_games}")
        
        if total_games == 0:
            print("WARNING: No games found for today!")
            # Check for games in the next few days
            future_games = db.session.query(Game).filter(Game.date >= today).limit(10).all()
            if len(future_games) > 0:
                print(f"\nFound {len(future_games)} upcoming games:")
                for g in future_games:
                    print(f"  - {g.date}: {g.away_team} @ {g.home_team} ({g.league})")
            return
        
        # Group by league
        leagues = {}
        for game in games:
            if game.league not in leagues:
                leagues[game.league] = []
            leagues[game.league].append(game)
        
        print(f"\nGames by league:")
        for league, league_games in leagues.items():
            print(f"  {league}: {len(league_games)} games")
        
        # Check stats for each league
        print("\n" + "-"*80)
        print("DETAILED STATS CHECK BY LEAGUE")
        print("-"*80)
        
        for league, league_games in leagues.items():
            print(f"\n{league} Games ({len(league_games)} total):")
            
            stats_fields = ['away_ppg', 'home_ppg', 'away_opp_ppg', 'home_opp_ppg']
            betting_fields = ['spread_line', 'line', 'edge']
            
            for i, game in enumerate(league_games[:3], 1):  # Show first 3 games
                print(f"\n  Game {i}: {game.away_team} @ {game.home_team}")
                print(f"    Game ID: {game.id}")
                print(f"    Time: {game.game_time}")
                
                # Check stats
                print(f"    Stats:")
                for field in stats_fields:
                    value = getattr(game, field, None)
                    status = "SET" if value is not None else "MISSING"
                    print(f"      {field}: {value} [{status}]")
                
                # Check betting data
                print(f"    Betting:")
                for field in betting_fields:
                    value = getattr(game, field, None)
                    status = "SET" if value is not None else "MISSING"
                    print(f"      {field}: {value} [{status}]")
                
                # Check spread_is_qualified
                qualified = getattr(game, 'spread_is_qualified', None)
                print(f"    spread_is_qualified: {qualified}")
            
            # Summary stats for this league
            games_with_stats = sum(1 for g in league_games if g.away_ppg is not None)
            games_with_spread = sum(1 for g in league_games if g.spread_line is not None)
            games_with_edge = sum(1 for g in league_games if g.edge is not None)
            games_qualified = sum(1 for g in league_games if getattr(g, 'spread_is_qualified', False))
            
            print(f"\n  {league} Summary:")
            print(f"    Games with stats (ppg): {games_with_stats}/{len(league_games)}")
            print(f"    Games with spread_line: {games_with_spread}/{len(league_games)}")
            print(f"    Games with edge: {games_with_edge}/{len(league_games)}")
            print(f"    Games spread_is_qualified: {games_qualified}/{len(league_games)}")

def test_spreads_route():
    """Test 1: Test the /spreads route"""
    print_section("TEST 1: /spreads Route")
    
    with app.test_client() as client:
        try:
            response = client.get('/spreads')
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print("SUCCESS: Route returns 200")
                content = response.data.decode('utf-8')
                print(f"Response length: {len(content)} bytes")
                
                # Check for error messages in HTML
                if 'error' in content.lower() and 'error-' not in content.lower():
                    print("WARNING: Found 'error' in response content")
                    # Find error messages
                    import re
                    errors = re.findall(r'error[^<]{0,100}', content.lower())
                    for err in errors[:3]:
                        print(f"  - {err}")
                
                if 'exception' in content.lower():
                    print("WARNING: Found 'exception' in response content")
                
            else:
                print(f"FAILED: Route returned {response.status_code}")
                print(f"Response: {response.data.decode('utf-8')[:500]}")
                
        except Exception as e:
            print(f"ERROR: Exception occurred: {e}")
            import traceback
            traceback.print_exc()

def test_matchup_data_endpoint():
    """Test 3: Test the /api/matchup_data/<game_id> endpoint"""
    print_section("TEST 3: /api/matchup_data Endpoint")
    
    with app.app_context():
        today = datetime.now().date()
        
        # Get a few game IDs
        games = db.session.query(Game).filter(
            Game.date >= today,
            Game.date < today + timedelta(days=1)
        ).limit(5).all()
        
        if len(games) == 0:
            print("WARNING: No games found for today, cannot test endpoint")
            # Try any game
            any_game = db.session.query(Game).first()
            if any_game:
                games = [any_game]
                print(f"Using game from {any_game.date} instead")
            else:
                print("ERROR: No games in database at all!")
                return
        
        with app.test_client() as client:
            for game in games:
                print(f"\nTesting game_id={game.id}: {game.away_team} @ {game.home_team} ({game.league})")
                
                try:
                    response = client.get(f'/api/matchup_data/{game.id}')
                    print(f"  Status Code: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.get_json()
                        print(f"  SUCCESS: Got JSON response")
                        print(f"  Keys in response: {list(data.keys())}")
                        
                        # Check for important fields
                        if 'error' in data:
                            print(f"  ERROR in response: {data['error']}")
                        else:
                            print(f"  rlm_data present: {'rlm_data' in data}")
                            print(f"  matchup_stats present: {'matchup_stats' in data}")
                            
                            if 'rlm_data' in data:
                                rlm = data['rlm_data']
                                print(f"  rlm_data type: {type(rlm)}")
                                if rlm:
                                    print(f"  rlm_data keys: {list(rlm.keys()) if isinstance(rlm, dict) else 'N/A'}")
                            
                            if 'matchup_stats' in data:
                                stats = data['matchup_stats']
                                if stats:
                                    print(f"  matchup_stats keys: {list(stats.keys()) if isinstance(stats, dict) else 'N/A'}")
                                    # Check for logos and records
                                    if isinstance(stats, dict):
                                        print(f"    away_logo: {'present' if stats.get('away_logo') else 'MISSING'}")
                                        print(f"    home_logo: {'present' if stats.get('home_logo') else 'MISSING'}")
                                        print(f"    away_record: {stats.get('away_record', 'MISSING')}")
                                        print(f"    home_record: {stats.get('home_record', 'MISSING')}")
                    else:
                        print(f"  FAILED: {response.status_code}")
                        print(f"  Response: {response.data.decode('utf-8')[:200]}")
                        
                except Exception as e:
                    print(f"  ERROR: {e}")
                    import traceback
                    traceback.print_exc()

def test_covers_scraping():
    """Test 5: Test Covers stats scraping"""
    print_section("TEST 5: Covers Stats Scraping")
    
    # Import the function
    try:
        from enhanced_scraping import get_covers_matchup_stats
        print("Successfully imported get_covers_matchup_stats")
        
        with app.app_context():
            today = datetime.now().date()
            
            # Get a game to test with
            game = db.session.query(Game).filter(
                Game.date >= today,
                Game.date < today + timedelta(days=1)
            ).first()
            
            if not game:
                print("WARNING: No games for today, trying any game")
                game = db.session.query(Game).first()
            
            if game:
                print(f"\nTesting with: {game.away_team} @ {game.home_team} ({game.league})")
                
                try:
                    result = get_covers_matchup_stats(
                        game.away_team,
                        game.home_team,
                        game.league
                    )
                    
                    if result:
                        print("SUCCESS: Got stats from Covers")
                        print(f"Result keys: {list(result.keys()) if isinstance(result, dict) else 'N/A'}")
                        if isinstance(result, dict):
                            print(f"  away_logo: {'present' if result.get('away_logo') else 'missing'}")
                            print(f"  home_logo: {'present' if result.get('home_logo') else 'missing'}")
                            print(f"  away_record: {result.get('away_record', 'missing')}")
                            print(f"  home_record: {result.get('home_record', 'missing')}")
                    else:
                        print("FAILED: No stats returned from Covers")
                        
                except Exception as e:
                    print(f"ERROR: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print("ERROR: No games in database to test with")
                
    except ImportError as e:
        print(f"ERROR: Cannot import get_covers_matchup_stats: {e}")

def test_vsin_data():
    """Test 6: Test VSIN RLM data fetching"""
    print_section("TEST 6: VSIN RLM Data Fetching")
    
    with app.app_context():
        today = datetime.now().date()
        
        # Get a few games to test
        games = db.session.query(Game).filter(
            Game.date >= today,
            Game.date < today + timedelta(days=1)
        ).limit(3).all()
        
        if len(games) == 0:
            print("WARNING: No games for today")
            games = db.session.query(Game).limit(3).all()
        
        for game in games:
            print(f"\nTesting: {game.away_team} @ {game.home_team} ({game.league})")
            
            try:
                rlm_data = MatchupIntelligence.fetch_rlm_data(game.id)
                
                print(f"  Result type: {type(rlm_data)}")
                
                if rlm_data:
                    print("  SUCCESS: Got RLM data")
                    if isinstance(rlm_data, dict):
                        print(f"  Keys: {list(rlm_data.keys())}")
                else:
                    print("  FAILED: No RLM data returned")
                    
            except Exception as e:
                print(f"  ERROR: {e}")
                import traceback
                traceback.print_exc()

def test_template_rendering():
    """Test 7: Test template rendering"""
    print_section("TEST 7: Template Rendering")
    
    try:
        from flask import render_template
        
        with app.app_context():
            today = datetime.now().date()
            
            # Get games
            games = db.session.query(Game).filter(
                Game.date >= today,
                Game.date < today + timedelta(days=1)
            ).all()
            
            print(f"Attempting to render spreads.html with {len(games)} games")
            
            try:
                html = render_template('spreads.html', games=games)
                print(f"SUCCESS: Template rendered ({len(html)} bytes)")
                
                # Check for Jinja2 errors in output
                if '{{' in html or '{%' in html:
                    print("WARNING: Found unrendered Jinja2 tags in output!")
                    # Show some examples
                    import re
                    unrendered = re.findall(r'(\{\{[^}]+\}\}|\{%[^%]+%\})', html)
                    for tag in unrendered[:5]:
                        print(f"  - {tag}")
                    
            except Exception as e:
                print(f"ERROR: Template rendering failed: {e}")
                import traceback
                traceback.print_exc()
                
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

def test_team_name_matching():
    """Test 9: Test team name matching for VSIN data"""
    print_section("TEST 9: Team Name Matching")
    
    try:
        from team_identity import TeamIdentity
        
        with app.app_context():
            today = datetime.now().date()
            
            # Get games from different leagues
            leagues = ['NBA', 'CBB', 'NHL']
            
            for league in leagues:
                print(f"\n{league} Team Name Matching:")
                
                games = db.session.query(Game).filter(
                    Game.league == league,
                    Game.date >= today,
                    Game.date < today + timedelta(days=1)
                ).limit(2).all()
                
                if len(games) == 0:
                    print(f"  No games found for {league}")
                    continue
                
                for game in games:
                    print(f"\n  Game: {game.away_team} @ {game.home_team}")
                    
                    # Try to get VSIN names
                    try:
                        away_vsin = TeamIdentity.get_vsin_name(game.away_team, league)
                        home_vsin = TeamIdentity.get_vsin_name(game.home_team, league)
                        
                        print(f"    Away: {game.away_team} -> VSIN: {away_vsin}")
                        print(f"    Home: {game.home_team} -> VSIN: {home_vsin}")
                        
                        if away_vsin and home_vsin:
                            print(f"    SUCCESS: Both teams matched")
                        else:
                            print(f"    FAILED: Team name matching incomplete")
                            
                    except Exception as e:
                        print(f"    ERROR: {e}")
                        
    except ImportError as e:
        print(f"ERROR: Cannot import TeamIdentity: {e}")

def main():
    print("="*80)
    print(" SPREADS PAGE COMPREHENSIVE TEST SUITE")
    print(" Date: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("="*80)
    
    try:
        # Run all tests
        test_database_games()
        test_spreads_route()
        test_matchup_data_endpoint()
        test_covers_scraping()
        test_vsin_data()
        test_template_rendering()
        test_team_name_matching()
        
        print("\n" + "="*80)
        print(" TEST SUITE COMPLETE")
        print("="*80)
        
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
