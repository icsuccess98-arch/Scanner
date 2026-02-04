#!/usr/bin/env python3
"""
Comprehensive Test Suite for Sports App Fixes
Tests:
1. CBB Team Name Mapping (17 problematic mid-major teams)
2. API Endpoint Response Structure
3. Database Stats Population
"""

import sys
sys.path.insert(0, '/home/runner/workspace')

from sports_app import app, db, Game
from datetime import datetime, timedelta
import json

print("=" * 80)
print("COMPREHENSIVE TEST SUITE - SPORTS APP FIXES")
print("=" * 80)

# ============================================================================
# TEST 1: CBB TEAM NAME MAPPING
# ============================================================================
print("\n" + "=" * 80)
print("TEST 1: CBB TEAM NAME MAPPING FOR 17 MID-MAJOR TEAMS")
print("=" * 80)

problematic_teams = [
    "SC Upstate",
    "Gardner-Webb",
    "N'Western St",
    "MD Eastern",
    "Kansas City",
    "High Point",
    "Lipscomb",
    "Austin Peay",
    "Purdue FW",
    "Charleston So",
    "Longwood",
    "Tulsa",
    "E Texas A&M",
    "AR-Pine Bluff",
    "Grambling",
    "C Arkansas",
    "MTSU",
    "FIU",
    "SC State"
]

with app.app_context():
    # Import the mapping function
    from enhanced_scraping import normalize_cbb_team_name
    
    print(f"\nTesting {len(problematic_teams)} problematic team names:\n")
    
    mapping_results = []
    for team in problematic_teams:
        normalized = normalize_cbb_team_name(team)
        status = "PASS" if normalized != team else "UNCHANGED"
        mapping_results.append({
            'original': team,
            'normalized': normalized,
            'status': status
        })
        print(f"  {status:9s} | {team:20s} -> {normalized}")
    
    passed = sum(1 for r in mapping_results if r['status'] == 'PASS')
    unchanged = sum(1 for r in mapping_results if r['status'] == 'UNCHANGED')
    print(f"\nMapping Summary: {passed} normalized, {unchanged} unchanged")
    
    # Check if these teams exist in the database
    print("\n" + "-" * 80)
    print("Checking Database for CBB Games with These Teams:")
    print("-" * 80)
    
    today = datetime.now().date()
    date_range_start = today - timedelta(days=7)
    date_range_end = today + timedelta(days=7)
    
    found_games = []
    for team in problematic_teams:
        games = db.session.query(Game).filter(
            Game.league == 'CBB',
            Game.date >= date_range_start,
            Game.date <= date_range_end,
            ((Game.away_team.like(f'%{team}%')) | (Game.home_team.like(f'%{team}%')))
        ).all()
        
        if games:
            for game in games:
                found_games.append(game)
                print(f"  Found: {game.away_team} @ {game.home_team} on {game.date} (ID: {game.id})")
    
    if not found_games:
        print("  No games found for these teams in the past/next 7 days")
    else:
        print(f"\nTotal CBB games found with problematic teams: {len(found_games)}")

# ============================================================================
# TEST 2: API ENDPOINT RESPONSE STRUCTURE
# ============================================================================
print("\n" + "=" * 80)
print("TEST 2: API ENDPOINT RESPONSE STRUCTURE")
print("=" * 80)

with app.app_context():
    today = datetime.now().date()
    
    # Test CBB Game
    print("\n" + "-" * 80)
    print("Testing CBB Game:")
    print("-" * 80)
    
    cbb_game = db.session.query(Game).filter(
        Game.league == 'CBB',
        Game.date >= today,
        Game.date < today + timedelta(days=3)
    ).first()
    
    if cbb_game:
        print(f"\nGame: {cbb_game.away_team} @ {cbb_game.home_team}")
        print(f"Date: {cbb_game.date}, ID: {cbb_game.id}")
        
        with app.test_client() as client:
            response = client.get(f'/api/matchup_data/{cbb_game.id}')
            print(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.get_json()
                
                # Check for required keys
                checks = {
                    '3PT%': '3PT%' in data,
                    'PPP': 'PPP' in data,
                    'Opp PPP': 'Opp PPP' in data,
                    'away_logo': 'away_logo' in data,
                    'home_logo': 'home_logo' in data,
                }
                
                print("\nKey Presence Checks:")
                all_passed = True
                for key, present in checks.items():
                    status = "PASS" if present else "FAIL"
                    if not present:
                        all_passed = False
                    value = data.get(key, "N/A") if present else "MISSING"
                    print(f"  {status:4s} | {key:20s}: {value}")
                
                # Check for incorrect keys
                if '3P%' in data:
                    print(f"  WARN | Found '3P%' (should be '3PT%'): {data['3P%']}")
                    all_passed = False
                
                # Check non-zero fallback values
                print("\nNon-Zero Fallback Checks:")
                ppp_value = data.get('PPP', 'N/A')
                opp_ppp_value = data.get('Opp PPP', 'N/A')
                
                ppp_ok = ppp_value not in ['N/A', None, 0, '0']
                opp_ppp_ok = opp_ppp_value not in ['N/A', None, 0, '0']
                
                print(f"  {'PASS' if ppp_ok else 'FAIL'} | PPP value: {ppp_value}")
                print(f"  {'PASS' if opp_ppp_ok else 'FAIL'} | Opp PPP value: {opp_ppp_value}")
                
                if not ppp_ok or not opp_ppp_ok:
                    all_passed = False
                
                # Print full response for inspection
                print("\nFull API Response:")
                print(json.dumps(data, indent=2, default=str))
                
                print(f"\nCBB API Test Result: {'PASS' if all_passed else 'FAIL'}")
            else:
                print(f"FAIL: API returned status {response.status_code}")
                print(response.get_data(as_text=True))
    else:
        print("No CBB games found in the next 3 days")
    
    # Test NBA Game
    print("\n" + "-" * 80)
    print("Testing NBA Game:")
    print("-" * 80)
    
    nba_game = db.session.query(Game).filter(
        Game.league == 'NBA',
        Game.date >= today,
        Game.date < today + timedelta(days=3)
    ).first()
    
    if nba_game:
        print(f"\nGame: {nba_game.away_team} @ {nba_game.home_team}")
        print(f"Date: {nba_game.date}, ID: {nba_game.id}")
        
        with app.test_client() as client:
            response = client.get(f'/api/matchup_data/{nba_game.id}')
            print(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.get_json()
                
                # Check for required keys
                checks = {
                    '3PT%': '3PT%' in data,
                    'PPP': 'PPP' in data,
                    'Opp PPP': 'Opp PPP' in data,
                    'away_logo': 'away_logo' in data,
                    'home_logo': 'home_logo' in data,
                }
                
                print("\nKey Presence Checks:")
                all_passed = True
                for key, present in checks.items():
                    status = "PASS" if present else "FAIL"
                    if not present:
                        all_passed = False
                    value = data.get(key, "N/A") if present else "MISSING"
                    print(f"  {status:4s} | {key:20s}: {value}")
                
                # Check non-zero fallback values
                print("\nNon-Zero Fallback Checks:")
                ppp_value = data.get('PPP', 'N/A')
                opp_ppp_value = data.get('Opp PPP', 'N/A')
                
                ppp_ok = ppp_value not in ['N/A', None, 0, '0']
                opp_ppp_ok = opp_ppp_value not in ['N/A', None, 0, '0']
                
                print(f"  {'PASS' if ppp_ok else 'FAIL'} | PPP value: {ppp_value}")
                print(f"  {'PASS' if opp_ppp_ok else 'FAIL'} | Opp PPP value: {opp_ppp_value}")
                
                if not ppp_ok or not opp_ppp_ok:
                    all_passed = False
                
                # Print abbreviated response
                print("\nAbbreviated API Response (key stats):")
                relevant_keys = ['away_team', 'home_team', '3PT%', 'PPP', 'Opp PPP', 'away_logo', 'home_logo']
                abbrev_data = {k: data.get(k, 'N/A') for k in relevant_keys if k in data}
                print(json.dumps(abbrev_data, indent=2, default=str))
                
                print(f"\nNBA API Test Result: {'PASS' if all_passed else 'FAIL'}")
            else:
                print(f"FAIL: API returned status {response.status_code}")
                print(response.get_data(as_text=True))
    else:
        print("No NBA games found in the next 3 days")

# ============================================================================
# TEST 3: DATABASE STATS POPULATION
# ============================================================================
print("\n" + "=" * 80)
print("TEST 3: DATABASE STATS POPULATION")
print("=" * 80)

with app.app_context():
    # Total games count
    total_games = db.session.query(Game).count()
    print(f"\nTotal Games in Database: {total_games}")
    
    # Games with stats populated
    print("\n" + "-" * 80)
    print("Games with Stats Populated:")
    print("-" * 80)
    
    games_with_stats = db.session.query(Game).filter(
        Game.away_ppg.isnot(None),
        Game.home_ppg.isnot(None)
    ).count()
    
    if total_games > 0:
        print(f"  Games with away_ppg and home_ppg: {games_with_stats} / {total_games} ({games_with_stats/total_games*100:.1f}%)")
    else:
        print(f"  Games with away_ppg and home_ppg: {games_with_stats} / {total_games} (0%)")
    
    # Break down by league
    for league in ['NBA', 'CBB', 'NHL', 'MLB']:
        league_total = db.session.query(Game).filter(Game.league == league).count()
        league_with_stats = db.session.query(Game).filter(
            Game.league == league,
            Game.away_ppg.isnot(None),
            Game.home_ppg.isnot(None)
        ).count()
        
        if league_total > 0:
            print(f"  {league}: {league_with_stats} / {league_total} ({league_with_stats/league_total*100:.1f}%)")
    
    # CBB games with Torvik data
    print("\n" + "-" * 80)
    print("CBB Games with Torvik Data:")
    print("-" * 80)
    
    cbb_total = db.session.query(Game).filter(Game.league == 'CBB').count()
    cbb_with_torvik = db.session.query(Game).filter(
        Game.league == 'CBB',
        Game.torvik_away_adj_o.isnot(None)
    ).count()
    
    if cbb_total > 0:
        print(f"  CBB games with torvik_away_adj_o: {cbb_with_torvik} / {cbb_total} ({cbb_with_torvik/cbb_total*100:.1f}%)")
    else:
        print(f"  CBB games with torvik_away_adj_o: {cbb_with_torvik} / {cbb_total} (0%)")
    
    # Sample a few CBB games to show Torvik data
    cbb_with_torvik_sample = db.session.query(Game).filter(
        Game.league == 'CBB',
        Game.torvik_away_adj_o.isnot(None)
    ).limit(3).all()
    
    if cbb_with_torvik_sample:
        print("\n  Sample CBB Games with Torvik Data:")
        for game in cbb_with_torvik_sample:
            print(f"    {game.away_team} @ {game.home_team} (ID: {game.id})")
            print(f"      torvik_away_adj_o: {game.torvik_away_adj_o}, torvik_home_adj_o: {game.torvik_home_adj_o}")
    
    # Games with spread_line
    print("\n" + "-" * 80)
    print("Games with Spread Line:")
    print("-" * 80)
    
    games_with_spread = db.session.query(Game).filter(
        Game.spread_line.isnot(None)
    ).count()
    
    if total_games > 0:
        print(f"  Games with spread_line: {games_with_spread} / {total_games} ({games_with_spread/total_games*100:.1f}%)")
    else:
        print(f"  Games with spread_line: {games_with_spread} / {total_games} (0%)")
    
    # Break down by league
    for league in ['NBA', 'CBB', 'NHL', 'MLB']:
        league_total = db.session.query(Game).filter(Game.league == league).count()
        league_with_spread = db.session.query(Game).filter(
            Game.league == league,
            Game.spread_line.isnot(None)
        ).count()
        
        if league_total > 0:
            print(f"  {league}: {league_with_spread} / {league_total} ({league_with_spread/league_total*100:.1f}%)")
    
    # Sample games with spread data
    games_with_spread_sample = db.session.query(Game).filter(
        Game.spread_line.isnot(None)
    ).limit(5).all()
    
    if games_with_spread_sample:
        print("\n  Sample Games with Spread Line:")
        for game in games_with_spread_sample:
            print(f"    {game.league}: {game.away_team} @ {game.home_team}")
            print(f"      spread_line: {game.spread_line}, spread_is_qualified: {game.spread_is_qualified}")
    else:
        print("\n  No games with spread line found in database")
    
    # Check why spreads might be NULL
    print("\n" + "-" * 80)
    print("Analysis: Why Spreads Might Be NULL:")
    print("-" * 80)
    
    # Check date distribution
    today = datetime.now().date()
    past_games = db.session.query(Game).filter(Game.date < today).count()
    future_games = db.session.query(Game).filter(Game.date >= today).count()
    
    print(f"  Past games: {past_games}")
    print(f"  Future games: {future_games}")
    
    # Check spread for future games
    if future_games > 0:
        future_with_spread = db.session.query(Game).filter(
            Game.date >= today,
            Game.spread_line.isnot(None)
        ).count()
        
        print(f"  Future games with spread: {future_with_spread} / {future_games} ({future_with_spread/future_games*100:.1f}%)")
    
    # Check if spread data exists but is 0
    games_with_zero_spread = db.session.query(Game).filter(
        Game.spread_line == 0
    ).count()
    
    print(f"  Games with spread_line = 0: {games_with_zero_spread}")

print("\n" + "=" * 80)
print("TEST SUITE COMPLETE")
print("=" * 80)
