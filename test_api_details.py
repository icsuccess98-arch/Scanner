#!/usr/bin/env python3
"""Detailed test of API endpoint response structure"""

import sys
sys.path.insert(0, '/home/runner/workspace')

from sports_app import app, db, Game
from datetime import datetime, timedelta

with app.app_context():
    today = datetime.now().date()
    
    # Get an NBA game
    nba_game = db.session.query(Game).filter(
        Game.league == 'NBA',
        Game.date >= today,
        Game.date < today + timedelta(days=1)
    ).first()
    
    if not nba_game:
        print("No NBA games found for today")
        sys.exit(1)
    
    print(f"Testing NBA game: {nba_game.away_team} @ {nba_game.home_team} (ID: {nba_game.id})")
    print(f"Database fields:")
    print(f"  away_ppg: {nba_game.away_ppg}")
    print(f"  home_ppg: {nba_game.home_ppg}")
    print(f"  away_opp_ppg: {nba_game.away_opp_ppg}")
    print(f"  home_opp_ppg: {nba_game.home_opp_ppg}")
    print(f"  spread_line: {nba_game.spread_line}")
    print(f"  spread_is_qualified: {nba_game.spread_is_qualified}")
    
    # Now test the API endpoint
    with app.test_client() as client:
        response = client.get(f'/api/matchup_data/{nba_game.id}')
        print(f"\nAPI Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.get_json()
            print(f"\nAPI Response keys: {list(data.keys())}")
            print(f"\nFull response:")
            import json
            print(json.dumps(data, indent=2, default=str))
