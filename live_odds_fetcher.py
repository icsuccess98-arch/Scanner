"""
Live Odds Fetcher - Uses The Odds API for real-time current lines
WagerTalk is used ONLY for closing lines and betting percentages
"""
import os
import requests
import logging
from datetime import datetime, timedelta
from functools import lru_cache
import time

# Cache for live odds (30 second TTL)
_live_odds_cache = {}
_cache_timestamp = 0
CACHE_TTL = 30  # 30 seconds

def get_live_odds(league='NBA'):
    """Fetch current lines from The Odds API."""
    global _live_odds_cache, _cache_timestamp
    
    # Check cache freshness
    current_time = time.time()
    cache_key = f"{league}_{datetime.now().strftime('%Y%m%d')}"
    
    if cache_key in _live_odds_cache and (current_time - _cache_timestamp) < CACHE_TTL:
        logging.debug(f"Using cached live odds for {league}")
        return _live_odds_cache[cache_key]
    
    api_key = os.environ.get('ODDS_API_KEY')
    if not api_key:
        logging.warning("ODDS_API_KEY not set, cannot fetch live odds")
        return {}
    
    # Map league to API sport key
    sport_keys = {
        'NBA': 'basketball_nba',
        'CBB': 'basketball_ncaab',
        'NFL': 'americanfootball_nfl',
        'CFB': 'americanfootball_ncaaf',
        'NHL': 'icehockey_nhl'
    }
    
    sport_key = sport_keys.get(league, 'basketball_nba')
    
    try:
        url = f'https://api.the-odds-api.com/v4/sports/{sport_key}/odds'
        params = {
            'apiKey': api_key,
            'regions': 'us',
            'markets': 'spreads,totals',
            'oddsFormat': 'american',
            'bookmakers': 'draftkings,fanduel,betmgm,caesars'
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code != 200:
            logging.warning(f"Odds API error: {response.status_code}")
            return _live_odds_cache.get(cache_key, {})
        
        data = response.json()
        
        # Process into our format: {matchup_key: {spread, total, ...}}
        result = {}
        
        for game in data:
            away_team = game.get('away_team', '')
            home_team = game.get('home_team', '')
            
            # Create normalized matchup key
            matchup_key = f"{normalize_team(away_team)}_{normalize_team(home_team)}"
            
            game_odds = {
                'away_team': away_team,
                'home_team': home_team,
                'commence_time': game.get('commence_time'),
                'spread': None,
                'spread_odds': None,
                'total': None,
                'total_odds': None
            }
            
            # Get first bookmaker's lines (prefer DraftKings)
            for bookmaker in game.get('bookmakers', []):
                for market in bookmaker.get('markets', []):
                    if market['key'] == 'spreads' and game_odds['spread'] is None:
                        for outcome in market.get('outcomes', []):
                            if outcome['name'] == away_team:
                                game_odds['spread'] = outcome.get('point')
                                game_odds['spread_odds'] = outcome.get('price')
                                break
                    
                    if market['key'] == 'totals' and game_odds['total'] is None:
                        for outcome in market.get('outcomes', []):
                            if outcome['name'] == 'Over':
                                game_odds['total'] = outcome.get('point')
                                game_odds['total_odds'] = outcome.get('price')
                                break
            
            result[matchup_key] = game_odds
        
        # Update cache
        _live_odds_cache[cache_key] = result
        _cache_timestamp = current_time
        
        logging.info(f"Live odds fetched for {league}: {len(result)} games")
        return result
        
    except Exception as e:
        logging.error(f"Error fetching live odds: {e}")
        return _live_odds_cache.get(cache_key, {})


def normalize_team(team_name):
    """Normalize team name for matching."""
    if not team_name:
        return ''
    
    # Remove common suffixes and normalize
    name = team_name.lower().strip()
    
    # NBA team normalizations
    nba_map = {
        'los angeles lakers': 'lakers',
        'la lakers': 'lakers',
        'los angeles clippers': 'clippers',
        'la clippers': 'clippers',
        'golden state warriors': 'warriors',
        'san antonio spurs': 'spurs',
        'oklahoma city thunder': 'thunder',
        'oklahoma city': 'thunder',
        'new york knicks': 'knicks',
        'brooklyn nets': 'nets',
        'boston celtics': 'celtics',
        'philadelphia 76ers': '76ers',
        'toronto raptors': 'raptors',
        'chicago bulls': 'bulls',
        'cleveland cavaliers': 'cavaliers',
        'detroit pistons': 'pistons',
        'indiana pacers': 'pacers',
        'milwaukee bucks': 'bucks',
        'atlanta hawks': 'hawks',
        'charlotte hornets': 'hornets',
        'miami heat': 'heat',
        'orlando magic': 'magic',
        'washington wizards': 'wizards',
        'denver nuggets': 'nuggets',
        'minnesota timberwolves': 'timberwolves',
        'portland trail blazers': 'blazers',
        'utah jazz': 'jazz',
        'dallas mavericks': 'mavericks',
        'houston rockets': 'rockets',
        'memphis grizzlies': 'grizzlies',
        'new orleans pelicans': 'pelicans',
        'phoenix suns': 'suns',
        'sacramento kings': 'kings'
    }
    
    return nba_map.get(name, name.split()[-1] if name else '')


def get_current_line(away_team, home_team, league='NBA'):
    """Get current live line for a specific matchup."""
    odds = get_live_odds(league)
    
    # Try to find the matchup
    away_norm = normalize_team(away_team)
    home_norm = normalize_team(home_team)
    
    # Try various key combinations
    for key, game_odds in odds.items():
        key_parts = key.split('_')
        if len(key_parts) >= 2:
            if away_norm in key_parts[0] or key_parts[0] in away_norm:
                if home_norm in key_parts[1] or key_parts[1] in home_norm:
                    return game_odds
    
    return None


if __name__ == '__main__':
    # Test
    logging.basicConfig(level=logging.DEBUG)
    odds = get_live_odds('NBA')
    print(f"Found {len(odds)} games")
    for key, game in odds.items():
        print(f"{key}: Spread {game['spread']}, Total {game['total']}")
