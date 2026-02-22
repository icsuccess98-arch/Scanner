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
            'markets': 'spreads,totals,h2h',  # Added moneylines (h2h)
            'oddsFormat': 'american',
            'bookmakers': 'draftkings,fanduel,betmgm,caesars,pinnacle'  # Added Pinnacle
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
                'total_odds': None,
                'away_ml': None,  # Away moneyline
                'home_ml': None,  # Home moneyline
                # Store all bookmaker data for Pinnacle comparison
                'bookmakers': {},
                'pinnacle_spread': None,
                'pinnacle_total': None,
                'pinnacle_away_ml': None,
                'pinnacle_home_ml': None
            }
            
            # Collect data from all bookmakers, prioritizing different books for different markets
            pinnacle_data = None
            draftkings_data = None
            
            for bookmaker in game.get('bookmakers', []):
                book_name = bookmaker.get('key', '').lower()
                book_data = {
                    'spreads': {},
                    'totals': {},
                    'h2h': {}
                }
                
                for market in bookmaker.get('markets', []):
                    if market['key'] == 'spreads':
                        for outcome in market.get('outcomes', []):
                            if outcome['name'] == away_team:
                                book_data['spreads']['away_spread'] = outcome.get('point')
                                book_data['spreads']['away_spread_odds'] = outcome.get('price')
                            elif outcome['name'] == home_team:
                                book_data['spreads']['home_spread'] = outcome.get('point')
                                book_data['spreads']['home_spread_odds'] = outcome.get('price')
                    
                    elif market['key'] == 'totals':
                        for outcome in market.get('outcomes', []):
                            if outcome['name'] == 'Over':
                                book_data['totals']['over_line'] = outcome.get('point')
                                book_data['totals']['over_odds'] = outcome.get('price')
                            elif outcome['name'] == 'Under':
                                book_data['totals']['under_line'] = outcome.get('point')
                                book_data['totals']['under_odds'] = outcome.get('price')
                    
                    elif market['key'] == 'h2h':  # Moneylines
                        for outcome in market.get('outcomes', []):
                            if outcome['name'] == away_team:
                                book_data['h2h']['away_ml'] = outcome.get('price')
                            elif outcome['name'] == home_team:
                                book_data['h2h']['home_ml'] = outcome.get('price')
                
                # Store bookmaker data
                game_odds['bookmakers'][book_name] = book_data
                
                # Special handling for Pinnacle (store separately)
                if book_name == 'pinnacle':
                    pinnacle_data = book_data
                    game_odds['pinnacle_spread'] = book_data['spreads'].get('away_spread')
                    game_odds['pinnacle_total'] = book_data['totals'].get('over_line')
                    game_odds['pinnacle_away_ml'] = book_data['h2h'].get('away_ml')
                    game_odds['pinnacle_home_ml'] = book_data['h2h'].get('home_ml')
                
                # Use DraftKings for main display (preferred over Pinnacle for public display)
                if book_name == 'draftkings':
                    draftkings_data = book_data
                elif book_name == 'fanduel' and draftkings_data is None:
                    draftkings_data = book_data
            
            # Set main display lines (prefer DraftKings, fallback to first available)
            display_data = draftkings_data or pinnacle_data
            if display_data:
                game_odds['spread'] = display_data['spreads'].get('away_spread')
                game_odds['spread_odds'] = display_data['spreads'].get('away_spread_odds')
                game_odds['total'] = display_data['totals'].get('over_line')
                game_odds['total_odds'] = display_data['totals'].get('over_odds')
                game_odds['away_ml'] = display_data['h2h'].get('away_ml')
                game_odds['home_ml'] = display_data['h2h'].get('home_ml')
            
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


def get_pinnacle_line(matchup_key: str, bet_type: str = 'spread') -> dict:
    """
    Get Pinnacle-specific line data for a matchup.
    
    Args:
        matchup_key: Normalized team matchup key (e.g., "lakers_warriors")
        bet_type: 'spread', 'total', or 'moneyline'
        
    Returns:
        Dict with Pinnacle line data or empty dict if not found
    """
    try:
        # Get cached odds data
        current_time = time.time()
        cache_key = f"NBA_{datetime.now().strftime('%Y%m%d')}"  # Assume NBA for now
        
        if cache_key in _live_odds_cache and (current_time - _cache_timestamp) < CACHE_TTL:
            odds = _live_odds_cache[cache_key]
            game_data = odds.get(matchup_key, {})
            
            if bet_type == 'spread':
                return {
                    'spread': game_data.get('pinnacle_spread'),
                    'away_odds': game_data.get('bookmakers', {}).get('pinnacle', {}).get('spreads', {}).get('away_spread_odds'),
                    'home_odds': game_data.get('bookmakers', {}).get('pinnacle', {}).get('spreads', {}).get('home_spread_odds')
                }
            elif bet_type == 'total':
                return {
                    'total': game_data.get('pinnacle_total'),
                    'over_odds': game_data.get('bookmakers', {}).get('pinnacle', {}).get('totals', {}).get('over_odds'),
                    'under_odds': game_data.get('bookmakers', {}).get('pinnacle', {}).get('totals', {}).get('under_odds')
                }
            elif bet_type == 'moneyline':
                return {
                    'away_ml': game_data.get('pinnacle_away_ml'),
                    'home_ml': game_data.get('pinnacle_home_ml')
                }
                
    except Exception as e:
        logging.error(f"Error getting Pinnacle line for {matchup_key}: {e}")
    
    return {}


def calculate_implied_probability(american_odds: int) -> float:
    """
    Calculate implied probability from American odds.
    
    Args:
        american_odds: American odds (e.g., -110, +150)
        
    Returns:
        Implied probability as decimal (0.0 to 1.0)
    """
    if american_odds is None:
        return 0.0
        
    try:
        if american_odds > 0:
            # Positive odds (underdog)
            return 100 / (american_odds + 100)
        else:
            # Negative odds (favorite)
            return abs(american_odds) / (abs(american_odds) + 100)
    except (TypeError, ZeroDivisionError):
        return 0.0


def get_market_probabilities(matchup_key: str, bet_type: str = 'moneyline') -> dict:
    """
    Get implied probabilities for a market from various bookmakers.
    
    Args:
        matchup_key: Normalized team matchup key
        bet_type: 'moneyline', 'spread', or 'total'
        
    Returns:
        Dict with probabilities from different books
    """
    try:
        current_time = time.time()
        cache_key = f"NBA_{datetime.now().strftime('%Y%m%d')}"
        
        if cache_key not in _live_odds_cache or (current_time - _cache_timestamp) >= CACHE_TTL:
            return {}
            
        odds = _live_odds_cache[cache_key]
        game_data = odds.get(matchup_key, {})
        bookmakers = game_data.get('bookmakers', {})
        
        probabilities = {}
        
        for book_name, book_data in bookmakers.items():
            book_probs = {}
            
            if bet_type == 'moneyline':
                away_ml = book_data.get('h2h', {}).get('away_ml')
                home_ml = book_data.get('h2h', {}).get('home_ml')
                
                if away_ml is not None and home_ml is not None:
                    away_prob = calculate_implied_probability(away_ml)
                    home_prob = calculate_implied_probability(home_ml)
                    total_prob = away_prob + home_prob
                    
                    book_probs = {
                        'away_prob': away_prob,
                        'home_prob': home_prob,
                        'vig': total_prob - 1.0 if total_prob > 1.0 else 0.0,
                        'no_vig_away': away_prob / total_prob if total_prob > 0 else 0.0,
                        'no_vig_home': home_prob / total_prob if total_prob > 0 else 0.0
                    }
                    
            elif bet_type == 'spread':
                away_spread_odds = book_data.get('spreads', {}).get('away_spread_odds')
                home_spread_odds = book_data.get('spreads', {}).get('home_spread_odds')
                
                if away_spread_odds is not None and home_spread_odds is not None:
                    away_prob = calculate_implied_probability(away_spread_odds)
                    home_prob = calculate_implied_probability(home_spread_odds)
                    total_prob = away_prob + home_prob
                    
                    book_probs = {
                        'away_cover_prob': away_prob,
                        'home_cover_prob': home_prob,
                        'vig': total_prob - 1.0 if total_prob > 1.0 else 0.0
                    }
                    
            elif bet_type == 'total':
                over_odds = book_data.get('totals', {}).get('over_odds')
                under_odds = book_data.get('totals', {}).get('under_odds')
                
                if over_odds is not None and under_odds is not None:
                    over_prob = calculate_implied_probability(over_odds)
                    under_prob = calculate_implied_probability(under_odds)
                    total_prob = over_prob + under_prob
                    
                    book_probs = {
                        'over_prob': over_prob,
                        'under_prob': under_prob,
                        'vig': total_prob - 1.0 if total_prob > 1.0 else 0.0,
                        'total_line': book_data.get('totals', {}).get('over_line')
                    }
            
            if book_probs:
                probabilities[book_name] = book_probs
                
        return probabilities
        
    except Exception as e:
        logging.error(f"Error calculating probabilities for {matchup_key}: {e}")
        return {}


def compare_to_pinnacle(matchup_key: str, bet_type: str = 'spread') -> dict:
    """
    Compare recreational book lines to Pinnacle (sharp reference).
    
    Args:
        matchup_key: Normalized team matchup key
        bet_type: 'spread', 'total', or 'moneyline'
        
    Returns:
        Dict with line comparisons showing where recreational books differ from Pinnacle
    """
    try:
        probabilities = get_market_probabilities(matchup_key, bet_type)
        pinnacle_data = probabilities.get('pinnacle', {})
        
        if not pinnacle_data:
            return {'error': 'Pinnacle data not available'}
        
        comparisons = {}
        
        for book_name, book_data in probabilities.items():
            if book_name == 'pinnacle':
                continue
                
            comparison = {'book': book_name}
            
            if bet_type == 'moneyline':
                pinnacle_away = pinnacle_data.get('no_vig_away', 0)
                pinnacle_home = pinnacle_data.get('no_vig_home', 0)
                book_away = book_data.get('no_vig_away', 0)
                book_home = book_data.get('no_vig_home', 0)
                
                comparison.update({
                    'away_prob_diff': book_away - pinnacle_away,
                    'home_prob_diff': book_home - pinnacle_home,
                    'vig_diff': book_data.get('vig', 0) - pinnacle_data.get('vig', 0)
                })
                
            elif bet_type == 'total':
                pinnacle_over = pinnacle_data.get('over_prob', 0)
                pinnacle_under = pinnacle_data.get('under_prob', 0)
                book_over = book_data.get('over_prob', 0)
                book_under = book_data.get('under_prob', 0)
                
                comparison.update({
                    'over_prob_diff': book_over - pinnacle_over,
                    'under_prob_diff': book_under - pinnacle_under,
                    'line_diff': (book_data.get('total_line', 0) or 0) - (pinnacle_data.get('total_line', 0) or 0),
                    'vig_diff': book_data.get('vig', 0) - pinnacle_data.get('vig', 0)
                })
            
            comparisons[book_name] = comparison
        
        return {
            'pinnacle_reference': pinnacle_data,
            'comparisons': comparisons
        }
        
    except Exception as e:
        logging.error(f"Error comparing to Pinnacle for {matchup_key}: {e}")
        return {'error': str(e)}


if __name__ == '__main__':
    # Test
    logging.basicConfig(level=logging.DEBUG)
    odds = get_live_odds('NBA')
    print(f"Found {len(odds)} games")
    for key, game in odds.items():
        print(f"{key}: Spread {game['spread']}, Total {game['total']}")
