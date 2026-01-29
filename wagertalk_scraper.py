"""
WagerTalk.com Scraper - Betting Action Data

Extracts Tickets % and Money % from WagerTalk.com/odds using Playwright.
Uses stealth mode to bypass bot detection.
"""

import logging
import re
import time
import random
import asyncio
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Cache for WagerTalk data
_wagertalk_cache = {}
_wagertalk_cache_time = {}
CACHE_TTL = 120  # 2 minute cache


def _is_cache_valid(key: str) -> bool:
    if key not in _wagertalk_cache:
        return False
    age = time.time() - _wagertalk_cache_time.get(key, 0)
    return age < CACHE_TTL


def _normalize_team_name(name: str) -> str:
    if not name:
        return ''
    return re.sub(r'\s+', ' ', name.strip())


async def _fetch_wagertalk_async(league: str = 'NBA') -> Dict[str, Dict]:
    """Async function to fetch WagerTalk data with Playwright."""
    from playwright.async_api import async_playwright
    
    result = {}
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # Stealth mode
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            """)
            
            page = await context.new_page()
            
            # Use "today" to get all sports with betting data loaded
            cb = random.random()
            url = f'https://www.wagertalk.com/odds?sport=today&cb={cb}'
            
            logger.info(f"Fetching WagerTalk: {url}")
            
            # Use domcontentloaded for faster loading, then wait for JS
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(8)  # Wait for JS to load percentages
            
            # Get all game rows
            rows = await page.query_selector_all('tr.reg, tr.alt')
            logger.info(f"WagerTalk found {len(rows)} rows")
            
            league_teams = _get_league_teams(league)
            games_found = 0
            
            for row in rows:
                try:
                    # Get team names
                    team_th = await row.query_selector('th.team')
                    if not team_th:
                        continue
                    
                    team_divs = await team_th.query_selector_all('div')
                    if len(team_divs) < 2:
                        continue
                    
                    away_team = _normalize_team_name(await team_divs[0].inner_text())
                    home_team = _normalize_team_name(await team_divs[1].inner_text())
                    
                    if not away_team or not home_team:
                        continue
                    
                    # Filter by league
                    if league_teams:
                        is_league_game = any(
                            t.lower() in away_team.lower() or t.lower() in home_team.lower()
                            for t in league_teams
                        )
                        if not is_league_game:
                            continue
                    
                    # Get book cells (b1-b14 contain betting data)
                    book_cells = await row.query_selector_all('td[class*="b"]')
                    
                    # Default values
                    away_bet_pct = 50
                    home_bet_pct = 50
                    away_money_pct = 50
                    home_money_pct = 50
                    over_bet_pct = 50
                    under_bet_pct = 50
                    over_money_pct = 50
                    under_money_pct = 50
                    
                    # Parse percentages from cells
                    for cell in book_cells:
                        try:
                            text = await cell.inner_text()
                            text = text.strip()
                            
                            if not text or text == '-':
                                continue
                            
                            # Parse patterns like "o50% 53%" or "u86% 59%"
                            # First number is Tickets %, second is Money %
                            pct_matches = re.findall(r'([ou]?)(\d{1,3})%', text)
                            
                            if len(pct_matches) >= 2:
                                prefix1, pct1 = pct_matches[0]
                                prefix2, pct2 = pct_matches[1]
                                
                                tickets_pct = int(pct1)
                                money_pct = int(pct2)
                                
                                # Determine if Over or Under based on prefix
                                if prefix1 == 'o':
                                    over_bet_pct = tickets_pct
                                    over_money_pct = money_pct
                                    under_bet_pct = 100 - tickets_pct
                                    under_money_pct = 100 - money_pct
                                elif prefix1 == 'u':
                                    under_bet_pct = tickets_pct
                                    under_money_pct = money_pct
                                    over_bet_pct = 100 - tickets_pct
                                    over_money_pct = 100 - money_pct
                                else:
                                    # Side bets (spread/ML)
                                    away_bet_pct = tickets_pct
                                    away_money_pct = money_pct
                                    home_bet_pct = 100 - tickets_pct
                                    home_money_pct = 100 - money_pct
                                    
                            elif len(pct_matches) == 1:
                                prefix, pct = pct_matches[0]
                                pct_val = int(pct)
                                
                                if prefix == 'o':
                                    over_bet_pct = pct_val
                                    under_bet_pct = 100 - pct_val
                                elif prefix == 'u':
                                    under_bet_pct = pct_val
                                    over_bet_pct = 100 - pct_val
                                    
                        except Exception as e:
                            continue
                    
                    # Detect sharp money (big difference between Tickets and Money)
                    sharp_detected = False
                    sharp_side = None
                    
                    tickets_diff = abs(over_bet_pct - over_money_pct)
                    if tickets_diff >= 15:
                        sharp_detected = True
                        if over_money_pct > over_bet_pct:
                            sharp_side = 'OVER'
                        else:
                            sharp_side = 'UNDER'
                    
                    key = f"{away_team} vs {home_team}"
                    result[key] = {
                        'away_team': away_team,
                        'home_team': home_team,
                        'away_bet_pct': away_bet_pct,
                        'home_bet_pct': home_bet_pct,
                        'away_money_pct': away_money_pct,
                        'home_money_pct': home_money_pct,
                        'over_bet_pct': over_bet_pct,
                        'under_bet_pct': under_bet_pct,
                        'over_money_pct': over_money_pct,
                        'under_money_pct': under_money_pct,
                        'sharp_detected': sharp_detected,
                        'sharp_side': sharp_side,
                        'source': 'wagertalk'
                    }
                    games_found += 1
                    
                except Exception as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
            
            await browser.close()
            logger.info(f"WagerTalk scraped {games_found} {league} games with percentages")
            
    except Exception as e:
        logger.error(f"Error fetching WagerTalk: {e}")
    
    return result


def fetch_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """
    Fetch betting data from WagerTalk using Playwright.
    Returns Tickets % and Money % for games.
    """
    cache_key = f"wagertalk_{league}_{datetime.now().strftime('%Y%m%d_%H')}"
    
    if _is_cache_valid(cache_key):
        logger.debug(f"Using cached WagerTalk data for {league}")
        return _wagertalk_cache[cache_key]
    
    try:
        # Run async function
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_fetch_wagertalk_async(league))
        loop.close()
        
        if result:
            _wagertalk_cache[cache_key] = result
            _wagertalk_cache_time[cache_key] = time.time()
        
        return result
        
    except Exception as e:
        logger.error(f"Error in fetch_wagertalk_data: {e}")
        return {}


def _get_league_teams(league: str) -> list:
    """Return team names for filtering by league."""
    if league == 'NBA':
        return ['Sacramento', 'Philadelphia', 'Milwaukee', 'Washington', 'Houston', 
                'Atlanta', 'Charlotte', 'Dallas', 'Detroit', 'Phoenix', 'Brooklyn',
                'Denver', 'Boston', 'Cleveland', 'Chicago', 'Miami', 'New York',
                'Orlando', 'Toronto', 'Indiana', 'Memphis', 'Minnesota', 'New Orleans',
                'Oklahoma City', 'Portland', 'San Antonio', 'Utah', 'Los Angeles',
                'Golden State', 'Lakers', 'Clippers', 'Warriors', 'Knicks', 'Nets',
                'Sixers', 'Celtics', 'Bucks', 'Wizards', 'Rockets', 'Hawks', 
                'Hornets', 'Mavericks', 'Pistons', 'Suns', 'Nuggets', 'Cavaliers',
                'Bulls', 'Heat', 'Magic', 'Raptors', 'Pacers', 'Grizzlies',
                'Timberwolves', 'Pelicans', 'Thunder', 'Blazers', 'Spurs', 'Jazz', 'Kings']
    elif league == 'CBB':
        return []  # Allow all for college basketball
    elif league == 'NFL':
        return ['Chiefs', 'Eagles', 'Bills', 'Cowboys', 'Ravens', 'Bengals',
                '49ers', 'Lions', 'Dolphins', 'Jets', 'Steelers', 'Chargers',
                'Broncos', 'Raiders', 'Seahawks', 'Vikings', 'Packers', 'Bears',
                'Saints', 'Falcons', 'Panthers', 'Buccaneers', 'Commanders',
                'Giants', 'Cardinals', 'Rams', 'Browns', 'Colts', 'Texans',
                'Titans', 'Jaguars', 'Patriots']
    elif league == 'NHL':
        return ['Bruins', 'Sabres', 'Red Wings', 'Panthers', 'Canadiens',
                'Senators', 'Lightning', 'Maple Leafs', 'Hurricanes', 'Blue Jackets',
                'Devils', 'Islanders', 'Rangers', 'Flyers', 'Penguins',
                'Capitals', 'Blackhawks', 'Avalanche', 'Stars', 'Wild',
                'Predators', 'Blues', 'Jets', 'Coyotes', 'Ducks',
                'Flames', 'Oilers', 'Kings', 'Sharks', 'Kraken', 'Canucks', 'Golden Knights']
    return []


def get_game_betting(away_team: str, home_team: str, league: str = 'NBA') -> Dict:
    """Get betting data for a specific game."""
    all_data = fetch_wagertalk_data(league)
    
    away_clean = _normalize_team_name(away_team).lower()
    home_clean = _normalize_team_name(home_team).lower()
    
    # Direct match
    for key, data in all_data.items():
        data_away = data.get('away_team', '').lower()
        data_home = data.get('home_team', '').lower()
        
        if (away_clean in data_away or data_away in away_clean) and \
           (home_clean in data_home or data_home in home_clean):
            return data
    
    # Fuzzy match
    for key, data in all_data.items():
        key_lower = key.lower()
        away_parts = away_clean.split()
        home_parts = home_clean.split()
        
        if any(p in key_lower for p in away_parts if len(p) > 3) and \
           any(p in key_lower for p in home_parts if len(p) > 3):
            return data
    
    # Default response
    return {
        'away_bet_pct': 50,
        'home_bet_pct': 50,
        'away_money_pct': 50,
        'home_money_pct': 50,
        'over_bet_pct': 50,
        'under_bet_pct': 50,
        'over_money_pct': 50,
        'under_money_pct': 50,
        'sharp_detected': False,
        'sharp_side': None,
        'source': 'default'
    }


# Convenience functions for backwards compatibility
def get_wagertalk_betting_data(away_team: str, home_team: str, league: str = 'NBA') -> Dict:
    """Convenience function to get betting data for a specific game."""
    return get_game_betting(away_team, home_team, league)


def get_all_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """Convenience function to get all betting data for a league."""
    return fetch_wagertalk_data(league)
