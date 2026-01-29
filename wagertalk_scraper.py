"""
WagerTalk.com Scraper - Betting Action Data

Extracts Tickets %, Money %, and Line Movement for BOTH Spreads and Totals.
Uses Playwright with stealth mode to bypass bot detection.
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


def _parse_line_with_odds(text: str) -> Dict:
    """
    Parse line with odds like '-12-10' or '227½u-15' or '-11½-12'
    Returns: {'line': '-12', 'odds': '-110'}
    """
    if not text or text == '-':
        return {'line': None, 'odds': None}
    
    text = text.strip()
    
    # Handle totals with o/u prefix like "227½u-15" or "230o-15"
    total_match = re.match(r'^(\d+[½]?)([ou])(-?\d{2})$', text)
    if total_match:
        line = total_match.group(1)
        ou = total_match.group(2)
        odds_num = total_match.group(3)
        line = f"{ou.upper()}{line}"
        # Convert odds: -15 -> -115, -10 -> -110
        if odds_num.startswith('-'):
            odds = f"-1{odds_num[1:]}"
        else:
            odds = f"+1{odds_num}"
        return {'line': line, 'odds': odds}
    
    # Handle spreads like "-12-10" or "-11½-12" or "+5-10"
    spread_match = re.match(r'^([+-]?\d+[½]?)(-\d{2})$', text)
    if spread_match:
        line = spread_match.group(1)
        odds_num = spread_match.group(2)
        # Convert odds: -10 -> -110, -12 -> -112
        odds = f"-1{odds_num[1:]}"
        return {'line': line, 'odds': odds}
    
    # Handle spread with + odds like "-3+05"
    spread_plus = re.match(r'^([+-]?\d+[½]?)(\+\d{2})$', text)
    if spread_plus:
        line = spread_plus.group(1)
        odds_num = spread_plus.group(2)
        odds = f"+1{odds_num[1:]}"
        return {'line': line, 'odds': odds}
    
    # Handle just a number like "230" or "227½" (assume -110)
    just_line = re.match(r'^(\d+[½]?)$', text)
    if just_line:
        return {'line': just_line.group(1), 'odds': '-110'}
    
    # Handle spread without odds like "-11½"
    spread_only = re.match(r'^([+-]?\d+[½]?)$', text)
    if spread_only:
        return {'line': spread_only.group(1), 'odds': '-110'}
    
    return {'line': text, 'odds': None}


def _parse_cell_rows(text: str) -> tuple:
    """
    Parse a cell with two rows (spread on top, totals on bottom).
    Returns: (top_value, bottom_value)
    """
    if not text:
        return (None, None)
    
    lines = text.strip().split('\n')
    top = lines[0].strip() if len(lines) > 0 else None
    bottom = lines[1].strip() if len(lines) > 1 else None
    
    return (top, bottom)


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
            
            cb = random.random()
            url = f'https://www.wagertalk.com/odds?sport=today&cb={cb}'
            
            logger.info(f"Fetching WagerTalk: {url}")
            
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(8)  # Wait for JS to load percentages
            
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
                    
                    # Get all cells - b1=Tickets, b2=Money, b3=Open, b4+=Current books
                    all_cells = await row.query_selector_all('td')
                    
                    # Initialize data
                    game_data = {
                        'away_team': away_team,
                        'home_team': home_team,
                        # Spread data
                        'spread_tickets_pct': 50,
                        'spread_money_pct': 50,
                        'spread_open_line': None,
                        'spread_open_odds': None,
                        'spread_current_line': None,
                        'spread_current_odds': None,
                        # Totals data
                        'total_tickets_pct': 50,
                        'total_money_pct': 50,
                        'total_open_line': None,
                        'total_open_odds': None,
                        'total_current_line': None,
                        'total_current_odds': None,
                        # Legacy fields for compatibility
                        'over_bet_pct': 50,
                        'under_bet_pct': 50,
                        'over_money_pct': 50,
                        'under_money_pct': 50,
                        'away_bet_pct': 50,
                        'home_bet_pct': 50,
                        'away_money_pct': 50,
                        'home_money_pct': 50,
                        'sharp_detected': False,
                        'sharp_side': None,
                        'spread_sharp_detected': False,
                        'spread_sharp_side': None,
                        'source': 'wagertalk'
                    }
                    
                    for i, cell in enumerate(all_cells):
                        try:
                            cell_class = await cell.get_attribute('class') or ''
                            text = await cell.inner_text()
                            
                            if not text or text.strip() == '-':
                                continue
                            
                            top, bottom = _parse_cell_rows(text)
                            
                            # b1 = Tickets column
                            # Format: Top = totals % (with o/u), Bottom = spread %
                            if 'b1' in cell_class:
                                # Parse both rows looking for o/u prefix
                                for val in [top, bottom]:
                                    if not val:
                                        continue
                                    # Totals have o/u prefix
                                    total_match = re.search(r'([ou])(\d{1,3})%', val)
                                    if total_match:
                                        prefix = total_match.group(1)
                                        pct = int(total_match.group(2))
                                        game_data['total_tickets_pct'] = pct
                                        if prefix == 'o':
                                            game_data['over_bet_pct'] = pct
                                            game_data['under_bet_pct'] = 100 - pct
                                        else:
                                            game_data['under_bet_pct'] = pct
                                            game_data['over_bet_pct'] = 100 - pct
                                    else:
                                        # No prefix = spread %
                                        spread_match = re.search(r'(\d{1,3})%', val)
                                        if spread_match:
                                            pct = int(spread_match.group(1))
                                            game_data['spread_tickets_pct'] = pct
                                            game_data['away_bet_pct'] = pct
                                            game_data['home_bet_pct'] = 100 - pct
                            
                            # b2 = Money column
                            # Format: Top = totals % (with o/u), Bottom = spread %
                            elif 'b2' in cell_class:
                                for val in [top, bottom]:
                                    if not val:
                                        continue
                                    # Totals have o/u prefix
                                    total_match = re.search(r'([ou])(\d{1,3})%', val)
                                    if total_match:
                                        prefix = total_match.group(1)
                                        pct = int(total_match.group(2))
                                        game_data['total_money_pct'] = pct
                                        if prefix == 'o':
                                            game_data['over_money_pct'] = pct
                                            game_data['under_money_pct'] = 100 - pct
                                        else:
                                            game_data['under_money_pct'] = pct
                                            game_data['over_money_pct'] = 100 - pct
                                    else:
                                        # No prefix = spread %
                                        spread_match = re.search(r'(\d{1,3})%', val)
                                        if spread_match:
                                            pct = int(spread_match.group(1))
                                            game_data['spread_money_pct'] = pct
                                            game_data['away_money_pct'] = pct
                                            game_data['home_money_pct'] = 100 - pct
                            
                            # b3 = Open lines
                            # Format: Top = total line, Bottom = spread line
                            elif 'b3' in cell_class:
                                for val in [top, bottom]:
                                    if not val:
                                        continue
                                    parsed = _parse_line_with_odds(val)
                                    # Totals have o/u in line or are just numbers > 100
                                    if parsed['line'] and (parsed['line'].startswith('O') or parsed['line'].startswith('U') or 
                                        (parsed['line'].replace('½', '').isdigit() and float(parsed['line'].replace('½', '.5')) > 100)):
                                        game_data['total_open_line'] = parsed['line']
                                        game_data['total_open_odds'] = parsed['odds']
                                    elif parsed['line']:
                                        game_data['spread_open_line'] = parsed['line']
                                        game_data['spread_open_odds'] = parsed['odds']
                            
                            # b4 = First book (DraftKings) - use as current line
                            elif 'b4' in cell_class:
                                for val in [top, bottom]:
                                    if not val:
                                        continue
                                    parsed = _parse_line_with_odds(val)
                                    # Totals have o/u in line or are just numbers > 100
                                    if parsed['line'] and (parsed['line'].startswith('O') or parsed['line'].startswith('U') or 
                                        (parsed['line'].replace('½', '').isdigit() and float(parsed['line'].replace('½', '.5')) > 100)):
                                        game_data['total_current_line'] = parsed['line']
                                        game_data['total_current_odds'] = parsed['odds']
                                    elif parsed['line']:
                                        game_data['spread_current_line'] = parsed['line']
                                        game_data['spread_current_odds'] = parsed['odds']
                            
                        except Exception as e:
                            continue
                    
                    # Detect sharp money for TOTALS
                    tickets_diff = abs(game_data['over_bet_pct'] - game_data['over_money_pct'])
                    if tickets_diff >= 15:
                        game_data['sharp_detected'] = True
                        if game_data['over_money_pct'] > game_data['over_bet_pct']:
                            game_data['sharp_side'] = 'OVER'
                        else:
                            game_data['sharp_side'] = 'UNDER'
                    
                    # Detect sharp money for SPREADS
                    spread_diff = abs(game_data['spread_tickets_pct'] - game_data['spread_money_pct'])
                    if spread_diff >= 15:
                        game_data['spread_sharp_detected'] = True
                        if game_data['spread_money_pct'] > game_data['spread_tickets_pct']:
                            game_data['spread_sharp_side'] = away_team
                        else:
                            game_data['spread_sharp_side'] = home_team
                    
                    key = f"{away_team} vs {home_team}"
                    result[key] = game_data
                    games_found += 1
                    
                    logger.debug(f"WagerTalk {away_team} vs {home_team}: "
                                f"Spread {game_data['spread_tickets_pct']}%/{game_data['spread_money_pct']}% "
                                f"({game_data['spread_open_line']} -> {game_data['spread_current_line']}), "
                                f"Total {game_data['over_bet_pct']}%/{game_data['over_money_pct']}% "
                                f"({game_data['total_open_line']} -> {game_data['total_current_line']})")
                    
                except Exception as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
            
            await browser.close()
            logger.info(f"WagerTalk scraped {games_found} {league} games with spread+totals data")
            
    except Exception as e:
        logger.error(f"Error fetching WagerTalk: {e}")
    
    return result


def fetch_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """
    Fetch betting data from WagerTalk using Playwright.
    Returns Tickets %, Money %, and Line Movement for Spreads and Totals.
    """
    cache_key = f"wagertalk_{league}_{datetime.now().strftime('%Y%m%d_%H')}"
    
    if _is_cache_valid(cache_key):
        logger.debug(f"Using cached WagerTalk data for {league}")
        return _wagertalk_cache[cache_key]
    
    try:
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
        'spread_tickets_pct': 50,
        'spread_money_pct': 50,
        'spread_open_line': None,
        'spread_open_odds': None,
        'spread_current_line': None,
        'spread_current_odds': None,
        'total_tickets_pct': 50,
        'total_money_pct': 50,
        'total_open_line': None,
        'total_open_odds': None,
        'total_current_line': None,
        'total_current_odds': None,
        'over_bet_pct': 50,
        'under_bet_pct': 50,
        'over_money_pct': 50,
        'under_money_pct': 50,
        'away_bet_pct': 50,
        'home_bet_pct': 50,
        'away_money_pct': 50,
        'home_money_pct': 50,
        'sharp_detected': False,
        'sharp_side': None,
        'spread_sharp_detected': False,
        'spread_sharp_side': None,
        'source': 'default'
    }


# Convenience functions for backwards compatibility
def get_wagertalk_betting_data(away_team: str, home_team: str, league: str = 'NBA') -> Dict:
    """Convenience function to get betting data for a specific game."""
    return get_game_betting(away_team, home_team, league)


def get_all_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """Convenience function to get all betting data for a league."""
    return fetch_wagertalk_data(league)
