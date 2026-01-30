"""
WagerTalk.com Scraper - Betting Action Data

Extracts Tickets %, Money %, and Line Movement for Spreads and Totals.
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

_wagertalk_cache = {}
_wagertalk_cache_time = {}
CACHE_TTL = 60  # 1 minute for faster updates


def _is_cache_valid(key: str) -> bool:
    if key not in _wagertalk_cache:
        return False
    age = time.time() - _wagertalk_cache_time.get(key, 0)
    return age < CACHE_TTL


def _normalize_team_name(name: str) -> str:
    if not name:
        return ''
    name = re.sub(r'\s+', ' ', name.strip())
    name = name.replace('Trail Blazers', 'Blazers')
    return name


NBA_TEAMS = [
    'Hawks', 'Celtics', 'Nets', 'Hornets', 'Bulls', 'Cavaliers', 'Mavericks',
    'Nuggets', 'Pistons', 'Warriors', 'Rockets', 'Pacers', 'Clippers', 'Lakers',
    'Grizzlies', 'Heat', 'Bucks', 'Timberwolves', 'Pelicans', 'Knicks', 'Thunder',
    'Magic', 'Sixers', '76ers', 'Suns', 'Blazers', 'Kings', 'Spurs', 'Raptors',
    'Jazz', 'Wizards', 'Atlanta', 'Boston', 'Brooklyn', 'Charlotte', 'Chicago',
    'Cleveland', 'Dallas', 'Denver', 'Detroit', 'Golden State', 'Houston',
    'Indiana', 'Los Angeles', 'Memphis', 'Miami', 'Milwaukee', 'Minnesota',
    'New Orleans', 'New York', 'Oklahoma City', 'Orlando', 'Philadelphia',
    'Phoenix', 'Portland', 'Sacramento', 'San Antonio', 'Toronto', 'Utah', 'Washington'
]


def _is_nba_team(name: str) -> bool:
    if not name:
        return False
    name_lower = name.lower()
    return any(team.lower() in name_lower for team in NBA_TEAMS)


async def _fetch_wagertalk_async(league: str = 'NBA') -> Dict[str, Dict]:
    """Async function to fetch WagerTalk data with Playwright."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("[WagerTalk] Playwright not installed")
        return {}
    
    result = {}
    browser = None
    
    try:
        async with async_playwright() as p:
            logger.info("[WagerTalk] Launching browser...")
            
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--single-process',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            """)
            
            page = await context.new_page()
            page.set_default_timeout(30000)
            
            cb = random.random()
            url = f'https://www.wagertalk.com/odds?sport=today&cb={cb}'
            
            logger.info(f"[WagerTalk] Navigating to: {url}")
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Faster wait - just enough for JS to render
            await asyncio.sleep(4)
            
            rows = await page.query_selector_all('tr.reg, tr.alt, tr[class*="game"], tbody tr')
            logger.info(f"[WagerTalk] Found {len(rows)} table rows")
            
            if len(rows) == 0:
                tables = await page.query_selector_all('table')
                logger.info(f"[WagerTalk] Found {len(tables)} tables on page")
                
                for table in tables:
                    rows.extend(await table.query_selector_all('tr'))
                logger.info(f"[WagerTalk] Total rows from all tables: {len(rows)}")
            
            games_found = 0
            
            # WagerTalk table format has alternating rows for spread/total per game
            # Each game typically has 2 rows - one for spread, one for total
            # Format: Team | Tickets | Money | Open | Current/DraftKings
            
            for row in rows:
                try:
                    row_text = await row.inner_text()
                    
                    if not row_text or len(row_text) < 10:
                        continue
                    
                    if league == 'NBA':
                        has_nba_team = any(_is_nba_team(team) for team in row_text.split('\n'))
                        if not has_nba_team:
                            continue
                    
                    cells = await row.query_selector_all('td, th')
                    if len(cells) < 3:
                        continue
                    
                    # Parse all cell values
                    cell_values = []
                    for cell in cells:
                        cell_text = await cell.inner_text()
                        cell_values.append(cell_text.strip())
                    
                    away_team = None
                    home_team = None
                    
                    # Spread data
                    spread_tickets_pct = 50
                    spread_money_pct = 50
                    open_spread = None
                    current_spread = None
                    
                    # Total data
                    over_tickets_pct = 50
                    over_money_pct = 50
                    open_total = None
                    current_total = None
                    
                    # Parse cells to find teams and percentages
                    percentages = []
                    lines_found = []
                    
                    for cell_text in cell_values:
                        if not cell_text:
                            continue
                        
                        # Find team names
                        if _is_nba_team(cell_text) and not away_team:
                            away_team = _normalize_team_name(cell_text.split('\n')[0])
                        elif _is_nba_team(cell_text) and away_team and not home_team:
                            home_team = _normalize_team_name(cell_text.split('\n')[0])
                        
                        # Extract percentages - WagerTalk format:
                        # "78%" = spread betting percentage
                        # "O78%" = Over percentage for totals
                        # "u60" or "u60%" = Under percentage for totals
                        
                        # Check for Over percentage (O prefix)
                        over_match = re.search(r'[oO](\d{1,3})%?', cell_text)
                        if over_match:
                            pct = int(over_match.group(1))
                            if pct > 0 and pct <= 100:
                                over_tickets_pct = pct
                                over_money_pct = pct  # Assume same if not specified
                        
                        # Check for Under percentage (u prefix)
                        under_match = re.search(r'[uU](\d{1,3})%?', cell_text)
                        if under_match and not over_match:
                            pct = int(under_match.group(1))
                            if pct > 0 and pct <= 100:
                                over_tickets_pct = 100 - pct  # Convert under to over
                                over_money_pct = 100 - pct
                        
                        # Regular spread percentage (no O or u prefix)
                        pct_match = re.search(r'^(\d{1,3})%$', cell_text)
                        if pct_match:
                            percentages.append(int(pct_match.group(1)))
                        
                        # Also match "XX%" without prefix (spread)
                        if not over_match and not under_match:
                            plain_pct = re.search(r'(\d{1,3})%', cell_text)
                            if plain_pct and cell_text[0].isdigit():
                                percentages.append(int(plain_pct.group(1)))
                        
                        # Extract spread lines (format: "-11.5", "+3.5", etc)
                        spread_match = re.search(r'([+-]?\d+\.?\d*)\s*$', cell_text)
                        if spread_match and not re.search(r'[oOuU]', cell_text):
                            try:
                                line = float(spread_match.group(1))
                                if abs(line) < 50:  # Reasonable spread
                                    lines_found.append(('spread', line))
                            except:
                                pass
                        
                        # Extract total lines (format: "227.5", "o227", "u215")
                        total_match = re.search(r'(\d{3}\.?\d*)', cell_text)
                        if total_match:
                            try:
                                total = float(total_match.group(1))
                                if 150 < total < 300:  # Reasonable NBA total
                                    lines_found.append(('total', total))
                            except:
                                pass
                    
                    # Assign percentages in order (tickets, money)
                    if len(percentages) >= 2:
                        spread_tickets_pct = percentages[0]
                        spread_money_pct = percentages[1]
                    elif len(percentages) == 1:
                        spread_tickets_pct = percentages[0]
                        spread_money_pct = percentages[0]
                    
                    # Find teams from full row text if not found
                    lines = row_text.split('\n')
                    if not away_team or not home_team:
                        team_candidates = [l.strip() for l in lines if _is_nba_team(l.strip())]
                        if len(team_candidates) >= 2:
                            away_team = _normalize_team_name(team_candidates[0])
                            home_team = _normalize_team_name(team_candidates[1])
                    
                    # Assign line values
                    for line_type, value in lines_found:
                        if line_type == 'spread':
                            if open_spread is None:
                                open_spread = value
                            else:
                                current_spread = value
                        elif line_type == 'total':
                            if open_total is None:
                                open_total = value
                            else:
                                current_total = value
                    
                    if away_team and home_team and away_team != home_team:
                        matchup_key = f"{away_team} vs {home_team}"
                        
                        if matchup_key not in result:
                            result[matchup_key] = {
                                'away_team': away_team,
                                'home_team': home_team,
                                # Spread betting data
                                'spread_tickets_pct': spread_tickets_pct,
                                'spread_money_pct': spread_money_pct,
                                'away_tickets_pct': spread_tickets_pct,
                                'home_tickets_pct': 100 - spread_tickets_pct,
                                'away_bet_pct': spread_tickets_pct,
                                'home_bet_pct': 100 - spread_tickets_pct,
                                'away_money_pct': spread_money_pct,
                                'home_money_pct': 100 - spread_money_pct,
                                # Spread lines
                                'open_spread': open_spread,
                                'current_spread': current_spread or open_spread,
                                # Totals betting data
                                'over_bet_pct': over_tickets_pct,
                                'under_bet_pct': 100 - over_tickets_pct,
                                'over_money_pct': over_money_pct,
                                'under_money_pct': 100 - over_money_pct,
                                'total_tickets_pct': over_tickets_pct,
                                'total_money_pct': over_money_pct,
                                # Total lines
                                'total_open_line': open_total,
                                'total_current_line': current_total or open_total,
                                # Sharp money detection
                                'sharp_detected': abs(spread_tickets_pct - spread_money_pct) >= 15,
                                'sharp_side': away_team if spread_money_pct > spread_tickets_pct else home_team,
                                'spread_sharp_detected': abs(spread_tickets_pct - spread_money_pct) >= 15,
                                'spread_sharp_side': away_team if spread_money_pct > spread_tickets_pct else home_team,
                                'source': 'wagertalk'
                            }
                            games_found += 1
                            logger.info(f"[WagerTalk] Found: {matchup_key} - Tickets {spread_tickets_pct}% / Money {spread_money_pct}% | Spread: {open_spread} → {current_spread} | Total: {open_total}")
                        else:
                            # Update existing entry with additional data (totals row)
                            existing = result[matchup_key]
                            if open_total and not existing.get('total_open_line'):
                                existing['total_open_line'] = open_total
                                existing['total_current_line'] = current_total or open_total
                            if over_tickets_pct != 50:
                                existing['over_bet_pct'] = over_tickets_pct
                                existing['under_bet_pct'] = 100 - over_tickets_pct
                    
                except Exception as e:
                    continue
            
            await browser.close()
            logger.info(f"[WagerTalk] SUCCESS - Found {games_found} {league} games")
            
    except Exception as e:
        logger.error(f"[WagerTalk] Error: {type(e).__name__}: {e}")
        if browser:
            try:
                await browser.close()
            except:
                pass
    
    return result


def _run_async_in_thread(league: str) -> Dict[str, Dict]:
    """Run async fetch in a separate thread for gunicorn compatibility."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(_fetch_wagertalk_async(league))
    finally:
        loop.close()


def get_wagertalk_odds(league: str = 'NBA') -> Dict[str, Dict]:
    """
    Fetch betting data from WagerTalk.
    Returns Tickets %, Money % for spreads.
    """
    cache_key = f"wagertalk_{league}_{datetime.now().strftime('%Y%m%d_%H')}"
    
    if _is_cache_valid(cache_key):
        cached_data = _wagertalk_cache[cache_key]
        logger.info(f"[WagerTalk] Using cached data: {len(cached_data)} games")
        return cached_data
    
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("[WagerTalk] Playwright not available")
        return {}
    
    result = {}
    
    try:
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(_run_async_in_thread, league)
                result = future.result(timeout=45)  # Faster timeout
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(_fetch_wagertalk_async(league))
            finally:
                loop.close()
        
        if result:
            _wagertalk_cache[cache_key] = result
            _wagertalk_cache_time[cache_key] = time.time()
            logger.info(f"[WagerTalk] Fetched {len(result)} games")
        
        return result
        
    except Exception as e:
        logger.warning(f"[WagerTalk] Fetch failed: {type(e).__name__}: {e}")
        return {}


def find_game_data(away_team: str, home_team: str, league: str = 'NBA') -> Optional[Dict]:
    """Find betting data for a specific matchup."""
    all_data = get_wagertalk_odds(league)
    
    away_norm = _normalize_team_name(away_team)
    home_norm = _normalize_team_name(home_team)
    
    matchup_key = f"{away_norm} vs {home_norm}"
    if matchup_key in all_data:
        return all_data[matchup_key]
    
    for key, data in all_data.items():
        if (away_norm.lower() in key.lower() or away_team.lower() in key.lower()) and \
           (home_norm.lower() in key.lower() or home_team.lower() in key.lower()):
            return data
    
    return None


def get_all_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """Alias for get_wagertalk_odds for backward compatibility."""
    return get_wagertalk_odds(league)


def fetch_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """Alias for get_wagertalk_odds for backward compatibility."""
    return get_wagertalk_odds(league)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Fetching WagerTalk odds data...")
    data = get_wagertalk_odds('NBA')
    
    print(f"\nFound {len(data)} NBA games:")
    for matchup, game_data in data.items():
        print(f"\n{matchup}:")
        print(f"  Tickets: {game_data.get('away_tickets_pct')}% / {game_data.get('home_tickets_pct')}%")
        print(f"  Money: {game_data.get('away_money_pct')}% / {game_data.get('home_money_pct')}%")
