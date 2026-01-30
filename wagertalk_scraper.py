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
                    
                    # Debug: Log cell values for first few rows with NBA teams
                    if games_found < 3 and any(_is_nba_team(cv) for cv in cell_values):
                        logger.info(f"[WagerTalk DEBUG] Cells: {cell_values[:8]}")
                    
                    # WagerTalk column layout:
                    # 0: Date/time, 1: Rotation#, 2: Teams, 3: Score (SKIP!), 4: empty, 
                    # 5: Tickets column, 6: Money column, 7: Lines
                    # IMPORTANT: Skip column 3 (score) - it looks like percentages but isn't!
                    
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
                    
                    # Parse specific columns by position
                    # Column 2: Teams, Column 5: Tickets, Column 6: Money, Column 7: Lines
                    tickets_percentages = []
                    money_percentages = []
                    lines_found = []
                    
                    # Get teams from column 2
                    if len(cell_values) > 2:
                        teams_cell = cell_values[2]
                        team_lines = teams_cell.split('\n')
                        for tl in team_lines:
                            tl = tl.strip()
                            if _is_nba_team(tl):
                                if not away_team:
                                    away_team = _normalize_team_name(tl)
                                elif not home_team:
                                    home_team = _normalize_team_name(tl)
                    
                    # Parse TICKETS column (index 5) - spread% and over/under%
                    if len(cell_values) > 5:
                        tickets_cell = cell_values[5].replace('\xa0', '')
                        for line in tickets_cell.split('\n'):
                            line = line.strip()
                            if not line:
                                continue
                            # Over percentage
                            over_m = re.match(r'^[oO](\d{1,3})%?$', line)
                            if over_m:
                                over_tickets_pct = int(over_m.group(1))
                                continue
                            # Under percentage  
                            under_m = re.match(r'^[uU](\d{1,3})%?$', line)
                            if under_m:
                                over_tickets_pct = 100 - int(under_m.group(1))
                                continue
                            # Spread percentage (plain number)
                            spread_m = re.match(r'^(\d{1,3})%?$', line)
                            if spread_m:
                                tickets_percentages.append(int(spread_m.group(1)))
                    
                    # Parse MONEY column (index 6) - spread% and over/under%
                    if len(cell_values) > 6:
                        money_cell = cell_values[6].replace('\xa0', '')
                        for line in money_cell.split('\n'):
                            line = line.strip()
                            if not line:
                                continue
                            # Over percentage
                            over_m = re.match(r'^[oO](\d{1,3})%?$', line)
                            if over_m:
                                over_money_pct = int(over_m.group(1))
                                continue
                            # Under percentage
                            under_m = re.match(r'^[uU](\d{1,3})%?$', line)
                            if under_m:
                                over_money_pct = 100 - int(under_m.group(1))
                                continue
                            # Spread percentage (plain number)
                            spread_m = re.match(r'^(\d{1,3})%?$', line)
                            if spread_m:
                                money_percentages.append(int(spread_m.group(1)))
                    
                    # Parse LINES column (index 7) - spread and total lines
                    if len(cell_values) > 7:
                        lines_cell = cell_values[7].replace('\xa0', '')
                        for line in lines_cell.split('\n'):
                            line = line.strip()
                            if not line:
                                continue
                            # Spread with odds: "-3-10" = -3 spread
                            spread_m = re.match(r'^([+-]?\d+\.?\d*)-(\d+)$', line)
                            if spread_m:
                                try:
                                    val = float(spread_m.group(1))
                                    if abs(val) < 50:
                                        lines_found.append(('spread', val))
                                except:
                                    pass
                                continue
                            # Total with fraction: "225½"
                            total_m = re.match(r'^(\d{3})½', line)
                            if total_m:
                                try:
                                    val = float(total_m.group(1)) + 0.5
                                    if 150 < val < 300:
                                        lines_found.append(('total', val))
                                except:
                                    pass
                                continue
                            # Plain total: "225"
                            plain_m = re.match(r'^(\d{3})$', line)
                            if plain_m:
                                try:
                                    val = float(plain_m.group(1))
                                    if 150 < val < 300:
                                        lines_found.append(('total', val))
                                except:
                                    pass
                    
                    # Assign spread percentages
                    if tickets_percentages:
                        spread_tickets_pct = tickets_percentages[0]
                    if money_percentages:
                        spread_money_pct = money_percentages[0]
                    
                    # Legacy fallback: Only parse remaining team names if not found
                    for idx, cell_text in enumerate(cell_values):
                        if not cell_text or idx in [3, 4]:  # Skip score column and empty
                            continue
                        
                        # Find team names (only if not already found)
                        if not away_team or not home_team:
                            if _is_nba_team(cell_text) and not away_team:
                                away_team = _normalize_team_name(cell_text.split('\n')[0])
                            elif _is_nba_team(cell_text) and away_team and not home_team:
                                home_team = _normalize_team_name(cell_text.split('\n')[0])
                    
                    # Legacy line parsing for cells not already parsed
                    for idx, cell_text in enumerate(cell_values):
                        if not cell_text or idx < 5:  # Skip first 5 columns
                            continue
                        
                        lines_in_cell = cell_text.replace('\xa0', '').split('\n')
                        
                        # Only look for lines (spreads/totals) not already found
                        if not lines_found:
                            for line in lines_in_cell:
                                line = line.strip()
                                if not line:
                                    continue
                                
                                # Match spread with odds: "-3-10" = -3 spread, -110 odds
                                spread_odds_match = re.match(r'^([+-]?\d+\.?\d*)-(\d+)$', line)
                                if spread_odds_match:
                                    try:
                                        spread_val = float(spread_odds_match.group(1))
                                        if abs(spread_val) < 50:
                                            lines_found.append(('spread', spread_val))
                                    except:
                                        pass
                                    continue
                                
                                # Match total with fraction: "225½" or "227½u-15"
                                total_frac_match = re.match(r'^(\d{3})½', line)
                                if total_frac_match:
                                    try:
                                        total_val = float(total_frac_match.group(1)) + 0.5
                                        if 150 < total_val < 300:
                                            lines_found.append(('total', total_val))
                                    except:
                                        pass
                                    continue
                                
                                # Match plain total: "225" 
                                total_plain_match = re.match(r'^(\d{3})$', line)
                                if total_plain_match:
                                    try:
                                        total_val = float(total_plain_match.group(1))
                                        if 150 < total_val < 300:
                                            lines_found.append(('total', total_val))
                                    except:
                                        pass
                    
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
                            logger.info(f"[WagerTalk] Found: {matchup_key} - Spread: {spread_tickets_pct}%/{spread_money_pct}% | O/U: {over_tickets_pct}%/{100-over_tickets_pct}% | Lines: {open_spread}→{current_spread}, Tot: {open_total}→{current_total}")
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
