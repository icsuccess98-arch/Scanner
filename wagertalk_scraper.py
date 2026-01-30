"""
WAGERTALK BETTING SPLITS SCRAPER
=================================

MANDATORY SOURCE: WagerTalk.com ONLY
No fallbacks. No alternatives.

Extracts betting splits exactly as shown:
- Tickets % (public betting)
- Money % (sharp money)
- Opening lines
- Current lines

Format understanding:
- "53%" = 53% on spread (away team)
- "O78%" = 78% on Over for totals
- "u60" or "u60%" = 60% on Under for totals

Uses aggressive anti-detection and session persistence.
"""

import logging
import time
import re
import asyncio
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Cache for WagerTalk data
_wagertalk_cache = {}
_wagertalk_cache_time = {}

# NBA team names for matching
NBA_TEAMS = [
    'hawks', 'celtics', 'nets', 'hornets', 'bulls', 'cavaliers', 'mavericks',
    'nuggets', 'pistons', 'warriors', 'rockets', 'pacers', 'clippers', 'lakers',
    'grizzlies', 'heat', 'bucks', 'timberwolves', 'wolves', 'pelicans', 'knicks',
    'thunder', 'magic', 'sixers', '76ers', 'suns', 'blazers', 'trail blazers',
    'kings', 'spurs', 'raptors', 'jazz', 'wizards',
    'atlanta', 'boston', 'brooklyn', 'charlotte', 'chicago', 'cleveland',
    'dallas', 'denver', 'detroit', 'golden state', 'houston', 'indiana',
    'la clippers', 'la lakers', 'los angeles', 'memphis', 'miami', 'milwaukee',
    'minnesota', 'new orleans', 'new york', 'oklahoma city', 'oklahoma',
    'orlando', 'philadelphia', 'phoenix', 'portland', 'sacramento',
    'san antonio', 'toronto', 'utah', 'washington'
]

def _is_nba_team(text: str) -> bool:
    """Check if text contains an NBA team name."""
    text_lower = text.lower().strip()
    return any(team in text_lower for team in NBA_TEAMS)

def _normalize_team_name(text: str) -> str:
    """Normalize team name to standard format."""
    text = text.strip()
    name_map = {
        'hawks': 'Hawks', 'atlanta': 'Hawks',
        'celtics': 'Celtics', 'boston': 'Celtics',
        'nets': 'Nets', 'brooklyn': 'Nets',
        'hornets': 'Hornets', 'charlotte': 'Hornets',
        'bulls': 'Bulls', 'chicago': 'Bulls',
        'cavaliers': 'Cavaliers', 'cavs': 'Cavaliers', 'cleveland': 'Cavaliers',
        'mavericks': 'Mavericks', 'mavs': 'Mavericks', 'dallas': 'Mavericks',
        'nuggets': 'Nuggets', 'denver': 'Nuggets',
        'pistons': 'Pistons', 'detroit': 'Pistons',
        'warriors': 'Warriors', 'golden state': 'Warriors',
        'rockets': 'Rockets', 'houston': 'Rockets',
        'pacers': 'Pacers', 'indiana': 'Pacers',
        'clippers': 'Clippers', 'la clippers': 'Clippers',
        'lakers': 'Lakers', 'la lakers': 'Lakers',
        'grizzlies': 'Grizzlies', 'memphis': 'Grizzlies',
        'heat': 'Heat', 'miami': 'Heat',
        'bucks': 'Bucks', 'milwaukee': 'Bucks',
        'timberwolves': 'Timberwolves', 'wolves': 'Timberwolves', 'minnesota': 'Timberwolves',
        'pelicans': 'Pelicans', 'new orleans': 'Pelicans',
        'knicks': 'Knicks', 'new york': 'Knicks',
        'thunder': 'Thunder', 'oklahoma city': 'Thunder', 'oklahoma': 'Thunder',
        'magic': 'Magic', 'orlando': 'Magic',
        'sixers': '76ers', '76ers': '76ers', 'philadelphia': '76ers',
        'suns': 'Suns', 'phoenix': 'Suns',
        'blazers': 'Trail Blazers', 'trail blazers': 'Trail Blazers', 'portland': 'Trail Blazers',
        'kings': 'Kings', 'sacramento': 'Kings',
        'spurs': 'Spurs', 'san antonio': 'Spurs',
        'raptors': 'Raptors', 'toronto': 'Raptors',
        'jazz': 'Jazz', 'utah': 'Jazz',
        'wizards': 'Wizards', 'washington': 'Wizards'
    }
    text_lower = text.lower()
    for key, val in name_map.items():
        if key in text_lower:
            return val
    return text.split('\n')[0].strip()

def _is_cache_valid(cache_key: str, ttl: int = 60) -> bool:
    """Check if cache is still valid."""
    if cache_key not in _wagertalk_cache:
        return False
    age = time.time() - _wagertalk_cache_time.get(cache_key, 0)
    return age < ttl

def _get_highlight_color(percentage: int) -> str:
    """Determine highlight color based on percentage threshold."""
    if percentage >= 75:
        return 'red'
    elif percentage >= 60:
        return 'yellow'
    elif percentage >= 50:
        return 'green'
    else:
        return 'none'

async def _fetch_wagertalk_async(league: str = 'NBA') -> Dict[str, Dict]:
    """Fetch betting data from WagerTalk using Playwright."""
    from playwright.async_api import async_playwright
    
    result = {}
    browser = None
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            """)
            
            page = await context.new_page()
            
            url = f"https://www.wagertalk.com/odds?sport=today&cb={time.time()}"
            logger.info(f"[WagerTalk] Navigating to: {url}")
            
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(4)
            
            rows = await page.query_selector_all('tr.reg, tr.alt, tr[class*="game"], tbody tr')
            logger.info(f"[WagerTalk] Found {len(rows)} table rows")
            
            if len(rows) == 0:
                tables = await page.query_selector_all('table')
                for table in tables:
                    rows.extend(await table.query_selector_all('tr'))
                logger.info(f"[WagerTalk] Total rows from all tables: {len(rows)}")
            
            games_found = 0
            
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
                    
                    cell_values = []
                    for cell in cells:
                        cell_text = await cell.inner_text()
                        cell_values.append(cell_text.strip())
                    
                    away_team = None
                    home_team = None
                    
                    # Spread betting data
                    spread_tickets_pct = 50
                    spread_money_pct = 50
                    open_spread = None
                    current_spread = None
                    
                    # Total betting data
                    over_tickets_pct = 50
                    over_money_pct = 50
                    under_tickets_pct = 50
                    under_money_pct = 50
                    open_total = None
                    current_total = None
                    
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
                        
                        # WagerTalk format parsing:
                        # "O78%" = 78% on Over
                        # "u60" or "u60%" = 60% on Under
                        # "53%" = 53% on spread (away team)
                        
                        # Check for Over percentage (O prefix)
                        over_match = re.search(r'[oO](\d{1,3})%?', cell_text)
                        if over_match:
                            pct = int(over_match.group(1))
                            if 0 < pct <= 100:
                                over_tickets_pct = pct
                                under_tickets_pct = 100 - pct
                        
                        # Check for Under percentage (u prefix) - NOT part of team name
                        under_match = re.search(r'^[uU](\d{1,3})%?$', cell_text)
                        if under_match:
                            pct = int(under_match.group(1))
                            if 0 < pct <= 100:
                                under_tickets_pct = pct
                                over_tickets_pct = 100 - pct
                        
                        # Regular spread percentage (plain number with %)
                        plain_pct = re.search(r'^(\d{1,3})%$', cell_text)
                        if plain_pct and not over_match and not under_match:
                            percentages.append(int(plain_pct.group(1)))
                        
                        # Extract spread lines (format: "-11.5", "+3.5", "-12-10")
                        spread_match = re.search(r'^([+-]?\d+\.?5?)-\d+$', cell_text)
                        if spread_match:
                            try:
                                line = float(spread_match.group(1))
                                if abs(line) < 50:
                                    lines_found.append(('spread', line))
                            except:
                                pass
                        
                        # Extract total lines (format: "227.5", "230")
                        total_match = re.search(r'^(\d{3}\.?5?)$', cell_text)
                        if total_match:
                            try:
                                total = float(total_match.group(1))
                                if 150 < total < 300:
                                    lines_found.append(('total', total))
                            except:
                                pass
                        
                        # Also match totals with odds attached (227½u-15)
                        total_with_odds = re.search(r'(\d{3})½?[oOuU]?-?\d*', cell_text)
                        if total_with_odds:
                            try:
                                total = float(total_with_odds.group(1))
                                if 150 < total < 300:
                                    lines_found.append(('total', total))
                            except:
                                pass
                    
                    # Assign spread percentages (first two percentages found)
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
                        
                        # Calculate highlight colors
                        max_spread_pct = max(spread_tickets_pct, 100 - spread_tickets_pct)
                        tickets_highlight = _get_highlight_color(max_spread_pct)
                        
                        # Sharp money detection (money % vs tickets % divergence)
                        away_sharp_diff = spread_money_pct - spread_tickets_pct
                        home_sharp_diff = (100 - spread_money_pct) - (100 - spread_tickets_pct)
                        spread_sharp_detected = abs(away_sharp_diff) >= 10 or abs(home_sharp_diff) >= 10
                        spread_sharp_side = away_team if away_sharp_diff >= 10 else (home_team if home_sharp_diff >= 10 else None)
                        
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
                                'under_bet_pct': under_tickets_pct,
                                'over_money_pct': over_money_pct,
                                'under_money_pct': under_money_pct,
                                'total_tickets_pct': over_tickets_pct,
                                'total_money_pct': over_money_pct,
                                # Total lines
                                'total_open_line': open_total,
                                'total_current_line': current_total or open_total,
                                # Highlight colors (recency)
                                'tickets_highlight': tickets_highlight,
                                'money_highlight': _get_highlight_color(max(spread_money_pct, 100 - spread_money_pct)),
                                # Sharp money detection
                                'sharp_detected': spread_sharp_detected,
                                'sharp_side': spread_sharp_side,
                                'spread_sharp_detected': spread_sharp_detected,
                                'spread_sharp_side': spread_sharp_side,
                                'source': 'wagertalk'
                            }
                            games_found += 1
                            logger.info(f"[WagerTalk] Found: {matchup_key} - Spread: {spread_tickets_pct}%/{spread_money_pct}% | O/U: {over_tickets_pct}%/{under_tickets_pct}% | Lines: {open_spread}→{current_spread}, Total: {open_total}")
                        else:
                            # Update existing entry with additional totals data
                            existing = result[matchup_key]
                            if open_total and not existing.get('total_open_line'):
                                existing['total_open_line'] = open_total
                                existing['total_current_line'] = current_total or open_total
                            if over_tickets_pct != 50:
                                existing['over_bet_pct'] = over_tickets_pct
                                existing['under_bet_pct'] = under_tickets_pct
                    
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
    Returns Tickets %, Money % for spreads and totals, plus lines.
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
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_async_in_thread, league)
            try:
                result = future.result(timeout=60)
            except FuturesTimeout:
                logger.warning("[WagerTalk] Timeout after 60s")
                result = {}
    except Exception as e:
        logger.error(f"[WagerTalk] Thread error: {e}")
        result = {}
    
    if result:
        _wagertalk_cache[cache_key] = result
        _wagertalk_cache_time[cache_key] = time.time()
        logger.info(f"[WagerTalk] Fetched {len(result)} games")
    
    return result
