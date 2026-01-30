"""
WagerTalk.com Odds Page Scraper - Betting Action Data
UPDATED: Uses /odds page instead of deprecated /nba-betting-splits

Extracts Tickets %, Money %, and Lines for NBA games.
Uses Playwright to handle JavaScript-loaded content.
"""

import logging
import re
import time
import asyncio
from typing import Dict, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

# Cache for WagerTalk data
_wagertalk_cache = {}
_wagertalk_cache_time = {}
CACHE_TTL = 180  # 3 minutes cache (odds change frequently)


def _is_cache_valid(key: str) -> bool:
    """Check if cached data is still valid."""
    if key not in _wagertalk_cache:
        return False
    age = time.time() - _wagertalk_cache_time.get(key, 0)
    return age < CACHE_TTL


def _normalize_team_name(name: str) -> str:
    """Normalize team name for matching."""
    if not name:
        return ''
    # Remove extra spaces
    name = re.sub(r'\s+', ' ', name.strip())
    # Handle common variations
    name = name.replace('Trail Blazers', 'Blazers')
    return name


async def _fetch_wagertalk_odds_async(league: str = 'NBA') -> Dict[str, Dict]:
    """
    Async function to fetch WagerTalk odds data with Playwright.
    
    Returns dict with matchup keys like "Bucks vs Wizards" containing:
    {
        'away_tickets_pct': 52,
        'home_tickets_pct': 48,
        'away_money_pct': 55,
        'home_money_pct': 45,
        'spread': '-12.5',
        'total': '227.5'
    }
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("[WagerTalk] Playwright not installed")
        return {}
    
    url = "https://www.wagertalk.com/odds"
    logger.info(f"[WagerTalk] Fetching odds data from {url}")
    
    result = {}
    
    try:
        async with async_playwright() as p:
            logger.info("[WagerTalk] Launching browser...")
            
            # Use chromium headless
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            page = await context.new_page()
            
            # Navigate to odds page
            logger.info(f"[WagerTalk] Navigating to {url}")
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for the NBA section to load
            logger.info("[WagerTalk] Waiting for NBA odds table...")
            try:
                await page.wait_for_selector('text=NBA', timeout=10000)
                # Give extra time for dynamic content to load
                await asyncio.sleep(3)
            except Exception as e:
                logger.warning(f"[WagerTalk] Timeout waiting for NBA section: {e}")
            
            # Get page content
            content = await page.content()
            
            # Parse the table rows
            # Look for NBA games (rows with team names)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find NBA section
            nba_section = None
            for header in soup.find_all('tr'):
                if 'NBA' in header.get_text():
                    nba_section = header
                    break
            
            if not nba_section:
                logger.warning("[WagerTalk] Could not find NBA section in odds page")
                await browser.close()
                return {}
            
            # Parse games after NBA header
            current_row = nba_section.find_next_sibling('tr')
            games_found = 0
            
            while current_row and games_found < 50:  # Limit to prevent infinite loops
                try:
                    # Check if this is a game row (has team names)
                    cells = current_row.find_all('td')
                    
                    if len(cells) < 6:  # Skip header or empty rows
                        current_row = current_row.find_next_sibling('tr')
                        continue
                    
                    # Stop if we hit another sport section
                    row_text = current_row.get_text()
                    if any(sport in row_text for sport in ['COLLEGE BASKETBALL', 'NHL', 'WRITE-IN']):
                        break
                    
                    # Extract team names (usually in column 2)
                    teams_cell = cells[1] if len(cells) > 1 else None
                    if not teams_cell:
                        current_row = current_row.find_next_sibling('tr')
                        continue
                    
                    teams_text = teams_cell.get_text(strip=True)
                    
                    # Skip if no team names
                    if not teams_text or len(teams_text) < 3:
                        current_row = current_row.find_next_sibling('tr')
                        continue
                    
                    # Try to extract away and home team
                    # Format is usually: "Away\nHome" or on separate rows
                    team_lines = [t.strip() for t in teams_text.split('\n') if t.strip()]
                    
                    if len(team_lines) >= 2:
                        away_team = _normalize_team_name(team_lines[0])
                        home_team = _normalize_team_name(team_lines[1])
                        
                        # Extract betting percentages if available
                        # Tickets column (index varies, look for percentage)
                        tickets_pct = None
                        money_pct = None
                        
                        for idx, cell in enumerate(cells):
                            cell_text = cell.get_text(strip=True)
                            if '%' in cell_text:
                                # This might be tickets or money percentage
                                # Try to extract the number
                                match = re.search(r'(\d+)%', cell_text)
                                if match:
                                    pct = int(match.group(1))
                                    if tickets_pct is None:
                                        tickets_pct = pct
                                    elif money_pct is None:
                                        money_pct = pct
                        
                        # Create matchup key
                        matchup_key = f"{away_team} vs {home_team}"
                        
                        # Only add if we found betting percentages
                        if tickets_pct is not None:
                            result[matchup_key] = {
                                'away_team': away_team,
                                'home_team': home_team,
                                'away_tickets_pct': tickets_pct,
                                'home_tickets_pct': 100 - tickets_pct if tickets_pct else None,
                                'away_money_pct': money_pct if money_pct else None,
                                'home_money_pct': 100 - money_pct if money_pct else None,
                            }
                            games_found += 1
                            logger.info(f"[WagerTalk] Found {matchup_key}: Tickets {tickets_pct}% / Money {money_pct}%")
                    
                except Exception as e:
                    logger.warning(f"[WagerTalk] Error parsing row: {e}")
                
                current_row = current_row.find_next_sibling('tr')
            
            await browser.close()
            
            logger.info(f"[WagerTalk] Extracted {len(result)} games from odds page")
            return result
            
    except Exception as e:
        logger.error(f"[WagerTalk] Error fetching odds data: {e}")
        return {}


def get_wagertalk_odds(league: str = 'NBA') -> Dict[str, Dict]:
    """
    Fetch betting data from WagerTalk odds page.
    
    Returns dict with matchup keys containing betting percentages.
    Uses caching to avoid excessive requests.
    """
    cache_key = f"wagertalk_odds_{league}"
    
    # Check cache
    if _is_cache_valid(cache_key):
        logger.info(f"[WagerTalk] Using cached data for {league}")
        return _wagertalk_cache[cache_key]
    
    # Check if Playwright is available
    try:
        from playwright.async_api import async_playwright
        playwright_available = True
    except ImportError:
        logger.warning("[WagerTalk] Playwright not available - install with: pip install playwright && playwright install chromium")
        return {}
    
    # Fetch new data
    try:
        # Run async function
        loop = None
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        data = loop.run_until_complete(_fetch_wagertalk_odds_async(league))
        
        # Cache the result
        if data:
            _wagertalk_cache[cache_key] = data
            _wagertalk_cache_time[cache_key] = time.time()
            logger.info(f"[WagerTalk] Cached {len(data)} games for {league}")
        
        return data
        
    except Exception as e:
        logger.error(f"[WagerTalk] Error in get_wagertalk_odds: {e}")
        return {}


def find_game_data(away_team: str, home_team: str, league: str = 'NBA') -> Optional[Dict]:
    """
    Find betting data for a specific matchup.
    
    Args:
        away_team: Away team name
        home_team: Home team name
        league: League (default NBA)
    
    Returns:
        Dict with betting data or None if not found
    """
    all_data = get_wagertalk_odds(league)
    
    away_norm = _normalize_team_name(away_team)
    home_norm = _normalize_team_name(home_team)
    
    # Try exact match
    matchup_key = f"{away_norm} vs {home_norm}"
    if matchup_key in all_data:
        return all_data[matchup_key]
    
    # Try flexible matching (in case team names don't match exactly)
    for key, data in all_data.items():
        if (away_norm in key or away_team in key) and (home_norm in key or home_team in key):
            logger.info(f"[WagerTalk] Matched {away_team} vs {home_team} to {key}")
            return data
    
    logger.warning(f"[WagerTalk] No data found for {away_team} vs {home_team}")
    return None


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Fetching WagerTalk odds data...")
    data = get_wagertalk_odds('NBA')
    
    print(f"\nFound {len(data)} NBA games:")
    for matchup, game_data in data.items():
        print(f"\n{matchup}:")
        print(f"  Tickets: {game_data.get('away_tickets_pct')}% away / {game_data.get('home_tickets_pct')}% home")
        print(f"  Money: {game_data.get('away_money_pct')}% away / {game_data.get('home_money_pct')}% home")
