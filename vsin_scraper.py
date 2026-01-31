"""
VSIN Scraper - Cookie-based authentication for betting splits data
"""
import os
import requests
import logging
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

COOKIES_FILE = 'vsin_cookies.json'

def load_cookies() -> dict:
    """Load cookies from file"""
    cookies = {}
    try:
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE, 'r') as f:
                cookie_list = json.load(f)
                for c in cookie_list:
                    cookies[c['name']] = c['value']
            logger.info(f"Loaded {len(cookies)} VSIN cookies")
    except Exception as e:
        logger.error(f"Error loading cookies: {e}")
    return cookies


def get_vsin_splits(sport: str = 'CBB') -> dict:
    """
    Fetch betting splits data from VSIN using stored cookies
    Returns dict with game keys and split percentages
    """
    sport_urls = {
        'NBA': 'https://data.vsin.com/nba/betting-splits/',
        'CBB': 'https://data.vsin.com/college-basketball/betting-splits/',
        'NFL': 'https://data.vsin.com/nfl/betting-splits/',
        'CFB': 'https://data.vsin.com/college-football/betting-splits/',
        'NHL': 'https://data.vsin.com/nhl/betting-splits/',
    }
    
    url = sport_urls.get(sport.upper(), sport_urls['CBB'])
    cookies = load_cookies()
    
    if not cookies:
        return {'success': False, 'message': 'No cookies found. Please login to VSIN first.', 'data': {}}
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://vsin.com/',
    }
    
    try:
        response = requests.get(url, cookies=cookies, headers=headers, timeout=30)
        
        if response.status_code != 200:
            return {'success': False, 'message': f'Failed to fetch: {response.status_code}', 'data': {}}
        
        html = response.text
        
        if 'logout' not in html.lower() and 'sign out' not in html.lower():
            if 'Subscribe' in html and 'Pro' in html and len(html) < 50000:
                return {'success': False, 'message': 'Session expired. Please re-login to VSIN.', 'data': {}}
        
        splits_data = parse_vsin_splits(html, sport)
        
        return {'success': True, 'data': splits_data, 'count': len(splits_data)}
        
    except Exception as e:
        logger.error(f"Error fetching VSIN splits: {e}")
        return {'success': False, 'message': str(e), 'data': {}}


def parse_vsin_splits(html: str, sport: str) -> dict:
    """Parse betting splits from VSIN HTML"""
    splits = {}
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')[2:]
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 10:
                    continue
                
                i = 0
                while i < len(cells) - 9:
                    teams_cell = cells[i].get_text(strip=True)
                    
                    if 'History' not in teams_cell:
                        i += 1
                        continue
                    
                    teams_text = re.sub(r'History.*$', '', teams_cell).strip()
                    
                    team_match = re.match(r'(.+?)(\s*\(\d+\)\s*)?([A-Z][a-z][a-z]+.*)', teams_text)
                    if not team_match:
                        i += 10
                        continue
                    
                    away_team = (team_match.group(1) + (team_match.group(2) or '')).strip()
                    home_team = team_match.group(3).strip()
                    
                    spread_handle = cells[i+2].get_text(strip=True)
                    spread_bets = cells[i+3].get_text(strip=True)
                    
                    handle_pcts = re.findall(r'(\d+)%', spread_handle)
                    bets_pcts = re.findall(r'(\d+)%', spread_bets)
                    
                    if len(handle_pcts) >= 2 and len(bets_pcts) >= 2:
                        away_handle = int(handle_pcts[0])
                        home_handle = int(handle_pcts[1])
                        away_bets = int(bets_pcts[0])
                        home_bets = int(bets_pcts[1])
                        
                        game_key = f"{away_team} @ {home_team}"
                        
                        splits[game_key] = {
                            'away_team': away_team,
                            'home_team': home_team,
                            'away_handle_pct': away_handle,
                            'home_handle_pct': home_handle,
                            'away_bets_pct': away_bets,
                            'home_bets_pct': home_bets,
                            'away_sharp': get_sharp_indicator(away_bets, away_handle),
                            'home_sharp': get_sharp_indicator(home_bets, home_handle),
                        }
                    
                    i += 10
        
        logger.info(f"Parsed {len(splits)} games from VSIN {sport} splits")
        
    except Exception as e:
        logger.error(f"Error parsing VSIN HTML: {e}")
    
    return splits


def extract_percentage(text: str) -> float:
    """Extract percentage value from text"""
    if not text:
        return None
    try:
        match = re.search(r'(\d+(?:\.\d+)?)\s*%?', text.strip())
        if match:
            return float(match.group(1))
    except:
        pass
    return None


def get_sharp_indicator(bet_pct: float, money_pct: float) -> str:
    """Determine if sharp money is indicated"""
    if bet_pct is None or money_pct is None:
        return None
    
    diff = money_pct - bet_pct
    
    if diff >= 30:
        return 'EXTREME_SHARP'
    elif diff >= 20:
        return 'STRONG_SHARP'
    elif diff >= 10:
        return 'MODERATE_SHARP'
    elif diff <= -30:
        return 'EXTREME_PUBLIC'
    elif diff <= -20:
        return 'STRONG_PUBLIC'
    elif diff <= -10:
        return 'MODERATE_PUBLIC'
    
    return None


def test_vsin_connection():
    """Test if VSIN cookies are valid"""
    result = get_vsin_splits('CBB')
    if result['success']:
        print(f"VSIN connection successful! Found {result['count']} splits entries.")
        return True
    else:
        print(f"VSIN connection failed: {result['message']}")
        return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_vsin_connection()
