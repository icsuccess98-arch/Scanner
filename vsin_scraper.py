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


def get_vsin_lines(sport: str = 'CBB') -> dict:
    """
    Fetch open and current lines from VSIN Line Tracker
    Returns dict with game keys and line data
    """
    sport_urls = {
        'NBA': 'https://data.vsin.com/nba/vegas-odds-linetracker/',
        'CBB': 'https://data.vsin.com/college-basketball/vegas-odds-linetracker/',
        'NFL': 'https://data.vsin.com/nfl/vegas-odds-linetracker/',
        'CFB': 'https://data.vsin.com/college-football/vegas-odds-linetracker/',
        'NHL': 'https://data.vsin.com/nhl/vegas-odds-linetracker/',
    }
    
    url = sport_urls.get(sport.upper(), sport_urls['CBB'])
    cookies = load_cookies()
    
    if not cookies:
        return {'success': False, 'message': 'No cookies found.', 'data': {}}
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,*/*',
        'Referer': 'https://vsin.com/',
    }
    
    try:
        response = requests.get(url, cookies=cookies, headers=headers, timeout=30)
        
        if response.status_code != 200:
            return {'success': False, 'message': f'Failed: {response.status_code}', 'data': {}}
        
        lines_data = parse_vsin_lines(response.text, sport)
        
        return {'success': True, 'data': lines_data, 'count': len(lines_data)}
        
    except Exception as e:
        logger.error(f"Error fetching VSIN lines: {e}")
        return {'success': False, 'message': str(e), 'data': {}}


def parse_vsin_lines(html: str, sport: str) -> dict:
    """Parse line tracker data from VSIN HTML"""
    lines = {}
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')[2:]
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                
                time_cell = cells[0].get_text(strip=True)
                teams_cell = cells[1].get_text(strip=True)
                open_cell = cells[2].get_text(strip=True)
                current_cell = cells[3].get_text(strip=True)
                
                if 'Time' in time_cell or not teams_cell:
                    continue
                
                time_clean = re.sub(r'Splits.*$', '', time_cell).strip()
                
                away_team, home_team = parse_team_names(teams_cell)
                if not away_team or not home_team:
                    continue
                
                open_spread = parse_spread_line(open_cell)
                current_spread = parse_spread_line(current_cell)
                
                game_key = f"{away_team} @ {home_team}"
                
                lines[game_key] = {
                    'away_team': away_team,
                    'home_team': home_team,
                    'time': time_clean,
                    'open_away_spread': open_spread.get('away_spread'),
                    'open_away_odds': open_spread.get('away_odds'),
                    'open_home_spread': open_spread.get('home_spread'),
                    'open_home_odds': open_spread.get('home_odds'),
                    'current_away_spread': current_spread.get('away_spread'),
                    'current_away_odds': current_spread.get('away_odds'),
                    'current_home_spread': current_spread.get('home_spread'),
                    'current_home_odds': current_spread.get('home_odds'),
                }
        
        logger.info(f"Parsed {len(lines)} games from VSIN {sport} line tracker")
        
    except Exception as e:
        logger.error(f"Error parsing VSIN lines: {e}")
    
    return lines


def parse_team_names(teams_text: str) -> tuple:
    """Parse concatenated team names into away and home teams"""
    teams_text = teams_text.strip()
    
    known_teams = [
        'San Antonio Spurs', 'Charlotte Hornets', 'Atlanta Hawks', 'Indiana Pacers',
        'New Orleans Pelicans', 'Philadelphia 76ers', 'Minnesota Timberwolves', 'Memphis Grizzlies',
        'Dallas Mavericks', 'Houston Rockets', 'Chicago Bulls', 'Miami Heat',
        'Los Angeles Lakers', 'Los Angeles Clippers', 'Golden State Warriors', 'Phoenix Suns',
        'Denver Nuggets', 'Boston Celtics', 'Milwaukee Bucks', 'Cleveland Cavaliers',
        'New York Knicks', 'Brooklyn Nets', 'Toronto Raptors', 'Detroit Pistons',
        'Orlando Magic', 'Washington Wizards', 'Sacramento Kings', 'Utah Jazz',
        'Portland Trail Blazers', 'Oklahoma City Thunder',
        'Texas Tech', 'C Florida', 'Duke', 'Virginia Tech', 'Georgetown', 'Butler',
        'Cincinnati', 'Houston', 'Pittsburgh', 'Clemson', 'Campbell', 'William', 'Mary',
        'North Carolina', 'Georgia Tech', 'Virginia', 'Boston College', 'Florida State',
        'Louisville', 'Syracuse', 'Notre Dame', 'Wake Forest', 'Stanford', 'California',
        'USC', 'UCLA', 'Arizona', 'Arizona State', 'Oregon', 'Oregon State', 'Washington',
        'Colorado', 'Utah', 'Kansas', 'Kansas State', 'Baylor', 'TCU', 'Iowa State',
        'Oklahoma', 'Oklahoma State', 'West Virginia', 'Texas', 'BYU', 'UCF', 'Cincinnati',
    ]
    
    for team in sorted(known_teams, key=len, reverse=True):
        if teams_text.startswith(team):
            remaining = teams_text[len(team):].strip()
            if remaining:
                return (team, remaining)
    
    match = re.match(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:\s*\(\d+\))?)([A-Z].*)', teams_text)
    if match:
        return (match.group(1).strip(), match.group(2).strip())
    
    mid = len(teams_text) // 2
    for i in range(mid, len(teams_text)):
        if teams_text[i].isupper() and i > 0 and teams_text[i-1].islower():
            return (teams_text[:i].strip(), teams_text[i:].strip())
    
    return (None, None)


def parse_spread_line(cell_text: str) -> dict:
    """Parse spread line like '-3.5-110+3.5-110' into components"""
    result = {'away_spread': None, 'away_odds': None, 'home_spread': None, 'home_odds': None}
    
    if not cell_text or cell_text == '--':
        return result
    
    pattern = r'([+-]?\d+\.?\d*)([-+]\d+)([+-]?\d+\.?\d*)([-+]\d+)'
    match = re.match(pattern, cell_text.replace(' ', ''))
    
    if match:
        result['away_spread'] = float(match.group(1))
        result['away_odds'] = int(match.group(2))
        result['home_spread'] = float(match.group(3))
        result['home_odds'] = int(match.group(4))
    
    return result


def get_all_vsin_data(sport: str = 'CBB') -> dict:
    """
    Get combined VSIN data: splits + lines
    Returns unified game data dict
    """
    splits_result = get_vsin_splits(sport)
    lines_result = get_vsin_lines(sport)
    
    combined = {}
    
    if lines_result['success']:
        for game_key, data in lines_result['data'].items():
            combined[game_key] = data.copy()
    
    if splits_result['success']:
        for game_key, data in splits_result['data'].items():
            if game_key in combined:
                combined[game_key].update({
                    'tickets_away': data.get('away_bets_pct'),
                    'tickets_home': data.get('home_bets_pct'),
                    'money_away': data.get('away_handle_pct'),
                    'money_home': data.get('home_handle_pct'),
                    'away_sharp': data.get('away_sharp'),
                    'home_sharp': data.get('home_sharp'),
                })
            else:
                combined[game_key] = {
                    'away_team': data.get('away_team'),
                    'home_team': data.get('home_team'),
                    'tickets_away': data.get('away_bets_pct'),
                    'tickets_home': data.get('home_bets_pct'),
                    'money_away': data.get('away_handle_pct'),
                    'money_home': data.get('home_handle_pct'),
                    'away_sharp': data.get('away_sharp'),
                    'home_sharp': data.get('home_sharp'),
                }
    
    return {
        'success': True,
        'data': combined,
        'count': len(combined),
        'splits_count': splits_result.get('count', 0),
        'lines_count': lines_result.get('count', 0)
    }


def test_vsin_connection():
    """Test if VSIN cookies are valid"""
    result = get_all_vsin_data('CBB')
    if result['success']:
        print(f"VSIN: {result['count']} games (lines: {result['lines_count']}, splits: {result['splits_count']})")
        for k, v in list(result['data'].items())[:3]:
            print(f"  {k}")
            print(f"    Open: {v.get('open_away_spread')} | Current: {v.get('current_away_spread')}")
            print(f"    Tickets: {v.get('tickets_away')}%-{v.get('tickets_home')}% | Money: {v.get('money_away')}%-{v.get('money_home')}%")
        return True
    else:
        print(f"VSIN failed: {result.get('message')}")
        return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_vsin_connection()
