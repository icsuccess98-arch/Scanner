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
        'TENNIS': 'https://data.vsin.com/tennis/betting-splits/',
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

        # Debug: Log HTML structure for troubleshooting
        logger.info(f"VSIN {sport} splits HTML length: {len(html)} chars")

        if 'logout' not in html.lower() and 'sign out' not in html.lower():
            if 'Subscribe' in html and 'Pro' in html and len(html) < 50000:
                return {'success': False, 'message': 'Session expired. Please re-login to VSIN.', 'data': {}}

        splits_data = parse_vsin_splits(html, sport)

        return {'success': True, 'data': splits_data, 'count': len(splits_data)}
        
    except Exception as e:
        logger.error(f"Error fetching VSIN splits: {e}")
        return {'success': False, 'message': str(e), 'data': {}}


def parse_vsin_splits(html: str, sport: str) -> dict:
    """Parse betting splits from VSIN HTML - improved parsing logic"""
    splits = {}

    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Find all tables with betting data
        tables = soup.find_all('table')
        logger.info(f"VSIN {sport} splits: Found {len(tables)} tables")
        for table in tables:
            rows = table.find_all('tr')

            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue

                # Look through all cells for team names and percentages
                row_text = ' '.join([c.get_text(strip=True) for c in cells])

                # Skip header rows
                if 'Handle' in row_text and 'Bets' in row_text:
                    continue

                teams_cell = None
                teams_cell_raw = None
                spread_handle_cell = None
                spread_bets_cell = None

                for idx, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    if 'History' in cell_text or ('@' in cell_text and len(cell_text) > 5):
                        teams_cell = cell_text
                        teams_cell_raw = cell
                        if idx + 2 < len(cells):
                            spread_handle_cell = cells[idx + 1].get_text(separator='|', strip=True)
                            spread_bets_cell = cells[idx + 2].get_text(separator='|', strip=True)
                        break

                if not teams_cell:
                    continue

                away_team = None
                home_team = None

                if teams_cell_raw:
                    sep_text = teams_cell_raw.get_text(separator='|', strip=True)
                    parts = [p.strip() for p in sep_text.split('|') if p.strip()]
                    team_parts = [p for p in parts if p not in ('History', 'Splits') and not re.match(r'^\d+\s+VSiN', p) and not re.match(r'^\(\d+\)$', p) and len(p) >= 2]
                    if len(team_parts) >= 2:
                        away_team = re.sub(r'\s*\(\d+\)\s*', '', team_parts[0]).strip()
                        home_team = re.sub(r'\s*\(\d+\)\s*', '', team_parts[1]).strip()

                if not away_team or not home_team:
                    teams_text = re.sub(r'History.*$', '', teams_cell).strip()
                    teams_text = re.sub(r'Splits.*$', '', teams_text).strip()
                    away_team, home_team = parse_team_names(teams_text)

                if not away_team or not home_team:
                    logger.debug(f"Could not parse teams from splits cell: '{teams_text}'")
                    continue

                # Parse percentages from handle and bets cells
                handle_pcts = []
                bets_pcts = []

                if spread_handle_cell:
                    handle_pcts = re.findall(r'(\d+)%', spread_handle_cell)
                if spread_bets_cell:
                    bets_pcts = re.findall(r'(\d+)%', spread_bets_cell)

                # Also check if percentages are in the teams cell itself (some formats)
                if not handle_pcts or not bets_pcts:
                    all_pcts = re.findall(r'(\d+)%', row_text)
                    if len(all_pcts) >= 4:
                        # First pair = handle, second pair = bets
                        handle_pcts = all_pcts[0:2]
                        bets_pcts = all_pcts[2:4]

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
                    logger.debug(f"Parsed VSIN splits: {game_key} - Handle: {away_handle}/{home_handle}, Bets: {away_bets}/{home_bets}")

        logger.info(f"Parsed {len(splits)} games from VSIN {sport} splits")

    except Exception as e:
        logger.error(f"Error parsing VSIN HTML: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return splits


def parse_tennis_splits(html: str, filter_dates=None) -> dict:
    """Parse tennis betting splits from VSIN HTML - tennis uses player names instead of team names
    
    Args:
        html: Raw HTML from VSIN tennis betting-splits page
        filter_dates: Optional set of date strings like {'Feb 22', 'Feb 23'} to filter matches
    """
    splits = {}
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        logger.info(f"VSIN Tennis splits: Found {len(tables)} tables")
        
        current_tournament = 'Tennis'
        current_date_str = ''
        current_date_short = ''
        
        for table in tables:
            cells = table.find_all(['td', 'th'])
            
            i = 0
            while i < len(cells):
                cell = cells[i]
                cls = cell.get('class', [])
                text = cell.get_text(strip=True)
                
                if 'text-center' in cls and ('ATP' in text or 'WTA' in text or 'ITF' in text):
                    date_match = re.search(r'((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*\w+\s+\d+)', text)
                    if date_match:
                        current_date_str = date_match.group(1)
                        month_day = re.search(r'(\w+\s+\d+)$', current_date_str)
                        current_date_short = month_day.group(1) if month_day else current_date_str
                    
                    tourn_match = re.match(r'((?:ATP|WTA|ITF)\s*-\s*.*?)(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)', text)
                    if tourn_match:
                        current_tournament = tourn_match.group(1).strip().rstrip('-').strip()
                    else:
                        current_tournament = text[:30]
                    
                    i += 1
                    continue
                
                if 'div_dark' in cls:
                    i += 1
                    continue
                
                text = cell.get_text(separator='|', strip=True)
                
                if 'font-12' in cls and '|' in text:
                    parts = text.split('|')
                    if len(parts) == 2 and len(parts[0]) > 2 and len(parts[1]) > 2:
                        player1 = parts[0].strip()
                        player2 = parts[1].strip()
                        
                        game_id = ''
                        modal_triggers = cell.find_all(attrs={'data-param2': True})
                        if modal_triggers:
                            game_id = modal_triggers[0].get('data-param2', '')
                        
                        odds_text = ''
                        pct_cells = []
                        
                        for j in range(1, 10):
                            if i + j >= len(cells):
                                break
                            next_cell = cells[i + j]
                            next_text = next_cell.get_text(separator='|', strip=True)
                            next_cls = next_cell.get('class', [])
                            
                            if 'font-12' in next_cls or 'text-center' in next_cls:
                                break
                            
                            if re.match(r'^[+-]?\d', next_text) and '|' in next_text and '%' not in next_text and not odds_text:
                                odds_text = next_text
                            elif '%' in next_text and '|' in next_text:
                                pct_cells.append(next_text)
                        
                        handle_pcts = []
                        bets_pcts = []
                        if len(pct_cells) >= 2:
                            handle_pcts = re.findall(r'(\d+)%', pct_cells[0])
                            bets_pcts = re.findall(r'(\d+)%', pct_cells[1])
                        elif len(pct_cells) == 1:
                            handle_pcts = re.findall(r'(\d+)%', pct_cells[0])
                        
                        odds_parts = odds_text.split('|') if odds_text else []
                        p1_odds = odds_parts[0].strip() if len(odds_parts) >= 1 else ''
                        p2_odds = odds_parts[1].strip() if len(odds_parts) >= 2 else ''
                        
                        game_key = f"{player1} vs {player2}"
                        
                        if filter_dates and current_date_short:
                            if not any(d in current_date_short for d in filter_dates):
                                i += 1
                                continue
                        
                        entry = {
                            'player1': player1,
                            'player2': player2,
                            'tournament': current_tournament,
                            'date': current_date_str,
                            'p1_odds': p1_odds,
                            'p2_odds': p2_odds,
                            'game_id': game_id,
                        }
                        
                        if len(handle_pcts) >= 2:
                            entry['p1_handle_pct'] = int(handle_pcts[0])
                            entry['p2_handle_pct'] = int(handle_pcts[1])
                        if len(bets_pcts) >= 2:
                            entry['p1_bets_pct'] = int(bets_pcts[0])
                            entry['p2_bets_pct'] = int(bets_pcts[1])
                        
                        has_handle = entry.get('p1_handle_pct') is not None
                        has_bets = entry.get('p1_bets_pct') is not None
                        
                        if has_handle and has_bets:
                            entry['p1_sharp'] = get_sharp_indicator(entry['p1_bets_pct'], entry['p1_handle_pct'])
                            entry['p2_sharp'] = get_sharp_indicator(entry['p2_bets_pct'], entry['p2_handle_pct'])
                        
                        if has_handle or has_bets:
                            if not has_handle:
                                logger.warning(f"Tennis split missing handle data: {game_key}")
                            if not has_bets:
                                logger.warning(f"Tennis split missing bets data: {game_key}")
                            splits[game_key] = entry
                            logger.debug(f"Tennis split: {game_key} | Handle: {entry.get('p1_handle_pct')}/{entry.get('p2_handle_pct')} Bets: {entry.get('p1_bets_pct')}/{entry.get('p2_bets_pct')}")
                
                i += 1
        
        logger.info(f"Parsed {len(splits)} tennis matches from VSIN splits")
    
    except Exception as e:
        logger.error(f"Error parsing tennis splits: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return splits


def _fetch_single_opening_line(game_id: str, cookies: dict, headers: dict) -> tuple:
    """Fetch opening line for a single game from VSIN modal endpoint."""
    try:
        resp = requests.get('https://data.vsin.com/modal/loadmodal.php',
                          params={'modalpage': 'dksplitsgame', 'gameid': game_id},
                          cookies=cookies, headers=headers, timeout=10)
        if resp.status_code != 200:
            return (game_id, None)
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        rows = soup.find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) < 3:
                continue
            
            for td in tds:
                divs = td.find_all('div', class_='game_highlight_dark')
                if len(divs) == 2:
                    texts = [d.get_text(strip=True) for d in divs]
                    if any(re.match(r'^[+-]?\d', t) for t in texts if t and t != '-'):
                        return (game_id, {
                            'p1_open': texts[0] if texts[0] != '-' else '',
                            'p2_open': texts[1] if texts[1] != '-' else ''
                        })
        
        return (game_id, None)
    except Exception as e:
        logger.debug(f"Error fetching opening line for {game_id}: {e}")
        return (game_id, None)


def fetch_tennis_opening_lines(game_ids: list, cookies: dict, headers: dict) -> dict:
    """Fetch opening lines from VSIN modal endpoint for tennis matches (concurrent).
    
    Args:
        game_ids: List of VSIN game IDs (e.g., ['TEN33688117', ...])
        cookies: VSIN authentication cookies
        headers: Request headers
    
    Returns:
        Dict mapping game_id -> {'p1_open': str, 'p2_open': str}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    opening_lines = {}
    valid_ids = [gid for gid in game_ids if gid]
    
    if not valid_ids:
        return opening_lines
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_fetch_single_opening_line, gid, cookies, headers): gid for gid in valid_ids}
        for future in as_completed(futures):
            game_id, result = future.result()
            if result:
                opening_lines[game_id] = result
    
    logger.info(f"VSIN Tennis: Fetched opening lines for {len(opening_lines)}/{len(valid_ids)} games")
    return opening_lines


def detect_tennis_rlm(match: dict) -> None:
    """Detect Reverse Line Movement for a tennis match using opening vs current odds.
    
    Uses the same Favorite/Underdog Decision Table as NBA/CBB:
    - Money (handle) on Favorite + Line moves toward Underdog = RLM on underdog
    - Money (handle) on Underdog + Line moves toward Favorite = RLM on favorite
    
    In tennis moneyline terms:
    - Favorite = player with lower (more negative) opening odds
    - Line moving toward a player = their odds shortening (becoming more favorable)
    - RLM = line shortens for a player who has MINORITY handle (money)
    
    Requires actual line movement. Handle-based divergence without line movement
    is flagged as sharp money (sharp_side), not RLM.
    """
    p1_handle = match.get('p1_handle_pct', 50)
    p2_handle = match.get('p2_handle_pct', 50)
    p1_bets = match.get('p1_bets_pct', 50)
    p2_bets = match.get('p2_bets_pct', 50)
    
    match['p1_rlm'] = False
    match['p2_rlm'] = False
    match['p1_line_move'] = ''
    match['p2_line_move'] = ''
    
    p1_open = match.get('p1_open_odds', '')
    p2_open = match.get('p2_open_odds', '')
    p1_curr = match.get('p1_odds', '')
    p2_curr = match.get('p2_odds', '')
    
    def parse_odds(odds_str):
        try:
            return int(odds_str.replace('+', ''))
        except (ValueError, AttributeError):
            return None
    
    p1_open_val = parse_odds(p1_open)
    p2_open_val = parse_odds(p2_open)
    p1_curr_val = parse_odds(p1_curr)
    p2_curr_val = parse_odds(p2_curr)
    
    if p1_open_val is not None and p1_curr_val is not None:
        if p1_curr_val < p1_open_val:
            match['p1_line_move'] = 'shortened'
        elif p1_curr_val > p1_open_val:
            match['p1_line_move'] = 'lengthened'
    
    if p2_open_val is not None and p2_curr_val is not None:
        if p2_curr_val < p2_open_val:
            match['p2_line_move'] = 'shortened'
        elif p2_curr_val > p2_open_val:
            match['p2_line_move'] = 'lengthened'
    
    if p1_open_val is not None and p1_curr_val is not None and p1_curr_val < p1_open_val:
        if p1_handle < 50:
            match['p1_rlm'] = True
            logger.info(f"RLM detected on {match['player1']}: opened {p1_open} → {p1_curr}, only {p1_handle}% money")
    
    if p2_open_val is not None and p2_curr_val is not None and p2_curr_val < p2_open_val:
        if p2_handle < 50:
            match['p2_rlm'] = True
            logger.info(f"RLM detected on {match['player2']}: opened {p2_open} → {p2_curr}, only {p2_handle}% money")


def get_vsin_tennis_data() -> dict:
    """Fetch and combine VSIN tennis splits data with RLM detection.
    Only returns matches for today and tomorrow since tennis spans late/early hours."""
    from datetime import datetime, timedelta
    
    cookies = load_cookies()
    if not cookies:
        return {'success': False, 'message': 'No cookies found.', 'matches': {}}
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,*/*',
        'Referer': 'https://data.vsin.com/tennis/betting-splits/',
    }
    
    today = datetime.now()
    filter_dates = {today.strftime('%b %-d')}
    logger.info(f"VSIN Tennis date filter: {filter_dates}")
    
    try:
        resp = requests.get('https://data.vsin.com/tennis/betting-splits/', 
                          cookies=cookies, headers=headers, timeout=30)
        if resp.status_code != 200:
            return {'success': False, 'message': f'Failed: {resp.status_code}', 'matches': {}}
        
        matches = parse_tennis_splits(resp.text, filter_dates=filter_dates)
        
        game_ids = [m.get('game_id', '') for m in matches.values() if m.get('game_id')]
        opening_lines = {}
        if game_ids:
            opening_lines = fetch_tennis_opening_lines(game_ids, cookies, headers)
        
        for key, match in matches.items():
            game_id = match.get('game_id', '')
            if game_id and game_id in opening_lines:
                match['p1_open_odds'] = opening_lines[game_id].get('p1_open', '')
                match['p2_open_odds'] = opening_lines[game_id].get('p2_open', '')
            else:
                match['p1_open_odds'] = ''
                match['p2_open_odds'] = ''
            
            detect_tennis_rlm(match)
            
            p1_handle = match.get('p1_handle_pct', 50)
            p2_handle = match.get('p2_handle_pct', 50)
            p1_bets = match.get('p1_bets_pct', 50)
            p2_bets = match.get('p2_bets_pct', 50)
            p1_divergence = abs(p1_handle - p1_bets)
            p2_divergence = abs(p2_handle - p2_bets)
            match['p1_divergence'] = p1_divergence
            match['p2_divergence'] = p2_divergence
            
            match['sharp_side'] = None
            if p1_handle > p1_bets and p1_divergence >= 15:
                match['sharp_side'] = match['player1']
            elif p2_handle > p2_bets and p2_divergence >= 15:
                match['sharp_side'] = match['player2']
        
        return {'success': True, 'matches': matches, 'count': len(matches)}
    
    except Exception as e:
        logger.error(f"Error fetching VSIN tennis: {e}")
        return {'success': False, 'message': str(e), 'matches': {}}


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
        'TENNIS': 'https://data.vsin.com/tennis/vegas-odds-linetracker/',
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

        html = response.text

        # Debug: Log HTML structure for troubleshooting
        logger.info(f"VSIN {sport} lines HTML length: {len(html)} chars")

        lines_data = parse_vsin_lines(html, sport)

        return {'success': True, 'data': lines_data, 'count': len(lines_data)}
        
    except Exception as e:
        logger.error(f"Error fetching VSIN lines: {e}")
        return {'success': False, 'message': str(e), 'data': {}}


def parse_vsin_lines(html: str, sport: str) -> dict:
    """Parse line tracker data from VSIN HTML - improved for various formats"""
    lines = {}

    try:
        soup = BeautifulSoup(html, 'html.parser')

        tables = soup.find_all('table')
        logger.info(f"VSIN {sport} lines: Found {len(tables)} tables")

        for table_idx, table in enumerate(tables):
            rows = table.find_all('tr')

            # Debug: Log first few rows structure
            if table_idx == 0 and rows:
                for row_idx, row in enumerate(rows[:3]):
                    cells = row.find_all(['td', 'th'])
                    cell_texts = [c.get_text(strip=True)[:30] for c in cells]
                    logger.debug(f"VSIN lines table row {row_idx}: {cell_texts}")

            # Skip header rows (first 1-2 rows typically)
            data_rows = rows[1:] if len(rows) > 1 else rows

            for row in data_rows:
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue

                # Try to find team names and spreads in the cells
                # Common structures:
                # [time, teams, open_spread, current_spread, ...]
                # [teams, open_spread, current_spread, ...]

                row_text = ' '.join([c.get_text(strip=True) for c in cells])

                # Skip header-like rows
                if 'Open' in row_text and 'Current' in row_text:
                    continue
                if 'Time' in cells[0].get_text(strip=True):
                    continue

                teams_cell = None
                teams_cell_raw = None
                teams_idx = -1
                for idx, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True)
                    if len(cell_text) > 5 and not re.match(r'^[-+]?\d', cell_text):
                        teams_cell = cell_text
                        teams_cell_raw = cell
                        teams_idx = idx
                        break

                if not teams_cell:
                    continue

                away_team = None
                home_team = None

                if teams_cell_raw:
                    sep_text = teams_cell_raw.get_text(separator='|', strip=True)
                    parts = [p.strip() for p in sep_text.split('|') if p.strip() and not re.match(r'^\d+$', p.strip())]
                    team_parts = [p for p in parts if len(p) >= 2 and not re.match(r'^[-+]?\d', p)]
                    if len(team_parts) >= 2:
                        away_team = re.sub(r'\s*\(\d+\)\s*', '', team_parts[0]).strip()
                        home_team = re.sub(r'\s*\(\d+\)\s*', '', team_parts[1]).strip()

                if not away_team or not home_team:
                    away_team, home_team = parse_team_names(teams_cell)

                if not away_team or not home_team:
                    continue

                # Look for spread values after teams cell
                open_spread = {'away_spread': None, 'away_odds': None, 'home_spread': None, 'home_odds': None}
                current_spread = {'away_spread': None, 'away_odds': None, 'home_spread': None, 'home_odds': None}

                # Gather remaining cells after teams
                remaining_cells = cells[teams_idx + 1:] if teams_idx >= 0 else cells[1:]

                # First spread-like value is open, second is current
                spread_cells_found = []
                for cell in remaining_cells:
                    cell_text = cell.get_text(strip=True)
                    if cell_text and cell_text != '--':
                        parsed = parse_spread_line(cell_text)
                        if parsed.get('away_spread') is not None or 'PK' in cell_text.upper() or 'EVEN' in cell_text.upper():
                            spread_cells_found.append(parsed)
                        if len(spread_cells_found) >= 2:
                            break

                if len(spread_cells_found) >= 1:
                    open_spread = spread_cells_found[0]
                if len(spread_cells_found) >= 2:
                    current_spread = spread_cells_found[1]

                game_key = f"{away_team} @ {home_team}"

                # Calculate line movement
                line_movement = None
                line_direction = 'stable'
                if open_spread.get('away_spread') is not None and current_spread.get('away_spread') is not None:
                    line_movement = current_spread['away_spread'] - open_spread['away_spread']
                    if line_movement < -0.4:
                        line_direction = 'toward_away'  # Line moved toward away team (away became bigger favorite)
                    elif line_movement > 0.4:
                        line_direction = 'toward_home'  # Line moved toward home team (home became bigger favorite)

                lines[game_key] = {
                    'away_team': away_team,
                    'home_team': home_team,
                    'open_away_spread': open_spread.get('away_spread'),
                    'open_away_odds': open_spread.get('away_odds'),
                    'open_home_spread': open_spread.get('home_spread'),
                    'open_home_odds': open_spread.get('home_odds'),
                    'current_away_spread': current_spread.get('away_spread'),
                    'current_away_odds': current_spread.get('away_odds'),
                    'current_home_spread': current_spread.get('home_spread'),
                    'current_home_odds': current_spread.get('home_odds'),
                    'line_movement': line_movement,
                    'line_direction': line_direction,
                }

                logger.debug(f"VSIN line parsed: {game_key} | Open: {open_spread.get('away_spread')} -> Current: {current_spread.get('away_spread')} | Movement: {line_movement}")

        logger.info(f"Parsed {len(lines)} games from VSIN {sport} line tracker")

    except Exception as e:
        logger.error(f"Error parsing VSIN lines: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return lines


def parse_team_names(teams_text: str) -> tuple:
    """Parse concatenated team names into away and home teams - improved for CBB"""
    teams_text = teams_text.strip()

    # Remove rankings like (1), (25), etc. but preserve the text
    teams_clean = re.sub(r'\s*\(\d+\)\s*', ' ', teams_text).strip()
    # Collapse multiple spaces
    teams_clean = re.sub(r'\s+', ' ', teams_clean)

    # Strategy 1: Split on @ symbol (standard format)
    if ' @ ' in teams_clean or '@' in teams_clean:
        parts = re.split(r'\s*@\s*', teams_clean, 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            return (parts[0].strip(), parts[1].strip())

    # Common short CBB team abbreviations (3-4 letters) that appear concatenated
    # These need special handling because they don't have obvious boundaries
    short_teams = [
        'SMU', 'VMI', 'LSU', 'TCU', 'UCF', 'USC', 'MIT', 'BYU', 'UAB', 'UVA', 'VCU',
        'UNC', 'UNLV', 'UTEP', 'UTSA', 'IPFW', 'IUPUI', 'UNI', 'URI', 'UIC',
        'SFA', 'WKU', 'NIU', 'CMU', 'EMU', 'WMU', 'FIU', 'FAU', 'FGCU', 'ETSU',
        'MTSU', 'SIUE', 'UMBC', 'UMKC', 'NJIT', 'SIU', 'UNO', 'UNM', 'UCSB', 'CSUN',
    ]

    # Check if text starts with a short team name
    for short in sorted(short_teams, key=len, reverse=True):
        if teams_clean.upper().startswith(short):
            remaining = teams_clean[len(short):].strip()
            if remaining and len(remaining) >= 3:
                return (short, remaining)

    # Check if text contains a short team name after another team
    for short in sorted(short_teams, key=len, reverse=True):
        idx = teams_clean.upper().find(short)
        if idx > 2:  # Short team is second (after first team)
            away = teams_clean[:idx].strip()
            home = teams_clean[idx:].strip()
            if len(away) >= 3 and len(home) >= 3:
                return (away, home)

    # NBA full team names (longest first for greedy match)
    nba_teams = [
        'San Antonio Spurs', 'Charlotte Hornets', 'Atlanta Hawks', 'Indiana Pacers',
        'New Orleans Pelicans', 'Philadelphia 76ers', 'Minnesota Timberwolves', 'Memphis Grizzlies',
        'Dallas Mavericks', 'Houston Rockets', 'Chicago Bulls', 'Miami Heat',
        'Los Angeles Lakers', 'Los Angeles Clippers', 'Golden State Warriors', 'Phoenix Suns',
        'Denver Nuggets', 'Boston Celtics', 'Milwaukee Bucks', 'Cleveland Cavaliers',
        'New York Knicks', 'Brooklyn Nets', 'Toronto Raptors', 'Detroit Pistons',
        'Orlando Magic', 'Washington Wizards', 'Sacramento Kings', 'Utah Jazz',
        'Portland Trail Blazers', 'Oklahoma City Thunder',
    ]

    for team in sorted(nba_teams, key=len, reverse=True):
        if teams_clean.startswith(team):
            remaining = teams_clean[len(team):].strip()
            if remaining:
                return (team, remaining)

    # Common multi-word CBB team names (helps with proper splitting)
    cbb_prefixes = [
        'North Carolina', 'South Carolina', 'Texas A&M', 'Texas Tech', 'Virginia Tech',
        'Georgia Tech', 'Boston College', 'Wake Forest', 'Florida State', 'Ohio State',
        'Penn State', 'Michigan State', 'Iowa State', 'Kansas State', 'Oklahoma State',
        'Mississippi State', 'Arizona State', 'Oregon State', 'Washington State', 'Colorado State',
        'San Diego State', 'Fresno State', 'Boise State', 'Utah State', 'New Mexico',
        'New Mexico State', 'Sam Houston', 'Stephen F Austin', 'Abilene Christian',
        'Central Florida', 'South Florida', 'North Texas', 'West Virginia', 'East Carolina',
        'Ball State', 'Kent State', 'Bowling Green', 'Old Dominion', 'James Madison',
        'George Mason', 'George Washington', 'Saint Louis', 'Saint Marys', "Saint John's",
        'Holy Cross', 'Boston University', 'Stony Brook', 'Long Beach State',
        'Cal State', 'San Jose State', 'Sacramento State', 'Portland State',
    ]

    for prefix in sorted(cbb_prefixes, key=len, reverse=True):
        if teams_clean.startswith(prefix):
            remaining = teams_clean[len(prefix):].strip()
            if remaining and len(remaining) >= 3:
                return (prefix, remaining)

    # Strategy 2: Look for multi-word team name patterns
    # CBB teams often have patterns like "North Carolina", "San Diego State", "South Carolina"
    # Match: CapitalWord (optional more words) followed by another CapitalWord start

    # First try to find two-word or three-word team names
    multi_word_pattern = r'^((?:[A-Z][a-zA-Z\.]+\s+){1,3}[A-Z][a-zA-Z\.]+)((?:[A-Z][a-zA-Z\.]+.*))'
    match = re.match(multi_word_pattern, teams_clean)
    if match:
        away = match.group(1).strip()
        home = match.group(2).strip()
        # Sanity check: both should have at least 3 chars
        if len(away) >= 3 and len(home) >= 3:
            return (away, home)

    # Strategy 3: Look for single word teams followed by another team
    # Match pattern: Word(s)Word(s) where second Word starts with capital
    single_pattern = r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)([A-Z][a-zA-Z].*)'
    match = re.match(single_pattern, teams_clean)
    if match:
        away = match.group(1).strip()
        home = match.group(2).strip()
        if len(away) >= 3 and len(home) >= 3:
            return (away, home)

    # Strategy 4: Find boundary where lowercase to uppercase transition happens
    for i in range(3, len(teams_clean) - 3):
        if teams_clean[i].isupper() and teams_clean[i-1].islower():
            away = teams_clean[:i].strip()
            home = teams_clean[i:].strip()
            if len(away) >= 3 and len(home) >= 3:
                return (away, home)

    # Strategy 5: Split roughly in the middle, find nearest word boundary
    mid = len(teams_clean) // 2
    for offset in range(20):
        for pos in [mid + offset, mid - offset]:
            if 3 <= pos < len(teams_clean) - 3:
                if teams_clean[pos] == ' ':
                    away = teams_clean[:pos].strip()
                    home = teams_clean[pos:].strip()
                    if len(away) >= 3 and len(home) >= 3:
                        return (away, home)

    logger.warning(f"Could not parse team names from: '{teams_text}'")
    return (None, None)


def parse_spread_line(cell_text: str) -> dict:
    """
    Parse spread line into components - handles multiple formats:
    - '-3.5-110+3.5-110' (concatenated)
    - '-3.5 -110 / +3.5 -110' (separated)
    - 'PK -110' or 'EVEN' (pick'em)
    - '-3' (no odds)
    - '-3.5' (just spread)
    """
    result = {'away_spread': None, 'away_odds': None, 'home_spread': None, 'home_odds': None}

    if not cell_text or cell_text == '--' or cell_text.strip() == '':
        return result

    text = cell_text.strip().upper()

    # Handle PK (pick'em) - spread is 0
    if 'PK' in text or text == 'EVEN':
        result['away_spread'] = 0.0
        result['home_spread'] = 0.0
        # Try to find odds
        odds_match = re.search(r'([-+]\d{3})', text)
        if odds_match:
            result['away_odds'] = int(odds_match.group(1))
            result['home_odds'] = int(odds_match.group(1))
        else:
            result['away_odds'] = -110
            result['home_odds'] = -110
        return result

    # Strategy 1: Concatenated format like '-3.5-110+3.5-110'
    pattern_concat = r'([+-]?\d+\.?\d*)([-+]\d{3})([+-]\d+\.?\d*)([-+]\d{3})'
    match = re.match(pattern_concat, text.replace(' ', ''))
    if match:
        result['away_spread'] = float(match.group(1))
        result['away_odds'] = int(match.group(2))
        result['home_spread'] = float(match.group(3))
        result['home_odds'] = int(match.group(4))
        return result

    # Strategy 2: Find all spread-like numbers in the text
    # Spreads are typically -/+ single or double digit with optional .5
    spread_matches = re.findall(r'([+-]?\d{1,2}(?:\.5)?)', text)
    odds_matches = re.findall(r'([-+]\d{3})', text)

    if spread_matches:
        # First spread value is away
        try:
            result['away_spread'] = float(spread_matches[0])
            if len(spread_matches) > 1:
                result['home_spread'] = float(spread_matches[1])
            else:
                # Mirror the spread (if away is -3.5, home is +3.5)
                result['home_spread'] = -result['away_spread']
        except ValueError:
            pass

    if odds_matches:
        try:
            result['away_odds'] = int(odds_matches[0])
            if len(odds_matches) > 1:
                result['home_odds'] = int(odds_matches[1])
            else:
                result['home_odds'] = result['away_odds']
        except ValueError:
            pass

    # Strategy 3: Just a single spread number like '-3.5' or '-3'
    if result['away_spread'] is None:
        single_spread = re.match(r'^([+-]?\d+(?:\.\d+)?)$', text.replace(' ', ''))
        if single_spread:
            result['away_spread'] = float(single_spread.group(1))
            result['home_spread'] = -result['away_spread']
            result['away_odds'] = -110
            result['home_odds'] = -110

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
            # Ensure line_movement and line_direction are always present
            if 'line_movement' not in combined[game_key]:
                combined[game_key]['line_movement'] = None
            if 'line_direction' not in combined[game_key]:
                combined[game_key]['line_direction'] = 'stable'

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
                    'line_movement': None,
                    'line_direction': 'stable',
                }

    # Try to match splits with lines using fuzzy team matching
    if splits_result['success'] and lines_result['success']:
        unmatched_splits = []
        for split_key, split_data in splits_result['data'].items():
            if split_key not in combined or combined[split_key].get('tickets_away') is None:
                unmatched_splits.append((split_key, split_data))

        for split_key, split_data in unmatched_splits:
            split_away = split_data.get('away_team', '').lower()
            split_home = split_data.get('home_team', '').lower()

            for line_key in combined:
                line_data = combined[line_key]
                line_away = line_data.get('away_team', '').lower()
                line_home = line_data.get('home_team', '').lower()

                # Fuzzy match: check if team names overlap
                if ((split_away in line_away or line_away in split_away) and
                    (split_home in line_home or line_home in split_home)):
                    if combined[line_key].get('tickets_away') is None:
                        combined[line_key].update({
                            'tickets_away': split_data.get('away_bets_pct'),
                            'tickets_home': split_data.get('home_bets_pct'),
                            'money_away': split_data.get('away_handle_pct'),
                            'money_home': split_data.get('home_handle_pct'),
                            'away_sharp': split_data.get('away_sharp'),
                            'home_sharp': split_data.get('home_sharp'),
                        })
                        logger.debug(f"Fuzzy matched splits: {split_key} -> {line_key}")
                        break

    logger.info(f"VSIN combined data for {sport}: {len(combined)} games (lines: {lines_result.get('count', 0)}, splits: {splits_result.get('count', 0)})")

    return {
        'success': True,
        'data': combined,
        'count': len(combined),
        'splits_count': splits_result.get('count', 0),
        'lines_count': lines_result.get('count', 0)
    }


def test_vsin_connection():
    """Test if VSIN cookies are valid"""
    print("=" * 60)
    print("Testing VSIN Connection")
    print("=" * 60)

    for sport in ['NBA', 'CBB']:
        print(f"\n--- Testing {sport} ---")
        result = get_all_vsin_data(sport)
        if result['success']:
            print(f"VSIN {sport}: {result['count']} games (lines: {result['lines_count']}, splits: {result['splits_count']})")
            for k, v in list(result['data'].items())[:5]:
                print(f"\n  Game: {k}")
                print(f"    Away: {v.get('away_team')} | Home: {v.get('home_team')}")
                open_spread = v.get('open_away_spread')
                current_spread = v.get('current_away_spread')
                print(f"    Open: {open_spread} | Current: {current_spread}")
                print(f"    Line Movement: {v.get('line_movement')} | Direction: {v.get('line_direction')}")
                print(f"    Tickets: {v.get('tickets_away')}%-{v.get('tickets_home')}% | Money: {v.get('money_away')}%-{v.get('money_home')}%")
        else:
            print(f"VSIN {sport} failed: {result.get('message')}")

    return True


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')
    test_vsin_connection()
