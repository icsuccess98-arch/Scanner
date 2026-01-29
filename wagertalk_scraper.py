"""
WagerTalk.com Scraper - Betting Action Data

Extracts team matchups from WagerTalk.com/odds.
Note: Betting percentages (Tickets/Money) load via JavaScript API calls.
This scraper provides team matchups with sensible default percentages.
"""

import logging
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class WagerTalkScraper:
    """
    Fast scraper for WagerTalk.com betting data using BeautifulSoup.
    Parses team matchups directly from HTML (no JavaScript needed).
    """
    
    BASE_URL = "https://www.wagertalk.com/odds"
    CACHE_TTL = 30  # 30 seconds cache for fast updates
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
        })
    
    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache:
            return False
        age = time.time() - self._cache_time.get(key, 0)
        return age < self.CACHE_TTL
    
    def _normalize_team_name(self, name: str) -> str:
        if not name:
            return ''
        name = re.sub(r'\s+', ' ', name.strip())
        return name
    
    def fetch_betting_data(self, league: str = 'NBA') -> Dict[str, Dict]:
        """
        Fetch game matchups and betting data.
        Uses fast BeautifulSoup parsing (no Playwright needed).
        """
        cache_key = f"wagertalk_{league}_{datetime.now().strftime('%Y%m%d_%H%M')}"
        
        if self._is_cache_valid(cache_key):
            logger.debug(f"Using cached WagerTalk data for {league}")
            return self._cache[cache_key]
        
        result = {}
        
        try:
            sport_map = {
                'NBA': 'nba',
                'CBB': 'ncaab',
                'NFL': 'nfl',
                'CFB': 'ncaaf',
                'NHL': 'nhl'
            }
            
            sport = sport_map.get(league, 'nba')
            cb = random.random()
            url = f"{self.BASE_URL}?sport={sport}&cb={cb}"
            
            logger.info(f"Fetching WagerTalk data for {league}: {url}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            game_rows = soup.find_all('tr', class_=['reg', 'alt'])
            logger.info(f"WagerTalk found {len(game_rows)} game rows")
            
            league_teams = self._get_league_teams(league)
            games_found = 0
            
            for row in game_rows:
                try:
                    team_cell = row.find('th', class_='team')
                    if not team_cell:
                        continue
                    
                    team_divs = team_cell.find_all('div')
                    if len(team_divs) < 2:
                        continue
                    
                    away_team = self._normalize_team_name(team_divs[0].text)
                    home_team = self._normalize_team_name(team_divs[1].text)
                    
                    if not away_team or not home_team:
                        continue
                    
                    is_league_game = any(
                        team.lower() in away_team.lower() or team.lower() in home_team.lower()
                        for team in league_teams
                    )
                    
                    if league_teams and not is_league_game:
                        continue
                    
                    away_bet_pct = 50
                    home_bet_pct = 50
                    away_money_pct = 50
                    home_money_pct = 50
                    
                    book_cells = row.find_all('td', class_=re.compile(r'book'))
                    
                    if len(book_cells) >= 2:
                        tickets_cell = book_cells[0]
                        money_cell = book_cells[1]
                        
                        tickets_divs = tickets_cell.find_all('div')
                        if len(tickets_divs) >= 2:
                            away_text = tickets_divs[0].text.strip()
                            home_text = tickets_divs[1].text.strip()
                            
                            away_match = re.search(r'(\d+)', away_text)
                            home_match = re.search(r'(\d+)', home_text)
                            
                            if away_match:
                                away_bet_pct = int(away_match.group(1))
                                home_bet_pct = 100 - away_bet_pct
                            elif home_match:
                                home_bet_pct = int(home_match.group(1))
                                away_bet_pct = 100 - home_bet_pct
                        
                        money_divs = money_cell.find_all('div')
                        if len(money_divs) >= 2:
                            away_text = money_divs[0].text.strip()
                            home_text = money_divs[1].text.strip()
                            
                            away_match = re.search(r'(\d+)', away_text)
                            home_match = re.search(r'(\d+)', home_text)
                            
                            if away_match:
                                away_money_pct = int(away_match.group(1))
                                home_money_pct = 100 - away_money_pct
                            elif home_match:
                                home_money_pct = int(home_match.group(1))
                                away_money_pct = 100 - home_money_pct
                    
                    sharp_detected = abs(away_money_pct - away_bet_pct) >= 5
                    sharp_side = None
                    if sharp_detected:
                        sharp_side = away_team if away_money_pct > away_bet_pct else home_team
                    
                    key = f"{away_team} vs {home_team}"
                    result[key] = {
                        'away_team': away_team,
                        'home_team': home_team,
                        'away_bet_pct': away_bet_pct,
                        'home_bet_pct': home_bet_pct,
                        'away_money_pct': away_money_pct,
                        'home_money_pct': home_money_pct,
                        'over_bet_pct': 50,
                        'under_bet_pct': 50,
                        'over_money_pct': 50,
                        'under_money_pct': 50,
                        'open_spread': 0,
                        'current_spread': 0,
                        'sharp_detected': sharp_detected,
                        'sharp_side': sharp_side
                    }
                    games_found += 1
                    
                except Exception as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
            
            self._cache[cache_key] = result
            self._cache_time[cache_key] = time.time()
            
            logger.info(f"WagerTalk scraped {games_found} {league} games")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching WagerTalk data: {e}")
            return result
    
    def _get_league_teams(self, league: str) -> list:
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
            return []  # Allow all for CBB
        elif league == 'NFL':
            return ['Chiefs', 'Eagles', 'Bills', 'Cowboys', 'Ravens', 'Bengals',
                    '49ers', 'Lions', 'Dolphins', 'Jets', 'Steelers', 'Chargers',
                    'Broncos', 'Raiders', 'Seahawks', 'Vikings', 'Packers', 'Bears',
                    'Saints', 'Falcons', 'Panthers', 'Buccaneers', 'Commanders',
                    'Giants', 'Cardinals', 'Rams', 'Browns', 'Colts', 'Texans',
                    'Titans', 'Jaguars', 'Patriots']
        return []
    
    def get_game_betting(self, away_team: str, home_team: str, league: str = 'NBA') -> Dict:
        """Get betting data for a specific game."""
        all_data = self.fetch_betting_data(league)
        
        away_clean = self._normalize_team_name(away_team).lower()
        home_clean = self._normalize_team_name(home_team).lower()
        
        for key, data in all_data.items():
            key_lower = key.lower()
            data_away = data.get('away_team', '').lower()
            data_home = data.get('home_team', '').lower()
            
            if (away_clean in data_away or data_away in away_clean) and \
               (home_clean in data_home or data_home in home_clean):
                return data
        
        for key, data in all_data.items():
            key_lower = key.lower()
            away_parts = away_clean.split()
            home_parts = home_clean.split()
            
            if any(p in key_lower for p in away_parts if len(p) > 3) and \
               any(p in key_lower for p in home_parts if len(p) > 3):
                return data
        
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
            'sharp_side': None
        }


wagertalk_scraper = WagerTalkScraper()


def get_wagertalk_betting_data(away_team: str, home_team: str, league: str = 'NBA') -> Dict:
    """Convenience function to get betting data for a specific game."""
    return wagertalk_scraper.get_game_betting(away_team, home_team, league)


def get_all_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """Convenience function to get all betting data for a league."""
    return wagertalk_scraper.fetch_betting_data(league)
