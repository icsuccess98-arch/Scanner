"""
WagerTalk.com Scraper - Betting Action Data

Extracts game matchups from WagerTalk.com/odds using BeautifulSoup.
Note: WagerTalk loads betting percentages (Tickets/Money) via protected JavaScript.
This scraper provides game matchups with default percentages.
"""

import logging
import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class WagerTalkScraper:
    """
    Fast scraper for WagerTalk.com game data using BeautifulSoup.
    Parses team matchups directly from server-rendered HTML.
    """
    
    BASE_URL = "https://www.wagertalk.com/odds"
    CACHE_TTL = 120  # 2 minute cache
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
    
    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache:
            return False
        age = time.time() - self._cache_time.get(key, 0)
        return age < self.CACHE_TTL
    
    def _normalize_team_name(self, name: str) -> str:
        if not name:
            return ''
        return re.sub(r'\s+', ' ', name.strip())
    
    def fetch_betting_data(self, league: str = 'NBA') -> Dict[str, Dict]:
        """
        Fetch game matchups from WagerTalk.
        Uses fast BeautifulSoup parsing (~2 seconds).
        """
        cache_key = f"wagertalk_{league}_{datetime.now().strftime('%Y%m%d_%H')}"
        
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
            url = f"{self.BASE_URL}?sport={sport}"
            
            logger.info(f"Fetching WagerTalk data for {league}: {url}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find game rows
            rows = soup.find_all('tr', class_=['reg', 'alt'])
            logger.info(f"WagerTalk found {len(rows)} game rows")
            
            league_teams = self._get_league_teams(league)
            games_found = 0
            
            for row in rows:
                try:
                    # Get team cell
                    team_th = row.find('th', class_='team')
                    if not team_th:
                        continue
                    
                    # Parse team names from divs
                    team_divs = team_th.find_all('div')
                    if len(team_divs) < 2:
                        continue
                    
                    away_team = self._normalize_team_name(team_divs[0].text)
                    home_team = self._normalize_team_name(team_divs[1].text)
                    
                    if not away_team or not home_team:
                        continue
                    
                    # Filter by league teams
                    if league_teams:
                        is_league_game = any(
                            t.lower() in away_team.lower() or t.lower() in home_team.lower()
                            for t in league_teams
                        )
                        if not is_league_game:
                            continue
                    
                    # Default betting percentages
                    # Note: WagerTalk loads actual percentages via protected JavaScript
                    away_bet_pct = 50
                    home_bet_pct = 50
                    away_money_pct = 50
                    home_money_pct = 50
                    
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
                        'sharp_detected': False,
                        'sharp_side': None,
                        'source': 'wagertalk'
                    }
                    games_found += 1
                    
                except Exception as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
            
            logger.info(f"WagerTalk scraped {games_found} {league} games")
            
            self._cache[cache_key] = result
            self._cache_time[cache_key] = time.time()
            
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
    
    def get_game_betting(self, away_team: str, home_team: str, league: str = 'NBA') -> Dict:
        """Get betting data for a specific game."""
        all_data = self.fetch_betting_data(league)
        
        away_clean = self._normalize_team_name(away_team).lower()
        home_clean = self._normalize_team_name(home_team).lower()
        
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


# Global scraper instance
wagertalk_scraper = WagerTalkScraper()


def get_wagertalk_betting_data(away_team: str, home_team: str, league: str = 'NBA') -> Dict:
    """Convenience function to get betting data for a specific game."""
    return wagertalk_scraper.get_game_betting(away_team, home_team, league)


def get_all_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """Convenience function to get all betting data for a league."""
    return wagertalk_scraper.fetch_betting_data(league)
