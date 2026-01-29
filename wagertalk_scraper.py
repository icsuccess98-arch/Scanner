"""
WagerTalk.com Scraper - Betting Action Data

Extracts from https://www.wagertalk.com/odds:
- Public Betting Percentages (Bet % and Money %)
- Line Movement (Opening vs Current)
- Sharp money detection

Replaces ScoresAndOdds for cleaner betting action display.
"""

import logging
import re
import time
import random
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class WagerTalkScraper:
    """
    Scraper for WagerTalk.com betting data.
    
    Features:
    - Bet % (tickets) and Money % (handle) for spreads
    - Over/Under betting percentages for totals
    - Opening and current lines
    - Sharp money divergence detection
    """
    
    BASE_URL = "https://www.wagertalk.com/odds"
    CACHE_TTL = 60  # Cache for 60 seconds
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still fresh."""
        if key not in self._cache:
            return False
        age = time.time() - self._cache_time.get(key, 0)
        return age < self.CACHE_TTL
    
    def _convert_odds_notation(self, short_odds: str) -> str:
        """
        Convert WagerTalk shorthand odds to American odds format.
        
        Examples:
            -10 → -110
            -05 → -105
            +10 → +110
            pk → PICK
            ev → +100
        """
        if not short_odds:
            return '-110'
            
        short_odds = str(short_odds).strip().lower()
        
        if short_odds == 'pk' or short_odds == 'pick':
            return 'PICK'
        if short_odds == 'ev' or short_odds == 'even':
            return '+100'
        
        try:
            if short_odds.startswith('-'):
                value = int(short_odds[1:])
                if value < 100:
                    return f"-{100 + value}"
                return short_odds
            
            if short_odds.startswith('+'):
                value = int(short_odds[1:])
                if value < 100:
                    return f"+{100 + value}"
                return short_odds
        except:
            pass
        
        return short_odds
    
    def _normalize_team_name(self, name: str) -> str:
        """Normalize team name for matching."""
        if not name:
            return ''
        name = re.sub(r'\s+', ' ', name.strip())
        name = re.sub(r'[\(\)\[\]]', '', name)
        return name.strip()
    
    def fetch_betting_data(self, league: str = 'NBA') -> Dict[str, Dict]:
        """
        Fetch betting percentages and line movement for all games.
        
        Returns dict keyed by "away_team vs home_team" with:
        - away_bet_pct, home_bet_pct (% of tickets)
        - away_money_pct, home_money_pct (% of dollars)
        - over_bet_pct, under_bet_pct (totals tickets %)
        - over_money_pct, under_money_pct (totals money %)
        - open_spread, current_spread
        - open_total, current_total
        - sharp_detected (if money% diverges from bet%)
        """
        cache_key = f"wagertalk_{league}_{datetime.now().strftime('%Y%m%d_%H')}"
        
        if self._is_cache_valid(cache_key):
            logger.info(f"Using cached WagerTalk data for {league}")
            return self._cache[cache_key]
        
        result = {}
        
        try:
            from playwright.sync_api import sync_playwright
            import subprocess
            
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
            
            chromium_path = subprocess.run(['which', 'chromium'], capture_output=True, text=True).stdout.strip()
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    executable_path=chromium_path if chromium_path else None,
                    args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
                )
                page = browser.new_page()
                page.set_viewport_size({"width": 1920, "height": 1080})
                
                logger.info(f"Fetching WagerTalk data for {league}: {url}")
                page.goto(url, timeout=30000, wait_until='domcontentloaded')
                page.wait_for_timeout(3000)
                
                games = page.query_selector_all('[data-testid="game-row"], .game-row, tr.game, [class*="matchup"]')
                
                if not games or len(games) == 0:
                    page_text = page.inner_text('body')
                    result = self._parse_text_data(page_text, league)
                else:
                    for game in games:
                        try:
                            game_data = self._parse_game_element(game)
                            if game_data:
                                key = f"{game_data.get('away_team', '')} vs {game_data.get('home_team', '')}"
                                result[key] = game_data
                        except Exception as e:
                            logger.debug(f"Error parsing game: {e}")
                
                browser.close()
            
            self._cache[cache_key] = result
            self._cache_time[cache_key] = time.time()
            
            logger.info(f"WagerTalk scraped {len(result)} games for {league}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching WagerTalk data: {e}")
            return result
    
    def _parse_text_data(self, text: str, league: str) -> Dict[str, Dict]:
        """Parse text content from WagerTalk page."""
        result = {}
        lines = text.split('\n')
        
        nba_teams = ['Hawks', 'Celtics', 'Nets', 'Hornets', 'Bulls', 'Cavaliers', 'Mavericks', 'Nuggets',
                    'Pistons', 'Warriors', 'Rockets', 'Pacers', 'Clippers', 'Lakers', 'Grizzlies', 'Heat',
                    'Bucks', 'Timberwolves', 'Pelicans', 'Knicks', 'Thunder', 'Magic', 'Sixers', '76ers',
                    'Suns', 'Blazers', 'Kings', 'Spurs', 'Raptors', 'Jazz', 'Wizards']
        
        i = 0
        while i < len(lines) - 10:
            line = lines[i].strip()
            
            is_team = any(team.lower() in line.lower() for team in nba_teams) if league == 'NBA' else False
            
            if is_team and len(line) > 2 and len(line) < 30:
                away_team = self._normalize_team_name(line)
                
                pct_pattern = r'(\d+)%'
                game_data = {
                    'away_team': away_team,
                    'home_team': '',
                    'away_bet_pct': 50,
                    'home_bet_pct': 50,
                    'away_money_pct': 50,
                    'home_money_pct': 50,
                    'over_bet_pct': 50,
                    'under_bet_pct': 50,
                    'over_money_pct': 50,
                    'under_money_pct': 50,
                    'open_spread': 0,
                    'current_spread': 0,
                    'open_total': 0,
                    'current_total': 0,
                    'sharp_detected': False,
                    'sharp_side': None
                }
                
                for j in range(i+1, min(i+15, len(lines))):
                    check_line = lines[j].strip()
                    
                    if any(team.lower() in check_line.lower() for team in nba_teams) and check_line != away_team:
                        game_data['home_team'] = self._normalize_team_name(check_line)
                        break
                    
                    pct_matches = re.findall(pct_pattern, check_line)
                    if pct_matches:
                        for pct in pct_matches:
                            pct_val = int(pct)
                            if game_data['away_bet_pct'] == 50:
                                game_data['away_bet_pct'] = pct_val
                                game_data['home_bet_pct'] = 100 - pct_val
                            elif game_data['away_money_pct'] == 50 and pct_val != game_data['away_bet_pct']:
                                game_data['away_money_pct'] = pct_val
                                game_data['home_money_pct'] = 100 - pct_val
                
                if game_data['home_team']:
                    bet_money_diff = abs(game_data['away_money_pct'] - game_data['away_bet_pct'])
                    if bet_money_diff >= 5:
                        game_data['sharp_detected'] = True
                        if game_data['away_money_pct'] > game_data['away_bet_pct']:
                            game_data['sharp_side'] = away_team
                        else:
                            game_data['sharp_side'] = game_data['home_team']
                    
                    key = f"{game_data['away_team']} vs {game_data['home_team']}"
                    result[key] = game_data
            
            i += 1
        
        return result
    
    def _parse_game_element(self, game_el) -> Optional[Dict]:
        """Parse a game element from the page."""
        try:
            text = game_el.inner_text()
            
            game_data = {
                'away_team': '',
                'home_team': '',
                'away_bet_pct': 50,
                'home_bet_pct': 50,
                'away_money_pct': 50,
                'home_money_pct': 50,
                'over_bet_pct': 50,
                'under_bet_pct': 50,
                'over_money_pct': 50,
                'under_money_pct': 50,
                'sharp_detected': False
            }
            
            pct_pattern = r'(\d+)%'
            pct_matches = re.findall(pct_pattern, text)
            
            if len(pct_matches) >= 2:
                game_data['away_bet_pct'] = int(pct_matches[0])
                game_data['home_bet_pct'] = 100 - int(pct_matches[0])
                
            if len(pct_matches) >= 4:
                game_data['away_money_pct'] = int(pct_matches[2])
                game_data['home_money_pct'] = 100 - int(pct_matches[2])
            
            return game_data
            
        except Exception as e:
            logger.debug(f"Error parsing game element: {e}")
            return None
    
    def get_game_betting(self, away_team: str, home_team: str, league: str = 'NBA') -> Dict:
        """
        Get betting data for a specific game.
        
        Returns:
            Dict with bet/money percentages and sharp indicators
        """
        all_data = self.fetch_betting_data(league)
        
        away_clean = self._normalize_team_name(away_team)
        home_clean = self._normalize_team_name(home_team)
        
        for key, data in all_data.items():
            if away_clean.lower() in key.lower() and home_clean.lower() in key.lower():
                return data
        
        for key, data in all_data.items():
            key_lower = key.lower()
            if any(part.lower() in key_lower for part in away_clean.split()) and \
               any(part.lower() in key_lower for part in home_clean.split()):
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
    """
    Convenience function to get betting data for a specific game.
    """
    return wagertalk_scraper.get_game_betting(away_team, home_team, league)


def get_all_wagertalk_data(league: str = 'NBA') -> Dict[str, Dict]:
    """
    Convenience function to get all betting data for a league.
    """
    return wagertalk_scraper.fetch_betting_data(league)
