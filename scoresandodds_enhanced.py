"""
ENHANCED SCORESANDODDS.COM SCRAPER
===================================

Extracts complete betting trends including:
- Opening lines (preserved)
- Current lines (live)
- Line movement
- Bets % (public betting - tickets)
- Money % (sharp money - dollars)
- Closing lines

This scraper uses Playwright for JavaScript-rendered content.
"""

import re
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import subprocess

logger = logging.getLogger(__name__)


class ScoresAndOddsEnhancedScraper:
    """
    Bulletproof scraper for ScoresAndOdds.com with betting trends.
    
    Key Features:
    - Extracts Bets % (tickets - public action)
    - Extracts Money % (dollars - sharp action)
    - Opening vs Current spread comparison
    - Line movement tracking
    - Closing line capture
    """
    
    BASE_URL = "https://www.scoresandodds.com"
    CACHE_TTL = 30  # 30 seconds for live data
    
    def __init__(self):
        self._cache = {}
        self._cache_time = {}
        self._browser_path = self._find_chromium()
    
    def _find_chromium(self) -> Optional[str]:
        """Find Chromium executable path."""
        try:
            result = subprocess.run(
                ['which', 'chromium'],
                capture_output=True,
                text=True
            )
            path = result.stdout.strip()
            return path if path else None
        except:
            return None
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still fresh."""
        if key not in self._cache:
            return False
        age = time.time() - self._cache_time.get(key, 0)
        return age < self.CACHE_TTL
    
    def get_betting_trends(self, league: str = 'NBA') -> List[Dict]:
        """
        Get complete betting trends for all games.
        
        Args:
            league: 'NBA' or 'CBB' (NCAA Basketball)
            
        Returns:
            List of dictionaries with:
            {
                'away_team': str,
                'home_team': str,
                'game_time': str,
                
                # Spread data
                'opening_spread': float,
                'current_spread': float,
                'spread_movement': float,
                
                # Total data
                'opening_total': float,
                'current_total': float,
                'total_movement': float,
                
                # Betting trends (THE KEY DATA)
                'spread_bets_away_pct': int,  # Tickets on away spread
                'spread_bets_home_pct': int,  # Tickets on home spread
                'spread_money_away_pct': int, # Dollars on away spread
                'spread_money_home_pct': int, # Dollars on home spread
                
                'total_bets_over_pct': int,   # Tickets on over
                'total_bets_under_pct': int,  # Tickets on under
                'total_money_over_pct': int,  # Dollars on over
                'total_money_under_pct': int, # Dollars on under
                
                # Sharp indicators
                'spread_sharp_side': str,     # 'Away'/'Home'/'Balanced'
                'total_sharp_side': str,      # 'Over'/'Under'/'Balanced'
                'reverse_line_movement': bool # RLM detected
            }
        """
        cache_key = f"trends_{league}"
        if self._is_cache_valid(cache_key):
            logger.info(f"Using cached betting trends for {league}")
            return self._cache[cache_key]
        
        try:
            url = f"{self.BASE_URL}/nba" if league == 'NBA' else f"{self.BASE_URL}/ncaab"
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    executable_path=self._browser_path,
                    args=[
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-blink-features=AutomationControlled'
                    ]
                )
                
                page = browser.new_page()
                
                # Set viewport and user agent
                page.set_viewport_size({"width": 1920, "height": 1080})
                page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                # Navigate and wait for content
                logger.info(f"Loading {url}...")
                page.goto(url, timeout=45000, wait_until='networkidle')
                
                # Wait for betting trends to load
                try:
                    page.wait_for_selector('[class*="percentage"], [class*="percent"]', timeout=5000)
                except PlaywrightTimeout:
                    logger.warning("Betting percentages not loaded, continuing anyway")
                
                # Extract game data
                games = self._extract_games_from_page(page, league)
                
                browser.close()
                
                # Cache results
                self._cache[cache_key] = games
                self._cache_time[cache_key] = time.time()
                
                logger.info(f"Extracted {len(games)} games with betting trends from ScoresAndOdds")
                return games
                
        except Exception as e:
            logger.error(f"Error fetching ScoresAndOdds betting trends: {e}")
            return []
    
    def _extract_games_from_page(self, page, league: str) -> List[Dict]:
        """Extract all game data from the page."""
        games = []
        
        try:
            # Get page content
            content = page.content()
            text = page.inner_text('body')
            lines = text.split('\n')
            
            # Team list for NBA
            nba_teams = [
                'Hawks', 'Celtics', 'Nets', 'Hornets', 'Bulls', 'Cavaliers',
                'Mavericks', 'Nuggets', 'Pistons', 'Warriors', 'Rockets',
                'Pacers', 'Clippers', 'Lakers', 'Grizzlies', 'Heat', 'Bucks',
                'Timberwolves', 'Pelicans', 'Knicks', 'Thunder', 'Magic',
                'Sixers', '76ers', 'Suns', 'Blazers', 'Kings', 'Spurs',
                'Raptors', 'Jazz', 'Wizards'
            ]
            
            i = 0
            while i < len(lines) - 30:
                line = lines[i].strip()
                
                # Check if this line is a team name
                is_team = False
                if league == 'NBA':
                    is_team = any(team in line for team in nba_teams)
                else:
                    # CBB: Capitalize words pattern
                    is_team = len(line.split()) <= 3 and line and line[0].isupper()
                
                if is_team and len(line) > 2 and len(line) < 30:
                    away_team = line
                    
                    # Find home team (next team name in sequence)
                    home_team = None
                    home_idx = None
                    
                    for j in range(i + 1, min(i + 50, len(lines))):
                        next_line = lines[j].strip()
                        
                        is_home = False
                        if league == 'NBA':
                            is_home = any(team in next_line for team in nba_teams)
                        else:
                            is_home = len(next_line.split()) <= 3 and next_line and next_line[0].isupper()
                        
                        if is_home and next_line != away_team and len(next_line) > 2:
                            home_team = next_line
                            home_idx = j
                            break
                    
                    if home_team:
                        # Extract game data between away and home team lines
                        game = self._parse_game_data(
                            lines[i:home_idx + 50],
                            away_team,
                            home_team
                        )
                        
                        if game:
                            games.append(game)
                        
                        i = home_idx
                
                i += 1
            
        except Exception as e:
            logger.error(f"Error extracting games: {e}")
        
        return games
    
    def _parse_game_data(self, game_lines: List[str], away_team: str, home_team: str) -> Optional[Dict]:
        """
        Parse complete game data including betting trends.
        
        ScoresAndOdds typically displays data in this order:
        - Team names
        - Spreads (opening, current)
        - Totals (opening, current)
        - Betting percentages (Bets %, Money %)
        """
        game = {
            'away_team': away_team,
            'home_team': home_team,
        }
        
        # Extract spreads
        spreads = []
        for line in game_lines[:20]:  # Check first 20 lines for spreads
            match = re.search(r'([+-]\d+\.?5?)', line)
            if match:
                try:
                    spread = float(match.group(1))
                    if -30 <= spread <= 30:  # Valid spread range
                        spreads.append(spread)
                except:
                    continue
        
        if len(spreads) >= 2:
            game['opening_spread'] = spreads[0]
            game['current_spread'] = spreads[-1]
            game['spread_movement'] = spreads[-1] - spreads[0]
        elif len(spreads) == 1:
            game['current_spread'] = spreads[0]
        
        # Extract totals
        totals = []
        for line in game_lines[:20]:
            # Look for O/U or total patterns
            if 'o/u' in line.lower() or 'total' in line.lower():
                match = re.search(r'(\d{2,3}\.?5?)', line)
                if match:
                    try:
                        total = float(match.group(1))
                        if 150 <= total <= 300:  # Valid total range
                            totals.append(total)
                    except:
                        continue
        
        if len(totals) >= 2:
            game['opening_total'] = totals[0]
            game['current_total'] = totals[-1]
            game['total_movement'] = totals[-1] - totals[0]
        elif len(totals) == 1:
            game['current_total'] = totals[0]
        
        # Extract betting percentages (KEY DATA)
        percentages = []
        for line in game_lines:
            # Look for percentage patterns
            pct_matches = re.findall(r'(\d{1,3})%', line)
            for match in pct_matches:
                try:
                    pct = int(match)
                    if 0 <= pct <= 100:
                        percentages.append(pct)
                except:
                    continue
        
        # ScoresAndOdds typically shows percentages in this order:
        # Spread: Bets Away%, Bets Home%, Money Away%, Money Home%
        # Total: Bets Over%, Bets Under%, Money Over%, Money Under%
        
        if len(percentages) >= 4:
            # Spread betting trends
            game['spread_bets_away_pct'] = percentages[0]
            game['spread_bets_home_pct'] = percentages[1]
            game['spread_money_away_pct'] = percentages[2]
            game['spread_money_home_pct'] = percentages[3]
            
            # Calculate sharp indicators for spread
            bets_diff = game['spread_bets_away_pct'] - game['spread_bets_home_pct']
            money_diff = game['spread_money_away_pct'] - game['spread_money_home_pct']
            
            # Sharp money detection (15% threshold)
            if money_diff > 15:
                game['spread_sharp_side'] = 'Away'
            elif money_diff < -15:
                game['spread_sharp_side'] = 'Home'
            else:
                game['spread_sharp_side'] = 'Balanced'
            
            # Reverse Line Movement detection
            # Sharp money on one side, line moves opposite direction
            game['reverse_line_movement'] = False
            if game.get('spread_movement'):
                movement = game['spread_movement']
                if game['spread_sharp_side'] == 'Away' and movement > 0:
                    game['reverse_line_movement'] = True
                elif game['spread_sharp_side'] == 'Home' and movement < 0:
                    game['reverse_line_movement'] = True
        
        if len(percentages) >= 8:
            # Total betting trends
            game['total_bets_over_pct'] = percentages[4]
            game['total_bets_under_pct'] = percentages[5]
            game['total_money_over_pct'] = percentages[6]
            game['total_money_under_pct'] = percentages[7]
            
            # Calculate sharp indicators for total
            money_diff = game['total_money_over_pct'] - game['total_money_under_pct']
            
            if money_diff > 15:
                game['total_sharp_side'] = 'Over'
            elif money_diff < -15:
                game['total_sharp_side'] = 'Under'
            else:
                game['total_sharp_side'] = 'Balanced'
        
        # Extract game time if available
        for line in game_lines[:10]:
            time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', line, re.IGNORECASE)
            if time_match:
                game['game_time'] = time_match.group(1)
                break
        
        return game if game.get('current_spread') or game.get('current_total') else None
    
    def get_closing_lines(self, league: str = 'NBA', date: Optional[str] = None) -> List[Dict]:
        """
        Get closing lines for completed games.
        
        Args:
            league: 'NBA' or 'CBB'
            date: Date in format 'YYYY-MM-DD' (default: yesterday)
            
        Returns:
            List of games with closing lines and final betting percentages
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        cache_key = f"closing_{league}_{date}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        try:
            url = f"{self.BASE_URL}/nba/scores/{date}" if league == 'NBA' else f"{self.BASE_URL}/ncaab/scores/{date}"
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    executable_path=self._browser_path,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                
                page = browser.new_page()
                page.goto(url, timeout=45000, wait_until='networkidle')
                
                games = self._extract_games_from_page(page, league)
                
                # Mark as closing lines
                for game in games:
                    game['date'] = date
                    game['closing_spread'] = game.get('current_spread')
                    game['closing_total'] = game.get('current_total')
                
                browser.close()
                
                # Cache results
                self._cache[cache_key] = games
                self._cache_time[cache_key] = time.time()
                
                logger.info(f"Fetched {len(games)} closing lines for {league} on {date}")
                return games
                
        except Exception as e:
            logger.error(f"Error fetching closing lines: {e}")
            return []
    
    def get_consensus_page(self, league: str = 'NBA') -> List[Dict]:
        """
        Get data from the consensus picks page (alternative source for betting trends).
        
        This page often has cleaner betting percentage data.
        """
        cache_key = f"consensus_{league}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        try:
            url = f"{self.BASE_URL}/nba/consensus-picks" if league == 'NBA' else f"{self.BASE_URL}/ncaab/consensus-picks"
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    executable_path=self._browser_path,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                
                page = browser.new_page()
                page.goto(url, timeout=45000, wait_until='networkidle')
                
                # Wait for consensus data
                page.wait_for_timeout(3000)
                
                text = page.inner_text('body')
                lines = text.split('\n')
                
                games = []
                
                # Parse consensus page (cleaner format)
                i = 0
                while i < len(lines) - 10:
                    line = lines[i].strip()
                    
                    # Team detection
                    if len(line) > 2 and len(line) < 25:
                        # Look for next team
                        for j in range(i + 1, min(i + 15, len(lines))):
                            next_line = lines[j].strip()
                            
                            if len(next_line) > 2 and len(next_line) < 25 and next_line != line:
                                # Found matchup
                                game = {
                                    'away_team': line,
                                    'home_team': next_line
                                }
                                
                                # Look for percentages nearby
                                pcts = []
                                for k in range(j, min(j + 15, len(lines))):
                                    pct_match = re.match(r'^(\d{1,3})%$', lines[k].strip())
                                    if pct_match:
                                        pcts.append(int(pct_match.group(1)))
                                    
                                    if len(pcts) >= 4:
                                        break
                                
                                if len(pcts) >= 4:
                                    # Assign betting percentages
                                    game['spread_bets_away_pct'] = pcts[0]
                                    game['spread_money_away_pct'] = pcts[1]
                                    game['spread_bets_home_pct'] = pcts[2]
                                    game['spread_money_home_pct'] = pcts[3]
                                    
                                    # Calculate sharp side
                                    money_diff = pcts[1] - pcts[3]
                                    if money_diff > 15:
                                        game['spread_sharp_side'] = 'Away'
                                    elif money_diff < -15:
                                        game['spread_sharp_side'] = 'Home'
                                    else:
                                        game['spread_sharp_side'] = 'Balanced'
                                    
                                    games.append(game)
                                
                                i = j
                                break
                    
                    i += 1
                
                browser.close()
                
                # Cache results
                self._cache[cache_key] = games
                self._cache_time[cache_key] = time.time()
                
                logger.info(f"Fetched {len(games)} games from consensus page")
                return games
                
        except Exception as e:
            logger.error(f"Error fetching consensus page: {e}")
            return []


# ============================================================
# EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    scraper = ScoresAndOddsEnhancedScraper()
    
    # Get NBA betting trends
    print("\n" + "="*100)
    print("NBA BETTING TRENDS (ScoresAndOdds.com)")
    print("="*100)
    
    nba_games = scraper.get_betting_trends('NBA')
    
    for game in nba_games[:3]:  # Show first 3 games
        print(f"\n{game.get('away_team')} @ {game.get('home_team')}")
        print(f"  Game Time: {game.get('game_time', 'TBD')}")
        
        print(f"\n  SPREAD:")
        print(f"    Opening: {game.get('opening_spread', 'N/A')}")
        print(f"    Current: {game.get('current_spread', 'N/A')}")
        print(f"    Movement: {game.get('spread_movement', 'N/A')}")
        
        print(f"\n  SPREAD BETTING TRENDS:")
        print(f"    Bets:  Away {game.get('spread_bets_away_pct', 'N/A')}% | Home {game.get('spread_bets_home_pct', 'N/A')}%")
        print(f"    Money: Away {game.get('spread_money_away_pct', 'N/A')}% | Home {game.get('spread_money_home_pct', 'N/A')}%")
        print(f"    Sharp Side: {game.get('spread_sharp_side', 'N/A')}")
        
        if game.get('reverse_line_movement'):
            print(f"    ⚠️ REVERSE LINE MOVEMENT DETECTED!")
        
        if game.get('total_bets_over_pct'):
            print(f"\n  TOTAL:")
            print(f"    Opening: {game.get('opening_total', 'N/A')}")
            print(f"    Current: {game.get('current_total', 'N/A')}")
            print(f"\n  TOTAL BETTING TRENDS:")
            print(f"    Bets:  Over {game.get('total_bets_over_pct', 'N/A')}% | Under {game.get('total_bets_under_pct', 'N/A')}%")
            print(f"    Money: Over {game.get('total_money_over_pct', 'N/A')}% | Under {game.get('total_money_under_pct', 'N/A')}%")
            print(f"    Sharp Side: {game.get('total_sharp_side', 'N/A')}")
        
        print("\n" + "-"*100)
    
    # Try consensus page as alternative
    print("\n" + "="*100)
    print("CONSENSUS PAGE DATA (Alternative Source)")
    print("="*100)
    
    consensus_games = scraper.get_consensus_page('NBA')
    
    for game in consensus_games[:2]:
        print(f"\n{game.get('away_team')} @ {game.get('home_team')}")
        print(f"  Bets:  Away {game.get('spread_bets_away_pct')}% | Home {game.get('spread_bets_home_pct')}%")
        print(f"  Money: Away {game.get('spread_money_away_pct')}% | Home {game.get('spread_money_home_pct')}%")
        print(f"  Sharp Side: {game.get('spread_sharp_side')}")
