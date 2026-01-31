"""
VSIN Scraper - Login with email/code authentication and scrape splits data
"""
import os
import requests
import logging
import json
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class VSINScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self.base_url = 'https://www.vsin.com'
        self.logged_in = False
        self.email = None
    
    def request_login_code(self, email: str) -> dict:
        """Step 1: Request login code to be sent to email"""
        self.email = email
        try:
            response = self.session.get(f'{self.base_url}/login')
            
            login_data = {
                'email': email,
            }
            
            response = self.session.post(
                f'{self.base_url}/api/auth/login',
                json=login_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info(f"Login code requested for {email}")
                return {'success': True, 'message': 'Code sent to your email'}
            else:
                alt_response = self.session.post(
                    f'{self.base_url}/auth/magic-link',
                    json={'email': email},
                    headers={'Content-Type': 'application/json'}
                )
                if alt_response.status_code == 200:
                    return {'success': True, 'message': 'Code sent to your email'}
                
                logger.warning(f"Login request failed: {response.status_code}")
                return {'success': False, 'message': f'Failed to request code: {response.status_code}'}
                
        except Exception as e:
            logger.error(f"Error requesting login code: {e}")
            return {'success': False, 'message': str(e)}
    
    def verify_code(self, code: str) -> dict:
        """Step 2: Verify the code sent to email"""
        if not self.email:
            return {'success': False, 'message': 'Email not set. Call request_login_code first.'}
        
        try:
            verify_data = {
                'email': self.email,
                'code': code.strip(),
            }
            
            response = self.session.post(
                f'{self.base_url}/api/auth/verify',
                json=verify_data,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                self.logged_in = True
                logger.info("Successfully logged in to VSIN")
                return {'success': True, 'message': 'Logged in successfully'}
            else:
                alt_response = self.session.post(
                    f'{self.base_url}/auth/verify-code',
                    json=verify_data,
                    headers={'Content-Type': 'application/json'}
                )
                if alt_response.status_code == 200:
                    self.logged_in = True
                    return {'success': True, 'message': 'Logged in successfully'}
                
                return {'success': False, 'message': f'Verification failed: {response.status_code}'}
                
        except Exception as e:
            logger.error(f"Error verifying code: {e}")
            return {'success': False, 'message': str(e)}
    
    def get_splits(self, sport: str = 'CBB') -> dict:
        """Get betting splits data after login"""
        if not self.logged_in:
            return {'success': False, 'message': 'Not logged in', 'data': {}}
        
        sport_map = {
            'NBA': 'nba',
            'CBB': 'ncaab',
            'NFL': 'nfl',
            'CFB': 'ncaaf',
            'NHL': 'nhl'
        }
        
        sport_key = sport_map.get(sport.upper(), 'ncaab')
        
        try:
            response = self.session.get(f'{self.base_url}/betting-splits/{sport_key}')
            
            if response.status_code != 200:
                return {'success': False, 'message': f'Failed to fetch splits: {response.status_code}', 'data': {}}
            
            splits_data = self._parse_splits_html(response.text, sport)
            
            return {'success': True, 'data': splits_data}
            
        except Exception as e:
            logger.error(f"Error fetching splits: {e}")
            return {'success': False, 'message': str(e), 'data': {}}
    
    def _parse_splits_html(self, html: str, sport: str) -> dict:
        """Parse splits data from HTML response"""
        from bs4 import BeautifulSoup
        
        splits = {}
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            game_rows = soup.find_all(['div', 'tr'], class_=lambda x: x and ('game' in x.lower() or 'matchup' in x.lower()))
            
            for row in game_rows:
                try:
                    teams = row.find_all(['span', 'td'], class_=lambda x: x and 'team' in x.lower())
                    if len(teams) >= 2:
                        away_team = teams[0].get_text(strip=True)
                        home_team = teams[1].get_text(strip=True)
                        
                        bet_pcts = row.find_all(['span', 'td'], class_=lambda x: x and ('bet' in x.lower() or 'ticket' in x.lower()))
                        money_pcts = row.find_all(['span', 'td'], class_=lambda x: x and ('money' in x.lower() or 'handle' in x.lower()))
                        
                        game_key = f"{away_team} @ {home_team}"
                        splits[game_key] = {
                            'away_team': away_team,
                            'home_team': home_team,
                            'away_bet_pct': self._extract_pct(bet_pcts[0]) if bet_pcts else None,
                            'home_bet_pct': self._extract_pct(bet_pcts[1]) if len(bet_pcts) > 1 else None,
                            'away_money_pct': self._extract_pct(money_pcts[0]) if money_pcts else None,
                            'home_money_pct': self._extract_pct(money_pcts[1]) if len(money_pcts) > 1 else None,
                        }
                except Exception as e:
                    continue
            
            logger.info(f"Parsed {len(splits)} games from VSIN splits")
            
        except Exception as e:
            logger.error(f"Error parsing splits HTML: {e}")
        
        return splits
    
    def _extract_pct(self, element) -> float:
        """Extract percentage value from element"""
        try:
            text = element.get_text(strip=True)
            text = text.replace('%', '').strip()
            return float(text)
        except:
            return None


_vsin_instance = None

def get_vsin_scraper():
    """Get singleton VSIN scraper instance"""
    global _vsin_instance
    if _vsin_instance is None:
        _vsin_instance = VSINScraper()
    return _vsin_instance


def vsin_request_code(email: str) -> dict:
    """Request login code for VSIN"""
    scraper = get_vsin_scraper()
    return scraper.request_login_code(email)


def vsin_verify_and_login(code: str) -> dict:
    """Verify code and complete VSIN login"""
    scraper = get_vsin_scraper()
    return scraper.verify_code(code)


def vsin_get_splits(sport: str = 'CBB') -> dict:
    """Get splits data from VSIN"""
    scraper = get_vsin_scraper()
    return scraper.get_splits(sport)
