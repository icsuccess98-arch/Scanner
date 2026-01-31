"""
VSIN Scraper - Browser-based login with email/code authentication
Uses Playwright for browser automation to handle Piano authentication
"""
import os
import logging
import json
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, Page, Browser

logger = logging.getLogger(__name__)

class VSINBrowserScraper:
    def __init__(self):
        self.browser = None
        self.page = None
        self.context = None
        self.playwright = None
        self.logged_in = False
        self.email = None
        self.cookies_file = '/tmp/vsin_cookies.json'
    
    def _start_browser(self):
        """Initialize browser if not already started"""
        if self.browser is None:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=True)
            self.context = self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            self.page = self.context.new_page()
            if os.path.exists(self.cookies_file):
                try:
                    with open(self.cookies_file, 'r') as f:
                        cookies = json.load(f)
                        self.context.add_cookies(cookies)
                        self.logged_in = True
                        logger.info("Loaded saved VSIN cookies")
                except Exception as e:
                    logger.warning(f"Could not load cookies: {e}")
    
    def _save_cookies(self):
        """Save cookies for future sessions"""
        try:
            cookies = self.context.cookies()
            with open(self.cookies_file, 'w') as f:
                json.dump(cookies, f)
            logger.info("Saved VSIN cookies")
        except Exception as e:
            logger.warning(f"Could not save cookies: {e}")
    
    def close(self):
        """Close browser"""
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
    
    def request_login_code(self, email: str) -> dict:
        """Step 1: Navigate to login and enter email to request code"""
        self.email = email
        try:
            self._start_browser()
            
            self.page.goto('https://vsin.com/', wait_until='networkidle', timeout=30000)
            time.sleep(2)
            
            self.page.evaluate("""() => {
                const loginLinks = document.querySelectorAll('a[href*="login"], a:contains("Login"), a:contains("LOG IN")');
                for (const link of loginLinks) {
                    if (link.textContent.includes('LOG IN') || link.textContent.includes('Login')) {
                        link.click();
                        return true;
                    }
                }
                const pianoLogin = document.querySelector('[data-piano], .piano-id, #piano-id');
                if (pianoLogin) pianoLogin.click();
                return false;
            }""")
            time.sleep(3)
            
            for frame in self.page.frames:
                try:
                    email_input = frame.query_selector('input[type="email"], input[name="email"], input[placeholder*="email" i]')
                    if email_input:
                        email_input.fill(email)
                        time.sleep(1)
                        frame.evaluate("""() => {
                            const btn = document.querySelector('button[type="submit"], input[type="submit"], button');
                            if (btn) btn.click();
                        }""")
                        time.sleep(3)
                        logger.info(f"Login code requested for {email}")
                        return {'success': True, 'message': f'Code sent to {email}. Check your email and paste the code here.'}
                except Exception:
                    continue
            
            email_input = self.page.query_selector('input[type="email"], input[name="email"], input[placeholder*="email" i]')
            if email_input:
                email_input.fill(email)
                time.sleep(1)
                self.page.evaluate("""() => {
                    const btn = document.querySelector('button[type="submit"], input[type="submit"], button');
                    if (btn) btn.click();
                }""")
                time.sleep(3)
                logger.info(f"Login code requested for {email}")
                return {'success': True, 'message': f'Code sent to {email}. Check your email and paste the code here.'}
            
            return {'success': False, 'message': 'Could not find email input field on VSIN login page. VSIN uses Piano authentication which may require manual login.'}
            
        except Exception as e:
            logger.error(f"Error requesting login code: {e}")
            return {'success': False, 'message': str(e)}
    
    def verify_code(self, code: str) -> dict:
        """Step 2: Enter the verification code"""
        if not self.email:
            return {'success': False, 'message': 'Email not set. Call request_login_code first.'}
        
        try:
            self._start_browser()
            
            code_input = self.page.query_selector('input[type="text"], input[name="code"], input[placeholder*="code" i], input[maxlength="6"]')
            if code_input:
                code_input.fill(code.strip())
                time.sleep(1)
                
                submit_btn = self.page.query_selector('button[type="submit"], input[type="submit"], button:has-text("Verify"), button:has-text("Submit"), button:has-text("Continue")')
                if submit_btn:
                    submit_btn.click()
                    time.sleep(5)
            
            frames = self.page.frames
            for frame in frames:
                code_input = frame.query_selector('input[type="text"], input[name="code"], input[maxlength="6"]')
                if code_input:
                    code_input.fill(code.strip())
                    submit_btn = frame.query_selector('button[type="submit"]')
                    if submit_btn:
                        submit_btn.click()
                        time.sleep(5)
                        break
            
            self.page.goto('https://data.vsin.com/college-basketball/betting-splits/', wait_until='networkidle', timeout=30000)
            time.sleep(2)
            
            if 'Sign in' not in self.page.content() or 'betting-splits' in self.page.url:
                self.logged_in = True
                self._save_cookies()
                logger.info("Successfully logged in to VSIN")
                return {'success': True, 'message': 'Logged in successfully'}
            
            return {'success': False, 'message': 'Verification may have failed. Try again or check if code is correct.'}
            
        except Exception as e:
            logger.error(f"Error verifying code: {e}")
            return {'success': False, 'message': str(e)}
    
    def get_splits(self, sport: str = 'CBB') -> dict:
        """Get betting splits data for a sport"""
        sport_urls = {
            'NBA': 'https://data.vsin.com/nba/betting-splits/',
            'CBB': 'https://data.vsin.com/college-basketball/betting-splits/',
            'NFL': 'https://data.vsin.com/nfl/betting-splits/',
            'CFB': 'https://data.vsin.com/college-football/betting-splits/',
            'NHL': 'https://data.vsin.com/nhl/betting-splits/',
        }
        
        url = sport_urls.get(sport.upper(), sport_urls['CBB'])
        
        try:
            self._start_browser()
            
            self.page.goto(url, wait_until='networkidle', timeout=30000)
            time.sleep(3)
            
            content = self.page.content()
            
            if 'Sign in' in content and 'LOG IN' in content:
                return {'success': False, 'message': 'Not logged in. Please login first.', 'data': {}}
            
            splits_data = self._parse_splits_page(sport)
            
            return {'success': True, 'data': splits_data}
            
        except Exception as e:
            logger.error(f"Error fetching splits: {e}")
            return {'success': False, 'message': str(e), 'data': {}}
    
    def _parse_splits_page(self, sport: str) -> dict:
        """Parse betting splits from the current page"""
        from bs4 import BeautifulSoup
        
        splits = {}
        try:
            html = self.page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 4:
                        team_cell = cells[0]
                        team_name = team_cell.get_text(strip=True)
                        
                        if team_name and len(team_name) > 2:
                            bet_pct = self._extract_pct_from_cell(cells[1] if len(cells) > 1 else None)
                            money_pct = self._extract_pct_from_cell(cells[2] if len(cells) > 2 else None)
                            
                            splits[team_name] = {
                                'team': team_name,
                                'bet_pct': bet_pct,
                                'money_pct': money_pct,
                            }
            
            game_containers = soup.find_all(['div', 'article'], class_=lambda x: x and any(k in (x if isinstance(x, str) else ' '.join(x)) for k in ['game', 'matchup', 'event', 'split']))
            
            for container in game_containers:
                teams = container.find_all(['span', 'div'], class_=lambda x: x and 'team' in str(x).lower())
                percentages = container.find_all(['span', 'div'], class_=lambda x: x and any(k in str(x).lower() for k in ['pct', 'percent', 'bet', 'money', 'handle']))
                
                if len(teams) >= 2:
                    away = teams[0].get_text(strip=True)
                    home = teams[1].get_text(strip=True)
                    
                    pct_values = [self._extract_pct_from_text(p.get_text()) for p in percentages]
                    pct_values = [p for p in pct_values if p is not None]
                    
                    game_key = f"{away} @ {home}"
                    splits[game_key] = {
                        'away_team': away,
                        'home_team': home,
                        'away_bet_pct': pct_values[0] if len(pct_values) > 0 else None,
                        'home_bet_pct': pct_values[1] if len(pct_values) > 1 else None,
                        'away_money_pct': pct_values[2] if len(pct_values) > 2 else None,
                        'home_money_pct': pct_values[3] if len(pct_values) > 3 else None,
                    }
            
            logger.info(f"Parsed {len(splits)} entries from VSIN {sport} splits")
            
        except Exception as e:
            logger.error(f"Error parsing splits: {e}")
        
        return splits
    
    def _extract_pct_from_cell(self, cell) -> float:
        """Extract percentage from a table cell"""
        if not cell:
            return None
        return self._extract_pct_from_text(cell.get_text())
    
    def _extract_pct_from_text(self, text: str) -> float:
        """Extract percentage value from text"""
        if not text:
            return None
        try:
            import re
            match = re.search(r'(\d+(?:\.\d+)?)\s*%?', text)
            if match:
                return float(match.group(1))
        except:
            pass
        return None


_vsin_instance = None

def get_vsin_scraper():
    """Get singleton VSIN scraper instance"""
    global _vsin_instance
    if _vsin_instance is None:
        _vsin_instance = VSINBrowserScraper()
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


def vsin_is_logged_in() -> bool:
    """Check if currently logged in to VSIN"""
    scraper = get_vsin_scraper()
    return scraper.logged_in
