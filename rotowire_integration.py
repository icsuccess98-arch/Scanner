"""
BULLETPROOF ROTOWIRE INTEGRATION - FIXED VERSION
Professional-grade injury and lineup tracking with robust error handling

FIXES IN THIS VERSION:
1. Impact score cap corrected (4.5 instead of 3.0)
2. Added helper functions that were missing
3. Improved team name matching
4. Better error messages
5. Added get_team_injury_status and get_game_injury_analysis functions
"""

import logging
import time
import requests
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


class InjuryStatus(Enum):
    """Standardized injury statuses with impact scores."""
    OUT = ("OUT", 3.0)
    DOUBTFUL = ("DOUBTFUL", 2.5)
    GTD = ("GTD", 2.0)
    QUESTIONABLE = ("Q", 1.5)
    PROBABLE = ("PROB", 0.5)
    UNKNOWN = ("UNKNOWN", 1.0)
    
    def __init__(self, label: str, impact: float):
        self.label = label
        self.impact = impact
    
    @classmethod
    def from_text(cls, text: str) -> 'InjuryStatus':
        """Parse injury status from text with fuzzy matching."""
        text_upper = text.upper().strip()
        
        if "OUT" in text_upper:
            return cls.OUT
        if "DOUBTFUL" in text_upper:
            return cls.DOUBTFUL
        if "QUESTIONABLE" in text_upper or text_upper == "Q":
            return cls.QUESTIONABLE
        if "PROBABLE" in text_upper:
            return cls.PROBABLE
        if "GTD" in text_upper or "GAME TIME" in text_upper:
            return cls.GTD
        
        return cls.UNKNOWN


@dataclass
class InjuredPlayer:
    """Represents an injured player with all relevant data."""
    name: str
    team: str
    position: str
    status: InjuryStatus
    injury: str
    source: str
    is_starter: bool = False
    minutes_per_game: float = 0.0
    points_per_game: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def impact_score(self) -> float:
        """Calculate injury impact score (0-4.5). FIXED: Cap at 4.5 not 3.0."""
        base_impact = self.status.impact
        if self.is_starter:
            base_impact *= 1.5
        return min(base_impact, 4.5)
    
    def __repr__(self) -> str:
        return f"{self.name} ({self.position}) - {self.status.label}: {self.injury}"


@dataclass
class LineupData:
    """Represents confirmed starting lineup."""
    team: str
    league: str
    starters: List[str]
    confirmed: bool
    source: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def strength_indicator(self) -> str:
        """Estimate lineup strength based on number of confirmed starters."""
        if not self.confirmed:
            return "UNKNOWN"
        count = len(self.starters)
        if count >= 5:
            return "FULL_STRENGTH"
        elif count >= 3:
            return "PARTIAL"
        else:
            return "WEAK"


class RotoWireCache:
    """Time-based cache with source tracking."""
    
    def __init__(self, ttl_seconds: int = 3600):
        self._cache: Dict[str, Tuple[any, datetime, str]] = {}
        self._ttl = timedelta(seconds=ttl_seconds)
    
    def get(self, key: str) -> Optional[Tuple[any, str]]:
        """Get cached value and source if not expired."""
        if key in self._cache:
            value, timestamp, source = self._cache[key]
            if datetime.now() - timestamp < self._ttl:
                return (value, source)
            del self._cache[key]
        return None
    
    def set(self, key: str, value: any, source: str):
        """Cache value with source and timestamp."""
        self._cache[key] = (value, datetime.now(), source)
    
    def clear(self):
        """Clear all cached data."""
        self._cache.clear()
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        now = datetime.now()
        valid_entries = sum(1 for _, ts, _ in self._cache.values() 
                           if now - ts < self._ttl)
        return {
            'total_entries': len(self._cache),
            'valid_entries': valid_entries,
            'ttl_seconds': self._ttl.total_seconds()
        }


class RotoWireScraper:
    """Professional RotoWire scraper with circuit breaker and rate limiting."""
    
    ROTOWIRE_BASE = "https://www.rotowire.com"
    ESPN_BASE = "https://www.espn.com"
    
    ROTOWIRE_URLS = {
        'NBA': {
            'injuries': f"{ROTOWIRE_BASE}/basketball/nba/injury-report.php",
            'lineups': f"{ROTOWIRE_BASE}/basketball/nba/daily-lineups.php"
        },
        'NFL': {
            'injuries': f"{ROTOWIRE_BASE}/football/nfl/injury-report.php",
            'lineups': f"{ROTOWIRE_BASE}/football/nfl/daily-lineups.php"
        },
        'NHL': {
            'injuries': f"{ROTOWIRE_BASE}/hockey/nhl/injury-report.php",
            'lineups': f"{ROTOWIRE_BASE}/hockey/nhl/daily-lineups.php"
        },
        'CBB': {
            'injuries': f"{ROTOWIRE_BASE}/college-basketball/injury-report.php",
            'lineups': None
        },
        'CFB': {
            'injuries': f"{ROTOWIRE_BASE}/college-football/injury-report.php",
            'lineups': None
        }
    }
    
    ESPN_URLS = {
        'NBA': f"{ESPN_BASE}/nba/injuries",
        'NFL': f"{ESPN_BASE}/nfl/injuries",
        'NHL': f"{ESPN_BASE}/nhl/injuries",
        'CBB': f"{ESPN_BASE}/mens-college-basketball/injuries",
        'CFB': f"{ESPN_BASE}/college-football/injuries"
    }
    
    TEAM_ALIASES = {
        'lakers': ['los angeles lakers', 'la lakers', 'l.a. lakers'],
        'clippers': ['los angeles clippers', 'la clippers', 'l.a. clippers'],
        'warriors': ['golden state warriors', 'gs warriors'],
        'trail blazers': ['portland trail blazers', 'blazers', 'portland'],
        '76ers': ['philadelphia 76ers', 'sixers'],
        'heat': ['miami heat'],
        'nets': ['brooklyn nets'],
    }
    
    def __init__(self, cache_ttl: int = 3600):
        self._cache = RotoWireCache(ttl_seconds=cache_ttl)
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self._last_request_time = 0
        self._failure_count = 0
        self._circuit_open = False
        self._circuit_open_until = 0
    
    def _rate_limit(self):
        """Enforce 0.5-second delay between requests (faster for better performance)."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.5:
            time.sleep(0.5 - elapsed)
        self._last_request_time = time.time()
    
    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker is open."""
        if self._circuit_open:
            if time.time() > self._circuit_open_until:
                logger.info("Circuit breaker closing, retrying RotoWire")
                self._circuit_open = False
                self._failure_count = 0
                return False
            return True
        return False
    
    def _record_failure(self):
        """Record failure and potentially open circuit breaker."""
        self._failure_count += 1
        if self._failure_count >= 3:
            self._circuit_open = True
            self._circuit_open_until = time.time() + 300
            logger.warning("Circuit breaker OPEN - 3 consecutive RotoWire failures")
    
    def _record_success(self):
        """Reset failure count on success."""
        self._failure_count = max(0, self._failure_count - 1)
    
    def _fetch_url(self, url: str, timeout: int = 15) -> Optional[BeautifulSoup]:
        """Fetch and parse URL with comprehensive error handling."""
        try:
            self._rate_limit()
            response = self._session.get(url, timeout=timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            self._record_success()
            return soup
        except requests.exceptions.Timeout:
            logger.error(f"RotoWire timeout: {url}")
            self._record_failure()
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"RotoWire HTTP error {e.response.status_code}: {url}")
            self._record_failure()
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"RotoWire request failed: {url} - {str(e)}")
            self._record_failure()
            return None
        except Exception as e:
            logger.error(f"RotoWire unexpected error: {url} - {str(e)}")
            self._record_failure()
            return None
    
    def get_injuries(self, team: str, league: str) -> Tuple[List[InjuredPlayer], str]:
        """Get injury report for a team with automatic ESPN fallback."""
        cache_key = f"injuries:{league}:{team}"
        
        cached = self._cache.get(cache_key)
        if cached:
            injuries, source = cached
            logger.debug(f"Cache hit: {team} injuries from {source}")
            return injuries, source
        
        # Skip college sports injury checks entirely (unreliable sources)
        if league in ('CBB', 'CFB'):
            self._cache.set(cache_key, [], 'skipped')
            return [], 'skipped'
        
        # If circuit breaker is open, skip all checks
        if self._check_circuit_breaker():
            return [], 'circuit_open'
        
        injuries = self._get_injuries_rotowire(team, league)
        if injuries is not None:
            self._cache.set(cache_key, injuries, 'rotowire')
            logger.info(f"RotoWire: Found {len(injuries)} injuries for {team}")
            return injuries, 'rotowire'
        
        # Only try ESPN for pro leagues (NBA, NFL, NHL)
        if league in ('NBA', 'NFL', 'NHL'):
            injuries = self._get_injuries_espn(team, league)
            if injuries is not None:
                self._cache.set(cache_key, injuries, 'espn')
                logger.info(f"ESPN: Found {len(injuries)} injuries for {team}")
                return injuries, 'espn'
        
        logger.debug(f"No injuries found for {team}")
        return [], 'none'
    
    def _get_injuries_rotowire(self, team: str, league: str) -> Optional[List[InjuredPlayer]]:
        """Scrape RotoWire injury report."""
        if league not in self.ROTOWIRE_URLS:
            return None
        
        url = self.ROTOWIRE_URLS[league]['injuries']
        soup = self._fetch_url(url)
        if not soup:
            return None
        
        injuries = []
        
        try:
            injury_sections = (
                soup.find_all('div', class_='injury-report__team') or
                soup.find_all('table', class_='tablesorter') or
                soup.find_all('div', class_='lineup__box')
            )
            
            for section in injury_sections:
                team_elem = (
                    section.find('div', class_='injury-report__team-name') or
                    section.find('a', class_='lineup__abbr') or
                    section.find('th', class_='teamheader')
                )
                
                if not team_elem:
                    continue
                
                team_name = team_elem.text.strip()
                if not self._team_matches(team, team_name):
                    continue
                
                injury_rows = (
                    section.find_all('div', class_='injury-report__row') or
                    section.find_all('tr')[1:]
                )
                
                for row in injury_rows:
                    try:
                        player_name = (
                            row.find('a', class_='injury-report__player-name') or
                            row.find('a') or
                            row.find('td')
                        )
                        
                        if not player_name:
                            continue
                        
                        name = player_name.text.strip()
                        
                        position_elem = (
                            row.find('span', class_='injury-report__position') or
                            row.find_all('td')[1] if len(row.find_all('td')) > 1 else None
                        )
                        position = position_elem.text.strip() if position_elem else 'N/A'
                        
                        status_elem = (
                            row.find('span', class_='injury-report__status') or
                            row.find_all('td')[2] if len(row.find_all('td')) > 2 else None
                        )
                        status_text = status_elem.text.strip() if status_elem else 'QUESTIONABLE'
                        status = InjuryStatus.from_text(status_text)
                        
                        injury_elem = (
                            row.find('span', class_='injury-report__injury') or
                            row.find_all('td')[3] if len(row.find_all('td')) > 3 else None
                        )
                        injury_desc = injury_elem.text.strip() if injury_elem else 'Undisclosed'
                        
                        is_starter = position in ['PG', 'SG', 'SF', 'PF', 'C', 'QB', 'RB', 'WR', 'TE', 'G', 'F', 'D', 'RW', 'LW']
                        
                        injuries.append(InjuredPlayer(
                            name=name,
                            team=team,
                            position=position,
                            status=status,
                            injury=injury_desc,
                            source='rotowire',
                            is_starter=is_starter
                        ))
                    except Exception as e:
                        logger.debug(f"Error parsing injury row: {str(e)}")
                        continue
            
            return injuries if injuries else None
            
        except Exception as e:
            logger.error(f"Error parsing RotoWire injuries: {str(e)}")
            return None
    
    def _get_injuries_espn(self, team: str, league: str) -> Optional[List[InjuredPlayer]]:
        """Scrape ESPN injury report as fallback."""
        if league not in self.ESPN_URLS:
            return None
        
        url = self.ESPN_URLS[league]
        soup = self._fetch_url(url)
        if not soup:
            return None
        
        injuries = []
        
        try:
            injury_tables = soup.find_all('div', class_='ResponsiveTable')
            
            for table in injury_tables:
                team_header = table.find_previous('h2') or table.find_previous('div', class_='team__name')
                if not team_header:
                    continue
                
                team_name = team_header.text.strip()
                if not self._team_matches(team, team_name):
                    continue
                
                rows = table.find_all('tr')
                for row in rows:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < 3:
                            continue
                        
                        name = cells[0].text.strip()
                        position = cells[1].text.strip() if len(cells) > 1 else 'N/A'
                        status_text = cells[2].text.strip() if len(cells) > 2 else 'QUESTIONABLE'
                        injury_desc = cells[3].text.strip() if len(cells) > 3 else 'Undisclosed'
                        
                        status = InjuryStatus.from_text(status_text)
                        
                        injuries.append(InjuredPlayer(
                            name=name,
                            team=team,
                            position=position,
                            status=status,
                            injury=injury_desc,
                            source='espn',
                            is_starter=False
                        ))
                    except Exception as e:
                        logger.debug(f"Error parsing ESPN injury row: {str(e)}")
                        continue
            
            return injuries if injuries else None
            
        except Exception as e:
            logger.error(f"Error parsing ESPN injuries: {str(e)}")
            return None
    
    def get_lineups(self, team: str, league: str) -> Tuple[Optional[LineupData], str]:
        """Get confirmed starting lineups from RotoWire."""
        if league in ['CBB', 'CFB']:
            logger.debug(f"{league} lineups not available on RotoWire")
            return None, 'none'
        
        cache_key = f"lineups:{league}:{team}"
        
        cached = self._cache.get(cache_key)
        if cached:
            lineup, source = cached
            logger.debug(f"Cache hit: {team} lineup from {source}")
            return lineup, source
        
        if self._check_circuit_breaker():
            logger.debug("Circuit breaker open, skipping lineup fetch")
            return None, 'none'
        
        lineup = self._get_lineups_rotowire(team, league)
        if lineup:
            self._cache.set(cache_key, lineup, 'rotowire')
            logger.info(f"RotoWire: Found lineup for {team} ({len(lineup.starters)} starters)")
            return lineup, 'rotowire'
        
        return None, 'none'
    
    def _get_lineups_rotowire(self, team: str, league: str) -> Optional[LineupData]:
        """Scrape RotoWire starting lineups."""
        if league not in self.ROTOWIRE_URLS or not self.ROTOWIRE_URLS[league]['lineups']:
            return None
        
        url = self.ROTOWIRE_URLS[league]['lineups']
        soup = self._fetch_url(url)
        if not soup:
            return None
        
        try:
            lineup_sections = soup.find_all('div', class_='lineup')
            
            for section in lineup_sections:
                team_elem = section.find('div', class_='lineup__abbr') or section.find('a', class_='lineup__team')
                if not team_elem:
                    continue
                
                team_name = team_elem.text.strip()
                if not self._team_matches(team, team_name):
                    continue
                
                starters = []
                starter_rows = section.find_all('div', class_='lineup__player') or section.find_all('a', class_='lineup__player')
                
                for row in starter_rows:
                    try:
                        player_name = row.find('a') or row
                        if player_name:
                            starters.append(player_name.text.strip())
                    except:
                        continue
                
                status_elem = section.find('div', class_='lineup__status')
                confirmed = False
                if status_elem:
                    status_text = status_elem.text.strip().upper()
                    confirmed = 'CONFIRMED' in status_text or 'EXPECTED' in status_text
                
                return LineupData(
                    team=team,
                    league=league,
                    starters=starters,
                    confirmed=confirmed,
                    source='rotowire'
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing RotoWire lineups: {str(e)}")
            return None
    
    def _team_matches(self, target: str, scraped: str) -> bool:
        """Improved team name matching with aliases."""
        target_clean = target.lower().strip().replace('.', '')
        scraped_clean = scraped.lower().strip().replace('.', '')
        
        if target_clean == scraped_clean:
            return True
        
        if target_clean in scraped_clean or scraped_clean in target_clean:
            return True
        
        for key, aliases in self.TEAM_ALIASES.items():
            target_in_aliases = any(alias in target_clean for alias in aliases + [key])
            scraped_in_aliases = any(alias in scraped_clean for alias in aliases + [key])
            if target_in_aliases and scraped_in_aliases:
                return True
        
        target_words = set(target_clean.split())
        scraped_words = set(scraped_clean.split())
        common_words = target_words & scraped_words
        
        stop_words = {'the', 'of', 'in', 'at', 'and', 'or'}
        meaningful_common = common_words - stop_words
        
        return len(meaningful_common) >= 2
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics for monitoring."""
        return self._cache.get_stats()
    
    def clear_cache(self):
        """Clear the cache."""
        self._cache.clear()


class InjuryImpactCalculator:
    """Calculate the impact of injuries on game qualification."""
    
    DISQUALIFY_THRESHOLD = 4.5
    WARNING_THRESHOLD = 3.0
    
    @classmethod
    def calculate_team_impact(cls, injuries: List[InjuredPlayer]) -> Dict:
        """Calculate total injury impact for a team."""
        if not injuries:
            return {
                'total_impact': 0.0,
                'out_count': 0,
                'doubtful_count': 0,
                'questionable_count': 0,
                'starter_out_count': 0,
                'should_disqualify': False,
                'warning_flag': False,
                'has_key_injuries': False,
                'details': [],
                'source': 'none'
            }
        
        total_impact = 0.0
        out_count = 0
        doubtful_count = 0
        questionable_count = 0
        starter_out_count = 0
        details = []
        
        for injury in injuries:
            impact = injury.impact_score
            total_impact += impact
            
            if injury.status == InjuryStatus.OUT:
                out_count += 1
                if injury.is_starter:
                    starter_out_count += 1
            elif injury.status == InjuryStatus.DOUBTFUL:
                doubtful_count += 1
            elif injury.status in [InjuryStatus.QUESTIONABLE, InjuryStatus.GTD]:
                questionable_count += 1
            
            details.append({
                'name': injury.name,
                'position': injury.position,
                'status': injury.status.label,
                'impact': impact,
                'is_starter': injury.is_starter
            })
        
        return {
            'total_impact': round(total_impact, 2),
            'out_count': out_count,
            'doubtful_count': doubtful_count,
            'questionable_count': questionable_count,
            'starter_out_count': starter_out_count,
            'should_disqualify': total_impact >= cls.DISQUALIFY_THRESHOLD,
            'warning_flag': cls.WARNING_THRESHOLD <= total_impact < cls.DISQUALIFY_THRESHOLD,
            'has_key_injuries': total_impact >= cls.WARNING_THRESHOLD,
            'details': details,
            'source': injuries[0].source if injuries else 'none'
        }
    
    @classmethod
    def calculate_game_impact(cls, away_injuries: List[InjuredPlayer], 
                             home_injuries: List[InjuredPlayer]) -> Dict:
        """Calculate injury impact for both teams in a game."""
        away_impact = cls.calculate_team_impact(away_injuries)
        home_impact = cls.calculate_team_impact(home_injuries)
        
        combined_impact = away_impact['total_impact'] + home_impact['total_impact']
        
        away_source = away_injuries[0].source if away_injuries else 'none'
        home_source = home_injuries[0].source if home_injuries else 'none'
        
        return {
            'away': away_impact,
            'home': home_impact,
            'combined_impact': round(combined_impact, 2),
            'game_should_disqualify': away_impact['should_disqualify'] or home_impact['should_disqualify'],
            'game_has_warning': away_impact['warning_flag'] or home_impact['warning_flag'],
            'sources': f"{away_source}/{home_source}"
        }


rotowire_scraper = RotoWireScraper(cache_ttl=3600)


def fetch_team_injuries(team: str, league: str) -> Tuple[List[InjuredPlayer], str]:
    """Fetch injuries for a team. Returns (injuries, source)."""
    return rotowire_scraper.get_injuries(team, league)


def fetch_team_lineup(team: str, league: str) -> Optional[LineupData]:
    """Fetch lineup for a team. Returns LineupData or None."""
    lineup, source = rotowire_scraper.get_lineups(team, league)
    return lineup


def check_game_injuries(away_team: str, home_team: str, league: str) -> Dict:
    """
    Check injuries for both teams in a game.
    Returns comprehensive injury analysis.
    """
    away_injuries, away_source = rotowire_scraper.get_injuries(away_team, league)
    home_injuries, home_source = rotowire_scraper.get_injuries(home_team, league)
    
    away_impact = InjuryImpactCalculator.calculate_team_impact(away_injuries)
    home_impact = InjuryImpactCalculator.calculate_team_impact(home_injuries)
    
    should_disqualify = away_impact['should_disqualify'] or home_impact['should_disqualify']
    
    asymmetric_concern = abs(away_impact['total_impact'] - home_impact['total_impact']) > 3.0
    
    if should_disqualify:
        if away_impact['should_disqualify'] and home_impact['should_disqualify']:
            recommendation = "SKIP - Both teams have major injuries"
        elif away_impact['should_disqualify']:
            recommendation = f"SKIP - {away_team} has major injuries (impact: {away_impact['total_impact']:.1f})"
        else:
            recommendation = f"SKIP - {home_team} has major injuries (impact: {home_impact['total_impact']:.1f})"
    elif asymmetric_concern:
        recommendation = "CAUTION - Significant injury imbalance between teams"
    elif away_impact['warning_flag'] or home_impact['warning_flag']:
        recommendation = "PROCEED - Minor injury concerns noted"
    else:
        recommendation = "PROCEED - No significant injuries"
    
    return {
        'away': away_impact,
        'home': home_impact,
        'should_disqualify': should_disqualify,
        'asymmetric_concern': asymmetric_concern,
        'recommendation': recommendation,
        'sources': f"{away_source}/{home_source}"
    }


def get_team_injury_status(team: str, league: str) -> Dict:
    """
    Get comprehensive injury status for a single team.
    Returns dict compatible with existing sports_app.py code.
    """
    injuries, source = rotowire_scraper.get_injuries(team, league)
    impact = InjuryImpactCalculator.calculate_team_impact(injuries)
    
    lineup, lineup_source = rotowire_scraper.get_lineups(team, league)
    
    return {
        'has_key_injuries': impact['has_key_injuries'],
        'impact_score': impact['total_impact'],
        'injured_starters': impact['starter_out_count'],
        'star_out': impact['starter_out_count'] >= 2,
        'out_count': impact['out_count'],
        'questionable_count': impact['questionable_count'],
        'details': impact['details'],
        'source': source,
        'lineup_confirmed': lineup.confirmed if lineup else False,
        'lineup_strength': lineup.strength_indicator if lineup else 'UNKNOWN',
        'should_disqualify': impact['should_disqualify'],
        'warning_flag': impact['warning_flag']
    }


def get_game_injury_analysis(away_team: str, home_team: str, league: str) -> Dict:
    """
    Complete injury analysis for a game.
    Wrapper for check_game_injuries with additional data.
    """
    result = check_game_injuries(away_team, home_team, league)
    
    away_lineup, _ = rotowire_scraper.get_lineups(away_team, league)
    home_lineup, _ = rotowire_scraper.get_lineups(home_team, league)
    
    both_lineups_confirmed = (
        away_lineup is not None and away_lineup.confirmed and
        home_lineup is not None and home_lineup.confirmed
    )
    
    result['lineups'] = {
        'away_confirmed': away_lineup.confirmed if away_lineup else False,
        'home_confirmed': home_lineup.confirmed if home_lineup else False,
        'both_confirmed': both_lineups_confirmed,
        'away_strength': away_lineup.strength_indicator if away_lineup else 'UNKNOWN',
        'home_strength': home_lineup.strength_indicator if home_lineup else 'UNKNOWN'
    }
    
    return result


def get_cache_stats() -> Dict:
    """Get cache statistics."""
    return rotowire_scraper.get_cache_stats()


def clear_cache():
    """Clear the cache."""
    rotowire_scraper.clear_cache()
