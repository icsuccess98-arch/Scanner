"""
Tests for constants module.
"""

import pytest
from constants import (
    VALID_LEAGUES,
    CacheTTL,
    RateLimits,
    EDGE_THRESHOLDS,
    TOTAL_RANGES,
    NBA_TEAM_COLORS,
    NBA_ABBREVIATIONS,
    NHL_ABBREVIATIONS,
    get_threshold,
    get_team_nickname,
    get_team_color,
    is_valid_league,
)


class TestValidLeagues:
    """Tests for league validation."""

    def test_valid_leagues(self):
        """Test all valid leagues are present."""
        assert 'NBA' in VALID_LEAGUES
        assert 'CBB' in VALID_LEAGUES
        assert 'NFL' in VALID_LEAGUES
        assert 'CFB' in VALID_LEAGUES
        assert 'NHL' in VALID_LEAGUES

    def test_is_valid_league(self):
        """Test is_valid_league function."""
        assert is_valid_league('NBA') is True
        assert is_valid_league('nba') is True  # Case insensitive
        assert is_valid_league('MLB') is False
        assert is_valid_league('') is False


class TestCacheTTL:
    """Tests for cache TTL constants."""

    def test_cache_values_positive(self):
        """Test all cache TTLs are positive."""
        assert CacheTTL.DASHBOARD > 0
        assert CacheTTL.LIVE_SCORES > 0
        assert CacheTTL.WAGERTALK > 0
        assert CacheTTL.TEAM_STATS > 0

    def test_relative_ordering(self):
        """Test cache TTLs have sensible relative ordering."""
        # Live data should have shorter TTL than historical
        assert CacheTTL.LIVE_SCORES < CacheTTL.TEAM_STATS
        assert CacheTTL.DASHBOARD < CacheTTL.HISTORICAL


class TestRateLimits:
    """Tests for rate limit constants."""

    def test_rate_limits_positive(self):
        """Test all rate limits are positive."""
        assert RateLimits.ESPN > 0
        assert RateLimits.ODDS_API > 0
        assert RateLimits.COVERS > 0
        assert RateLimits.DEFAULT > 0

    def test_odds_api_conservative(self):
        """Test Odds API has conservative rate limit (limited monthly quota)."""
        assert RateLimits.ODDS_API < 1.0  # Less than 1 req/sec


class TestEdgeThresholds:
    """Tests for edge threshold configuration."""

    def test_all_leagues_have_thresholds(self):
        """Test all valid leagues have thresholds defined."""
        for league in ['NBA', 'CBB', 'NHL', 'NFL', 'CFB']:
            assert league in EDGE_THRESHOLDS

    def test_threshold_keys_present(self):
        """Test required keys are present for each league."""
        for league, thresholds in EDGE_THRESHOLDS.items():
            assert 'total_edge' in thresholds
            assert 'spread_edge' in thresholds
            assert 'min_ev' in thresholds

    def test_get_threshold_function(self):
        """Test get_threshold helper function."""
        assert get_threshold('NBA', 'total_edge') == 8.0
        assert get_threshold('NHL', 'total_edge') == 0.5
        assert get_threshold('INVALID', 'total_edge', default=999) == 999


class TestTotalRanges:
    """Tests for total ranges by league."""

    def test_all_leagues_have_ranges(self):
        """Test all major leagues have ranges defined."""
        for league in ['NBA', 'CBB', 'NFL', 'CFB', 'NHL']:
            assert league in TOTAL_RANGES

    def test_ranges_are_tuples(self):
        """Test ranges are tuples with min and max."""
        for league, range_tuple in TOTAL_RANGES.items():
            assert isinstance(range_tuple, tuple)
            assert len(range_tuple) == 2
            min_val, max_val = range_tuple
            assert min_val < max_val


class TestTeamMappings:
    """Tests for team mapping dictionaries."""

    def test_nba_teams_present(self):
        """Test major NBA teams are in mappings."""
        assert 'Lakers' in NBA_TEAM_COLORS
        assert 'Celtics' in NBA_TEAM_COLORS
        assert '76ers' in NBA_TEAM_COLORS

    def test_nba_abbreviations_valid(self):
        """Test NBA abbreviations map to valid team names."""
        assert NBA_ABBREVIATIONS['LAL'] == 'Lakers'
        assert NBA_ABBREVIATIONS['BOS'] == 'Celtics'
        assert NBA_ABBREVIATIONS['PHI'] == '76ers'

    def test_nhl_teams_present(self):
        """Test major NHL teams are in mappings."""
        assert 'Bruins' in NHL_ABBREVIATIONS.values()
        assert 'Canadiens' in NHL_ABBREVIATIONS.values()
        assert 'Maple Leafs' in NHL_ABBREVIATIONS.values()


class TestGetTeamNickname:
    """Tests for team nickname lookup."""

    def test_nba_abbreviation_lookup(self):
        """Test NBA abbreviation to nickname lookup."""
        assert get_team_nickname('LAL', 'NBA') == 'Lakers'
        assert get_team_nickname('BOS', 'NBA') == 'Celtics'

    def test_nhl_abbreviation_lookup(self):
        """Test NHL abbreviation to nickname lookup."""
        assert get_team_nickname('BOS', 'NHL') == 'Bruins'
        assert get_team_nickname('MTL', 'NHL') == 'Canadiens'

    def test_unknown_identifier(self):
        """Test unknown identifier returns original."""
        assert get_team_nickname('UNKNOWN', 'NBA') == 'UNKNOWN'


class TestGetTeamColor:
    """Tests for team color lookup."""

    def test_nba_team_colors(self):
        """Test NBA team color lookup."""
        assert get_team_color('Lakers', 'NBA') == '#552583'
        assert get_team_color('Celtics', 'NBA') == '#007A33'

    def test_unknown_team_color(self):
        """Test unknown team returns default gray."""
        assert get_team_color('Unknown Team', 'NBA') == '#666666'

    def test_invalid_league_color(self):
        """Test invalid league returns default gray."""
        assert get_team_color('Lakers', 'MLB') == '#666666'
