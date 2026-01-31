"""
Tests for utility functions.
"""

import pytest
from utils import (
    extract_teams_from_competitors,
    get_team_name_from_competitor,
    safe_divide,
    safe_average,
    safe_min,
    safe_max,
    normalize_team_name,
    parse_spread_string,
    parse_total_string,
    sanitize_url_for_logging,
    sql_escape,
)


class TestExtractTeams:
    """Tests for team extraction from ESPN data."""

    def test_extract_both_teams(self, sample_espn_competitors):
        """Test extracting away and home teams."""
        away, home = extract_teams_from_competitors(sample_espn_competitors)
        assert away is not None
        assert home is not None
        assert away['team']['shortDisplayName'] == 'Celtics'
        assert home['team']['shortDisplayName'] == 'Lakers'

    def test_extract_from_empty_list(self, empty_competitors):
        """Test extraction from empty list."""
        away, home = extract_teams_from_competitors(empty_competitors)
        assert away is None
        assert home is None

    def test_extract_from_malformed_data(self, malformed_competitors):
        """Test extraction from malformed data."""
        away, home = extract_teams_from_competitors(malformed_competitors)
        # Should find the one with homeAway='away'
        assert away is not None
        assert home is None  # No valid home team


class TestGetTeamName:
    """Tests for team name extraction."""

    def test_get_display_name(self, sample_espn_competitors):
        """Test getting display name."""
        away = sample_espn_competitors[0]
        name = get_team_name_from_competitor(away, 'displayName')
        assert name == 'Boston Celtics'

    def test_get_short_name(self, sample_espn_competitors):
        """Test getting short display name."""
        home = sample_espn_competitors[1]
        name = get_team_name_from_competitor(home, 'shortDisplayName')
        assert name == 'Lakers'

    def test_get_abbreviation(self, sample_espn_competitors):
        """Test getting abbreviation."""
        away = sample_espn_competitors[0]
        name = get_team_name_from_competitor(away, 'abbreviation')
        assert name == 'BOS'

    def test_none_competitor(self):
        """Test with None competitor."""
        name = get_team_name_from_competitor(None)
        assert name == ''

    def test_missing_key(self, sample_espn_competitors):
        """Test with missing key."""
        away = sample_espn_competitors[0]
        name = get_team_name_from_competitor(away, 'nonexistent')
        assert name == ''


class TestSafeDivide:
    """Tests for safe division."""

    def test_normal_division(self):
        """Test normal division."""
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(7, 2) == 3.5

    def test_divide_by_zero(self):
        """Test division by zero."""
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(10, 0, default=999) == 999

    def test_divide_by_none(self):
        """Test division by None."""
        assert safe_divide(10, None) == 0.0

    def test_invalid_inputs(self):
        """Test invalid inputs."""
        assert safe_divide("abc", 2) == 0.0
        assert safe_divide(10, "abc") == 0.0


class TestSafeAverage:
    """Tests for safe average calculation."""

    def test_normal_average(self):
        """Test normal average."""
        assert safe_average([1, 2, 3, 4, 5]) == 3.0
        assert safe_average([10, 20]) == 15.0

    def test_empty_list(self):
        """Test empty list."""
        assert safe_average([]) == 0.0
        assert safe_average([], default=100) == 100

    def test_none_values(self):
        """Test list with None values."""
        assert safe_average([1, None, 3, None, 5]) == 3.0

    def test_all_none(self):
        """Test list with all None values."""
        assert safe_average([None, None, None]) == 0.0


class TestSafeMinMax:
    """Tests for safe min/max."""

    def test_safe_min(self):
        """Test safe minimum."""
        assert safe_min([5, 2, 8, 1, 9]) == 1
        assert safe_min([]) == 0.0
        assert safe_min([None, 5, None]) == 5

    def test_safe_max(self):
        """Test safe maximum."""
        assert safe_max([5, 2, 8, 1, 9]) == 9
        assert safe_max([]) == 0.0
        assert safe_max([None, 5, None]) == 5


class TestNormalizeTeamName:
    """Tests for team name normalization."""

    def test_sixers_alias(self):
        """Test Sixers -> 76ers normalization."""
        assert normalize_team_name('sixers') == '76ers'
        assert normalize_team_name('Sixers') == '76ers'

    def test_blazers_alias(self):
        """Test Blazers -> Trail Blazers."""
        assert normalize_team_name('blazers') == 'Trail Blazers'
        assert normalize_team_name('portland') == 'Trail Blazers'

    def test_no_change_needed(self):
        """Test names that don't need normalization."""
        assert normalize_team_name('Lakers') == 'Lakers'
        assert normalize_team_name('Celtics') == 'Celtics'

    def test_empty_string(self):
        """Test empty string input."""
        assert normalize_team_name('') == ''

    def test_case_insensitive(self):
        """Test case insensitivity."""
        assert normalize_team_name('SIXERS') == '76ers'
        assert normalize_team_name('okc') == 'Thunder'


class TestParseSpreadString:
    """Tests for spread string parsing."""

    def test_negative_spread(self):
        """Test negative spread parsing."""
        assert parse_spread_string('-3.5') == -3.5
        assert parse_spread_string('-7') == -7.0

    def test_positive_spread(self):
        """Test positive spread parsing."""
        assert parse_spread_string('+3.5') == 3.5
        assert parse_spread_string('+7') == 7.0

    def test_half_symbol(self):
        """Test half symbol parsing."""
        assert parse_spread_string('-3½') == -3.5
        assert parse_spread_string('+7½') == 7.5

    def test_pick_even(self):
        """Test PK/EVEN parsing."""
        assert parse_spread_string('PK') == 0.0
        assert parse_spread_string('EVEN') == 0.0
        assert parse_spread_string('pick') == 0.0

    def test_empty_input(self):
        """Test empty input."""
        assert parse_spread_string('') is None
        assert parse_spread_string(None) is None


class TestParseTotalString:
    """Tests for total string parsing."""

    def test_simple_total(self):
        """Test simple total parsing."""
        assert parse_total_string('220.5') == 220.5
        assert parse_total_string('215') == 215.0

    def test_with_ou_prefix(self):
        """Test O/U prefix parsing."""
        assert parse_total_string('O220.5') == 220.5
        assert parse_total_string('U215') == 215.0

    def test_half_symbol(self):
        """Test half symbol parsing."""
        assert parse_total_string('220½') == 220.5

    def test_empty_input(self):
        """Test empty input."""
        assert parse_total_string('') is None
        assert parse_total_string(None) is None


class TestSanitizeUrl:
    """Tests for URL sanitization."""

    def test_api_key_redaction(self):
        """Test API key is redacted."""
        url = 'https://api.example.com?apiKey=secret123&other=value'
        result = sanitize_url_for_logging(url)
        assert 'secret123' not in result
        assert '[REDACTED]' in result
        assert 'other=value' in result

    def test_multiple_sensitive_params(self):
        """Test multiple sensitive params are redacted."""
        url = 'https://api.example.com?apiKey=key1&token=tok123&data=ok'
        result = sanitize_url_for_logging(url)
        assert 'key1' not in result
        assert 'tok123' not in result
        assert 'data=ok' in result

    def test_no_sensitive_params(self):
        """Test URL without sensitive params unchanged."""
        url = 'https://api.example.com?foo=bar&baz=qux'
        result = sanitize_url_for_logging(url)
        assert result == url


class TestSqlEscape:
    """Tests for SQL escaping."""

    def test_normal_string(self):
        """Test normal string escaping."""
        assert sql_escape('Lakers') == 'Lakers'

    def test_single_quotes(self):
        """Test single quote escaping."""
        assert sql_escape("O'Neal") == "O''Neal"

    def test_multiple_quotes(self):
        """Test multiple quotes."""
        assert sql_escape("It's a 'test'") == "It''s a ''test''"

    def test_none_input(self):
        """Test None input."""
        assert sql_escape(None) is None
