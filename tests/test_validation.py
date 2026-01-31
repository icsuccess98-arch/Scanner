"""
Tests for validation utilities.
"""

import pytest
from utils import (
    validate_percentage,
    validate_spread,
    validate_total,
    validate_float_param,
    validate_string_param,
)


class TestValidatePercentage:
    """Tests for percentage validation."""

    def test_valid_percentages(self):
        """Test valid percentage values."""
        assert validate_percentage(0) == 0
        assert validate_percentage(50) == 50
        assert validate_percentage(100) == 100
        assert validate_percentage(50.5) == 50.5

    def test_invalid_percentages(self):
        """Test invalid percentage values."""
        assert validate_percentage(-1) is None
        assert validate_percentage(101) is None
        assert validate_percentage(-50) is None
        assert validate_percentage(150) is None

    def test_none_input(self):
        """Test None input."""
        assert validate_percentage(None) is None

    def test_string_input(self):
        """Test string input that can be converted."""
        assert validate_percentage("50") == 50.0
        assert validate_percentage("invalid") is None

    def test_edge_cases(self):
        """Test edge cases at boundaries."""
        assert validate_percentage(0.0) == 0.0
        assert validate_percentage(100.0) == 100.0
        assert validate_percentage(0.001) == 0.001
        assert validate_percentage(99.999) == 99.999


class TestValidateSpread:
    """Tests for spread validation."""

    def test_valid_nba_spreads(self):
        """Test valid NBA spread values."""
        assert validate_spread(-3.5, 'NBA') == -3.5
        assert validate_spread(7, 'NBA') == 7
        assert validate_spread(0, 'NBA') == 0
        assert validate_spread(-15, 'NBA') == -15

    def test_invalid_nba_spreads(self):
        """Test invalid NBA spread values (too large)."""
        assert validate_spread(30, 'NBA') is None
        assert validate_spread(-30, 'NBA') is None

    def test_valid_cbb_spreads(self):
        """Test valid CBB spread values (larger range)."""
        assert validate_spread(25, 'CBB') == 25
        assert validate_spread(-30, 'CBB') == -30

    def test_valid_nhl_spreads(self):
        """Test valid NHL spread values (small range)."""
        assert validate_spread(-1.5, 'NHL') == -1.5
        assert validate_spread(1.5, 'NHL') == 1.5
        assert validate_spread(5, 'NHL') is None  # Too large for NHL

    def test_none_input(self):
        """Test None input."""
        assert validate_spread(None) is None


class TestValidateTotal:
    """Tests for total validation."""

    def test_valid_nba_totals(self):
        """Test valid NBA total values."""
        assert validate_total(220, 'NBA') == 220
        assert validate_total(200.5, 'NBA') == 200.5
        assert validate_total(250, 'NBA') == 250

    def test_invalid_nba_totals(self):
        """Test invalid NBA total values."""
        assert validate_total(100, 'NBA') is None  # Too low
        assert validate_total(350, 'NBA') is None  # Too high

    def test_valid_nhl_totals(self):
        """Test valid NHL total values."""
        assert validate_total(6.5, 'NHL') == 6.5
        assert validate_total(5, 'NHL') == 5

    def test_invalid_nhl_totals(self):
        """Test invalid NHL total values."""
        assert validate_total(15, 'NHL') is None  # Too high
        assert validate_total(2, 'NHL') is None  # Too low

    def test_none_input(self):
        """Test None input."""
        assert validate_total(None) is None


class TestValidateFloatParam:
    """Tests for generic float validation."""

    def test_valid_float(self):
        """Test valid float values."""
        val, err = validate_float_param(10.5)
        assert val == 10.5
        assert err is None

    def test_string_to_float(self):
        """Test string conversion to float."""
        val, err = validate_float_param("25.5")
        assert val == 25.5
        assert err is None

    def test_min_bound(self):
        """Test minimum bound validation."""
        val, err = validate_float_param(5, min_val=10)
        assert val is None
        assert "must be >=" in err

    def test_max_bound(self):
        """Test maximum bound validation."""
        val, err = validate_float_param(15, max_val=10)
        assert val is None
        assert "must be <=" in err

    def test_none_allowed(self):
        """Test None when allowed."""
        val, err = validate_float_param(None, allow_none=True)
        assert val is None
        assert err is None

    def test_none_not_allowed(self):
        """Test None when not allowed."""
        val, err = validate_float_param(None, allow_none=False)
        assert val is None
        assert "required" in err

    def test_invalid_string(self):
        """Test invalid string input."""
        val, err = validate_float_param("not a number")
        assert val is None
        assert "Invalid" in err


class TestValidateStringParam:
    """Tests for string validation."""

    def test_valid_string(self):
        """Test valid string values."""
        val, err = validate_string_param("Lakers")
        assert val == "Lakers"
        assert err is None

    def test_max_length(self):
        """Test maximum length validation."""
        val, err = validate_string_param("Lakers", max_length=3)
        assert val is None
        assert "exceeds max length" in err

    def test_allowed_values(self):
        """Test allowed values validation."""
        val, err = validate_string_param("NBA", allowed_values={'NBA', 'NHL', 'CBB'})
        assert val == "NBA"
        assert err is None

        val, err = validate_string_param("MLB", allowed_values={'NBA', 'NHL', 'CBB'})
        assert val is None
        assert "must be one of" in err

    def test_empty_string(self):
        """Test empty string handling."""
        val, err = validate_string_param("", allow_empty=False)
        assert val is None
        assert "cannot be empty" in err

        val, err = validate_string_param("", allow_empty=True)
        assert val == ""
        assert err is None

    def test_whitespace_stripping(self):
        """Test whitespace is stripped."""
        val, err = validate_string_param("  Lakers  ")
        assert val == "Lakers"
        assert err is None
