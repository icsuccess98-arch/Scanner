"""
Pytest configuration and fixtures for sports betting app tests.
"""

import pytest
import sys
from pathlib import Path

# Add workspace to path for imports
workspace = Path(__file__).parent.parent
sys.path.insert(0, str(workspace))


@pytest.fixture
def sample_espn_competitors():
    """Sample ESPN-style competitor data for testing."""
    return [
        {
            'homeAway': 'away',
            'team': {
                'displayName': 'Boston Celtics',
                'shortDisplayName': 'Celtics',
                'abbreviation': 'BOS'
            },
            'score': '115'
        },
        {
            'homeAway': 'home',
            'team': {
                'displayName': 'Los Angeles Lakers',
                'shortDisplayName': 'Lakers',
                'abbreviation': 'LAL'
            },
            'score': '110'
        }
    ]


@pytest.fixture
def sample_game_data(sample_espn_competitors):
    """Sample complete game data."""
    return {
        'id': '401584721',
        'date': '2025-01-31T00:00:00Z',
        'competitions': [{
            'id': '401584721',
            'competitors': sample_espn_competitors,
            'venue': {
                'fullName': 'Crypto.com Arena',
                'city': 'Los Angeles'
            }
        }],
        'status': {
            'type': {
                'name': 'STATUS_SCHEDULED',
                'completed': False
            }
        }
    }


@pytest.fixture
def sample_betting_data():
    """Sample betting splits data."""
    return {
        'away_tickets_pct': 65,
        'home_tickets_pct': 35,
        'away_money_pct': 45,
        'home_money_pct': 55,
        'spread': -3.5,
        'total': 220.5,
        'opening_spread': -4.0,
        'opening_total': 218.5,
    }


@pytest.fixture
def empty_competitors():
    """Empty competitor list for edge case testing."""
    return []


@pytest.fixture
def malformed_competitors():
    """Malformed competitor data for error handling tests."""
    return [
        {'homeAway': 'away'},  # Missing team data
        {'team': {'displayName': 'Lakers'}},  # Missing homeAway
    ]
