# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

730's Locks is a sports betting analytics platform built on Python Flask. It calculates edge-based betting opportunities for TOTALS (Over/Under) across NBA, CBB, NFL, CFB, and NHL using formula-based analysis, sharp money detection, and historical performance tracking.

## Commands

### Development
```bash
python sports_app.py                    # Run dev server on port 5000
```

### Production
```bash
gunicorn --bind=0.0.0.0:5000 --workers=2 --timeout=120 sports_app:app
```

### Dependencies
```bash
pip install -r requirements.txt
playwright install chromium             # Required for web scraping
```

## Required Environment Variables

```bash
DATABASE_URL=postgresql://user:pass@host:5432/dbname
ODDS_API_KEY=your_key_from_the-odds-api.com
SPORTS_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...  # Optional
```

## Architecture

### Core Application
- **sports_app.py** - Monolithic Flask app (~11k lines) containing all routes, SQLAlchemy models (`Game`, `Pick`), and business logic
- **stocks_app.py** - Separate app for The Strat stock trading patterns

### Services Layer
- **services/edge_calculator.py** - Raw/true edge calculations, vig removal, confidence tier determination
- **services/line_movement.py** - Reverse Line Movement (RLM) and sharp money detection
- **config/thresholds.py** - Single source of truth for betting thresholds (SUPERMAX, HIGH, MEDIUM, LOW tiers)

### Data Pipeline
- **sports_scanner.py** - Game/odds fetching engine
- **live_odds_fetcher.py** - The Odds API integration (Bovada lines)
- **rlm_detector.py** - Sharp money detection
- **wagertalk_scraper.py** - Betting percentages (Bet%, Money%)
- **automated_loading_system.py** - Logo loading, TeamRankings scraping

### Key Routes
| Route | Purpose |
|-------|---------|
| `GET /` | Main dashboard |
| `POST /fetch_games` | Fetch today's games + odds |
| `POST /fetch_odds` | Refresh betting lines |
| `POST /post_discord` | Post daily Supermax lock |
| `POST /post_discord_window/<window>` | Post by time window (EARLY/MID/LATE) |
| `POST /check_results` | Validate pick outcomes |
| `GET /api/live_scores` | Real-time game scores |

### Database Models
- **Game** - Games with lines, movement tracking, betting action percentages
- **Pick** - Historical picks with results (W/L/P), edge values, pick_type (TOTALS/SPREAD)

## User Preferences

- **TOTALS ONLY** - No spreads, no moneylines
- **Alt lines max -180 odds floor** (Bovada via The Odds API)
- **SUPERMAX** = highest absolute edge across all qualified picks
- **Bovada-style team names** - Short nicknames, no mascots
- **Discord message format** - Keep consistent, never change formats
- Do not modify `.replit` unless absolutely necessary
- Do not modify files in the `archive` folder

## Edge Calculation

```
Projected Total = (Team PPG + Opponent PPG) / 2
Raw Edge = |Projected Total - Market Line|
```

Confidence tiers based on edge thresholds defined in `config/thresholds.py`:
- **SUPERMAX**: edge ≥ 12% + EV ≥ 3.0 + history ≥ 70%
- **HIGH**: edge ≥ 10% + EV ≥ 1.0 + history ≥ 65%
- **MEDIUM**: edge ≥ 8% + EV ≥ -1.0 + history ≥ 60%
- **LOW**: edge ≥ 6% + EV ≥ -2.0 + history ≥ 55%

## External Data Sources

- **ESPN API** - Team statistics, schedules (no auth required)
- **The Odds API** - Betting lines (requires ODDS_API_KEY)
- **Bart Torvik** - CBB advanced analytics (web scraping)
- **TeamRankings.com** - Advanced metrics (web scraping)
- **WagerTalk.com** - Betting percentages
- **Covers.com** - H2H, ATS records
- **yfinance** - Stock data (stocks_app.py)

## Caching Strategy

- Dashboard: 30 seconds
- Team stats: 1 hour
- Historical rates: 6 hours
- Live odds: 30 seconds
