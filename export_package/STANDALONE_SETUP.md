# 730's Locks - Standalone Setup Guide

Run this sports betting calculator on any machine with Python 3.11+.

## Required Files

Copy these files to your project:

```
730s-locks/
├── sports_app.py           # Main Flask application
├── requirements.txt        # Python dependencies
├── templates/
│   ├── dashboard.html      # Main dashboard UI
│   └── history.html        # Pick history UI
└── static/
    ├── icon-192.png        # App icon (small)
    ├── icon-512.png        # App icon (large)
    ├── manifest.json       # PWA manifest
    ├── sw.js               # Service worker
    └── offline.html        # Offline fallback page
```

## Environment Variables

Create a `.env` file or set these environment variables:

```bash
# REQUIRED
DATABASE_URL=postgresql://user:password@host:5432/dbname
ODDS_API_KEY=your_odds_api_key_here

# OPTIONAL
FLASK_SECRET_KEY=your-secret-key-here
SPORTS_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...
```

### Getting API Keys

1. **ODDS_API_KEY**: Sign up at https://the-odds-api.com (free tier: 500 requests/month)
2. **SPORTS_DISCORD_WEBHOOK**: Create a webhook in your Discord server settings

## Setup Instructions

### 1. Install Python 3.11+

```bash
# macOS
brew install python@3.11

# Ubuntu/Debian
sudo apt install python3.11 python3.11-venv

# Windows
# Download from python.org
```

### 2. Create Virtual Environment

```bash
cd 730s-locks
python3.11 -m venv venv
source venv/bin/activate  # Linux/macOS
# OR
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up PostgreSQL Database

**Option A: Local PostgreSQL**
```bash
# Install PostgreSQL
brew install postgresql  # macOS
sudo apt install postgresql  # Ubuntu

# Create database
createdb 730_locks

# Set connection string
export DATABASE_URL="postgresql://localhost/730_locks"
```

**Option B: Cloud PostgreSQL (Neon, Supabase, etc.)**
```bash
# Use the connection string from your provider
export DATABASE_URL="postgresql://user:password@host/database"
```

### 5. Set API Keys

```bash
export ODDS_API_KEY="your_key_here"
export SPORTS_DISCORD_WEBHOOK="https://discord.com/api/webhooks/..."
```

### 6. Run the Application

**Development:**
```bash
python sports_app.py
```

**Production:**
```bash
gunicorn --bind 0.0.0.0:5000 --workers 2 --timeout 120 sports_app:app
```

The app will be available at: http://localhost:5000

## Features

- **Dashboard**: View today's games with edge calculations
- **Totals Picks**: O/U picks with 60% historical threshold
- **Spread Picks**: Spread picks with margin validation
- **Alt Lines**: Automatic best alternate line selection (Bovada, -180 or better)
- **Discord Posting**: Post Supermax Lock to Discord
- **History**: Track pick results (W/L/P)

## Automation (Optional)

Use cron or Task Scheduler for automated daily runs:

```bash
# Fetch games at 8 AM ET
0 8 * * * curl -X POST http://localhost:5000/fetch_games

# Post Lock of the Day at 11 AM ET (weekdays)
0 11 * * 1-4 curl -X POST http://localhost:5000/post_discord

# Weekend - Post multiple locks
0 10 * * 5-0 curl -X POST http://localhost:5000/post_discord_window/EARLY
30 12 * * 5-0 curl -X POST http://localhost:5000/post_discord_window/MID
0 17 * * 5-0 curl -X POST http://localhost:5000/post_discord_window/LATE

# Check results at 11 PM ET
0 23 * * * curl -X POST http://localhost:5000/check_results
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard |
| `/history` | GET | Pick history |
| `/fetch_games` | POST | Fetch today's games + odds + history |
| `/fetch_odds` | POST | Fetch betting lines from Bovada |
| `/fetch_history` | POST | Check historical O/U rates |
| `/post_discord` | POST | Post Supermax Lock to Discord |
| `/check_results` | POST | Check pending pick results |
| `/health` | GET | Health check |

## Troubleshooting

**"No games found"**
- Check your ODDS_API_KEY is valid
- Verify games are scheduled for today

**"Database connection error"**
- Verify DATABASE_URL is correct
- Ensure PostgreSQL is running

**"Discord posting failed"**
- Check webhook URL is valid
- Verify webhook has permission to post

**"No qualified picks"**
- Games must meet edge thresholds (NBA/CBB: 8.0, NFL/CFB: 3.5, NHL: 0.5)
- Games must meet 60% historical O/U rate OR margin validation for spreads

## Data Sources

- **Games/Stats**: ESPN API (free, no key required)
- **Betting Lines**: The Odds API → Bovada only
- **Results**: ESPN API (automatic checking)
