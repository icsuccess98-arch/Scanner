# Trading Systems Project

## Overview
This project develops and manages trading systems, focusing on a Sports Betting Calculator for TOTALS (Over/Under) betting and a Stocks App based on The Strat methodology. The Sports Betting Calculator analyzes sports data from ESPN and Bovada to identify profitable Over/Under betting opportunities with strict edge thresholds, emphasizing advanced analytics for NBA and CBB. The Stocks App identifies specific trading patterns across various timeframes for a watchlist of popular stocks.

## User Preferences
- Keep Discord message format consistent - never change formats
- Bovada-style team names (short nicknames, no mascots)
- SUPERMAX = highest absolute edge across all qualified picks
- TOTALS ONLY - no spreads, no moneylines
- Alt lines max -180 odds floor
- Only make changes to the `replit.nix` and `.replit` files if it is absolutely necessary.
- Do not make changes to files in the `archive` folder.

## System Architecture

### UI/UX
The application features a jewel-tone color scheme and is optimized for mobile with a sticky action bar and card-style displays. Interactive game cards use hover effects and a golden glow for SUPERMAX picks. Branding includes "730's Locks" and league-specific ESPN CDN logos with gradient colors. The Dashboard provides edge analysis and a 52-week bankroll builder. Game cards specifically display TOTALS information, and top picks feature color-coded edge bars and a 5-star confidence rating. An "Away Favorite" golden badge is used for confidence boosts. The interface is inspired by the Pikkit app for a gamified, mobile-first experience, including dynamic team logos, real-time records, conference standings, and bet type tabs. A gamified betting checklist displays live line movement, bet/money percentages, and sharp money detection, utilizing visual indicators and status badges.

### Technical Implementation
The system includes two independent applications: a Sports App and a Stocks App, running on port 5000.

**Sports App (TOTALS-ONLY)**
The Sports App's core is a pure formula-based sports model using ESPN official season stats, Bart Torvik (for CBB), and Bovada lines. It incorporates a Matchup Intelligence feature with advanced analytics dropdowns on game cards, displaying an Edge Summary (Overall, Net Rating, Rebounding, Turnovers, Shooting Eff, Defensive Eff), Power Rating comparisons, Efficiency Comparisons, Shooting Profile, Ball Control, Rebounding, Pace & Free Throws, and Strength of Schedule. All stats show both season averages and last 5 games performance. Fifteen key metrics are highlighted, along with footer stats for H2H L10, ATS L10, and team records.
The sports model uses strict formulas for `Projected_Total` based on team PPG and Opp PPG. Picks are qualified if they meet absolute difference thresholds (e.g., NBA: ±8.0 points, NFL: ±3.5 points) and historical O/U performance filters (e.g., L5 100%, L20 85%+). Confidence is tiered as SUPERMAX, HIGH, STANDARD, and LOW, with SUPERMAX being the highest absolute edge. Alt lines from The Odds API (Bovada) are mandatory, with a max -185 odds floor. Confidence badges like "Away Favorite" and "Defense Edge" are implemented. The system includes date-keyed caching, PostgreSQL with indexes, robust logging, and performance optimizations like dashboard caching and response compression. All spread-related code, 1H ML models, injury scraping, and unit logic have been removed to focus purely on TOTALS.

**Stocks App**
The Stocks App implements Rob Smith's "The Strat" trading patterns, offering a mobile-responsive interface for daily, weekly, and monthly timeframes. It identifies patterns such as Inside Bars (1), Outside Bars (3), 2U/2D, Failed 2U/2D, Double Inside, Full Time Frame Continuity (FTFC), and A++ Setups (Inside bar + FTFC). It monitors a watchlist of 40 popular stocks and categorizes setups via tabs.

### Feature Specifications
The Sports Scanner fetches game data, stats, and odds for NBA, CBB, NFL, CFB, and NHL to identify qualified TOTALS picks.

## External Dependencies
-   **Sports Data**:
    -   ESPN API (team statistics, schedules)
    -   Bovada (betting lines via The Odds API)
    -   The Odds API (alternate lines)
    -   NBA API (nba_api package)
    -   Bart Torvik (CBB advanced analytics)
    -   TeamRankings.com (matchup pages)
    -   Cleaning the Glass (CTG) (NBA advanced stats)
    -   WagerTalk.com (betting action: Bet %, Money %)
    -   Covers.com (H2H, ATS records)
-   **Stocks Data**:
    -   yfinance (Yahoo Finance)
-   **Communication**:
    -   Discord Webhooks