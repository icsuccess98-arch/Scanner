# Trading Systems Project

## Overview
This project develops and manages three independent trading systems:
1.  **Sports Betting Calculator**: Analyzes sports data to identify profitable betting opportunities for Over/Under totals and spreads. Its core purpose is to provide data-driven picks based on a strictly defined mathematical model.
2.  **Forex/Metals/Indices Trading Bot**: An automated trading bot exclusively integrated with Discord for notifications and interactions.
3.  **Crypto Perpetuals Trading System**: A system designed for trading cryptocurrency perpetuals, leveraging the Coinbase Advanced Trade API.

The overarching vision is to provide robust, automated, and data-backed trading and betting solutions across different financial and sports markets.

## User Preferences
- Keep Discord message format consistent - never change formats
- No Telegram for Forex workflows
- Bovada-style team names (short nicknames, no mascots)
- Lock of the Day = highest absolute edge across all qualified picks
- Only make changes to the `replit.nix` and `.replit` files if it is absolutely necessary.
- Do not make changes to files in the `archive` folder.

## System Architecture

### UI/UX
-   **Color Palette**: Jewel-tone color scheme (emerald, sapphire, amber, crimson, ice).
-   **Mobile Optimization**: Sticky mobile action bar for easy thumb access, header buttons hidden on mobile.
-   **Interactive Elements**: Game cards feature hover effects with shadows, lock cards have a golden glow, and qualified cards display a green gradient background.
-   **Branding**: "730's Locks" branding for the sports model web app.
-   **Dashboard**: Features Edge Analysis with Average Edge, Best Edge, and Direction Split. Includes a 52-week bankroll builder with an interactive savings tracker and progress bar.
-   **League Logos**: Official ESPN CDN logos for NBA, NFL, NHL, NCAA. League-specific gradient colors for game card borders.
-   **Data Display**: Game cards show side-by-side TOTALS and SPREAD sections with lines, projections, edge, and pick. Cleaned-up edge display.

### Technical Implementation
-   **Sports Model (Locked)**:
    -   **Data Sources**: Exclusively uses ESPN Official Season Stats (PPG, Opp PPG) and Bovada for current Over/Under total lines.
    -   **Formulas**: Strict, unmodifiable formulas for `Expected Team Score`, `Projected Total`, and `Difference`.
    -   **Thresholds**: League-specific thresholds (NBA/CBB: ±8.0 points, NFL/CFB: ±3.5 points, NHL: ±0.5 points) for pick validation.
    -   **Bet Direction**: Binary rules for OVER/UNDER based on `Projected_Total` vs. `Bovada_Line` plus threshold.
    -   **Historical Qualification**: Totals picks require 60%+ historical O/U hit rate from last 10 games. H2H history also considered if 3+ games exist. Spreads use margin-based validation.
    -   **Alt Lines**: Fetched from Bovada, selecting the best value under/over the main line, with odds strictly -180 or better.
    -   **Pinnacle EV Comparison**: Fetches Pinnacle odds alongside Bovada to calculate Expected Value (EV). EV formula: `(p_true * decimal_payout) - 1` where `p_true` is Pinnacle's implied probability. Picks must have non-negative EV (≥0%) to qualify.
    -   **Result Checking**: Automatic result checking refreshes approximately 2.5-3.5 hours after game start.
    -   **Automation**: Daily scheduled tasks for fetching games, stats, odds, posting picks to Discord, and checking results.

### Betting Models (4 Total)
The sports betting calculator uses four distinct models for pick generation:

1.  **Standard Totals (O/U)** - Model 1
    -   Core O/U picks based on ESPN stats vs Bovada lines
    -   Edge threshold: NBA/CBB ±8.0 pts, NFL/CFB ±3.5 pts, NHL ±0.5 pts
    -   Historical qualification: 60%+ O/U hit rate (either team) from last 10 games
    -   H2H qualification: If 3+ H2H games exist, H2H O/U rate must also be 60%+
    -   Alt line selection: Best alternate line with odds -180 or better

2.  **Standard Spreads** - Model 2
    -   Spread picks using expected margin vs Bovada spread lines
    -   Same edge thresholds as totals
    -   Historical qualification: Average margin must support spread line (75% threshold)
    -   Alt spread selection: Best alternate with odds -180 or better

3.  **Away Favorite + O/U** - Model 3 (User record: 51-14)
    -   Premium model: Away team is favorite (spread_line > 0) AND O/U meets edge threshold
    -   Stricter historical requirement: 70%+ away team O/U hit rate (vs 60% base)
    -   Must also pass standard totals qualification (edge + base history)
    -   Receives +2 weighted bonus in TOP 5 ranking
    -   High-confidence plays when road favorite + totals align

4.  **NBA Away Favorite 1H Money Line** - Model 4 (Planned)
    -   NBA games where away team is the favorite
    -   Take 1st Half money line on the away favorite
    -   Uses The Odds API market key: `h2h_h1`
    -   Requires event-specific API calls

### TOP 5 Ranking Algorithm
-   Weighted Score = Edge + (History% × 0.15) + Model Bonus
-   Model 3 (Away Fav + O/U) gets +2 bonus
-   Allows high-history picks (85%+9) to outrank lower-history (67%+11)
-   **Data Management**:
    -   **Caching**: Date-keyed caching for ESPN lookups to speed up subsequent calls.
    -   **Database**: Uses SQLite with indexes for faster queries. Foreign key integrity maintained with safe deletion helpers.
    -   **Data Validation**: ESPN event date validation, commence_time validation for Odds API, and clearing of stale league data.
-   **System Stability**: Gunicorn timeout increased to 120s with 2 workers, robust logging, team alias expansion and name matching, and unified `teams_match()` function.
-   **Discord Integration**: Automated posting of picks to Discord with history tracking. Weekend scheduling includes staggered "EARLY", "MIDDAY", and "LATE" locks.

### Feature Specifications
-   **Sports Scanner**: Fetches NBA, CBB, NFL, CFB, NHL games, stats, and odds to identify and post qualified picks.
-   **Forex Bot**: Executes trading workflows (Daily, Weekly, Monthly) and posts updates to Discord.
-   **Crypto Bot**: Manages crypto perpetuals trading for 35 high-volume tickers via Coinbase Advanced Trade API, with Discord notifications.

## External Dependencies
-   **Sports Data**:
    -   ESPN API (for team statistics, schedules, and historical data)
    -   Bovada (for betting lines and alternate lines)
    -   The Odds API (for betting odds)
-   **Trading Platforms**:
    -   OANDA API (for Forex/metals/indices trading)
    -   Coinbase Advanced Trade API (for crypto perpetuals trading)
-   **Communication**:
    -   Discord Webhooks (for all automated notifications and pick postings)