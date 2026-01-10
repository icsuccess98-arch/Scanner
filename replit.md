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
    -   **Historical Qualification**: Totals picks require a 70% historical O/U hit rate (based on average total as proxy for historical line) from the last 10 games (ESPN team schedules). H2H history also considered if 3+ games exist. Spreads use margin-based validation where average margin must meet a percentage of the spread line.
    -   **Alt Lines**: Fetched from Bovada, selecting the best value under/over the main line, with odds strictly -180 or better.
    -   **Pinnacle EV Comparison**: Fetches Pinnacle odds alongside Bovada to calculate Expected Value (EV). EV formula: `(p_true * decimal_payout) - 1` where `p_true` is Pinnacle's implied probability. Picks must have non-negative EV (≥0%) to qualify. EV badges displayed in UI showing Bovada vs Pinnacle comparison.
    -   **Result Checking**: Automatic result checking refreshes approximately 2.5-3.5 hours after game start, integrated with live score refresh every 30 seconds.
    -   **Automation**: Daily scheduled tasks for fetching games, stats, odds, posting picks to Discord, and checking results.
    -   **Spread Betting**: Integrated alongside totals, using the same locked formula for expected scores and league-specific thresholds for qualification.
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