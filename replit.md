# Trading Systems Project

## Overview
This project develops and manages three independent trading systems: a Sports Betting Calculator, a Forex/Metals/Indices Trading Bot, and a Crypto Perpetuals Trading System. The Sports Betting Calculator analyzes sports data to identify profitable betting opportunities for Over/Under totals and spreads. The Forex/Metals/Indices Trading Bot is an automated system integrated with Discord for notifications and interactions. The Crypto Perpetuals Trading System is designed for trading cryptocurrency perpetuals, leveraging the Coinbase Advanced Trade API. The overarching vision is to provide robust, automated, and data-backed trading and betting solutions across different financial and sports markets.

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
-   **Mobile Optimization**: Sticky mobile action bar, header buttons hidden on mobile.
-   **Interactive Elements**: Game cards with hover effects and shadows, lock cards with a golden glow, qualified cards with a green gradient background.
-   **Branding**: "730's Locks" branding for the sports model web app.
-   **Dashboard**: Features Edge Analysis (Average, Best, Direction Split) and a 52-week bankroll builder with savings tracker.
-   **League Logos**: Official ESPN CDN logos for NBA, NFL, NHL, NCAA, with league-specific gradient colors for game card borders.
-   **Data Display**: Game cards show side-by-side TOTALS and SPREAD sections with lines, projections, edge, and pick.
-   **Edge Visualization**: Top 5 picks display color-coded edge bars proportional to edge strength.
-   **Confidence Stars**: 5-star rating system (★★★★★) based on confidence tier.
-   **B2B Badges**: Red "B2B" badges on game cards for back-to-back games.
-   **Injury Indicator Styles**: CSS ready for major (red), minor (yellow), clean (green) injury status badges.

### Technical Implementation
-   **Sports Model**:
    -   **Data Sources**: Exclusively uses ESPN Official Season Stats and Bovada for lines.
    -   **Formulas**: Strict, unmodifiable formulas for `Expected Team Score`, `Projected Total`, and `Difference`.
    -   **Thresholds**: League-specific thresholds for pick validation (e.g., NBA/CBB: ±8.0 points).
    -   **Bet Direction**: Binary rules for OVER/UNDER based on `Projected_Total` vs. `Bovada_Line` plus threshold.
    -   **Historical Qualification**: Totals picks require 60%+ historical O/U hit rate from last 30 games (NBA/CBB/NHL) or 16 games (NFL/CFB). Spreads use margin-based validation.
    -   **Alt Lines**: Fetched from Bovada, selecting best value with odds -180 or better.
    -   **Pinnacle EV Comparison**: Fetches Pinnacle odds to calculate Expected Value (EV), with a minimum EV threshold of 1.0%.
    -   **Result Checking**: Automatic result checking refreshes 2.5-3.5 hours after game start.
    -   **Automation**: Daily scheduled tasks for fetching data, posting picks, and checking results.
    -   **Advanced Qualification Factors**:
        -   **Recent Form Weighting**: Tracks team form (UP, DOWN, STABLE) and disqualifies spread picks if form is declining.
        -   **Injury Data Integration**: Utilizes RotoWire.com (primary) and ESPN API (fallback) for injury reports and starting lineups. Implements circuit breaker, rate limiting, and caching. Calculates status-weighted impact scores and disqualifies picks based on injury severity.
        -   **Sharp Money Detection (Line Movement)**: Stores opening lines and disqualifies picks if sharp money moves against the model's direction by 1.5+ points.
        -   **Spread Sign Validation**: Cross-references spread signs against moneyline odds, auto-correcting mismatches.
        -   **Strength of Schedule Factor**: Calculates a multiplier based on opponent's PPG allowed for future projection adjustments.
        -   **Vig-Adjusted Edge Calculation**: Removes bookmaker vig from raw edge calculations for accurate assessment.
        -   **Bulletproof Pre-Send Validation**: A final validation layer with 7 checks (Edge threshold, Model qualification, Historical qualification, EV non-negative, Injury validation, Game status, Spread validation). Defines confidence tiers (SUPERMAX, HIGH, MEDIUM, LOW) for picks.
        -   **Timezone Validation**: Correctly converts UTC game times to Eastern Time.
        -   **Historical Betting Lines Service**: Fetches actual Vegas closing lines from The Odds API for true ATS and O/U hit rates.
        -   **Bulletproof Current Line System**: Calculates current line hit rates using free ESPN data + current Vegas lines, defining confidence tiers.
-   **Betting Models (4 Total)**:
    1.  **Standard Totals (O/U)**: Core O/U picks based on ESPN stats vs Bovada lines, with specific edge and historical qualification thresholds.
    2.  **Standard Spreads**: Spread picks using expected margin vs Bovada spread lines, with separate historical qualification.
    3.  **Away Favorite + O/U**: Premium model for games where away team is favorite and O/U meets edge, with stricter historical requirements.
    4.  **NBA Away Favorite 1H Money Line**: For NBA games where away team is favorite, takes 1st Half money line.
-   **TOP 5 Ranking Algorithm**: Uses a weighted score combining edge, historical percentage, and model bonuses.
-   **Data Management**: Date-keyed caching for ESPN lookups, SQLite database with indexes, and data validation.
-   **System Stability**: Gunicorn timeout increased to 120s, robust logging, team alias expansion and name matching.
-   **Discord Integration**: Automated posting of picks to Discord with history tracking and staggered weekend scheduling.
-   **Performance Optimizations**:
    -   **Database Indexes**: Composite indexes on Game model (idx_date_league, idx_qualified, idx_spread_qualified, idx_event_id, idx_composite_search) for fast queries.
    -   **Dashboard Caching**: 30-second TTL cache with thread-safe locking for dashboard data.
    -   **Parallel Processing**: ThreadPoolExecutor with 10 workers for batch injury checks.
    -   **Response Compression**: Flask-Compress for gzip/deflate compression on all responses.
    -   **Weather Integration**: OpenWeatherMap API for NFL/CFB games with indoor stadium detection (DOME_STADIUMS list), impact scoring for wind/temperature/precipitation, and auto-disqualification for extreme weather (≥5 point impact).
    -   **Win Rate Analytics**: Comprehensive `/api/win_rate_analytics` endpoint tracking performance by league, confidence tier, day of week, time window, injury source, and pick type.

### Feature Specifications
-   **Sports Scanner**: Fetches NBA, CBB, NFL, CFB, NHL games, stats, and odds to identify and post qualified picks.
-   **Forex Bot**: Executes daily, weekly, and monthly trading workflows and posts updates to Discord.
-   **Crypto Bot**: Manages crypto perpetuals trading for 35 high-volume tickers via Coinbase Advanced Trade API, with Discord notifications.

## External Dependencies
-   **Sports Data**:
    -   ESPN API (team statistics, schedules, historical data)
    -   Bovada (betting lines and alternate lines)
    -   The Odds API (betting odds, historical data)
    -   RotoWire.com (injury reports, starting lineups)
-   **Trading Platforms**:
    -   OANDA API (Forex/metals/indices trading)
    -   Coinbase Advanced Trade API (crypto perpetuals trading)
-   **Communication**:
    -   Discord Webhooks (automated notifications and pick postings)