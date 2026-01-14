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
-   **Sports Model (PURE FORMULA-BASED)**:
    -   **Data Sources**: ONLY ESPN Official Season Stats (PPG, Opp PPG) and Bovada lines.
    -   **Formulas (STRICT - NO MODIFICATIONS)**:
        -   `Expected_A = (Team A PPG + Team B Opp PPG) / 2`
        -   `Expected_B = (Team B PPG + Team A Opp PPG) / 2`
        -   `Projected_Total = Expected_A + Expected_B`
        -   `Difference = Projected_Total - Bovada_Line`
    -   **Thresholds (ABSOLUTE RULE)**: A bet is ONLY valid if absolute Difference meets:
        -   NBA: ±8.0 points
        -   CBB: ±8.0 points
        -   NFL: ±3.5 points
        -   CFB: ±3.5 points
        -   NHL: ±0.5 points
    -   **Bet Direction (BINARY)**:
        -   OVER: If `Projected_Total >= Bovada_Line + Threshold`
        -   UNDER: If `Bovada_Line >= Projected_Total + Threshold`
    -   **Lock of the Day**: Highest absolute edge across all qualified picks.
    -   **TOP 5 Ranking**: Sorted by EDGE only (highest edge = best pick).
    -   **Star Ratings**: Based on edge only (5★ for 12+, 4★ for 10+, 3★ for 8+, 2★ otherwise).
-   **Data Management**: Date-keyed caching for ESPN lookups, PostgreSQL database with indexes, and data validation.
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