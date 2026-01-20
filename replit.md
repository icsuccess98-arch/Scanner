# Trading Systems Project

## Overview
This project develops and manages trading systems including a Sports Betting Calculator focused on TOTALS (Over/Under) betting. The Sports Betting Calculator analyzes sports data using ESPN stats and Bovada lines to identify profitable Over/Under betting opportunities with strict edge thresholds.

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
-   **Color Palette**: Jewel-tone color scheme (emerald, sapphire, amber, crimson, ice).
-   **Mobile Optimization**: Sticky mobile action bar, header buttons hidden on mobile, card-style TOP 5 picks with full team names.
-   **Interactive Elements**: Game cards with hover effects and shadows, SUPERMAX card with golden glow.
-   **Branding**: "730's Locks" branding, SUPERMAX for top pick.
-   **Dashboard**: Features Edge Analysis (Average, Best) and a 52-week bankroll builder with savings tracker.
-   **League Logos**: Official ESPN CDN logos for NBA, NFL, NHL, NCAA, with league-specific gradient colors.
-   **Data Display**: Game cards show TOTALS section only with line, projection, edge, and pick.
-   **Edge Visualization**: Top 5 picks display color-coded edge bars proportional to edge strength.
-   **Confidence Stars**: 5-star rating system (★★★★★) based on edge only.
-   **Away Favorite Badge**: Golden badge for away favorite games (confidence boost).

### Technical Implementation (TOTALS-ONLY)
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
    -   **Qualification Requirements**: Edge threshold + Direction set
    -   **Historical O/U Performance**: Uses last 15 games O/U hit rate to strengthen picks
    -   **Confidence Tiers**: SUPERMAX (≥12 edge), HIGH (≥10), STANDARD (≥8), LOW (<8) - no units displayed
    -   **SUPERMAX**: Highest absolute edge across all qualified TOTALS picks
    -   **TOP 5 Ranking**: Sorted by EDGE only (highest edge = best pick)
    -   **Star Ratings**: Based on edge only (5★ for 12+, 4★ for 10+, 3★ for 8+, 2★ otherwise)
    -   **Alt Lines**: Uses alternate totals lines from The Odds API (Bovada) when they provide better edge, max -180 odds
    -   **Away Favorite Model**: Confidence boost when away team is favorite AND meets totals threshold
-   **Data Management**: Date-keyed caching for ESPN lookups, PostgreSQL database with indexes, and data validation.
-   **System Stability**: Gunicorn timeout increased to 120s, robust logging, team alias expansion.
-   **Discord Integration**: Automated posting of picks to Discord.
-   **Performance Optimizations**:
    -   **Database Indexes**: Composite indexes on Game model for fast queries.
    -   **Dashboard Caching**: 30-second TTL cache with thread-safe locking.
    -   **Response Compression**: Flask-Compress for gzip/deflate compression.

### Code Optimization (Jan 2026)
-   **Spread Code Removed**: All spread-related processing logic removed
-   **1H ML Model Removed**: Model 4 (NBA 1st half moneyline) completely removed
-   **RotoWire/Injury Code Removed**: All injury scraping and checking removed for speed
-   **Units Logic Removed**: No units displayed - confidence based purely on edge thresholds
-   **Away Favorite Badge**: Orange (#FFA500) ⭐ AWAY FAV badge on game cards and TOP 5 picks
-   **Defense Mismatch Badge**: Blue (#00BFFF) 🛡️ DEF EDGE badge for favorable defensive matchups
    -   OVER picks: Shows when facing bottom 10 defense (weak defense = more points)
    -   UNDER picks: Shows when facing top 10 defense (strong defense = fewer points)
    -   NBA/CBB only (basketball games)
-   **File Size**: Reduced to 8,048 lines
-   **Focus**: Pure TOTALS (Over/Under) functionality only with confidence badges:
    1. **Totals Model**: Standard O/U picks with edge thresholds
    2. **Away Favorite Badge**: Games where away team is favorite AND meets O/U threshold (orange badge)
    3. **Defense Edge Badge**: Games where pick direction aligns with defensive matchup (blue badge)

### Player Props Streak Tracker (Jan 2026)
-   **Separate Tab**: /props route with dedicated "Fetch Player Stats" button
-   **Elite 10 Section**: Premium tab showing top 10 unique players with highest streaks
    -   Golden glow styling with trophy badge
    -   Card-based layout with rank numbers 1-10
    -   Shows L5, L10, L20 hit rates, defensive rank, and AI projection
-   **Injury Filtering**: Automatically excludes injured, questionable, doubtful, and day-to-day players
    -   Fetches ESPN injury report before processing
    -   Only shows active, healthy players
-   **MANDATORY FILTERS (ALL must pass)**:
    -   Bottom 10 Defense ONLY (ranks 21-30) - favorable matchup required
    -   100% in Last 5 (5/5 games must hit)
    -   90%+ in Last 10 (9/10 or 10/10)
    -   95%+ in Last 20 (19/20 or 20/20)
-   **Bulk Fetching**: Single API call for all player game logs (15-30 seconds vs 5+ minutes)
-   **NBA API Integration**: Uses nba_api for player game logs and team stats
-   **Streak Tracking**: Finds consecutive hits on props (10+ games in a row)
-   **Multiple Thresholds**: Dynamically checks various thresholds per prop (e.g., 8+, 10+, 12+ points)
-   **Prop Types**: Points, Rebounds, Assists, P+R, P+A, R+A, P+R+A, 3PM, Steals, Blocks, Steal+Block
-   **Format**: Full team names, streak as "X / LY" (e.g., "20 / L20")
-   **Defensive Rankings**: Real opponent defensive rankings from NBA API (higher rank = worse defense = easier matchup)
-   **AI Projection**: 100-game Monte Carlo simulation based on player's recent performance
-   **Columns**: Team, Player, Prop, Streak, Def Rank, AI Proj
-   **Mobile Layout**: Card-based responsive design with 3-column stat grid
-   **Tab Navigation**: Elite 10 tab (default) and All Props tab with filters

### Feature Specifications
-   **Sports Scanner**: Fetches NBA, CBB, NFL, CFB, NHL games, stats, and odds to identify qualified TOTALS picks.

## External Dependencies
-   **Sports Data**:
    -   ESPN API (team statistics, schedules)
    -   Bovada (betting lines via The Odds API)
    -   The Odds API (alternate lines)
    -   NBA API (nba_api package for player stats and game logs)
-   **Communication**:
    -   Discord Webhooks (automated notifications)
