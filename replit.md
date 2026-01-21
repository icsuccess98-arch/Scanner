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
    -   **Qualification Requirements**: Edge threshold + Direction set + O/U hit rate filters
    -   **Historical O/U Performance**: ESPN-based hit rate tracking with MANDATORY filters:
        -   L5 100% (5/5 games must hit)
        -   L20 85%+ (17/20 or better)
    -   **Confidence Tiers**: SUPERMAX (≥12 edge), HIGH (≥10), STANDARD (≥8), LOW (<8) - no units displayed
    -   **SUPERMAX**: Highest absolute edge across all qualified TOTALS picks
    -   **TOP 5 Ranking**: Sorted by EDGE only (highest edge = best pick)
    -   **Star Ratings**: Based on edge only (5★ for 12+, 4★ for 10+, 3★ for 8+, 2★ otherwise)
    -   **Alt Lines**: MANDATORY - Uses alternate totals lines from The Odds API (Bovada) only, max -185 odds. Games without alt lines are not qualified.
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
-   **Defense Edge Badge**: Blue (#00BFFF) 🛡️ DEF EDGE badge for favorable defensive matchups (confidence indicator, not a filter)
    -   OVER picks: Shows when facing bottom 10 defense (worst defenses = more points allowed)
    -   UNDER picks: Shows when facing top 10 defense (best defenses = fewer points allowed)
    -   NBA/CBB only (basketball games) - badge adds confidence, not required for qualification
-   **File Size**: ~9,080 lines
-   **Focus**: Pure TOTALS (Over/Under) functionality only with confidence badges:
    1. **Totals Model**: Standard O/U picks with edge thresholds
    2. **Away Favorite Badge**: Games where away team is favorite AND meets O/U threshold (orange badge)
    3. **Defense Edge Badge**: Games where pick direction aligns with defensive matchup (blue badge)

### Player Props Analysis Protocol (Jan 2026) - Joe's Methodology
-   **Separate Tab**: /props route with dedicated "Fetch Player Stats" button
-   **Multi-League Support**: League selector dropdown for NBA, EuroLeague, and EuroCup
    -   NBA: Uses nba_api for player game logs with Bovada lines
    -   EuroLeague/EuroCup: Uses euroleague-api package for European basketball
-   **EDGE CALCULATION**:
    -   `Edge% = (AI_Projection - Prop_Line) / Prop_Line × 100`
    -   AI Projection = 20-game average with defensive adjustment (100-game simulation)
    -   Sorted by: Favorable defense > L20 hit rate > Streak > AI Proj
-   **L20 HIT RATE METHODOLOGY** (Updated Jan 2026):
    -   Primary filter: L20 hit rate 85%+ (17/20 or better)
    -   Consecutive streak tracked for display (e.g., 36/L36 = 36 in a row)
    -   Streak format: X/LY where X = consecutive hits, Y = sample size
-   **PLAY CLASSIFICATION** (Based on L20 hit rate):
    -   **PREMIUM PLAY**: 100% L20 (20/20) - gold glow
    -   **STRONG PLAY**: 95%+ L20 (19/20) - green
    -   **PLAY**: 85%+ L20 (17-18/20) - purple
-   **MANDATORY FILTER**: L20 hit rate 90%+ (18/20 minimum)
    -   Players must have 20+ games of data
    -   Line exists from Bovada (priority) or backup books (betonlineag, lowvig)
    -   No classification badges - only ELITE star for favorable defense matchups
-   **ELITE PICKS**: Bottom 10 defenses (ranks 21-30) = favorable matchups
    -   Picks against worst defenses prioritized at top
    -   Golden ELITE badge for favorable defense matchups
-   **DISQUALIFICATION RULES (Auto-AVOID)**:
    -   L20 hit rate below 85%
    -   Fewer than 20 games of data
    -   No sportsbook line available
-   **Defensive Rank Display**: Shows "Xth" - rank 21-30 = worst defenses (favorable for OVER)
-   **Elite 10 Section**: Top 10 picks by L20 hit rate with defense priority
    -   Favorable defense matchups sorted first
    -   Shows streak display (e.g., "36/L36"), L5/L10/L20 hit rates, Edge%, Def Rank, AI Proj
-   **Display Columns**: Team, Player, Prop, Bovada, Streak, Edge%, Class, Def Rank vs Stat, AI Proj, Trend
-   **Prop Types**: Points, Rebounds, Assists, P+R, P+A, R+A, P+R+A, 3PM, Steals, Blocks, Steal+Block
-   **Mobile Layout**: Card-based responsive design with Edge% and Classification badges
-   **UI Theme**: Royal gold theme with dark background matching dashboard aesthetics

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
