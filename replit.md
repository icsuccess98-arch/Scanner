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

## Separate Applications
This project has two separate apps that run independently on port 5000:
1. **Sports App** (`python sports_app.py`) - Sports betting with Matchup Intelligence
2. **Stocks App** (`python stocks_app.py`) - Stock setups using The Strat methodology

Only one app can run at a time. Configure the workflow to switch between apps.

### Sports App Pages
- **Dashboard (/)**: TOTALS picks with edge qualification filters
- **Spreads (/spreads)**: Full daily slate showing ALL games with spread data (no totals filtering)
- **Bankroll (/bankroll)**: 52-week bankroll builder tracker
- **History (/history)**: Pick history and results tracking

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
    -   **Data Sources**: ESPN Official Season Stats (PPG, Opp PPG), Bart Torvik (CBB only), and Bovada lines.
    -   **CBB Torvik Integration**: For college basketball, uses Bart Torvik advanced analytics:
        -   Adjusted Offensive/Defensive Efficiency (Adj O/D)
        -   Tempo (possessions per game)
        -   Barthag and Rankings
        -   Projection formula uses adjusted efficiency + tempo for more accurate CBB totals
    -   **Matchup Intelligence Feature** (Jan 2026): Advanced analytics dropdown on game cards showing:
        -   **Edge Summary** (NEW): Overall edge calculation with category breakdowns:
            -   Overall Edge display with team advantage (+X.X | Team)
            -   NET RATING bar - Net rating differential
            -   REBOUNDING bar - Rebounding advantage
            -   TURNOVERS bar - Turnover differential  
            -   SHOOTING EFF bar - Offensive efficiency edge
            -   DEFENSIVE EFF bar - Defensive efficiency edge
            -   Visual bars showing direction/magnitude of advantage
        -   Power Rating comparison with team ranks, percentiles, and colored bars
        -   Offensive/Defensive Efficiency comparisons (pts/100 possessions)
        -   Efficiency Comparison section with flip button showing offense vs defense matchups
        -   **Shooting Profile**: eFG%, 3PT%, 3PM/Game, PPG with Season + L5 columns
        -   **Ball Control**: TOV%, TOV/Game with Season + L5 columns
        -   **Rebounding**: ORB/Game, DRB/Game with Season + L5 columns
        -   **Pace & Free Throws**: FT Rate, FT% with Season + L5 columns + Tempo display
        -   **Season vs L5**: All stats show both season averages and last 5 games performance
        -   **Strength of Schedule (SOS)**: Schedule difficulty rankings for context
        -   **Analyst Insight**: AI-generated summary of matchup advantages
        -   **Live Data Integration** (Jan 2026): Multiple data sources for bulletproof stats:
            -   **TeamRankings.com Matchup Pages** (PRIMARY): Scrapes 4 FREE matchup pages per game:
                -   `/stats` - Basic stats (PPG, rebounds, assists, shooting %, etc.) ~39 stats per team
                -   `/efficiency` - Off/Def Efficiency, eFG%, Turnover rates, Rebound %
                -   `/splits` - Season + Last 3 Games performance comparison ~30 L3 stats per team
                -   `/power-ratings` - SOS Rank, Predictive Rating, Last 10 Rating, Luck Rating
                -   URL format: `teamrankings.com/{league}/matchup/{away}-{home}-{date}/{page}`
                -   TOTAL: 74+ away stats, 85+ home stats scraped per matchup
            -   **NBA.com API** (nba_api): L5 game stats via TeamGameLog endpoint with 30-min cache
            -   **Cleaning the Glass (CTG)**: FREE tier Four Factors scraper for NBA:
                -   Scrapes all 30 NBA teams' four factors from ctg team pages
                -   Returns eFG%, TOV%, ORB%, FT Rate for offense and defense WITH league ranks
                -   PPP (Points Per Possession) and Opp PPP for scoring efficiency
                -   All values include "#X" rank prefix (e.g., "#7 13.9%")
            -   Cache layer prevents API rate limiting and speeds up page loads
        -   **15 Key Metrics** (Jan 2026): All metrics have orange highlighting with border-left accent:
            -   **Scoring**: PPP, Opp PPP (with ranks from CTG)
            -   **Rebounding**: ORB%, DRB% (with ranks from CTG)
            -   **Ball Control**: TOV%, F-TOV% (with ranks from CTG)
            -   **Efficiency**: O Eff, D Eff (from TeamRankings)
            -   **Shooting**: eFG%, Opp eFG% (with ranks from CTG)
            -   **3PT**: 3PT%, Opp 3PT% (from TeamRankings)
            -   **Free Throws**: FT Rate, Opp FT Rate (with ranks from CTG)
            -   **Schedule**: SOS Rank (from TeamRankings)
            -   **Footer Stats** (Jan 2026): Additional context in table footer:
                -   H2H L10: Head-to-head last 10 W/L from Covers.com matchup pages (e.g., "6-4" with leader name)
                -   ATS L10: Against the spread record from Covers.com (e.g., "4-6-0" with leader name)
                -   Record: Team W/L records (e.g., "23-23" vs "11-36")
                -   Edge Count: X/15 format showing statistical edges
        -   **Pikkit-Style Game Cards** (Jan 2026): Gamified mobile-first interface inspired by Pikkit app:
            -   Header: Team names with PPG stats on each side, date/time in center
            -   Bet Type Tabs: Spread | Total | Moneyline (clickable tabs with active state)
            -   Placeholder Mode: Cards load instantly; tap "Model Breakdown" for live data
            -   Performance: Avoids 10-20 second API delays by loading data on-demand
        -   **Betting Checklist** (Jan 2026): Gamified Pikkit-style betting action display:
            -   Data source: ScoresAndOdds main page + consensus-picks page (via Playwright headless browser)
            -   Scrapes BOTH main page (opening/current lines) AND consensus page (bets/money percentages)
            -   Auto-refresh: Updates every 30 minutes, force refreshes 2 hours before game time
            -   **Gamified UI Design** (Pikkit-inspired):
                -   LIVE badge with purple gradient header
                -   Line Movement card with Open/Current display and movement indicator
                -   Bet % bars with team names and colored bars (red/green) showing percentage split
                -   Money % bars with team names and colored bars (amber/purple) for handle split
                -   Quick checklist grid (2x2): Edge Leader, RLM Check, Trap Game, Sharp Money
                -   Status badge at bottom ("LOOKING GOOD" or "PROCEED WITH CAUTION")
            -   Sharp money detection: When money % diverges from bets % by ≥10%
            -   Lopsided betting (60%+) indicates public heavily favoring one side
            -   Visual indicators: Colored bars, emojis, gradient backgrounds
        -   Only shows for CBB/NBA games with advanced data available
        -   Mobile-optimized responsive design for iPhone
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

### Stock Setups Scanner (Jan 2026)
-   **The Strat Methodology**: Implements Rob Smith's Strat trading patterns
-   **Mobile App**: /stocks route with responsive mobile-first design
-   **Timeframes**: Daily, Weekly, Monthly tabs (matches Discord scanner)
-   **Pattern Recognition**:
    -   Inside Bars (1): Current bar contained within previous bar's range
    -   Outside Bars (3): Current bar engulfs previous bar's range
    -   2U/2D: Directional continuation patterns (2U = up, 2D = down)
    -   Failed 2U/2D: Reversal patterns when 2U closes red or 2D closes green
    -   Double Inside: Two consecutive inside bars
    -   FTFC (Full Time Frame Continuity): All timeframes aligned bullish or bearish (daily only)
    -   A++ Setups: Inside bar + FTFC alignment (highest probability, daily only)
-   **Stock Watchlist**: 40 popular stocks including SPY, QQQ, mega caps, and growth stocks
-   **Data Source**: yfinance (Yahoo Finance) for real-time stock data
-   **Category Tabs**: All, A++, Inside (1), Outside (3), 2U/2D, Failed 2, FTFC
-   **Timeframe Handling**: FTFC and A++ tabs hidden for weekly/monthly (only calculated for daily)
-   **Bottom Navigation**: Links to Home, Stocks, Bankroll, History pages

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
