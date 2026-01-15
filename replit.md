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
    -   **Qualification Requirements**: Edge threshold + Direction set + No star player injuries
    -   **Historical O/U Performance**: Uses last 15 games O/U hit rate to strengthen picks
    -   **Injury Disqualification**: Uses RotoWire to check star player injuries
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
    -   **Parallel Processing**: ThreadPoolExecutor for batch injury checks.
    -   **Response Compression**: Flask-Compress for gzip/deflate compression.
-   **Professional Betting Calculators (Jan 2026)**:
    -   **ProVigCalc**: Standard vig percentage calculation (~4.76% for -110/-110)
    -   **KellyCalculator**: Kelly Criterion for optimal bet sizing (0.25 fractional, 5% max)
    -   **RestDayCalculator**: B2B fatigue modeling (NBA -4.0, NHL -2.5 for back-to-backs)
    -   **ConfidenceTierCalculator**: ELITE (12+), HIGH (10+), MEDIUM (8+), LOW (3+) tiers
    -   **WeatherCalculator**: NFL/CFB outdoor game weather impact on totals
    -   **PaceCalculator**: Pace/tempo analysis for OVER/UNDER tendency
-   **TOP 5 Picks Display Enhancements**:
    -   **Confidence Tier Badges**: Color-coded (ELITE=green, HIGH=light green, MEDIUM=amber, LOW=red)
    -   **Kelly %**: Recommended bet size based on edge probability
    -   **Rest Impact**: Shows fatigue impact when applicable
    -   **Stars/Bars/Away Fav**: Preserved existing functionality

### Code Optimization (Jan 2026)
-   **Spread Code Removed**: All spread-related processing logic removed (334 lines)
-   **1H ML Model Removed**: Model 4 (NBA 1st half moneyline) completely removed
-   **File Size**: Reduced from 9066 to 8732 lines
-   **Focus**: Pure TOTALS (Over/Under) functionality only

### Modular Architecture (Jan 2026)
-   **calculators/**: Extracted professional betting calculators (ProVigCalc, Kelly, Pace, Weather, RestDay, ConfidenceTier)
-   **services/**: Background scheduler, line movement tracking, edge calculation
-   **Background Scheduler**: Auto-refreshes dashboard cache every 5 minutes (no UI blocking)
-   **Line Movement Service**: Real-time tracking with steam move alerts (>1.5 point moves in 15 min)

### CBB Sample Size Filter (Jan 2026)
-   **MIN_GAMES_PLAYED**: CBB requires 15+ games for reliable PPG statistics
-   **Rationale**: Many CBB teams have unreliable early-season stats with <10 games

### Line Movement Dashboard (Jan 2026)
-   **Heat Map Display**: Color-coded movement indicators (green=favorable, red=unfavorable, gray=stable)
-   **Steam Alerts**: Automatic detection of rapid line movements (>1.5 points in 15 minutes)
-   **Real-Time Updates**: Auto-refresh every 30 seconds when panel is open
-   **Toggle Button**: Click "Line Movement" button to show/hide the tracker

### Feature Specifications
-   **Sports Scanner**: Fetches NBA, CBB, NFL, CFB, NHL games, stats, and odds to identify qualified TOTALS picks.

## External Dependencies
-   **Sports Data**:
    -   ESPN API (team statistics, schedules)
    -   Bovada (betting lines via The Odds API)
    -   The Odds API (alternate lines)
    -   RotoWire.com (injury reports)
-   **Communication**:
    -   Discord Webhooks (automated notifications)
