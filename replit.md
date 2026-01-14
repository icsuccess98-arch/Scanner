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
    -   **Pinnacle EV Comparison**: Fetches Pinnacle odds alongside Bovada to calculate Expected Value (EV). EV formula: `(p_true * decimal_payout) - 1` where `p_true` is Pinnacle's implied probability. MIN_EV_THRESHOLD = 1.0% - picks with EV below 1% are excluded; picks without Pinnacle data (NULL EV) are allowed through.
    -   **Result Checking**: Automatic result checking refreshes approximately 2.5-3.5 hours after game start.
    -   **Automation**: Daily scheduled tasks for fetching games, stats, odds, posting picks to Discord, and checking results.

### Advanced Qualification Factors (Backend)
Additional data-driven factors that disqualify picks during game scanning:

1.  **Recent Form Weighting**
    -   Calculates PPG from last 5 games vs season average
    -   Tracks form trending: UP (recent > season + 2pts), DOWN (recent < season - 2pts), or STABLE
    -   Spreads disqualified if betting on a team with declining form AND recent margin doesn't support the pick

2.  **Injury Data Integration**
    -   Single ESPN API call per team (no nested player lookups)
    -   Count-based impact scoring: 1st injured = 2.5 pts, 2nd = 2.0 pts, 3rd+ = 1.0 pts
    -   Thresholds: 3.0 pts (concern), 4.5 pts (significant), 6.0 pts (severe)
    -   Disqualifies OVER picks if either team has significant injuries
    -   Disqualifies spread picks if team being bet on has key injuries

3.  **Sharp Money Detection (Line Movement)**
    -   Stores opening line when first fetched from Bovada (both totals and spreads)
    -   Compares current line to opening line to detect movement
    -   Sharp movement threshold: 1.5+ points
    -   `SHARP_AGREES`: Line moved in direction of our pick (confirmation)
    -   `SHARP_DISAGREES`: Line moved against our pick (disqualification trigger)
    -   Picks disqualified when sharp money moves against the model's direction
    -   Works for both totals (O/U directions) and spreads (HOME/AWAY directions)

4.  **Spread Sign Validation (SpreadValidator)**
    -   Cross-references spread signs against moneyline odds
    -   Detects sign mismatches (e.g., spread says favorite but ML says underdog)
    -   Auto-corrects invalid spreads by flipping the sign
    -   Logs all corrections for audit trail

5.  **Strength of Schedule Factor**
    -   `calculate_sos_factor()` compares opponent's PPG allowed to league average
    -   Returns multiplier (>1 = tough schedule, <1 = easy schedule)
    -   Available for future projection adjustments

6.  **Vig-Adjusted Edge Calculation (VigCalculator)**
    -   Removes bookmaker vig from raw edge calculations for accurate edge assessment
    -   Lookup table based on actual odds: -110 = 4.76% reduction, -115 = 5.5%, -120 = 6.5%, etc.
    -   Plus money lines get smaller reductions (~2%), heavy favorites get larger reductions (up to 10%)
    -   Multiplier always clamped to ≤1.0 to prevent edge inflation
    -   Used by unified_spread_qualification to enforce edge threshold BEFORE margin/form/injury checks

7.  **Bulletproof Pre-Send Validation (BulletproofPickValidator)**
    -   Final validation layer before picks are posted to Discord
    -   Runs 7 checks on every pick: Edge threshold, Model qualification, Historical qualification, EV non-negative (NULL allowed), Injury validation, Game status, Spread validation
    -   Confidence tier ranking: SUPERMAX (edge 12+, EV 3%+, history 70%), HIGH (edge 10+, EV 1.5%+, history 65%), MEDIUM (edge 8+, EV 0.5%+, history 60%), LOW (edge 6+, history 55%)
    -   TOP 5 picks filter out NONE tier (picks that don't meet minimum thresholds)
    -   Lock of the Day badge shows actual confidence tier instead of hardcoded "SUPERMAX"
    -   Detailed logging shows passed/rejected picks with reasons
    -   Test endpoint: `/api/deep_test` runs 45 tests across 8 layers

8.  **Timezone Validation (History Page)**
    -   Game start times stored as UTC from Odds API
    -   Correctly converts UTC to Eastern Time for upcoming/past game separation
    -   6 tests verify: past game detection, future game detection, UTC-ET offset (4-5h), naive datetime handling, edge cases (1 min ago, 1 hour ahead)

9.  **Historical Betting Lines Service** (Jan 2026)
    -   Fetches actual Vegas closing lines from The Odds API historical endpoint
    -   Calculates true ATS (Against The Spread) hit rates using actual lines
    -   Calculates O/U hit rates against actual historical totals
    -   **ATS Cover Formula**: `spread_result = actual_margin + closing_spread; covered = spread_result > 0`
    -   **Push Handling**: Pushes (spread_result == 0 or actual_total == closing_line) are excluded from hit rate calculations
    -   Requires paid API tier; gracefully falls back to ESPN data + current line comparison when 401 returned
    -   Results cached with 12-hour TTL to protect API quota
    -   Test endpoint: `/api/test_historical_lines?team=X&league=Y&direction=O|U`

10. **Bulletproof Current Line System** (Jan 2026)
    -   NO PAID API NEEDED - Uses free ESPN data + current Vegas lines
    -   `BulletproofCurrentLineCalculator` class applies current line to past game results
    -   Logic: "If today's spread/total existed for last 10 games, how often would team have covered/hit?"
    -   **Push Exclusion**: Pushes (exact line matches within 0.5 pts) excluded from calculations
    -   **Confidence Tiers**: SUPERMAX (70%+), HIGH (65%+), MEDIUM (60%+), LOW (<60%)
    -   **SUPERMAX ONLY**: History qualification requires 70%+ hit rate (SUPERMAX tier only)
    -   Both totals AND spreads must have at least one team with 70%+ hit rate to qualify
    -   League-specific minimum games: NBA/CBB/NHL=8, NFL/CFB=4
    -   Functions: `calculate_ou_hit_rate()`, `calculate_spread_cover_rate()` both use bulletproof formulas

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
    -   **Separate qualification tracking**: Uses `spread_history_qualified` column (independent from totals `history_qualified`)
    -   Historical qualification: HOME favorites need 85% margin threshold; AWAY underdogs must have positive margin
    -   Alt spread selection: Best alternate with odds -180 or better

3.  **Away Favorite + O/U** - Model 3 (User record: 51-14)
    -   Premium model: Away team is favorite (spread_line > 0) AND O/U meets edge threshold
    -   Stricter historical requirement: 70%+ away team O/U hit rate (vs 60% base)
    -   Must also pass standard totals qualification (edge + base history)
    -   Receives +2 weighted bonus in TOP 5 ranking
    -   High-confidence plays when road favorite + totals align

4.  **NBA Away Favorite 1H Money Line** - Model 4 (Implemented)
    -   NBA games where away team is the favorite (spread_line > 0)
    -   Take 1st Half money line on the away favorite
    -   Uses The Odds API market key: `h2h_h1`
    -   Historical qualification: 65%+ away team 1H win rate (last 15-20 games)
    -   H2H qualification: 60%+ if 5+ games exist between the teams
    -   Uses ESPN event summary for period-by-period linescore data
    -   Displayed in TOP 5 with orange styling to distinguish from other models

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