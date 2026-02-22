# Trading Systems Project

## Overview
This project develops and manages trading systems, focusing on a Sports Betting Calculator for TOTALS (Over/Under) betting and a Stocks App based on The Strat methodology. The Sports Betting Calculator analyzes sports data from ESPN and Bovada to identify profitable Over/Under betting opportunities with strict edge thresholds, emphasizing advanced analytics for NBA and CBB. The Stocks App identifies specific trading patterns across various timeframes for a watchlist of popular stocks.

## User Preferences
- Keep Discord message format consistent - never change formats
- Bovada-style team names (short nicknames, no mascots)
- SUPERMAX = highest absolute edge across all qualified picks
- Primary markets: Point spreads and Moneylines (Totals page separate)
- Alt lines max -180 odds floor
- Only make changes to the `replit.nix` and `.replit` files if it is absolutely necessary.
- Do not make changes to files in the `archive` folder.
- Header displays "Seven Thirty SPORTS" logo with basketball icon, Pacifico font, and purple gradient

## System Architecture

### UI/UX
The application features a jewel-tone color scheme and is optimized for mobile with a sticky action bar and card-style displays. Interactive game cards use hover effects and a golden glow for SUPERMAX picks. Branding includes "Seven Thirty" header in Pacifico font with purple gradient, and league-specific ESPN CDN logos. The Dashboard provides edge analysis and a 52-week bankroll builder. Game cards specifically display TOTALS information, and top picks feature color-coded edge bars and a 5-star confidence rating. An "Away Favorite" golden badge is used for confidence boosts. The interface is inspired by the Pikkit app for a gamified, mobile-first experience, including dynamic team logos, real-time records, conference standings, and bet type tabs.

**Premium Animations (Jan 2026):**
- Animated Numbers: `animateNumber()` function with smooth 600ms easeOut counting effect
- League Tabs: Hover lift effect, active pulse glow (tabPulse 2s), count badge pop animation (countPop 0.4s)
- Animated Badges: SUPERMAX golden glow (superMaxGlow 1.5s), Best Bet shimmer effect (badgeShimmer 3s), RLM pulse (rlmPulse 2s), Sharp Money gradient styling

**VSIN Betting Action Component (Jan 2026):**
- Displays on each game card when VSIN data is available
- Line Movement: Shows Open → Current spread with color-coded arrows (green=down/favorable, red=up/unfavorable)
- Betting Splits: Tickets % (public bets) and Money % (handle) for each team
- Majority highlighting: Gold color indicates the side with majority action
- Sharp Money Alert: Green lightning bolt badge when money-tickets divergence exceeds 15%
- VSIN badge in header indicates exclusive VSIN.com data source

### Sharp Money & Betting Strategy
The system implements professional sharp money detection based on divergence between bet % and money %:
- Sharp money = lower bet % but higher money/handle % (e.g., 40% bets but 70% money = sharp side)
- Divergence thresholds: 10%+ moderate, 20%+ strong, 30%+ extreme
- RLM (Reverse Line Movement) = line moves AGAINST where majority of PUBLIC BETS (tickets) are
  - Uses bets/tickets % (public action) to determine the "public side" - NOT handle/money %
  - Sharp money (handle) is the CAUSE of the line moving against public consensus
  - Unified logic across NBA, CBB, and Tennis (Feb 2026)
  - Threshold: >=54% bets to establish public majority, fallback to 60% money if bets unavailable
  - Example: 68% public bets on Team A, but line moves toward Team B = RLM on Team B
- Stagnant line warning: Heavy money (65%+) but line hasn't moved = Vegas resisting

**Betting Checklist (6 criteria):**
1. Opening line confirmed
2. Current line validated
3. Money, stats, and line movement align
4. Situational factors clear (no B2B fatigue)
5. No trap signals detected
6. Spread size acceptable (<10 points preferred)

**Non-Negotiable Filters (Pre-Analysis Elimination):**
- Avoid bottom-tier teams with poor L10 records (shown as "Fade" teams - bet against, not with)
- Prefer teams with momentum (Hot Teams = 8+ L10 wins)
- Home-court advantage STRONGLY favored (Remaining Teams shows ONLY home teams)
- Rarely play spreads above -10
- Recent performance (L3-L5) matters more than season-long narratives

**CBB Daily Slate Analysis Categories:**
1. **Cold Teams (L10)** - Teams with ≤3 L10 wins
2. **Hot Teams (L10)** - Teams with 8+ wins in last 10 games
3. **Bad Defense (L5)** - Teams with defensive efficiency > 105 (KenPom adj_d)
4. **Remaining Teams** - Home teams with 5+ L10 wins (home-court advantage filter applied)

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
    -   KenPom API (CBB_API_KEY) - Official KenPom API for CBB advanced metrics:
        - Authentication: Bearer token in Authorization header
        - Endpoints:
          - `https://kenpom.com/api.php?endpoint=ratings&y=2026` - Team efficiency ratings (AdjOE, AdjDE, AdjEM, Tempo, SOS)
          - `https://kenpom.com/api.php?endpoint=four-factors&y=2026` - Four Factors data (eFG%, TOV%, ORB%, FT Rate with rankings)
        - Four Factors API fields: eFG_Pct, RankeFG_Pct, TO_Pct, RankTO_Pct, OR_Pct, RankOR_Pct, FT_Rate, RankFT_Rate (offense) and D-prefixed versions for defense
        - Used for: Top 25 detection, bad defense identification (adj_d > 105), Key Metrics table (KenPom Rank, Adj O/D/EM, Tempo, Four Factors)
        - All data cached daily to minimize API calls
    -   TeamRankings.com (matchup pages)
    -   Cleaning the Glass (CTG) (NBA advanced stats)
    -   VSIN.com (replaces WagerTalk - cookie-based authentication):
        - Line Tracker: Open and current spread lines from DraftKings
        - Betting Splits: Tickets % (bets) and Handle % (money)
        - Sharp money detection based on tickets vs handle divergence
        - Cookies stored in `vsin_cookies.json` (expires March 2026)
    -   Covers.com (H2H, ATS records)
    
### Bovada Filtering (Jan 2026)
Games are filtered to only show matchups available on Bovada via The Odds API:
- `get_bovada_games(league)` fetches Bovada game list with 10-minute cache
- `is_bovada_game()` uses fuzzy matching to filter displayed games
- Ensures users only see games they can actually bet on

### Pre-Game Stats Persistence (Jan 2026)
When Covers.com data is available for a game (before it starts), all stats are captured and saved to the database:
- ATS records (overall and home/road)
- Last 10 records (overall and ATS)
- Home/Road records

These stats persist even when the game starts (Covers removes live games from their matchup page). The data priority is:
1. Live Covers data (if available)
2. Database pre-game cache (captured before game started)
3. ESPN fallback (limited - no ATS data)

The `pregame_stats_captured` flag on each game indicates whether pre-game data has been saved. Stats update after game completion when fresh Covers data becomes available the next day.

### Team Name Normalization (Jan 2026)
Universal team name matching for KenPom and Covers.com:
- `strip_accents()` function removes diacritics (San José → San Jose)
- `normalize_cbb_team_name()` handles aliases, abbreviations, and case-insensitive matching
- `find_covers_stats()` helper with multiple fallback strategies:
  1. Direct lookup by team name
  2. Normalized lookup via CBB_TEAM_NAME_ALIASES
  3. Accent-stripped lookup
  4. Fuzzy match with "St" → "State" expansion
- CBB_TEAM_NAME_ALIASES covers 100+ variations (UNCA, WIN, CIT, STON, NCST, WAKE, etc.)
- KENPOM_TEAM_SLUGS maps team names to KenPom API slugs

### CBB Stats & Logo Fixes (Feb 2026)
- **KenPom Stats Merge**: Fixed missing PPG/3P%/eFG% by merging CTG data into season dicts BEFORE building result
- **Logo Fallback**: Updated CBB logo lookup to use NCAA generic logo fallback instead of NBA logo
- **3P% Key Update**: Changed 3PT% to 3P% to match KenPom naming convention
- **Adj O/D as PPG**: For CBB, PPG now displays Adj O (offensive efficiency) when traditional PPG unavailable

### VSIN Tennis Integration (Feb 2026)
- `parse_tennis_splits()` in vsin_scraper.py parses VSIN tennis-specific HTML format
- Pipe-separated player names, moneyline odds, handle %, and bets % per match
- Tournament detection via ATP/WTA/ITF text-center cells
- `get_vsin_tennis_data()` fetches splits, applies RLM detection and sharp money analysis (15%+ divergence)
- Tennis route loads VSIN data in parallel with Discord picks and Tennis Abstract stats
- VSIN Betting Action section on tennis page: collapsible, shows sharp/RLM alerts first, split bars for handle/bets
- Supports ~70+ matches per day across ATP, WTA, and ITF tournaments
- **Opening Lines via Modal** (Feb 2026): Game IDs extracted from `data-param2` modal trigger attributes
  - `fetch_tennis_opening_lines()` concurrently fetches opening odds from `/modal/loadmodal.php?modalpage=dksplitsgame&gameid=X`
  - `detect_tennis_rlm()` detects reverse line movement in tennis moneylines:
    - RLM = line shortens for player with MINORITY public bets (tickets), requires actual line movement
    - Uses bets (tickets %) for RLM detection - line moving against public consensus
    - Sharp money (high handle % from few bets) is the CAUSE of the line movement
    - Example: Player opens +109, moves to -126 with only 32% bets but 82% handle = RLM
  - Template shows "Open → Current" line movement with color-coded arrows (green=shortened, red=lengthened)

-   **Stocks Data**:
    -   yfinance (Yahoo Finance)
-   **Communication**:
    -   Discord Webhooks