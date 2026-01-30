-- ============================================================================
-- DATABASE MIGRATION FOR 730 SPORTS APP
-- Adds 29 new columns for betting action tracking and RLM detection
-- ============================================================================

-- Betting action lines (WagerTalk data)
ALTER TABLE game ADD COLUMN opening_spread FLOAT;
ALTER TABLE game ADD COLUMN opening_total FLOAT;
ALTER TABLE game ADD COLUMN closed_spread FLOAT;
ALTER TABLE game ADD COLUMN closed_total FLOAT;
ALTER TABLE game ADD COLUMN closed_spread_odds VARCHAR(10);
ALTER TABLE game ADD COLUMN closed_total_odds VARCHAR(10);
ALTER TABLE game ADD COLUMN current_spread FLOAT;
ALTER TABLE game ADD COLUMN current_total FLOAT;
ALTER TABLE game ADD COLUMN game_started BOOLEAN DEFAULT 0;

-- Betting percentages (from WagerTalk)
ALTER TABLE game ADD COLUMN away_tickets_pct FLOAT;
ALTER TABLE game ADD COLUMN home_tickets_pct FLOAT;
ALTER TABLE game ADD COLUMN away_money_pct FLOAT;
ALTER TABLE game ADD COLUMN home_money_pct FLOAT;
ALTER TABLE game ADD COLUMN over_tickets_pct FLOAT;
ALTER TABLE game ADD COLUMN under_tickets_pct FLOAT;
ALTER TABLE game ADD COLUMN over_money_pct FLOAT;
ALTER TABLE game ADD COLUMN under_money_pct FLOAT;

-- RLM detection results
ALTER TABLE game ADD COLUMN rlm_detected BOOLEAN DEFAULT 0;
ALTER TABLE game ADD COLUMN rlm_severity VARCHAR(20);
ALTER TABLE game ADD COLUMN rlm_confidence FLOAT;
ALTER TABLE game ADD COLUMN rlm_sharp_side VARCHAR(50);
ALTER TABLE game ADD COLUMN rlm_explanation TEXT;
ALTER TABLE game ADD COLUMN totals_rlm_detected BOOLEAN DEFAULT 0;
ALTER TABLE game ADD COLUMN totals_rlm_severity VARCHAR(20);
ALTER TABLE game ADD COLUMN totals_rlm_confidence FLOAT;
ALTER TABLE game ADD COLUMN totals_rlm_sharp_side VARCHAR(10);
ALTER TABLE game ADD COLUMN totals_rlm_explanation TEXT;

-- ============================================================================
-- POPULATE EXISTING DATA
-- ============================================================================

-- Set opening/closed lines from current lines for existing games
UPDATE game 
SET opening_spread = spread_line,
    closed_spread = spread_line,
    current_spread = spread_line
WHERE spread_line IS NOT NULL 
  AND opening_spread IS NULL;

UPDATE game 
SET opening_total = line,
    closed_total = line,
    current_total = line
WHERE line IS NOT NULL 
  AND opening_total IS NULL;

-- Mark games in the past as started
-- Note: Adjust date comparison based on your timezone
UPDATE game 
SET game_started = 1
WHERE date < DATE('now');

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check new columns exist
-- SQLite:
PRAGMA table_info(game);

-- PostgreSQL:
-- SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'game';

-- Check data populated
SELECT 
    id,
    away_team,
    home_team,
    opening_spread,
    closed_spread,
    current_spread,
    game_started,
    rlm_detected
FROM game 
WHERE date = DATE('now')
LIMIT 5;

-- ============================================================================
-- NOTES
-- ============================================================================

-- 1. Backup your database before running this!
-- 2. For SQLite: Run using: sqlite3 your_database.db < migration.sql
-- 3. For PostgreSQL: Run using: psql -d your_database -f migration.sql
-- 4. Verify all columns were added successfully
-- 5. Restart your app after migration
