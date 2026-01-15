-- ============================================================================
-- CRITICAL DATABASE FIX - Run This Immediately!
-- ============================================================================
-- This fixes the column name mismatch causing your error

-- PART 1: Rename columns to match the code
-- ============================================================================

-- Fix the O/U percentage columns
ALTER TABLE game RENAME COLUMN home_team_ou_pct TO home_ou_pct;
ALTER TABLE game RENAME COLUMN away_team_ou_pct TO away_ou_pct;

-- PART 2: Add any missing columns that the optimized code needs
-- ============================================================================

-- Sample size (for 30-game validation)
ALTER TABLE game ADD COLUMN IF NOT EXISTS sample_size INTEGER DEFAULT 0;

-- Pace metrics
ALTER TABLE game ADD COLUMN IF NOT EXISTS away_pace FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS home_pace FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS projected_pace FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS pace_impact FLOAT DEFAULT 0.0;

-- Rest/fatigue
ALTER TABLE game ADD COLUMN IF NOT EXISTS days_rest_away INTEGER;
ALTER TABLE game ADD COLUMN IF NOT EXISTS days_rest_home INTEGER;
ALTER TABLE game ADD COLUMN IF NOT EXISTS is_back_to_back_away BOOLEAN DEFAULT FALSE;
ALTER TABLE game ADD COLUMN IF NOT EXISTS is_back_to_back_home BOOLEAN DEFAULT FALSE;
ALTER TABLE game ADD COLUMN IF NOT EXISTS travel_distance FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS rest_impact FLOAT DEFAULT 0.0;

-- Weather (NFL/CFB)
ALTER TABLE game ADD COLUMN IF NOT EXISTS weather_temp FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS weather_wind FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS weather_precip VARCHAR(20);
ALTER TABLE game ADD COLUMN IF NOT EXISTS weather_impact FLOAT DEFAULT 0.0;
ALTER TABLE game ADD COLUMN IF NOT EXISTS is_dome BOOLEAN DEFAULT FALSE;

-- Odds
ALTER TABLE game ADD COLUMN IF NOT EXISTS bovada_over_odds INTEGER;
ALTER TABLE game ADD COLUMN IF NOT EXISTS bovada_under_odds INTEGER;
ALTER TABLE game ADD COLUMN IF NOT EXISTS pinnacle_over_odds INTEGER;
ALTER TABLE game ADD COLUMN IF NOT EXISTS pinnacle_under_odds INTEGER;
ALTER TABLE game ADD COLUMN IF NOT EXISTS total_ev FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS vig_pct FLOAT;

-- Qualification
ALTER TABLE game ADD COLUMN IF NOT EXISTS confidence_tier VARCHAR(10);
ALTER TABLE game ADD COLUMN IF NOT EXISTS recommended_units FLOAT;

-- Totals specific
ALTER TABLE game ADD COLUMN IF NOT EXISTS projected_total FLOAT;
ALTER TABLE game ADD COLUMN IF NOT EXISTS true_edge FLOAT;

-- PART 3: Add performance indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_game_date_qualified 
ON game(date, is_qualified) WHERE is_qualified = true;

CREATE INDEX IF NOT EXISTS idx_game_league_date 
ON game(league, date DESC);

CREATE INDEX IF NOT EXISTS idx_game_confidence 
ON game(confidence_tier, true_edge DESC) WHERE is_qualified = true;

CREATE INDEX IF NOT EXISTS idx_game_teams_date 
ON game(away_team, home_team, date DESC);

CREATE INDEX IF NOT EXISTS idx_pick_date_result 
ON pick(date DESC, result);

CREATE INDEX IF NOT EXISTS idx_pick_tier_result 
ON pick(confidence_tier, result) WHERE result IN ('W', 'L');

-- Update statistics for query planner
ANALYZE game;
ANALYZE pick;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Check that columns exist
SELECT 
    'Columns fixed' as status,
    COUNT(*) as count
FROM information_schema.columns 
WHERE table_name = 'game' 
  AND column_name IN ('home_ou_pct', 'away_ou_pct', 'sample_size', 
                      'projected_pace', 'pace_impact', 'rest_impact',
                      'confidence_tier', 'recommended_units');

-- Should show: "Columns fixed" | 8+

-- Check indexes
SELECT 
    'Indexes created' as status,
    COUNT(*) as count
FROM pg_indexes 
WHERE tablename = 'game' 
  AND indexname LIKE 'idx_game_%';

-- Should show: "Indexes created" | 4+

-- Done! Restart your app and it should work.
