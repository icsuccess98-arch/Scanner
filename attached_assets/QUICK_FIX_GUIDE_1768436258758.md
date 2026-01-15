# 🚨 QUICK FIX - Database Column Mismatch Error

## Problem
Your app is looking for columns named `home_ou_pct` and `away_ou_pct`, but your database has `home_team_ou_pct` and `away_team_ou_pct`.

## Solution (Choose ONE method)

---

### **METHOD 1: Python Migration Script** (RECOMMENDED - Easiest)

```bash
# Run the Python migration script
python3 migrate_database.py
```

**What it does:**
- ✅ Renames `home_team_ou_pct` → `home_ou_pct`
- ✅ Renames `away_team_ou_pct` → `away_ou_pct`
- ✅ Adds all missing columns
- ✅ Creates performance indexes
- ✅ Verifies everything worked

**Expected output:**
```
🔄 Applying database fixes...
  Renaming home_team_ou_pct → home_ou_pct...
  ✅ Done
  Renaming away_team_ou_pct → away_ou_pct...
  ✅ Done

🔄 Adding missing columns...
  ✅ Added sample_size
  ✅ Added projected_pace
  ...

✅ DATABASE FIXED! Restart your app now.
```

---

### **METHOD 2: SQL Script** (If you prefer psql)

```bash
# Connect to your database
psql $DATABASE_URL

# Then paste this:
ALTER TABLE game RENAME COLUMN home_team_ou_pct TO home_ou_pct;
ALTER TABLE game RENAME COLUMN away_team_ou_pct TO away_ou_pct;

# Exit psql
\q
```

---

### **METHOD 3: Quick Manual Fix** (Fastest for testing)

Just run these two commands:

```bash
psql $DATABASE_URL -c "ALTER TABLE game RENAME COLUMN home_team_ou_pct TO home_ou_pct;"
psql $DATABASE_URL -c "ALTER TABLE game RENAME COLUMN away_team_ou_pct TO away_ou_pct;"
```

---

## After Fixing

```bash
# Restart your app
pkill -f sports_app.py
python sports_app.py

# Should see:
# ✅ Database ready with performance indexes
# Running on http://0.0.0.0:5000/
```

---

## Verification

Visit your app and you should see the dashboard with no errors!

If you still get errors, check:

```bash
# Verify columns exist
psql $DATABASE_URL -c "SELECT column_name FROM information_schema.columns WHERE table_name = 'game' AND column_name LIKE '%ou_pct%';"

# Should show:
# home_ou_pct
# away_ou_pct
```

---

## Why This Happened

Your database was created with the old column names (`home_team_ou_pct`) but the optimized code uses shorter names (`home_ou_pct`).

The migration scripts fix this mismatch and add all the other optimized features:
- ✅ 30-game sample tracking
- ✅ Pace analysis
- ✅ Rest day fatigue
- ✅ Weather impact
- ✅ Simple unit recommendations
- ✅ Performance indexes

---

## Need Help?

If you get any errors during migration, just share the error message and I'll fix it immediately.

Most common issues:
1. **"column does not exist"** - Already renamed, skip to restart
2. **"column already exists"** - Already added, skip to restart
3. **"permission denied"** - Need database admin access

---

**TL;DR: Run `python3 migrate_database.py` then restart your app!** ✅
