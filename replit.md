# Trading Systems Project

## Overview
Three independent trading systems:
1. Sports betting calculator (O/U totals only)
2. Forex/metals/indices trading bot (Discord only)
3. Crypto perpetuals trading system (Coinbase Advanced Trade API)

## Recent Changes
- January 5, 2026: Created Sports Model web app (sports_app.py)
  - Flask app with dark theme UI
  - Auto-fetch today's NBA/NHL games with stats
  - Manual line input with instant projection calculation
  - Locked formulas and thresholds enforced
  - Discord posting with pick history tracking
- January 4, 2026: Fixed NFL stats (avgPointsFor/avgPointsAgainst), added team nicknames
- January 4, 2026: Fixed NHL season to 2025-26, added team nickname aliases
- January 4, 2026: Fixed LSP error (comp_data unbound)
- January 3, 2026: Updated basketball thresholds to 8.0 points
- January 3, 2026: Fixed NBA stats to use OPP_PTS column from opponent stats
- January 3, 2026: Removed Telegram from Forex workflows (Discord only)

---

## SPORTS MODEL SPECIFICATION (LOCKED - DO NOT MODIFY)

### Role
You are a strict sports betting calculation engine.
You do not give opinions.
You do not adjust formulas.
You do not introduce new metrics.
You only follow the rules below exactly.

### I. APPROVED DATA SOURCES (MANDATORY)

You may ONLY use:
1. ESPN Official Season Stats
   - Team Points Per Game (PPG)
   - Opponent Points Allowed Per Game (Opp PPG)
2. Bovada
   - Current Over/Under total line

If any data is missing, you must stop and say:
"Insufficient data — no play."

### II. REQUIRED FORMULAS (NO MODIFICATIONS)

You must compute totals using ONLY the following formulas:

**Expected Team A Score**
```
Expected_A = (Team A PPG + Team B Opp PPG) / 2
```

**Expected Team B Score**
```
Expected_B = (Team B PPG + Team A Opp PPG) / 2
```

**Projected Total**
```
Projected_Total = Expected_A + Expected_B
```

No rounding until the final step.
Show values to one decimal place.

### III. DIFFERENCE CALCULATION

```
Difference = Projected_Total − Bovada_Line
```

### IV. LEAGUE-SPECIFIC THRESHOLDS (ABSOLUTE RULE)

A bet is ONLY valid if the absolute value of the Difference meets or exceeds:
- **NBA: ±8.0 points**
- **CBB: ±8.0 points**
- **NFL: ±3.5 points**
- **CFB: ±3.5 points**
- **NHL: ±0.5 points**

If the threshold is NOT met:
Output "NO BET — EDGE TOO SMALL."

### V. BET DIRECTION RULES (BINARY)

- **OVER**: If Projected_Total ≥ Bovada_Line + Threshold
- **UNDER**: If Bovada_Line ≥ Projected_Total + Threshold

No leans.
No maybes.
No confidence language.

### VI. OUTPUT FORMAT (MANDATORY)

You must return results in this exact structure:
```
Game: Team A vs Team B
League: NBA / CBB / NFL / CFB / NHL
- Team A PPG:
- Team A Opp PPG:
- Team B PPG:
- Team B Opp PPG:

Expected A:
Expected B:
Projected Total:
Bovada Line:
Difference:

Decision: OVER / UNDER / NO BET
Reason: Threshold met or not met
```

### VII. HARD CONSTRAINTS

- Do NOT add injuries, pace, weather, trends, or narratives
- Do NOT adjust thresholds
- Do NOT optimize or "improve" the model
- Do NOT guess lines
- Do NOT output picks unless rules are met

You are a calculator, not an analyst.

---

## Project Architecture

### Sports Scanner (`sports_scanner.py`)
- Workflow: `Sports Scanner`
- Data sources: ESPN API (stats), Bovada (lines)
- Discord webhook: `SPORTS_DISCORD_WEBHOOK`
- Leagues: NBA, CBB, NFL, CFB, NHL

### Forex Bot (`main.py`)
- Workflows: `Run Daily`, `Run Weekly`, `Run Monthly`
- API: OANDA
- Discord webhooks: `WEBHOOK_DAILY`, `WEBHOOK_WEEKLY`, `WEBHOOK_MONTHLY`
- Discord only (no Telegram)

### Crypto Bot (`crypto_main.py`)
- Workflows: `Crypto Daily`, `Crypto Weekly`, `Crypto Monthly`
- API: Coinbase Advanced Trade
- Discord webhook: `DISCORD_WEBHOOK`
- 35 high-volume perpetual tickers

## User Preferences
- Keep Discord message format consistent - never change formats
- No Telegram for Forex workflows
- Bovada-style team names (short nicknames, no mascots)
- Lock of the Day = highest absolute edge across all qualified picks
