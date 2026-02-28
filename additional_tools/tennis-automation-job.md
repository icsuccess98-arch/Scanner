# Tennis Prediction Engine - Cron Automation Setup

## Overview
Automates 730's Locks Tennis Prediction Engine for daily morning analysis and pick delivery with surface-specific intelligence.

## Cron Schedule
```bash
# Run tennis prediction engine every day at 8:30 AM EST (before NBA/NHL)
30 8 * * * /home/icsuccess98/.openclaw/workspace/tools/tennis-daily-automation.sh
```

## Automation Script
See: `/home/icsuccess98/.openclaw/workspace/tools/tennis-daily-automation.sh`

## Process Flow
1. **8:30 AM**: Run tennis prediction engine
2. **Surface Analysis**: Clay/Grass/Hard court specific modeling
3. **Generate**: A-grade picks with +8% EV minimum
4. **Format**: 730's Locks tennis analysis format
5. **Deliver**: To Telegram channel (@getmoneybuybtc)
6. **Archive**: Results for performance tracking

## Tennis-Specific Features
- **Surface Intelligence**: Clay (endurance), Grass (serve power), Hard (balanced)
- **Form Cycle Analysis**: Peak/decline identification with match recency weighting
- **Head-to-Head Regression**: Style matchup modeling with sample size weighting
- **Tournament Context**: Grand Slam vs Masters weight adjustments
- **Elo Integration**: Surface-specific Tennis Abstract Elo ratings

## Performance Tracking
- All picks tracked in `/tennis_predictions_YYYYMMDD.json`
- Surface-specific performance analysis
- Tournament tier ROI tracking
- Player form cycle accuracy monitoring
- H2H prediction validation

## Quality Standards
- **A+ Grade**: +12% EV, 88%+ model agreement, 85%+ surface confidence
- **A Grade**: +8% EV, 82%+ model agreement, 75%+ surface confidence
- **B Grade**: +4% EV, 70%+ model agreement, 60%+ surface confidence
- **PASS**: Below thresholds (no action)

## Data Sources
- **Tennis Abstract**: Historical Elo ratings and player statistics
- **Current Matchups**: Live tournament draws and scheduling
- **Surface Data**: Court-specific performance metrics
- **Market Consensus**: Aggregated betting market intelligence

## Integration Notes
- **4-Brain Methodology**: Elo → Form → H2H → Market
- **Surface Weights**: Clay (return +25%), Grass (serve +35%), Hard (balanced)
- **Kelly Sizing**: Conservative 25% Kelly with 3-unit maximum
- **Premium Pricing**: $199/month specialized tennis tier
- **Discord Integration**: Feeds existing picks validation system

## Specialized Tennis Edge
- Form regression analysis with surface-specific form factors
- Tournament draw position analysis for fatigue modeling
- Surface transition effects (clay to grass season)
- Player motivation modeling for different tournament tiers
- Weather/condition impacts for outdoor tournaments