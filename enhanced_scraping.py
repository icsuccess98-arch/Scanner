"""
ENHANCED WEB SCRAPING MODULE
============================

Bulletproof implementations for:
1. Covers.com - Matchup section for current day's slate
2. ScoresAndOdds.com - Opening, current, and live line movement
3. College Basketball team logos (hardcoded from official sources)

Improvements:
- Parallel scraping for speed
- Intelligent caching
- Error handling with fallbacks
- Opening line preservation
- Real-time line movement tracking
- Closing line capture

"""

import requests
from bs4 import BeautifulSoup
import re
import time
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import logging
from functools import lru_cache
import json

logger = logging.getLogger(__name__)


# ============================================================
# COLLEGE BASKETBALL TEAM LOGOS (HARDCODED)
# ============================================================

CBB_TEAM_LOGOS = {
    # ACC
    'Duke': 'https://a.espncdn.com/i/teamlogos/ncaa/500/150.png',
    'North Carolina': 'https://a.espncdn.com/i/teamlogos/ncaa/500/153.png',
    'Virginia': 'https://a.espncdn.com/i/teamlogos/ncaa/500/258.png',
    'Louisville': 'https://a.espncdn.com/i/teamlogos/ncaa/500/97.png',
    'Syracuse': 'https://a.espncdn.com/i/teamlogos/ncaa/500/183.png',
    'Miami': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2390.png',
    'Florida State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/52.png',
    'Clemson': 'https://a.espncdn.com/i/teamlogos/ncaa/500/228.png',
    'NC State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/152.png',
    'Wake Forest': 'https://a.espncdn.com/i/teamlogos/ncaa/500/154.png',
    'Virginia Tech': 'https://a.espncdn.com/i/teamlogos/ncaa/500/259.png',
    'Pittsburgh': 'https://a.espncdn.com/i/teamlogos/ncaa/500/221.png',
    'Georgia Tech': 'https://a.espncdn.com/i/teamlogos/ncaa/500/59.png',
    'Boston College': 'https://a.espncdn.com/i/teamlogos/ncaa/500/103.png',
    'Notre Dame': 'https://a.espncdn.com/i/teamlogos/ncaa/500/87.png',
    
    # Big Ten
    'Michigan State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/127.png',
    'Michigan': 'https://a.espncdn.com/i/teamlogos/ncaa/500/130.png',
    'Ohio State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/194.png',
    'Illinois': 'https://a.espncdn.com/i/teamlogos/ncaa/500/356.png',
    'Wisconsin': 'https://a.espncdn.com/i/teamlogos/ncaa/500/275.png',
    'Purdue': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2509.png',
    'Indiana': 'https://a.espncdn.com/i/teamlogos/ncaa/500/84.png',
    'Iowa': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2294.png',
    'Maryland': 'https://a.espncdn.com/i/teamlogos/ncaa/500/120.png',
    'Minnesota': 'https://a.espncdn.com/i/teamlogos/ncaa/500/135.png',
    'Penn State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/213.png',
    'Rutgers': 'https://a.espncdn.com/i/teamlogos/ncaa/500/164.png',
    'Northwestern': 'https://a.espncdn.com/i/teamlogos/ncaa/500/77.png',
    'Nebraska': 'https://a.espncdn.com/i/teamlogos/ncaa/500/158.png',
    
    # Big 12
    'Kansas': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2305.png',
    'Baylor': 'https://a.espncdn.com/i/teamlogos/ncaa/500/239.png',
    'Texas': 'https://a.espncdn.com/i/teamlogos/ncaa/500/251.png',
    'Texas Tech': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2641.png',
    'Kansas State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2306.png',
    'Oklahoma': 'https://a.espncdn.com/i/teamlogos/ncaa/500/201.png',
    'Oklahoma State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/197.png',
    'West Virginia': 'https://a.espncdn.com/i/teamlogos/ncaa/500/277.png',
    'TCU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2628.png',
    'Iowa State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/66.png',
    
    # SEC
    'Kentucky': 'https://a.espncdn.com/i/teamlogos/ncaa/500/96.png',
    'Tennessee': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2633.png',
    'Auburn': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2.png',
    'Alabama': 'https://a.espncdn.com/i/teamlogos/ncaa/500/333.png',
    'Arkansas': 'https://a.espncdn.com/i/teamlogos/ncaa/500/8.png',
    'LSU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/99.png',
    'Florida': 'https://a.espncdn.com/i/teamlogos/ncaa/500/57.png',
    'Missouri': 'https://a.espncdn.com/i/teamlogos/ncaa/500/142.png',
    'Mississippi State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/344.png',
    'Ole Miss': 'https://a.espncdn.com/i/teamlogos/ncaa/500/145.png',
    'South Carolina': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2579.png',
    'Georgia': 'https://a.espncdn.com/i/teamlogos/ncaa/500/61.png',
    'Vanderbilt': 'https://a.espncdn.com/i/teamlogos/ncaa/500/238.png',
    'Texas A&M': 'https://a.espncdn.com/i/teamlogos/ncaa/500/245.png',
    
    # Pac-12 (if still relevant)
    'Arizona': 'https://a.espncdn.com/i/teamlogos/ncaa/500/12.png',
    'UCLA': 'https://a.espncdn.com/i/teamlogos/ncaa/500/26.png',
    'USC': 'https://a.espncdn.com/i/teamlogos/ncaa/500/30.png',
    'Oregon': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2483.png',
    'Arizona State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/9.png',
    'Colorado': 'https://a.espncdn.com/i/teamlogos/ncaa/500/38.png',
    'Washington': 'https://a.espncdn.com/i/teamlogos/ncaa/500/264.png',
    'Stanford': 'https://a.espncdn.com/i/teamlogos/ncaa/500/24.png',
    'Utah': 'https://a.espncdn.com/i/teamlogos/ncaa/500/254.png',
    'Oregon State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/204.png',
    'Washington State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/265.png',
    'California': 'https://a.espncdn.com/i/teamlogos/ncaa/500/25.png',
    
    # Big East
    'Villanova': 'https://a.espncdn.com/i/teamlogos/ncaa/500/222.png',
    'Creighton': 'https://a.espncdn.com/i/teamlogos/ncaa/500/156.png',
    'Xavier': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2752.png',
    'Providence': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2507.png',
    'Marquette': 'https://a.espncdn.com/i/teamlogos/ncaa/500/269.png',
    'Seton Hall': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2550.png',
    'Butler': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2086.png',
    'UConn': 'https://a.espncdn.com/i/teamlogos/ncaa/500/41.png',
    'St. Johns': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2599.png',
    'Georgetown': 'https://a.espncdn.com/i/teamlogos/ncaa/500/46.png',
    'DePaul': 'https://a.espncdn.com/i/teamlogos/ncaa/500/305.png',
    
    # Other Major Programs
    'Gonzaga': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2250.png',
    'Houston': 'https://a.espncdn.com/i/teamlogos/ncaa/500/248.png',
    'Memphis': 'https://a.espncdn.com/i/teamlogos/ncaa/500/235.png',
    'San Diego State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/21.png',
    'Saint Marys': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2608.png',
    'BYU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/252.png',
    'VCU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2670.png',
    'Dayton': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2168.png',
    'Davidson': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2166.png',
    'Wichita State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2724.png',
    'New Mexico': 'https://a.espncdn.com/i/teamlogos/ncaa/500/167.png',
    'Nevada': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2440.png',
    'UNLV': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2439.png',
    'San Francisco': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2626.png',
    
    # Mid-Major and Smaller Programs (commonly missing)
    'FDU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/161.png',
    'Fairleigh Dickinson': 'https://a.espncdn.com/i/teamlogos/ncaa/500/161.png',
    'Stonehill': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2776.png',
    'NC A&T': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2448.png',
    'North Carolina A&T': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2448.png',
    'FGCU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/526.png',
    'Florida Gulf Coast': 'https://a.espncdn.com/i/teamlogos/ncaa/500/526.png',
    'App State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2026.png',
    'Appalachian State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2026.png',
    'Towson': 'https://a.espncdn.com/i/teamlogos/ncaa/500/119.png',
    'Liberty': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2335.png',
    'Coastal Carolina': 'https://a.espncdn.com/i/teamlogos/ncaa/500/324.png',
    'UAB': 'https://a.espncdn.com/i/teamlogos/ncaa/500/5.png',
    'UNC Wilmington': 'https://a.espncdn.com/i/teamlogos/ncaa/500/350.png',
    'UNCW': 'https://a.espncdn.com/i/teamlogos/ncaa/500/350.png',
    'UNC Asheville': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2427.png',
    'UNCA': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2427.png',
    'UNC Greensboro': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2430.png',
    'UNCG': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2430.png',
    'Northern Kentucky': 'https://a.espncdn.com/i/teamlogos/ncaa/500/94.png',
    'NKU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/94.png',
    'Robert Morris': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2523.png',
    'Merrimack': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2379.png',
    'Central Connecticut': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2115.png',
    'CCSU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2115.png',
    'Quinnipiac': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2514.png',
    'Hofstra': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2275.png',
    'Drexel': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2182.png',
    'Delaware': 'https://a.espncdn.com/i/teamlogos/ncaa/500/48.png',
    'Elon': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2210.png',
    'Charleston': 'https://a.espncdn.com/i/teamlogos/ncaa/500/232.png',
    'College of Charleston': 'https://a.espncdn.com/i/teamlogos/ncaa/500/232.png',
    'James Madison': 'https://a.espncdn.com/i/teamlogos/ncaa/500/256.png',
    'JMU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/256.png',
    'Old Dominion': 'https://a.espncdn.com/i/teamlogos/ncaa/500/295.png',
    'ODU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/295.png',
    'Norfolk State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2450.png',
    'Hampton': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2261.png',
    'Morgan State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2415.png',
    'Coppin State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2154.png',
    'Howard': 'https://a.espncdn.com/i/teamlogos/ncaa/500/47.png',
    'Longwood': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2344.png',
    'High Point': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2272.png',
    'Gardner-Webb': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2241.png',
    'Winthrop': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2737.png',
    'Radford': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2515.png',
    'Presbyterian': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2503.png',
    'Charleston Southern': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2127.png',
    'Mount St. Marys': 'https://a.espncdn.com/i/teamlogos/ncaa/500/116.png',
    'St. Francis PA': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2598.png',
    'Sacred Heart': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2529.png',
    'St. Francis Brooklyn': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2597.png',
    'Bryant': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2803.png',
    'Wagner': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2681.png',
    'LIU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2344.png',
    'Long Island': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2344.png',
    'Fairfield': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2217.png',
    'Manhattan': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2363.png',
    'Siena': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2561.png',
    'Marist': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2368.png',
    'Canisius': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2099.png',
    'Niagara': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2447.png',
    'Rider': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2520.png',
    'Iona': 'https://a.espncdn.com/i/teamlogos/ncaa/500/314.png',
    'UMBC': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2378.png',
    'Vermont': 'https://a.espncdn.com/i/teamlogos/ncaa/500/261.png',
    'Albany': 'https://a.espncdn.com/i/teamlogos/ncaa/500/399.png',
    'NJIT': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2885.png',
    'Binghamton': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2066.png',
    'Maine': 'https://a.espncdn.com/i/teamlogos/ncaa/500/311.png',
    'Hartford': 'https://a.espncdn.com/i/teamlogos/ncaa/500/42.png',
    'New Hampshire': 'https://a.espncdn.com/i/teamlogos/ncaa/500/160.png',
    'Stony Brook': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2619.png',
    'UMass Lowell': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2349.png',
    'Detroit Mercy': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2174.png',
    'Louisiana Monroe': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2433.png',
    'Georgia Southern': 'https://a.espncdn.com/i/teamlogos/ncaa/500/290.png',
    'Le Moyne': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2330.png',
    'Sam Houston': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2534.png',
    'Louisiana Tech': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2348.png',
    'Pacific': 'https://a.espncdn.com/i/teamlogos/ncaa/500/279.png',
    'Santa Clara': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2541.png',
    'LMU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2350.png',
    'Loyola Marymount': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2350.png',
    'Loyola Chicago': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2350.png',
    'Pepperdine': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2492.png',
    'Portland': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2501.png',
    'Ball State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2050.png',
    'Toledo': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2649.png',
    'Bowling Green': 'https://a.espncdn.com/i/teamlogos/ncaa/500/189.png',
    'Kent State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2309.png',
    'Akron': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2006.png',
    'Ohio': 'https://a.espncdn.com/i/teamlogos/ncaa/500/195.png',
    'Buffalo': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2084.png',
    'Central Michigan': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2117.png',
    'Western Michigan': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2711.png',
    'Eastern Michigan': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2199.png',
    'Northern Illinois': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2459.png',
    'Miami OH': 'https://a.espncdn.com/i/teamlogos/ncaa/500/193.png',
    'Miami (OH)': 'https://a.espncdn.com/i/teamlogos/ncaa/500/193.png',
    'Cincinnati': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2132.png',
    'UCF': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2116.png',
    'SMU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2567.png',
    'Temple': 'https://a.espncdn.com/i/teamlogos/ncaa/500/218.png',
    'South Florida': 'https://a.espncdn.com/i/teamlogos/ncaa/500/58.png',
    'USF Bulls': 'https://a.espncdn.com/i/teamlogos/ncaa/500/58.png',
    'Tulane': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2655.png',
    'Tulsa': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2658.png',
    'Charlotte': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2429.png',
    'North Texas': 'https://a.espncdn.com/i/teamlogos/ncaa/500/249.png',
    'Rice': 'https://a.espncdn.com/i/teamlogos/ncaa/500/242.png',
    'UTSA': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2636.png',
    'UTEP': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2638.png',
    'FAU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2226.png',
    'Florida Atlantic': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2226.png',
    'FIU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2229.png',
    'Florida International': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2229.png',
    'East Carolina': 'https://a.espncdn.com/i/teamlogos/ncaa/500/151.png',
    'Monmouth': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2405.png',
    'Northeastern': 'https://a.espncdn.com/i/teamlogos/ncaa/500/111.png',
    'North Florida': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2454.png',
    'Lipscomb': 'https://a.espncdn.com/i/teamlogos/ncaa/500/288.png',
    'Jacksonville': 'https://a.espncdn.com/i/teamlogos/ncaa/500/294.png',
    'Stetson': 'https://a.espncdn.com/i/teamlogos/ncaa/500/56.png',
    'North Alabama': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2453.png',
    'Kennesaw State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/338.png',
    'Austin Peay': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2046.png',
    'West Georgia': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2713.png',
    'Mercer': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2382.png',
    'VMI': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2678.png',
    'The Citadel': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2643.png',
    'Citadel': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2643.png',
    'Furman': 'https://a.espncdn.com/i/teamlogos/ncaa/500/231.png',
    'Samford': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2535.png',
    'Chattanooga': 'https://a.espncdn.com/i/teamlogos/ncaa/500/236.png',
    'ETSU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2193.png',
    'East Tennessee State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2193.png',
    'Wofford': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2747.png',
    'Western Carolina': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2717.png',
    'UNC Charlotte': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2429.png',
    'UNCC': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2429.png',
    'Drake': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2181.png',
    'Bradley': 'https://a.espncdn.com/i/teamlogos/ncaa/500/71.png',
    'Indiana State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/282.png',
    'Missouri State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2623.png',
    'Southern Illinois': 'https://a.espncdn.com/i/teamlogos/ncaa/500/79.png',
    'SIU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/79.png',
    'Evansville': 'https://a.espncdn.com/i/teamlogos/ncaa/500/339.png',
    'Valparaiso': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2674.png',
    'UIC': 'https://a.espncdn.com/i/teamlogos/ncaa/500/82.png',
    'Illinois Chicago': 'https://a.espncdn.com/i/teamlogos/ncaa/500/82.png',
    'Denver': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2172.png',
    'Omaha': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2437.png',
    'Nebraska Omaha': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2437.png',
    'South Dakota': 'https://a.espncdn.com/i/teamlogos/ncaa/500/233.png',
    'South Dakota State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2571.png',
    'SDSU Jackrabbits': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2571.png',
    'North Dakota': 'https://a.espncdn.com/i/teamlogos/ncaa/500/155.png',
    'North Dakota State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2449.png',
    'NDSU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2449.png',
    'Oral Roberts': 'https://a.espncdn.com/i/teamlogos/ncaa/500/198.png',
    'St. Thomas': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2900.png',
    'Kansas City': 'https://a.espncdn.com/i/teamlogos/ncaa/500/140.png',
    'UMKC': 'https://a.espncdn.com/i/teamlogos/ncaa/500/140.png',
    'Grand Canyon': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2253.png',
    'GCU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2253.png',
    'California Baptist': 'https://a.espncdn.com/i/teamlogos/ncaa/500/472.png',
    'CBU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/472.png',
    'Tarleton State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2627.png',
    'Tarleton': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2627.png',
    'Utah Valley': 'https://a.espncdn.com/i/teamlogos/ncaa/500/3084.png',
    'Abilene Christian': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2000.png',
    'ACU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2000.png',
    'Seattle': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2547.png',
    'Seattle U': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2547.png',
    'Boise State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/68.png',
    'Air Force': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2005.png',
    'Fresno State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/278.png',
    'Utah State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/328.png',
    'San Jose State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/23.png',
    'Wyoming': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2751.png',
    'Colorado State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/36.png',
    'New Mexico State': 'https://a.espncdn.com/i/teamlogos/ncaa/500/166.png',
    'NMSU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/166.png',
    'Army': 'https://a.espncdn.com/i/teamlogos/ncaa/500/349.png',
    'Navy': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2426.png',
    'Holy Cross': 'https://a.espncdn.com/i/teamlogos/ncaa/500/107.png',
    'Colgate': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2142.png',
    'Bucknell': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2083.png',
    'Lafayette': 'https://a.espncdn.com/i/teamlogos/ncaa/500/322.png',
    'Lehigh': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2329.png',
    'American': 'https://a.espncdn.com/i/teamlogos/ncaa/500/44.png',
    'American University': 'https://a.espncdn.com/i/teamlogos/ncaa/500/44.png',
    'Boston University': 'https://a.espncdn.com/i/teamlogos/ncaa/500/104.png',
    'BU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/104.png',
    'Loyola Maryland': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2352.png',
    'Loyola (MD)': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2352.png',
    'Cornell': 'https://a.espncdn.com/i/teamlogos/ncaa/500/172.png',
    'Columbia': 'https://a.espncdn.com/i/teamlogos/ncaa/500/171.png',
    'Princeton': 'https://a.espncdn.com/i/teamlogos/ncaa/500/163.png',
    'Penn': 'https://a.espncdn.com/i/teamlogos/ncaa/500/219.png',
    'Pennsylvania': 'https://a.espncdn.com/i/teamlogos/ncaa/500/219.png',
    'Harvard': 'https://a.espncdn.com/i/teamlogos/ncaa/500/108.png',
    'Yale': 'https://a.espncdn.com/i/teamlogos/ncaa/500/43.png',
    'Dartmouth': 'https://a.espncdn.com/i/teamlogos/ncaa/500/159.png',
    'Brown': 'https://a.espncdn.com/i/teamlogos/ncaa/500/225.png',
    'Richmond': 'https://a.espncdn.com/i/teamlogos/ncaa/500/257.png',
    'George Mason': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2244.png',
    'GMU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2244.png',
    'Saint Louis': 'https://a.espncdn.com/i/teamlogos/ncaa/500/139.png',
    'SLU': 'https://a.espncdn.com/i/teamlogos/ncaa/500/139.png',
    'La Salle': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2325.png',
    'Fordham': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2230.png',
    'Duquesne': 'https://a.espncdn.com/i/teamlogos/ncaa/500/2184.png',
    'St. Bonaventure': 'https://a.espncdn.com/i/teamlogos/ncaa/500/179.png',
    'UMass': 'https://a.espncdn.com/i/teamlogos/ncaa/500/113.png',
    'Massachusetts': 'https://a.espncdn.com/i/teamlogos/ncaa/500/113.png',
    'Rhode Island': 'https://a.espncdn.com/i/teamlogos/ncaa/500/227.png',
    'URI': 'https://a.espncdn.com/i/teamlogos/ncaa/500/227.png',
    'George Washington': 'https://a.espncdn.com/i/teamlogos/ncaa/500/45.png',
    'GW': 'https://a.espncdn.com/i/teamlogos/ncaa/500/45.png',
}

CBB_TEAM_NAME_ALIASES = {
    # ============================================================
    # COMPREHENSIVE TEAM NAME MAPPING
    # Maps ESPN, Covers, VSIN names → KenPom canonical names
    # ============================================================
    
    # HTML entities
    'Texas A&amp;M': 'Texas A&M',
    'Texas A&M Aggies': 'Texas A&M',
    
    # St. John's variations - KenPom uses "St. John's"
    "St. John's": "St. John's",
    "Saint John's": "St. John's",
    'St Johns': "St. John's",
    "St. John's Red Storm": "St. John's",
    'SJU': "St. John's",
    "St John's": "St. John's",
    
    # UConn - KenPom uses "Connecticut"
    'UConn': 'Connecticut',
    'Connecticut Huskies': 'Connecticut',
    'CONN': 'Connecticut',
    'UCONN': 'Connecticut',
    
    # Saint Mary's - KenPom uses "Saint Mary's"
    'Saint Marys': "Saint Mary's",
    'Saint Marys (CA)': "Saint Mary's",
    'SMC': "Saint Mary's",
    "St. Mary's": "Saint Mary's",
    "St Mary's": "Saint Mary's",
    "St. Marys": "Saint Mary's",
    "St Marys": "Saint Mary's",
    "Saint Mary's Gaels": "Saint Mary's",
    "Saint Mary's CA": "Saint Mary's",
    # Ole Miss - KenPom uses "Mississippi"  
    'Ole Miss': 'Mississippi',
    'Mississippi Rebels': 'Mississippi',
    'MISS': 'Mississippi',
    'Ole Miss Rebels': 'Mississippi',
    
    # NC State - KenPom uses "N.C. State"
    'NC State': 'N.C. State',
    'North Carolina State': 'N.C. State',
    'NC St.': 'N.C. State',
    'NC St': 'N.C. State',
    'NCST': 'N.C. State',
    'N Carolina St': 'N.C. State',
    'Wolfpack': 'N.C. State',
    # Pittsburgh
    'Pitt': 'Pittsburgh',
    'Pitt Panthers': 'Pittsburgh',
    # San Diego State - KenPom uses "San Diego St."
    'San Diego State': 'San Diego St.',
    'SDSU': 'San Diego St.',
    'San Diego St': 'San Diego St.',
    'S Diego St': 'San Diego St.',
    'SDST': 'San Diego St.',
    # USC/UCLA
    'USC Trojans': 'USC',
    'So. California': 'USC',
    'Southern Cal': 'USC',
    'UCLA Bruins': 'UCLA',
    # Big Ten State Schools - KenPom uses periods (e.g., "Michigan St.")
    'Michigan State': 'Michigan St.',
    'Michigan St': 'Michigan St.',
    'Mich. St.': 'Michigan St.',
    'Mich St': 'Michigan St.',
    'MSU': 'Michigan St.',
    'Michigan State Spartans': 'Michigan St.',
    
    'Ohio State': 'Ohio St.',
    'Ohio St': 'Ohio St.',
    'OSU': 'Ohio St.',
    'Ohio State Buckeyes': 'Ohio St.',
    
    'Penn State': 'Penn St.',
    'Penn St': 'Penn St.',
    'PSU': 'Penn St.',
    'Penn State Nittany Lions': 'Penn St.',
    
    'Iowa State': 'Iowa St.',
    'Iowa St': 'Iowa St.',
    'ISU': 'Iowa St.',
    'Iowa State Cyclones': 'Iowa St.',
    
    'Kansas State': 'Kansas St.',
    'Kansas St': 'Kansas St.',
    'K-State': 'Kansas St.',
    'KSU': 'Kansas St.',
    'Kansas State Wildcats': 'Kansas St.',
    
    'Oklahoma State': 'Oklahoma St.',
    'Oklahoma St': 'Oklahoma St.',
    'Okla. St.': 'Oklahoma St.',
    'Okla St': 'Oklahoma St.',
    'Ok State': 'Oklahoma St.',
    'OKS': 'Oklahoma St.',
    'OKST': 'Oklahoma St.',
    'Oklahoma State Cowboys': 'Oklahoma St.',
    # ACC Schools - KenPom uses "Florida St."
    'Florida State': 'Florida St.',
    'Fla. State': 'Florida St.',
    'Fla St': 'Florida St.',
    'FSU': 'Florida St.',
    'Florida State Seminoles': 'Florida St.',
    'Georgia Tech': 'Georgia Tech',
    'Ga. Tech': 'Georgia Tech',
    'Ga Tech': 'Georgia Tech',
    'GT': 'Georgia Tech',
    'VA Tech': 'Virginia Tech',
    'Va. Tech': 'Virginia Tech',
    'Va Tech': 'Virginia Tech',
    'VT': 'Virginia Tech',
    'Boston Col.': 'Boston College',
    'Boston Col': 'Boston College',
    'BC': 'Boston College',
    'Wake': 'Wake Forest',
    'Wake Forest Demon Deacons': 'Wake Forest',
    # Loyola Chicago
    'Loyola-Chicago': 'Loyola Chicago',
    'Loyola (CHI)': 'Loyola Chicago',
    'Loyola-Chi': 'Loyola Chicago',
    'Loyola Chi': 'Loyola Chicago',
    'LUC': 'Loyola Chicago',
    # Louisville
    'Louisville Cards': 'Louisville',
    'Louisville Cardinals': 'Louisville',
    'UofL': 'Louisville',
    # San Francisco
    'San Fran': 'San Francisco',
    'USF': 'San Francisco',
    'S Francisco': 'San Francisco',
    # Detroit
    'Detroit': 'Detroit Mercy',
    'Detroit Titans': 'Detroit Mercy',
    'DET': 'Detroit Mercy',
    # Louisiana Monroe
    'UL Monroe': 'Louisiana Monroe',
    'UL-Monroe': 'Louisiana Monroe',
    'ULM': 'Louisiana Monroe',
    'La. Monroe': 'Louisiana Monroe',
    'La.-Monroe': 'Louisiana Monroe',
    'La Monroe': 'Louisiana Monroe',
    # Georgia Southern
    'Ga. Southern': 'Georgia Southern',
    'GA Southern': 'Georgia Southern',
    'GASO': 'Georgia Southern',
    'Ga Southern': 'Georgia Southern',
    'Georgia So.': 'Georgia Southern',
    # Winthrop
    'Winthrop Eagles': 'Winthrop',
    'WIN': 'Winthrop',
    # Le Moyne
    'Le Moyne': 'Le Moyne',
    'LeMoyne': 'Le Moyne',
    # Sam Houston - KenPom uses "Sam Houston St."
    'Sam Houston': 'Sam Houston St.',
    'Sam Houston State': 'Sam Houston St.',
    'SHSU': 'Sam Houston St.',
    'Sam Hous St': 'Sam Houston St.',
    # Louisiana Tech
    'La. Tech': 'Louisiana Tech',
    'Louisiana Tech': 'Louisiana Tech',
    'LA Tech': 'Louisiana Tech',
    'La Tech': 'Louisiana Tech',
    'LaTech': 'Louisiana Tech',
    # TCU/BYU
    'TCU Horned Frogs': 'TCU',
    'BYU Cougars': 'BYU',
    'Brigham Young': 'BYU',
    # Northern Kentucky
    'N. Kentucky': 'Northern Kentucky',
    'N Kentucky': 'Northern Kentucky',
    'Northern Ky.': 'Northern Kentucky',
    'NKY': 'Northern Kentucky',
    'NKU': 'Northern Kentucky',
    # Appalachian State - KenPom uses "Appalachian St."
    'Appalachian State': 'Appalachian St.',
    'App State': 'Appalachian St.',
    'App St': 'Appalachian St.',
    'App. State': 'Appalachian St.',
    'APP': 'Appalachian St.',
    # Middle Tennessee - KenPom uses "Middle Tennessee"
    'MTSU': 'Middle Tennessee',
    'Middle Tenn.': 'Middle Tennessee',
    'Middle Tenn': 'Middle Tennessee',
    'Mid. Tennessee': 'Middle Tennessee',
    'M Tennessee': 'Middle Tennessee',
    'Middle Tennessee State': 'Middle Tennessee',
    'Middle Tennessee St': 'Middle Tennessee',
    # Western Kentucky
    'WKU': 'Western Kentucky',
    'W. Kentucky': 'Western Kentucky',
    'W Kentucky': 'Western Kentucky',
    'West Kentucky': 'Western Kentucky',
    # Eastern Kentucky
    'EKU': 'Eastern Kentucky',
    'E. Kentucky': 'Eastern Kentucky',
    'E Kentucky': 'Eastern Kentucky',
    'East Kentucky': 'Eastern Kentucky',
    # Southern Illinois
    'SIU': 'Southern Illinois',
    'S. Illinois': 'Southern Illinois',
    'S Illinois': 'Southern Illinois',
    'So. Illinois': 'Southern Illinois',
    # Northern Illinois
    'NIU': 'Northern Illinois',
    'N. Illinois': 'Northern Illinois',
    'N Illinois': 'Northern Illinois',
    'No. Illinois': 'Northern Illinois',
    # Central Michigan
    'CMU': 'Central Michigan',
    'C. Michigan': 'Central Michigan',
    'C Michigan': 'Central Michigan',
    'Cent. Michigan': 'Central Michigan',
    # Eastern Michigan
    'EMU': 'Eastern Michigan',
    'E. Michigan': 'Eastern Michigan',
    'E Michigan': 'Eastern Michigan',
    'East Michigan': 'Eastern Michigan',
    # Western Michigan
    'WMU': 'Western Michigan',
    'W. Michigan': 'Western Michigan',
    'W Michigan': 'Western Michigan',
    'West Michigan': 'Western Michigan',
    # Bowling Green
    'BGSU': 'Bowling Green',
    'BG': 'Bowling Green',
    'Bowl. Green': 'Bowling Green',
    # Ball State - KenPom uses "Ball St."
    'Ball State': 'Ball St.',
    'Ball St': 'Ball St.',
    
    # Kent State - KenPom uses "Kent St."
    'Kent State': 'Kent St.',
    'Kent St': 'Kent St.',
    # Fresno State - KenPom uses "Fresno St."
    'Fresno State': 'Fresno St.',
    'Fresno St': 'Fresno St.',
    'FRESNO': 'Fresno St.',
    'FRESST': 'Fresno St.',
    
    # Boise State - KenPom uses "Boise St."
    'Boise State': 'Boise St.',
    'Boise St': 'Boise St.',
    'BOISE': 'Boise St.',
    'BSU': 'Boise St.',
    
    # Colorado State - KenPom uses "Colorado St."
    'Colorado State': 'Colorado St.',
    'Colorado St': 'Colorado St.',
    'Colo. St.': 'Colorado St.',
    'Colo St': 'Colorado St.',
    'CSU': 'Colorado St.',
    'CSTATE': 'Colorado St.',
    
    # Utah State - KenPom uses "Utah St."
    'Utah State': 'Utah St.',
    'Utah St': 'Utah St.',
    'USU': 'Utah St.',
    
    # New Mexico State - KenPom uses "New Mexico St."
    'New Mexico State': 'New Mexico St.',
    'New Mexico St': 'New Mexico St.',
    'NM State': 'New Mexico St.',
    'NMSU': 'New Mexico St.',
    
    # Arizona State - KenPom uses "Arizona St."
    'Arizona State': 'Arizona St.',
    'Arizona St': 'Arizona St.',
    'Ariz. St.': 'Arizona St.',
    'Ariz St': 'Arizona St.',
    'ASU': 'Arizona St.',
    'Arizona State Sun Devils': 'Arizona St.',
    
    # Oregon State - KenPom uses "Oregon St."
    'Oregon State': 'Oregon St.',
    'Oregon St': 'Oregon St.',
    'ORST': 'Oregon St.',
    
    # Washington State - KenPom uses "Washington St."
    'Washington State': 'Washington St.',
    'Washington St': 'Washington St.',
    'Wash. St.': 'Washington St.',
    'Wash St': 'Washington St.',
    'WSU': 'Washington St.',
    # UNC schools
    'UNC': 'North Carolina',
    'N. Carolina': 'North Carolina',
    'N Carolina': 'North Carolina',
    'No. Carolina': 'North Carolina',
    'UNC Wilmington': 'UNC Wilmington',
    'UNCW': 'UNC Wilmington',
    'NC Wilmington': 'UNC Wilmington',
    'UNC Greensboro': 'UNC Greensboro',
    'UNCG': 'UNC Greensboro',
    'NC Greensboro': 'UNC Greensboro',
    'UNC Asheville': 'UNC Asheville',
    'UNCA': 'UNC Asheville',
    # South Carolina
    'S. Carolina': 'South Carolina',
    'S Carolina': 'South Carolina',
    'So. Carolina': 'South Carolina',
    # Arkansas State
    'Arkansas St.': 'Arkansas State',
    'Arkansas St': 'Arkansas State',
    'Ark. St.': 'Arkansas State',
    'Ark St': 'Arkansas State',
    # Georgia State
    'Georgia St.': 'Georgia State',
    'Georgia St': 'Georgia State',
    'Ga. St.': 'Georgia State',
    'Ga. State': 'Georgia State',
    'Ga St': 'Georgia State',
    'GSU': 'Georgia State',
    # Memphis
    'Memphis Tigers': 'Memphis',
    'MEM': 'Memphis',
    # UAB
    'UAB Blazers': 'UAB',
    'Alabama-Birmingham': 'UAB',
    # UTEP
    'Texas-El Paso': 'UTEP',
    'UT El Paso': 'UTEP',
    # UTSA
    'Texas-San Antonio': 'UTSA',
    'UT San Antonio': 'UTSA',
    # FAU/FIU
    'Fla. Atlantic': 'Florida Atlantic',
    'Fla Atlantic': 'Florida Atlantic',
    'FL Atlantic': 'Florida Atlantic',
    'FAU': 'Florida Atlantic',
    'Fla. International': 'FIU',
    'Fla International': 'FIU',
    'FL International': 'FIU',
    'Florida Intl': 'FIU',
    # VCU
    'Virginia Commonwealth': 'VCU',
    'Va. Commonwealth': 'VCU',
    'Va Commonwealth': 'VCU',
    # George Mason
    'Geo. Mason': 'George Mason',
    'Geo Mason': 'George Mason',
    'GMU': 'George Mason',
    # George Washington
    'Geo. Washington': 'George Washington',
    'Geo Washington': 'George Washington',
    'GW': 'George Washington',
    'GWU': 'George Washington',
    # Grand Canyon
    'Gr. Canyon': 'Grand Canyon',
    'GCU': 'Grand Canyon',
    # South Florida
    'S. Florida': 'South Florida',
    'S Florida': 'South Florida',
    'So. Florida': 'South Florida',
    # Central Florida
    'UCF': 'Central Florida',
    'C. Florida': 'Central Florida',
    'Cent. Florida': 'Central Florida',
    # Other common abbreviations
    'LSU Tigers': 'LSU',
    'UK': 'Kentucky',
    'KU': 'Kansas',
    'IU': 'Indiana',
    'Marquette Golden Eagles': 'Marquette',
    'MARQ': 'Marquette',
    'Creighton Bluejays': 'Creighton',
    'CREI': 'Creighton',
    'Villanova Wildcats': 'Villanova',
    'NOVA': 'Villanova',
    'Xavier Musketeers': 'Xavier',
    'XAV': 'Xavier',
    'Butler Bulldogs': 'Butler',
    'BUT': 'Butler',
    # Citadel
    'The Citadel': 'Citadel',
    'CIT': 'Citadel',
    # Stony Brook
    'Stony Brook Seawolves': 'Stony Brook',
    'STON': 'Stony Brook',
    'SBU': 'Stony Brook',
    # Elon
    'Elon Phoenix': 'Elon',
    'ELON': 'Elon',
    # San Jose State
    'San Jose St': 'San Jose State',
    'San Jose St.': 'San Jose State',
    'San José St': 'San Jose State',
    'San José State': 'San Jose State',
    'SJSU': 'San Jose State',
    'SJ State': 'San Jose State',
    # Sacramento State
    'Sacramento St': 'Sacramento State',
    'Sacramento St.': 'Sacramento State',
    'SAC': 'Sacramento State',
    'Sac St': 'Sacramento State',
    'Sac State': 'Sacramento State',
    'CSUS': 'Sacramento State',
    # New Mexico
    'UNM': 'New Mexico',
    'New Mexico Lobos': 'New Mexico',
    'N Mexico': 'New Mexico',
    # Montana
    'MONT': 'Montana',
    'Montana Grizzlies': 'Montana',
    # Saint Mary's (CA)
    'SMC': "Saint Mary's",
    'St Marys': "Saint Mary's",
    'St. Marys': "Saint Mary's",
    "St Mary's": "Saint Mary's",
    "St. Mary's": "Saint Mary's",
    "Saint Marys": "Saint Mary's",
    "Saint Mary's CA": "Saint Mary's",
    'Saint Mary\'s Gaels': "Saint Mary's",
    # Gonzaga
    'GONZ': 'Gonzaga',
    'Gonzaga Bulldogs': 'Gonzaga',
    'Zags': 'Gonzaga',
    # More Covers.com abbreviations
    'CONN': 'UConn',
    'UCONN': 'UConn',
    'CREI': 'Creighton',
    'CREIGH': 'Creighton',
    'SETON': 'Seton Hall',
    'SHU': 'Seton Hall',
    'NOVA': 'Villanova',
    'VILL': 'Villanova',
    'GTOWN': 'Georgetown',
    'DEPAUL': 'DePaul',
    'XAV': 'Xavier',
    'XAVIER': 'Xavier',
    'BUTLER': 'Butler',
    'BUTL': 'Butler',
    'STAN': 'Stanford',
    'STANFORD': 'Stanford',
    'USC': 'USC',
    'UCLA': 'UCLA',
    'OREGON': 'Oregon',
    'ORE': 'Oregon',
    'ORST': 'Oregon State',
    'OSU': 'Oregon State',
    'WSU': 'Washington State',
    'WASH': 'Washington',
    'UW': 'Washington',
    'ARIZ': 'Arizona',
    'ASU': 'Arizona State',
    'COLO': 'Colorado',
    'UTAH': 'Utah',
    'CAL': 'California',
    'BAYLOR': 'Baylor',
    'BAY': 'Baylor',
    'TCU': 'TCU',
    'OKLA': 'Oklahoma',
    'OKS': 'Oklahoma State',
    'OKST': 'Oklahoma State',
    'TEX': 'Texas',
    'TXTECH': 'Texas Tech',
    'TTU': 'Texas Tech',
    'ISU': 'Iowa State',
    'KU': 'Kansas',
    'KANSAS': 'Kansas',
    'KSU': 'Kansas State',
    'KSTATE': 'Kansas State',
    'WVU': 'West Virginia',
    'WVIR': 'West Virginia',
    'CINCY': 'Cincinnati',
    'CIN': 'Cincinnati',
    'UCF': 'UCF',
    'USF': 'South Florida',
    'TEMPLE': 'Temple',
    'TEM': 'Temple',
    'MEMPHIS': 'Memphis',
    'MEM': 'Memphis',
    'TULANE': 'Tulane',
    'TUL': 'Tulane',
    'TULSA': 'Tulsa',
    'Wichita State': 'Wichita St.',
    'WICHITA': 'Wichita St.',
    'WICH': 'Wichita St.',
    'Wichita St': 'Wichita St.',
    'SMU': 'SMU',
    'HOUSTON': 'Houston',
    'HOU': 'Houston',
    'UH': 'Houston',
    'RICE': 'Rice',
    'BOISE': 'Boise State',
    'BSU': 'Boise State',
    'SDSU': 'San Diego State',
    'SDST': 'San Diego State',
    'UNLV': 'UNLV',
    'NEVADA': 'Nevada',
    'NEV': 'Nevada',
    'FRESNO': 'Fresno State',
    'FRESST': 'Fresno State',
    'CSTATE': 'Colorado State',
    'CSU': 'Colorado State',
    'WYOMING': 'Wyoming',
    'WYO': 'Wyoming',
    'NMEX': 'New Mexico',
    'NMSU': 'New Mexico State',
    'UTEP': 'UTEP',
    'NTEX': 'North Texas',
    'UNT': 'North Texas',
    # More common abbreviations from Covers.com
    'WAKE': 'Wake Forest',
    'PROV': 'Providence',
    'MARQ': 'Marquette',
    'DUKE': 'Duke',
    'GONZ': 'Gonzaga',
    'KANS': 'Kansas',
    'ARIZ': 'Arizona',
    'ALAB': 'Alabama',
    'HOUS': 'Houston',
    'TENN': 'Tennessee',
    'PURD': 'Purdue',
    'FLOR': 'Florida',
    'CLEM': 'Clemson',
    'IOWA': 'Iowa',
    'WISC': 'Wisconsin',
    'MICH': 'Michigan',
    'OREG': 'Oregon',
    'STAN': 'Stanford',
    'BAYLOR': 'Baylor',
    'UCONN': 'UConn',
    'SETON': 'Seton Hall',
    'SHSU': 'Sam Houston',
    'TXST': 'Texas State',
    'TXAM': 'Texas A&M',
    'TAMU': 'Texas A&M',
    'ARIZ ST': 'Arizona St.',
    
    # Illinois - Big Ten
    'Illinois Fighting Illini': 'Illinois',
    'ILL': 'Illinois',
    'ILLI': 'Illinois',
    'ILLIN': 'Illinois',
    'Illini': 'Illinois',
    'U of I': 'Illinois',
    
    # Saint Peter's - MAAC
    "Saint Peter's": "Saint Peter's",
    "St. Peter's": "Saint Peter's",
    "St Peter's": "Saint Peter's",
    "Saint Peters": "Saint Peter's",
    "St. Peters": "Saint Peter's",
    "St Peters": "Saint Peter's",
    'SPU': "Saint Peter's",
    'STPET': "Saint Peter's",
    "Saint Peter's Peacocks": "Saint Peter's",
    
    # Rider - MAAC
    'Rider Broncs': 'Rider',
    'RIDE': 'Rider',
    'RID': 'Rider',
    
    # ============================================================
    # ADDITIONAL ESPN shortDisplayName -> KenPom mappings
    # ============================================================
    'N Illinois': 'Northern Illinois',
    'Miami OH': 'Miami OH',
    'Miami (OH)': 'Miami OH',
    'Miami FL': 'Miami FL',
    'Miami (FL)': 'Miami FL',
    'Mississippi St.': 'Mississippi St.',
    'Illinois St.': 'Illinois St.',
    'Arkansas St.': 'Arkansas St.',
    'Indiana St.': 'Indiana St.',
    'Missouri St.': 'Missouri St.',
    'Stephen F Austin': 'Stephen F. Austin',
    'SFA': 'Stephen F. Austin',
    'Cal Fullerton': 'Cal St. Fullerton',
    'Long Beach St': 'Long Beach St.',
    'Long Beach State': 'Long Beach St.',
    'Portland St': 'Portland St.',
    'Portland State': 'Portland St.',
    'Wright St': 'Wright St.',
    'Wright State': 'Wright St.',
    'Kennesaw St': 'Kennesaw St.',
    'Kennesaw State': 'Kennesaw St.',
    'Jacksonville St': 'Jacksonville St.',
    'Jacksonville State': 'Jacksonville St.',
    'Weber St': 'Weber St.',
    'Weber State': 'Weber St.',
    'North Dakota St': 'North Dakota St.',
    'North Dakota State': 'North Dakota St.',
    'NDSU': 'North Dakota St.',
    'South Dakota St': 'South Dakota St.',
    'South Dakota State': 'South Dakota St.',
    'Tarleton St': 'Tarleton St.',
    'Tarleton State': 'Tarleton St.',
    'E Tennessee St': 'East Tennessee St.',
    'East Tennessee St': 'East Tennessee St.',
    'East Tennessee State': 'East Tennessee St.',
    'ETSU': 'East Tennessee St.',
    'Youngstown St': 'Youngstown St.',
    'Youngstown State': 'Youngstown St.',
    'YSU': 'Youngstown St.',
    'Tennessee St': 'Tennessee St.',
    'Tennessee State': 'Tennessee St.',
    'Cleveland St': 'Cleveland St.',
    'Cleveland State': 'Cleveland St.',
    'Idaho St': 'Idaho St.',
    'Idaho State': 'Idaho St.',
    'Montana St': 'Montana St.',
    'Montana State': 'Montana St.',
    'Northwestern St': 'Northwestern St.',
    'Northwestern State': 'Northwestern St.',
    'Grambling St': 'Grambling St.',
    'Grambling State': 'Grambling St.',
    'Norfolk St': 'Norfolk St.',
    'Norfolk State': 'Norfolk St.',
    'Alabama St': 'Alabama St.',
    'Alabama State': 'Alabama St.',
    'Murray St': 'Murray St.',
    'Murray State': 'Murray St.',
    'Morehead St': 'Morehead St.',
    'Morehead State': 'Morehead St.',
    
    # ============================================================
    # VSIN-SPECIFIC FIXES (from log warnings)
    # ============================================================
    # San José State (accent removal)
    'San José St': 'San Jose St.',
    'San José State': 'San Jose St.',
    
    # Sacramento St
    'Sacramento St': 'Sacramento St.',
    
    # Louisiana Tech / Sam Houston
    'Sam Houston': 'Sam Houston St.',
    
    # Northern Illinois
    'N Illinois': 'Northern Illinois',
    'Northern Ill': 'Northern Illinois',
    
    # Central Connecticut
    'C Connecticut': 'Central Connecticut',
    'C Conn': 'Central Connecticut',
    
    # George Washington
    'G Washington': 'George Washington',
    'GW': 'George Washington',
    
    # Eastern Washington / Northern Colorado
    'E Washington': 'Eastern Washington',
    'N Colorado': 'Northern Colorado',
    
    # Abilene Christian
    'Abilene Chrstn': 'Abilene Christian',
    'Abilene Chr': 'Abilene Christian',
    
    # Georgia Southern
    'GA Southern': 'Georgia Southern',
    'Ga Southern': 'Georgia Southern',
    
    # Hawai'i
    "Hawai'i": 'Hawaii',
    'Hawaii': 'Hawaii',
    
    # Houston Christian / Texas A&M-CC
    'Hou Christian': 'Houston Christian',
    'Houston Chr': 'Houston Christian',
    'Texas A&M-CC': 'Texas A&M Corpus Chris',
    'Texas A&amp;M-CC': 'Texas A&M Corpus Chris',
    'TAMUCC': 'Texas A&M Corpus Chris',
    
    # UT Rio Grande Valley
    'UT Rio Grande': 'UT Rio Grande Valley',
    'UTRGV': 'UT Rio Grande Valley',
    
    # Northwestern State
    "N'Western St": 'Northwestern St.',
    'NW State': 'Northwestern St.',
    
    # Southern Indiana
    'So Indiana': 'Southern Indiana',
    'S Indiana': 'Southern Indiana',
    
    # SIUE
    'SIUE': 'SIUE',
    'SIU Edwardsville': 'SIUE',
    
    # UAlbany
    'UAlbany': 'Albany',
    'U Albany': 'Albany',
    
    # Chicago State
    'Chicago St': 'Chicago St.',
    'Chicago State': 'Chicago St.',
    
    # Boston University / Boston College
    'Boston U': 'Boston University',
    'BU': 'Boston University',
    
    # Purdue Fort Wayne
    'Purdue FW': 'Purdue Fort Wayne',
    'PFW': 'Purdue Fort Wayne',
    'IPFW': 'Purdue Fort Wayne',
    'Purdue-Fort Wayne': 'Purdue Fort Wayne',
    'Ft Wayne': 'Purdue Fort Wayne',
    
    # Utah State  
    'Utah State': 'Utah St.',
    
    # LIU / Long Island
    'Long Island': 'LIU',
    
    # Louisiana (Ragin' Cajuns) - Sun Belt
    'Louisiana': 'Louisiana',
    'UL Lafayette': 'Louisiana',
    'Louisiana-Lafayette': 'Louisiana',
    'La. Lafayette': 'Louisiana',
    'LA Lafayette': 'Louisiana',
    'LA-Lafayette': 'Louisiana',
    'ULL': 'Louisiana',
    'Ragin Cajuns': 'Louisiana',
    "Ragin' Cajuns": 'Louisiana',
    
    # James Madison - Sun Belt
    'James Madison': 'James Madison',
    'JMU': 'James Madison',
    'J Madison': 'James Madison',
    'Jas Madison': 'James Madison',
    
    # South Dakota - Summit League
    'South Dakota': 'South Dakota',
    'S Dakota': 'South Dakota',
    'So Dakota': 'South Dakota',
    'SD': 'South Dakota',
    
    # Kansas City (UMKC) - Summit League
    'Kansas City': 'UMKC',
    'Mo Kansas City': 'UMKC',
    'Missouri-Kansas City': 'UMKC',
    'Missouri KC': 'UMKC',
    'KC Roos': 'UMKC',
    
    # Furman - SoCon
    'Furman': 'Furman',
    'FUR': 'Furman',
    'Furman Paladins': 'Furman',
    
    # East Texas A&M - SWAC
    'E Texas A&M': 'East Texas A&M',
    'East Texas A&M': 'East Texas A&M',
    'ETAMU': 'East Texas A&M',
    'E Texas AM': 'East Texas A&M',
    
    # Northwestern State - Southland
    "N'Western St": 'Northwestern St.',
    'NW State': 'Northwestern St.',
    'NWST': 'Northwestern St.',
    'Northwestern State': 'Northwestern St.',
    
    # Central Arkansas - ASUN
    'C Arkansas': 'Central Arkansas',
    'UCA': 'Central Arkansas',
    'Cent Arkansas': 'Central Arkansas',
    
    # Arkansas-Pine Bluff - SWAC
    'AR-Pine Bluff': 'Arkansas-Pine Bluff',
    'Arkansas-Pine Bluff': 'Arkansas-Pine Bluff',
    'UAPB': 'Arkansas-Pine Bluff',
    'Ark Pine Bluff': 'Arkansas-Pine Bluff',
    
    # Grambling State - SWAC
    'Grambling': 'Grambling St.',
    'GRAM': 'Grambling St.',
    
    # Middle Tennessee - C-USA
    'Middle Tenn': 'Middle Tennessee',
    'Mid Tenn': 'Middle Tennessee',
    'MTSU': 'Middle Tennessee',
    
    # South Dakota State - Summit
    'S Dakota St': 'South Dakota St.',
    'South Dakota St': 'South Dakota St.',
    'SDSU': 'South Dakota St.',
    
    # St. Thomas Minnesota - Summit
    'St Thomas': 'St. Thomas (MN)',
    'St Thomas (MN)': 'St. Thomas (MN)',
    'Saint Thomas': 'St. Thomas (MN)',
    'St Thomas MN': 'St. Thomas (MN)',
    
    # Gardner-Webb - Big South
    'Gardner-Webb': 'Gardner-Webb',
    'GWU': 'Gardner-Webb',
    'Gardner Webb': 'Gardner-Webb',
    
    # Longwood - Big South
    'Longwood': 'Longwood',
    'Long Wood': 'Longwood',
    
    # North Alabama - ASUN
    'N Alabama': 'North Alabama',
    'UNA': 'North Alabama',
    'North Ala': 'North Alabama',
    
    # FIU - AAC
    'FIU': 'FIU',
    'Florida Intl': 'FIU',
    'Florida International': 'FIU',
    
    # SC State - MEAC
    'SC State': 'SC State',
    'SC St': 'SC State',
    'South Carolina State': 'SC State',
    
    # Maryland Eastern Shore - MEAC
    'MD Eastern': 'Maryland Eastern Shore',
    'UMES': 'Maryland Eastern Shore',
    'Md Eastern Shore': 'Maryland Eastern Shore',
}

def strip_accents(text: str) -> str:
    """Remove accents/diacritics from text (é -> e, ñ -> n, etc.)."""
    import unicodedata
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def get_all_team_aliases(normalized_name: str) -> list:
    """
    Get all possible aliases for a normalized team name.
    Returns list of all keys that map to this normalized name.
    Useful for reverse lookups when external sources use abbreviations.
    """
    aliases = [normalized_name]
    normalized_lower = normalized_name.lower().strip()

    for alias, norm in CBB_TEAM_NAME_ALIASES.items():
        if norm.lower().strip() == normalized_lower:
            aliases.append(alias)

    # Also add common variations
    # State abbreviation variations
    if ' State' in normalized_name:
        base = normalized_name.replace(' State', '')
        aliases.extend([f'{base} St', f'{base} St.', base])

    # Saint/St variations
    if normalized_name.startswith('Saint '):
        st_ver = 'St. ' + normalized_name[6:]
        aliases.extend([st_ver, st_ver.replace('. ', ' '), st_ver.replace('.', '')])
    elif normalized_name.startswith('St ') or normalized_name.startswith('St. '):
        saint_ver = 'Saint ' + normalized_name.split(' ', 1)[1] if ' ' in normalized_name else normalized_name
        aliases.append(saint_ver)

    return list(set(aliases))


def normalize_cbb_team_name(name: str) -> str:
    """Normalize CBB team name for consistent matching."""
    if not name:
        return ''
    # Handle HTML entities
    name = name.replace('&amp;', '&').strip()

    # Strip accents/diacritics (San José -> San Jose)
    name_ascii = strip_accents(name)

    # Check exact aliases first (with original name)
    if name in CBB_TEAM_NAME_ALIASES:
        return CBB_TEAM_NAME_ALIASES[name]

    # Check exact aliases with ASCII version (accents stripped)
    if name_ascii in CBB_TEAM_NAME_ALIASES:
        return CBB_TEAM_NAME_ALIASES[name_ascii]

    # Try uppercase version for abbreviations (UNCA, NCST, WIN, etc.)
    name_upper = name_ascii.upper()
    if name_upper in CBB_TEAM_NAME_ALIASES:
        return CBB_TEAM_NAME_ALIASES[name_upper]

    # Try case-insensitive match on all aliases
    name_lower = name_ascii.lower()
    for alias, normalized in CBB_TEAM_NAME_ALIASES.items():
        if strip_accents(alias).lower() == name_lower:
            return normalized

    return name.strip()

def get_cbb_logo(team_name: str) -> Optional[str]:
    """
    Get college basketball team logo URL.
    
    Args:
        team_name: Team name (e.g., 'Duke', 'North Carolina')
        
    Returns:
        Logo URL or None if not found
    """
    if not team_name:
        return None
    
    # Normalize the name
    team_name = normalize_cbb_team_name(team_name)
    
    # Direct match
    if team_name in CBB_TEAM_LOGOS:
        return CBB_TEAM_LOGOS[team_name]
    
    # Check aliases
    if team_name in CBB_TEAM_NAME_ALIASES:
        normalized = CBB_TEAM_NAME_ALIASES[team_name]
        if normalized in CBB_TEAM_LOGOS:
            return CBB_TEAM_LOGOS[normalized]
    
    # Fuzzy match - case insensitive
    team_lower = team_name.lower()
    for key, url in CBB_TEAM_LOGOS.items():
        key_lower = key.lower()
        if key_lower == team_lower:
            return url
        if key_lower in team_lower or team_lower in key_lower:
            return url
    
    # Try without common suffixes
    for suffix in [' Eagles', ' Bulldogs', ' Tigers', ' Bears', ' Cardinals', ' Wildcats', 
                   ' Huskies', ' Spartans', ' Wolverines', ' Buckeyes', ' Hoosiers']:
        if team_name.endswith(suffix):
            base_name = team_name[:-len(suffix)]
            if base_name in CBB_TEAM_LOGOS:
                return CBB_TEAM_LOGOS[base_name]
    
    return None


# ============================================================
# COVERS.COM SCRAPER (BULLETPROOF)
# ============================================================

class CoversScraper:
    """
    Fast, bulletproof scraper for Covers.com matchup section.
    
    Extracts:
    - Current day's slate
    - Team matchups
    - Betting trends
    - Public/sharp money percentages
    - Line movements
    - Last 10 records
    """
    
    BASE_URL = "https://www.covers.com"
    CACHE_TTL = 60  # Cache for 1 minute
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.covers.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
        self._cache = {}
        self._cache_time = {}
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still fresh."""
        if key not in self._cache:
            return False
        age = time.time() - self._cache_time.get(key, 0)
        return age < self.CACHE_TTL
    
    def get_todays_matchups(self, league: str = 'NBA') -> List[Dict]:
        """
        Get all matchups for today.
        
        Args:
            league: 'NBA' or 'CBB'
            
        Returns:
            List of matchup dictionaries with:
            - away_team, home_team
            - matchup_id
            - game_time
            - spread, total
            - away_money_pct, home_money_pct
            - away_tickets_pct, home_tickets_pct
        """
        cache_key = f"matchups_{league}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        try:
            # Determine URL based on league
            if league == 'NBA':
                url = f"{self.BASE_URL}/sport/basketball/nba/matchups"
            elif league == 'CBB':
                url = f"{self.BASE_URL}/sport/basketball/ncaab/matchups"
            else:
                logger.warning(f"Unsupported league: {league}")
                return []
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            matchups = []
            
            # Find all game containers
            game_containers = soup.find_all('div', class_=re.compile(r'.*matchup.*', re.I))
            
            for container in game_containers:
                try:
                    matchup = self._parse_matchup_container(container, league)
                    if matchup:
                        matchups.append(matchup)
                except Exception as e:
                    logger.debug(f"Error parsing matchup container: {e}")
                    continue
            
            # Cache results
            self._cache[cache_key] = matchups
            self._cache_time[cache_key] = time.time()
            
            logger.info(f"Fetched {len(matchups)} matchups from Covers.com for {league}")
            return matchups
            
        except Exception as e:
            logger.error(f"Error fetching Covers matchups: {e}")
            return []
    
    def _parse_matchup_container(self, container, league: str) -> Optional[Dict]:
        """Parse individual matchup container."""
        matchup = {}
        
        # Extract teams
        team_elements = container.find_all('a', class_=re.compile(r'.*team.*', re.I))
        if len(team_elements) >= 2:
            matchup['away_team'] = team_elements[0].text.strip()
            matchup['home_team'] = team_elements[1].text.strip()
        else:
            return None
        
        # Extract matchup ID from URL
        link = container.find('a', href=re.compile(r'/matchup/'))
        if link:
            matchup_id = link['href'].split('/')[-1]
            matchup['matchup_id'] = matchup_id
            matchup['url'] = f"{self.BASE_URL}{link['href']}"
        
        # Extract betting percentages if available
        pct_elements = container.find_all(text=re.compile(r'\d+%'))
        if len(pct_elements) >= 4:
            matchup['away_money_pct'] = int(re.search(r'(\d+)%', pct_elements[0]).group(1))
            matchup['away_tickets_pct'] = int(re.search(r'(\d+)%', pct_elements[1]).group(1))
            matchup['home_money_pct'] = int(re.search(r'(\d+)%', pct_elements[2]).group(1))
            matchup['home_tickets_pct'] = int(re.search(r'(\d+)%', pct_elements[3]).group(1))
        
        return matchup if matchup.get('matchup_id') else None
    
    def get_matchup_details(self, matchup_id: str, league: str = 'NBA') -> Dict:
        """
        Get detailed matchup data including betting action and trends.
        
        Args:
            matchup_id: Matchup ID from Covers
            league: 'NBA' or 'CBB'
            
        Returns:
            Dictionary with detailed matchup information
        """
        cache_key = f"details_{matchup_id}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        try:
            if league == 'NBA':
                url = f"{self.BASE_URL}/sport/basketball/nba/matchup/{matchup_id}"
            else:
                url = f"{self.BASE_URL}/sport/basketball/ncaab/matchup/{matchup_id}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            details = {
                'matchup_id': matchup_id,
                'league': league,
            }
            
            # Extract betting percentages
            betting_action = soup.find('div', class_=re.compile(r'.*betting.*action.*', re.I))
            if betting_action:
                percentages = betting_action.find_all(text=re.compile(r'\d+%'))
                if len(percentages) >= 4:
                    details['away_money_pct'] = int(re.search(r'(\d+)%', percentages[0]).group(1))
                    details['away_tickets_pct'] = int(re.search(r'(\d+)%', percentages[1]).group(1))
                    details['home_money_pct'] = int(re.search(r'(\d+)%', percentages[2]).group(1))
                    details['home_tickets_pct'] = int(re.search(r'(\d+)%', percentages[3]).group(1))
            
            # Extract Last 10 records
            records = soup.find_all('div', class_=re.compile(r'.*record.*', re.I))
            for record in records:
                text = record.text
                match = re.search(r'(\d+)-(\d+)-(\d+)', text)
                if match:
                    if 'away' in text.lower() or records.index(record) == 0:
                        details['away_l10_record'] = text.strip()
                    else:
                        details['home_l10_record'] = text.strip()
            
            # Cache results
            self._cache[cache_key] = details
            self._cache_time[cache_key] = time.time()
            
            return details
            
        except Exception as e:
            logger.error(f"Error fetching Covers matchup details: {e}")
            return {}
    
    def get_team_trends(self, team_name: str, league: str = 'NBA') -> Dict:
        """
        Get team trends and records from Covers.com team page.
        
        Returns:
            Dictionary with:
            - overall_record: "28-18"
            - home_record: "15-8"
            - road_record: "13-10"
            - ats_record: "24-21-1"
            - ats_home: "14-12-0"
            - ats_road: "10-9-1"
            - last_10: "5-5"
            - last_10_ats: "5-5-0"
        """
        cache_key = f"trends_{league}_{team_name}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        trends = {}
        try:
            # Normalize team name for URL
            team_slug = team_name.lower().replace(' ', '-').replace("'", '')
            
            if league == 'NBA':
                url = f"{self.BASE_URL}/sport/basketball/nba/teams/main/{team_slug}/trends"
            else:
                url = f"{self.BASE_URL}/sport/basketball/ncaab/teams/main/{team_slug}/trends"
            
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return trends
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Parse trends table rows
            rows = soup.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    label = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                    
                    if 'overall' in label:
                        trends['overall_record'] = value
                    elif 'home' in label and 'ats' not in label:
                        trends['home_record'] = value
                    elif 'road' in label or 'away' in label:
                        if 'ats' in label:
                            trends['ats_road'] = value
                        else:
                            trends['road_record'] = value
                    elif 'ats' in label and 'home' in label:
                        trends['ats_home'] = value
                    elif 'ats' in label:
                        trends['ats_record'] = value
                    elif 'last 10' in label:
                        if 'ats' in label:
                            trends['last_10_ats'] = value
                        else:
                            trends['last_10'] = value
            
            # Cache results
            self._cache[cache_key] = trends
            self._cache_time[cache_key] = time.time()
            
            logger.debug(f"Fetched trends for {team_name}: {trends}")
            return trends
            
        except Exception as e:
            logger.debug(f"Error fetching team trends for {team_name}: {e}")
            return trends


def get_covers_matchup_stats(league: str = 'NBA') -> Dict:
    """
    Scrape all matchup stats from Covers.com matchups page.
    Returns a dict keyed by team abbreviation AND full name with full stats.
    
    BULLETPROOF implementation that parses:
    - Win/Loss records (overall and home/road)
    - ATS records (overall and home/road)
    - Last 10 records with ATS
    """
    stats_by_team = {}
    
    # NBA Team abbreviation to nickname mapping
    nba_abbr_to_name = {
        'LAL': 'Lakers', 'BOS': 'Celtics', 'NYK': 'Knicks', 'NY': 'Knicks',
        'PHI': 'Sixers', 'MIA': 'Heat', 'CHI': 'Bulls', 'DET': 'Pistons',
        'CLE': 'Cavaliers', 'TOR': 'Raptors', 'ORL': 'Magic', 'SAC': 'Kings',
        'WAS': 'Wizards', 'DEN': 'Nuggets', 'NO': 'Pelicans', 'NOP': 'Pelicans',
        'MEM': 'Grizzlies', 'GS': 'Warriors', 'GSW': 'Warriors', 
        'LAC': 'Clippers', 'PHO': 'Suns', 'PHX': 'Suns', 'MIN': 'Timberwolves',
        'MIL': 'Bucks', 'ATL': 'Hawks', 'IND': 'Pacers', 'CHA': 'Hornets',
        'BK': 'Nets', 'BKN': 'Nets', 'HOU': 'Rockets', 'DAL': 'Mavericks',
        'SAS': 'Spurs', 'SA': 'Spurs', 'POR': 'Trail Blazers', 'UTA': 'Jazz', 'OKC': 'Thunder'
    }
    
    # CBB Team abbreviation to full name mapping (common teams)
    cbb_abbr_to_name = {
        'GASO': 'GA Southern', 'ULM': 'UL Monroe', 'KENT': 'Kent State', 'AKR': 'Akron',
        'PRIN': 'Princeton', 'COR': 'Cornell', 'SIE': 'Siena', 'NIAG': 'Niagara',
        'IUPU': 'IU Indy', 'YSU': 'Youngstown St', 'PENN': 'Penn', 'COLUM': 'Columbia',
        'MSM': 'Mount St Marys', 'SPU': "Saint Peter's", 'STPET': "Saint Peter's", 
        "Saint Peter's": "Saint Peter's", "St. Peter's": "Saint Peter's", "St Peters": "Saint Peter's",
        'RIDE': 'Rider', 'Rider': 'Rider',
        'LOYCH': 'Loyola Chicago',
        'VCU': 'VCU', 'HARV': 'Harvard', 'BRWN': 'Brown', 'MAR': 'Marist', 'CAN': 'Canisius',
        'DUKE': 'Duke', 'UNC': 'North Carolina', 'UK': 'Kentucky', 'KU': 'Kansas',
        'ILL': 'Illinois', 'ILLI': 'Illinois', 'ILLIN': 'Illinois', 'Illinois': 'Illinois',
        'NEB': 'Nebraska', 'NEBR': 'Nebraska', 'Nebraska': 'Nebraska',
        'GONZ': 'Gonzaga', 'PURD': 'Purdue', 'ARIZ': 'Arizona', 'TENN': 'Tennessee',
        'HOU': 'Houston', 'BAMA': 'Alabama', 'CONN': 'UConn', 'TXAM': 'Texas A&M',
        'MARQ': 'Marquette', 'ISU': 'Iowa State', 'CREI': 'Creighton', 'MICH': 'Michigan',
        'MSU': 'Michigan State', 'WIS': 'Wisconsin', 'IU': 'Indiana', 'OSU': 'Ohio State',
        'OKLA': 'Oklahoma', 'TEX': 'Texas', 'BAY': 'Baylor', 'TCU': 'TCU', 'TTU': 'Texas Tech',
        'UCLA': 'UCLA', 'USC': 'USC', 'OREG': 'Oregon', 'WASH': 'Washington', 'COLO': 'Colorado',
        'STAN': 'Stanford', 'CAL': 'California', 'UTAH': 'Utah', 'ASU': 'Arizona State',
        'NAVY': 'Navy', 'ARMY': 'Army', 'AF': 'Air Force', 'ND': 'Notre Dame',
        'SCAR': 'South Carolina', 'MISS': 'Ole Miss', 'LSU': 'LSU', 'ARK': 'Arkansas',
        'AUB': 'Auburn', 'UGA': 'Georgia', 'MIZ': 'Missouri', 'FLA': 'Florida', 'VANDY': 'Vanderbilt',
        'WAKE': 'Wake Forest', 'VT': 'Virginia Tech', 'UVA': 'Virginia', 'LOU': 'Louisville',
        'SYR': 'Syracuse', 'CLEM': 'Clemson', 'FSU': 'Florida State', 'GT': 'Georgia Tech',
        'PITT': 'Pittsburgh', 'BC': 'Boston College', 'MIA': 'Miami', 'NCST': 'NC State',
        'SDSU': 'San Diego State', 'NEV': 'Nevada', 'UNLV': 'UNLV', 'CSU': 'Colorado State',
        'FRES': 'Fresno State', 'BSU': 'Boise State', 'USU': 'Utah State', 'SJSU': 'San Jose State',
        'BYU': 'BYU', 'CINC': 'Cincinnati', 'UCF': 'UCF', 'USF': 'USF', 'MEM': 'Memphis',
        'SMU': 'SMU', 'TULN': 'Tulane', 'ECU': 'East Carolina', 'TEM': 'Temple', 'WVU': 'West Virginia',
        'XAV': 'Xavier', 'BUT': 'Butler', 'PROV': 'Providence', 'SETON': 'Seton Hall',
        'DEP': 'DePaul', 'GTWN': 'Georgetown', 'SJU': "St. John's", 'VILL': 'Villanova',
        'DAV': 'Davidson', 'RICH': 'Richmond', 'GMAS': 'George Mason', 'FORD': 'Fordham',
        'SLU': 'Saint Louis', 'MASS': 'UMass', 'URI': 'Rhode Island', 'LAS': 'La Salle',
        'DUQ': 'Duquesne', 'SBU': 'St. Bonaventure', 'ZONA': 'Arizona',
        # Directional schools - key abbreviations from Covers.com
        'W MI': 'Western Michigan', 'WMU': 'Western Michigan', 'W MICH': 'Western Michigan',
        'E MI': 'Eastern Michigan', 'EMU': 'Eastern Michigan', 'E MICH': 'Eastern Michigan',
        'C MI': 'Central Michigan', 'CMU': 'Central Michigan', 'C MICH': 'Central Michigan',
        'S IL': 'Southern Illinois', 'SIU': 'Southern Illinois', 'S ILL': 'Southern Illinois',
        'N IL': 'Northern Illinois', 'NIU': 'Northern Illinois', 'N ILL': 'Northern Illinois',
        'W IL': 'Western Illinois', 'WIU': 'Western Illinois',
        'E IL': 'Eastern Illinois', 'EIU': 'Eastern Illinois',
        'ILL ST': 'Illinois State', 'ILST': 'Illinois State',
        'ST J': "St. John's", 'STJO': "St. John's", "ST JOHN'S": "St. John's",
        'AKRO': 'Akron', 'AKRON': 'Akron',
        'OHIO': 'Ohio', 'OH': 'Ohio',
        'LITT': 'Little Rock', 'LR': 'Little Rock', 'UALR': 'Little Rock',
        'UT M': 'UT Martin', 'UTM': 'UT Martin', 'UTMT': 'UT Martin',
        'UMAS': 'UMass', 'UMASS': 'UMass',
        # Missing Covers abbreviations for today's slate games
        'MTSU': 'MTSU', 'MT': 'MTSU', 'M TENN': 'MTSU',
        'DEL': 'Delaware', 'DELA': 'Delaware', 'Delaware': 'Delaware',
        'DEST': 'Delaware St', 'DSU': 'Delaware St', 'DEL ST': 'Delaware St',
        'GWASH': 'G Washington', 'GW': 'G Washington', 'GWU': 'G Washington',
        'SCUP': 'SC Upstate', 'UPST': 'SC Upstate',
        'KENN': 'Kennesaw St', 'KSU': 'Kennesaw St', 'KENST': 'Kennesaw St',
        'JVST': 'Jax State', 'JSU': 'Jax State', 'JKST': 'Jax State',
        'UCIRV': 'UC Irvine', 'UCI': 'UC Irvine',
        'UCSB': 'Santa Barbara', 'SB': 'Santa Barbara',
        'STJOS': "Saint Joseph's", 'SJU': "Saint Joseph's", "ST JOE": "Saint Joseph's",
        'GMAS': 'George Mason', 'GMU': 'George Mason', 'GM': 'George Mason',
        'SIENA': 'Siena', 'SIE': 'Siena',
        'STPET': "Saint Peter's", "SPU": "Saint Peter's",
        'ORAL': 'Oral Roberts', 'ORU': 'Oral Roberts',
        'STTHOM': 'St Thomas (MN)', 'STMN': 'St Thomas (MN)',
        'FGCU': 'FGCU', 'FGC': 'FGCU',
        'BELL': 'Bellarmine', 'BLRM': 'Bellarmine',
        'MDESH': 'MD Eastern', 'UMES': 'MD Eastern',
        'MORG': 'Morgan St', 'MST': 'Morgan St',
        'ALCN': 'Alcorn St', 'ALST': 'Alcorn St',
        'ARPB': 'AR-Pine Bluff', 'UAPB': 'AR-Pine Bluff',
        'CARK': 'C Arkansas', 'UCA': 'C Arkansas',
        'LIPS': 'Lipscomb', 'LIP': 'Lipscomb',
        'CMICH': 'C Michigan', 'CMCH': 'C Michigan',
        'ULL': 'Louisiana', 'LA': 'Louisiana', 'ULAF': 'Louisiana',
        'NWST': "N'Western St", 'NWLA': "N'Western St",
        'ETAM': 'E Texas A&M', 'ETAMU': 'E Texas A&M',
        'FIU': 'FIU', 'FINT': 'FIU',
        'WKU': 'Western KY', 'WKEN': 'Western KY',
        'GWEB': 'Gardner-Webb', 'GWB': 'Gardner-Webb',
        'PRES': 'Presbyterian', 'PC': 'Presbyterian',
        'SOIN': 'So Indiana', 'USI': 'So Indiana',
        'SEMO': 'SE Missouri', 'SEM': 'SE Missouri',
        'UCSD': 'UC San Diego', 'USD': 'UC San Diego',
        'HAW': "Hawai'i", 'HAWI': "Hawai'i", 'UH': "Hawai'i",
        'UMAL': 'UMass Lowell', 'UML': 'UMass Lowell',
        'UALB': 'UAlbany', 'ALB': 'UAlbany',
        'WAGN': 'Wagner', 'WAG': 'Wagner',
        'CCSU': 'C Connecticut', 'CCST': 'C Connecticut',
        'ACU': 'Abilene Chrstn', 'ABCH': 'Abilene Chrstn',
        'CBU': 'CA Baptist', 'CABA': 'CA Baptist',
        # Philadelphia 76ers for NBA
        'PHI': 'Sixers', '76ERS': 'Sixers', 'SIXERS': 'Sixers',
        'GSW': 'Warriors', 'GS': 'Warriors',
    }
    
    # NHL Team abbreviation to nickname mapping
    nhl_abbr_to_name = {
        'ANA': 'Ducks', 'ARI': 'Coyotes', 'BOS': 'Bruins', 'BUF': 'Sabres',
        'CGY': 'Flames', 'CAR': 'Hurricanes', 'CHI': 'Blackhawks', 'COL': 'Avalanche',
        'CBJ': 'Blue Jackets', 'DAL': 'Stars', 'DET': 'Red Wings', 'EDM': 'Oilers',
        'FLA': 'Panthers', 'LA': 'Kings', 'LAK': 'Kings', 'MIN': 'Wild',
        'MTL': 'Canadiens', 'NSH': 'Predators', 'NJ': 'Devils', 'NJD': 'Devils',
        'NYI': 'Islanders', 'NYR': 'Rangers', 'OTT': 'Senators', 'PHI': 'Flyers',
        'PIT': 'Penguins', 'SJ': 'Sharks', 'SJS': 'Sharks', 'SEA': 'Kraken',
        'STL': 'Blues', 'TB': 'Lightning', 'TBL': 'Lightning', 'TOR': 'Maple Leafs',
        'VAN': 'Canucks', 'VGK': 'Golden Knights', 'WSH': 'Capitals', 'WPG': 'Jets',
        'UTA': 'Utah Hockey Club'
    }
    
    if league == 'NBA':
        abbr_to_name = nba_abbr_to_name
    elif league == 'CBB':
        abbr_to_name = cbb_abbr_to_name
    elif league == 'NHL':
        abbr_to_name = nhl_abbr_to_name
    else:
        abbr_to_name = nba_abbr_to_name
    
    try:
        today_str = datetime.now().strftime('%Y-%m-%d')
        if league == 'NBA':
            url = f'https://www.covers.com/sports/nba/matchups?selectedDate={today_str}'
        elif league == 'CBB':
            url = f'https://www.covers.com/sports/ncaab/matchups?selectedDate={today_str}'
        elif league == 'NHL':
            url = f'https://www.covers.com/sports/nhl/matchups?selectedDate={today_str}'
        else:
            url = f'https://www.covers.com/sports/nba/matchups?selectedDate={today_str}'
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            logger.warning(f"Covers.com returned status {response.status_code}")
            return stats_by_team
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all game boxes - pregamebox for upcoming, include both types
        gameboxes = soup.find_all(class_='pregamebox')
        logger.info(f"Found {len(gameboxes)} games on Covers.com {league}")
        
        for gamebox in gameboxes:
            try:
                # Get team abbreviations from gamebox-team-anchor elements
                team_anchors = gamebox.find_all(class_=re.compile(r'gamebox-team-anchor'))
                if len(team_anchors) < 2:
                    continue
                
                away_abbr = team_anchors[0].get_text(strip=True)
                home_abbr = team_anchors[1].get_text(strip=True)
                
                # Get name from mapping, or extract from href URL
                away_name = abbr_to_name.get(away_abbr, away_abbr)
                home_name = abbr_to_name.get(home_abbr, home_abbr)
                
                # For CBB, also try to extract full name from href URL
                if league == 'CBB':
                    away_href = team_anchors[0].get('href', '')
                    home_href = team_anchors[1].get('href', '')
                    # Extract name from URL like "/teams/main/georgia-southern-eagles"
                    if '/teams/main/' in away_href:
                        url_name = away_href.split('/teams/main/')[-1].rsplit('-', 1)[0]  # Remove mascot
                        away_url_name = ' '.join(w.capitalize() for w in url_name.split('-'))
                    else:
                        away_url_name = away_abbr
                    if '/teams/main/' in home_href:
                        url_name = home_href.split('/teams/main/')[-1].rsplit('-', 1)[0]
                        home_url_name = ' '.join(w.capitalize() for w in url_name.split('-'))
                    else:
                        home_url_name = home_abbr
                
                # Parse stats table
                table = gamebox.find('table')
                if not table:
                    continue
                
                away_stats = {'abbr': away_abbr, 'name': away_name}
                home_stats = {'abbr': home_abbr, 'name': home_name}
                
                rows = table.find_all('tr')
                for row_idx, row in enumerate(rows):
                    cells = row.find_all('td')
                    
                    # Table format: [away_breakdown, away_overall, home_overall, home_breakdown]
                    # Row 0 = Win/Loss, Row 1 = ATS, Row 2 = Last 10
                    if len(cells) >= 4:
                        away_breakdown = cells[0].get_text(strip=True)
                        away_overall = cells[1].get_text(strip=True)
                        home_overall = cells[2].get_text(strip=True)
                        home_breakdown = cells[3].get_text(strip=True)
                        
                        if row_idx == 0:  # Win/Loss row
                            away_stats['record'] = away_overall
                            away_stats['road_record'] = away_breakdown
                            home_stats['record'] = home_overall
                            home_stats['home_record'] = home_breakdown
                        elif row_idx == 1:  # ATS row
                            away_stats['ats'] = away_overall
                            away_stats['ats_road'] = away_breakdown
                            home_stats['ats'] = home_overall
                            home_stats['ats_home'] = home_breakdown
                        elif row_idx == 2:  # Last 10 row
                            away_stats['l10'] = away_overall
                            away_stats['l10_ats'] = away_breakdown
                            home_stats['l10'] = home_overall
                            home_stats['l10_ats'] = home_breakdown
                
                # Store by abbr, mapped name, and URL-extracted name for easy lookup
                stats_by_team[away_abbr] = away_stats
                stats_by_team[away_name] = away_stats
                stats_by_team[home_abbr] = home_stats
                stats_by_team[home_name] = home_stats
                
                # For CBB, also store by URL-extracted names
                if league == 'CBB':
                    if away_url_name and away_url_name != away_abbr:
                        stats_by_team[away_url_name] = away_stats
                    if home_url_name and home_url_name != home_abbr:
                        stats_by_team[home_url_name] = home_stats
                
            except Exception as e:
                logger.debug(f"Error parsing gamebox: {e}")
                continue
        
        logger.info(f"Parsed stats for {len(stats_by_team)//2} teams from Covers.com")
        return stats_by_team
        
    except Exception as e:
        logger.error(f"Error fetching Covers matchups: {e}")
        return stats_by_team


def get_nba_team_stats() -> Dict:
    """
    Fetch comprehensive NBA team stats including ATS, Last 10, Home/Road records.
    Uses ESPN API for reliable data.
    """
    stats = {}
    try:
        # Fetch from ESPN standings API which has more details
        url = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams'
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for team in data.get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', []):
                team_info = team.get('team', {})
                team_name = team_info.get('displayName', '').split()[-1]
                team_abbr = team_info.get('abbreviation', '')
                
                # Get record from team summary
                record = team_info.get('record', {})
                items = record.get('items', [])
                
                for item in items:
                    if item.get('type') == 'total':
                        stats_list = item.get('stats', [])
                        wins = losses = 0
                        for s in stats_list:
                            if s.get('name') == 'wins':
                                wins = int(s.get('value', 0))
                            if s.get('name') == 'losses':
                                losses = int(s.get('value', 0))
                        stats[team_name] = {
                            'overall_record': f"{wins}-{losses}",
                            'wins': wins,
                            'losses': losses
                        }
                        stats[team_abbr] = stats[team_name]
        
        logger.info(f"Fetched NBA team stats for {len(stats)} teams")
    except Exception as e:
        logger.warning(f"Error fetching NBA team stats: {e}")
    
    return stats


# ============================================================
# SCORESANDODDS.COM SCRAPER (BULLETPROOF)
# ============================================================

class ScoresAndOddsScraper:
    """
    Fast, bulletproof scraper for ScoresAndOdds.com.
    
    Extracts:
    - Opening lines
    - Current lines
    - Live line movement
    - Closing lines (post-game)
    - Betting percentages
    """
    
    BASE_URL = "https://www.scoresandodds.com"
    CACHE_TTL = 30  # Cache for 30 seconds (more frequent updates)
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Referer': 'https://www.scoresandodds.com/',
        })
        self._cache = {}
        self._cache_time = {}
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still fresh."""
        if key not in self._cache:
            return False
        age = time.time() - self._cache_time.get(key, 0)
        return age < self.CACHE_TTL
    
    def get_line_movements(self, league: str = 'NBA') -> List[Dict]:
        """
        Get line movements for all games.
        
        Args:
            league: 'NBA' or 'CBB'
            
        Returns:
            List of dictionaries with:
            - away_team, home_team
            - opening_spread, current_spread
            - line_movement (difference)
            - away_money_pct, home_money_pct
            - opening_total, current_total
        """
        cache_key = f"lines_{league}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        try:
            # Determine URL
            if league == 'NBA':
                url = f"{self.BASE_URL}/nba"
            elif league == 'CBB':
                url = f"{self.BASE_URL}/ncaab"
            else:
                return []
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            games = []
            
            # Find all game rows
            game_rows = soup.find_all('tr', class_=re.compile(r'.*game.*', re.I))
            
            for row in game_rows:
                try:
                    game = self._parse_game_row(row)
                    if game:
                        games.append(game)
                except Exception as e:
                    logger.debug(f"Error parsing game row: {e}")
                    continue
            
            # Cache results
            self._cache[cache_key] = games
            self._cache_time[cache_key] = time.time()
            
            logger.info(f"Fetched {len(games)} games from ScoresAndOdds for {league}")
            return games
            
        except Exception as e:
            logger.error(f"Error fetching ScoresAndOdds lines: {e}")
            return []
    
    def _parse_game_row(self, row) -> Optional[Dict]:
        """Parse individual game row."""
        game = {}
        
        # Extract teams
        team_cells = row.find_all('td', class_=re.compile(r'.*team.*', re.I))
        if len(team_cells) >= 2:
            game['away_team'] = team_cells[0].text.strip()
            game['home_team'] = team_cells[1].text.strip()
        else:
            return None
        
        # Extract spreads
        spread_cells = row.find_all('td', class_=re.compile(r'.*spread.*', re.I))
        if spread_cells:
            # Try to find opening and current
            spreads = []
            for cell in spread_cells:
                text = cell.text.strip()
                match = re.search(r'([+-]?\d+\.?\d*)', text)
                if match:
                    spreads.append(float(match.group(1)))
            
            if len(spreads) >= 2:
                game['opening_spread'] = spreads[0]
                game['current_spread'] = spreads[-1]
                game['line_movement'] = spreads[-1] - spreads[0]
            elif len(spreads) == 1:
                game['current_spread'] = spreads[0]
        
        # Extract totals
        total_cells = row.find_all('td', class_=re.compile(r'.*total.*|.*o/u.*', re.I))
        if total_cells:
            totals = []
            for cell in total_cells:
                text = cell.text.strip()
                match = re.search(r'(\d+\.?\d*)', text)
                if match:
                    totals.append(float(match.group(1)))
            
            if len(totals) >= 2:
                game['opening_total'] = totals[0]
                game['current_total'] = totals[-1]
            elif len(totals) == 1:
                game['current_total'] = totals[0]
        
        # Extract betting percentages
        pct_cells = row.find_all('td', class_=re.compile(r'.*pct.*|.*percent.*', re.I))
        if pct_cells:
            percentages = []
            for cell in pct_cells:
                text = cell.text.strip()
                match = re.search(r'(\d+)%', text)
                if match:
                    percentages.append(int(match.group(1)))
            
            if len(percentages) >= 2:
                game['away_money_pct'] = percentages[0]
                game['home_money_pct'] = percentages[1]
        
        return game if game.get('away_team') and game.get('home_team') else None
    
    def get_closing_lines(self, league: str = 'NBA', date: Optional[str] = None) -> List[Dict]:
        """
        Get closing lines for completed games.
        
        Args:
            league: 'NBA' or 'CBB'
            date: Date in format 'YYYY-MM-DD' (default: yesterday)
            
        Returns:
            List of games with closing lines
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        cache_key = f"closing_{league}_{date}"
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]
        
        try:
            if league == 'NBA':
                url = f"{self.BASE_URL}/nba/scores/{date}"
            elif league == 'CBB':
                url = f"{self.BASE_URL}/ncaab/scores/{date}"
            else:
                return []
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            games = []
            
            # Parse completed games
            game_rows = soup.find_all('tr', class_=re.compile(r'.*final.*|.*complete.*', re.I))
            
            for row in game_rows:
                try:
                    game = self._parse_game_row(row)
                    if game:
                        game['date'] = date
                        game['closing_spread'] = game.get('current_spread')
                        game['closing_total'] = game.get('current_total')
                        games.append(game)
                except Exception as e:
                    logger.debug(f"Error parsing completed game: {e}")
                    continue
            
            # Cache results
            self._cache[cache_key] = games
            self._cache_time[cache_key] = time.time()
            
            logger.info(f"Fetched {len(games)} closing lines from ScoresAndOdds for {league} on {date}")
            return games
            
        except Exception as e:
            logger.error(f"Error fetching closing lines: {e}")
            return []


# ============================================================
# INTEGRATED SCRAPING MANAGER
# ============================================================

class IntegratedScrapingManager:
    """
    Manages all scraping with parallel execution for speed.
    """
    
    def __init__(self):
        self.covers = CoversScraper()
        self.scores_odds = ScoresAndOddsScraper()
    
    def get_complete_slate(self, league: str = 'NBA') -> List[Dict]:
        """
        Get complete slate with data from both sources in parallel.
        
        Args:
            league: 'NBA' or 'CBB'
            
        Returns:
            List of games with combined data from Covers and ScoresAndOdds
        """
        start_time = time.time()
        
        # Fetch from both sources in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            covers_future = executor.submit(self.covers.get_todays_matchups, league)
            scores_future = executor.submit(self.scores_odds.get_line_movements, league)
            
            covers_matchups = covers_future.result()
            scores_games = scores_future.result()
        
        # Merge data
        merged_games = self._merge_game_data(covers_matchups, scores_games)
        
        # Add logos for CBB
        if league == 'CBB':
            for game in merged_games:
                game['away_logo'] = get_cbb_logo(game.get('away_team', ''))
                game['home_logo'] = get_cbb_logo(game.get('home_team', ''))
        
        elapsed = time.time() - start_time
        logger.info(f"Complete slate fetched in {elapsed:.2f}s: {len(merged_games)} games")
        
        return merged_games
    
    def _merge_game_data(self, covers_data: List[Dict], scores_data: List[Dict]) -> List[Dict]:
        """
        Merge data from Covers and ScoresAndOdds.
        
        Matching strategy:
        1. Try exact team name match
        2. Try fuzzy match (remove common suffixes, check contains)
        3. Create separate entries if no match
        """
        merged = []
        matched_scores_indices = set()
        
        # Match Covers games with ScoresAndOdds
        for covers_game in covers_data:
            best_match = None
            best_match_idx = None
            best_score = 0
            
            for idx, scores_game in enumerate(scores_data):
                if idx in matched_scores_indices:
                    continue
                
                score = self._match_teams(
                    covers_game.get('away_team', ''),
                    covers_game.get('home_team', ''),
                    scores_game.get('away_team', ''),
                    scores_game.get('home_team', '')
                )
                
                if score > best_score:
                    best_score = score
                    best_match = scores_game
                    best_match_idx = idx
            
            # Merge if good match found
            if best_score >= 0.7:  # 70% confidence threshold
                game = {**covers_game, **best_match}
                merged.append(game)
                matched_scores_indices.add(best_match_idx)
            else:
                # Add covers game without ScoresAndOdds data
                merged.append(covers_game)
        
        # Add remaining ScoresAndOdds games
        for idx, scores_game in enumerate(scores_data):
            if idx not in matched_scores_indices:
                merged.append(scores_game)
        
        return merged
    
    def _match_teams(self, away1: str, home1: str, away2: str, home2: str) -> float:
        """
        Calculate match score between two game pairings.
        
        Returns:
            Score from 0.0 to 1.0 (1.0 = perfect match)
        """
        def normalize(name: str) -> str:
            """Normalize team name for comparison."""
            name = name.lower()
            # Remove common suffixes
            for suffix in [' basketball', ' men', ' women', ' ncaa']:
                name = name.replace(suffix, '')
            return name.strip()
        
        away1_norm = normalize(away1)
        home1_norm = normalize(home1)
        away2_norm = normalize(away2)
        home2_norm = normalize(home2)
        
        # Check exact match
        if away1_norm == away2_norm and home1_norm == home2_norm:
            return 1.0
        
        # Check reversed (though rare)
        if away1_norm == home2_norm and home1_norm == away2_norm:
            return 0.9
        
        # Check partial matches
        away_match = (away1_norm in away2_norm or away2_norm in away1_norm)
        home_match = (home1_norm in home2_norm or home2_norm in home1_norm)
        
        if away_match and home_match:
            return 0.8
        elif away_match or home_match:
            return 0.4
        
        return 0.0


# ============================================================
# EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    manager = IntegratedScrapingManager()
    
    # Get NBA slate
    print("\n" + "="*80)
    print("NBA SLATE")
    print("="*80)
    nba_games = manager.get_complete_slate('NBA')
    for game in nba_games[:3]:  # Show first 3
        print(f"\n{game.get('away_team')} @ {game.get('home_team')}")
        print(f"  Spread: {game.get('opening_spread')} → {game.get('current_spread')}")
        print(f"  Movement: {game.get('line_movement')}")
        print(f"  Money %: Away {game.get('away_money_pct')}% | Home {game.get('home_money_pct')}%")
    
    # Get CBB slate with logos
    print("\n" + "="*80)
    print("CBB SLATE (with logos)")
    print("="*80)
    cbb_games = manager.get_complete_slate('CBB')
    for game in cbb_games[:3]:  # Show first 3
        print(f"\n{game.get('away_team')} @ {game.get('home_team')}")
        print(f"  Away Logo: {game.get('away_logo') or 'N/A'}")
        print(f"  Home Logo: {game.get('home_logo') or 'N/A'}")
        print(f"  Spread: {game.get('opening_spread')} → {game.get('current_spread')}")
    
    # Get closing lines
    print("\n" + "="*80)
    print("CLOSING LINES (Yesterday)")
    print("="*80)
    closing = manager.scores_odds.get_closing_lines('NBA')
    for game in closing[:3]:
        print(f"\n{game.get('away_team')} @ {game.get('home_team')}")
        print(f"  Closing Spread: {game.get('closing_spread')}")
        print(f"  Closing Total: {game.get('closing_total')}")


# ============================================================
# CBB COVERS + KENPOM SCRAPER
# ============================================================

# KenPom team name mappings (Covers name -> KenPom URL slug)
KENPOM_TEAM_SLUGS = {
    'Duke': 'Duke', 'North Carolina': 'North.Carolina', 'UNC': 'North.Carolina',
    'Kentucky': 'Kentucky', 'Kansas': 'Kansas', 'Gonzaga': 'Gonzaga',
    'Auburn': 'Auburn', 'Houston': 'Houston', 'Tennessee': 'Tennessee',
    'Alabama': 'Alabama', 'Purdue': 'Purdue', 'Florida': 'Florida',
    'Iowa State': 'Iowa.St.', 'Iowa St': 'Iowa.St.', 'Iowa St.': 'Iowa.St.',
    'Michigan State': 'Michigan.St.', 'Michigan St': 'Michigan.St.',
    'Ohio State': 'Ohio.St.', 'Ohio St': 'Ohio.St.', 'Ohio St.': 'Ohio.St.',
    'Texas Tech': 'Texas.Tech', 'Texas': 'Texas', 'Arizona': 'Arizona',
    'UCLA': 'UCLA', 'UConn': 'Connecticut', 'Connecticut': 'Connecticut',
    'Baylor': 'Baylor', 'Illinois': 'Illinois', 'Wisconsin': 'Wisconsin',
    'Michigan': 'Michigan', 'Indiana': 'Indiana', 'Maryland': 'Maryland',
    'Oregon': 'Oregon', 'Creighton': 'Creighton', 'Marquette': 'Marquette',
    'St. John\'s': 'St..John\'s', "St. John's": "St..John's",
    'Villanova': 'Villanova', 'Xavier': 'Xavier', 'Cincinnati': 'Cincinnati',
    'Louisville': 'Louisville', 'Memphis': 'Memphis', 'SMU': 'SMU',
    'TCU': 'TCU', 'Texas A&M': 'Texas.A&M', 'Arkansas': 'Arkansas',
    'LSU': 'LSU', 'Missouri': 'Missouri', 'Mississippi St': 'Mississippi.St.',
    'Mississippi State': 'Mississippi.St.', 'Ole Miss': 'Mississippi',
    'South Carolina': 'South.Carolina', 'Georgia': 'Georgia',
    'Vanderbilt': 'Vanderbilt', 'Oklahoma': 'Oklahoma',
    'Oklahoma St': 'Oklahoma.St.', 'Oklahoma State': 'Oklahoma.St.',
    'Kansas St': 'Kansas.St.', 'Kansas State': 'Kansas.St.',
    'West Virginia': 'West.Virginia', 'BYU': 'BYU', 'Colorado': 'Colorado',
    'Utah': 'Utah', 'Arizona St': 'Arizona.St.', 'Arizona State': 'Arizona.St.',
    'Penn St': 'Penn.St.', 'Penn State': 'Penn.St.', 'Rutgers': 'Rutgers',
    'Minnesota': 'Minnesota', 'Nebraska': 'Nebraska', 'Iowa': 'Iowa',
    'Northwestern': 'Northwestern', 'Syracuse': 'Syracuse',
    'Wake Forest': 'Wake.Forest', 'NC State': 'N.C..State',
    'Virginia Tech': 'Virginia.Tech', 'Clemson': 'Clemson',
    'Virginia': 'Virginia', 'Miami': 'Miami.FL', 'Florida St': 'Florida.St.',
    'Florida State': 'Florida.St.', 'Boston College': 'Boston.College',
    'Georgia Tech': 'Georgia.Tech', 'Pitt': 'Pittsburgh', 'Pittsburgh': 'Pittsburgh',
    'Notre Dame': 'Notre.Dame', 'Stanford': 'Stanford', 'Cal': 'California',
    'USC': 'USC', 'Washington': 'Washington', 'Washington St': 'Washington.St.',
    'San Diego St': 'San.Diego.St.', 'San Diego State': 'San.Diego.St.',
    'Nevada': 'Nevada', 'UNLV': 'UNLV', 'New Mexico': 'New.Mexico',
    'Boise St': 'Boise.St.', 'Boise State': 'Boise.St.',
    'VCU': 'VCU', 'Richmond': 'Richmond', 'Dayton': 'Dayton',
    'Saint Louis': 'Saint.Louis', 'UMass': 'Massachusetts',
    'George Mason': 'George.Mason', 'St. Bonaventure': 'St..Bonaventure',
    'Wichita St': 'Wichita.St.', 'Wichita State': 'Wichita.St.',
    'Murray St': 'Murray.St.', 'Murray State': 'Murray.St.',
    'Drake': 'Drake', 'Loyola Chicago': 'Loyola.Chicago',
    'UNC Asheville': 'UNC.Asheville', 'UNC Greensboro': 'UNC.Greensboro',
    'UNCG': 'UNC.Greensboro', 'Winthrop': 'Winthrop', 'The Citadel': 'Citadel',
    # Additional team mappings for Covers.com abbreviations
    'UNCA': 'UNC.Asheville', 'WIN': 'Winthrop', 'CIT': 'Citadel',
    'Citadel': 'Citadel', 'STON': 'Stony.Brook', 'Stony Brook': 'Stony.Brook',
    'ELON': 'Elon', 'Elon': 'Elon', 'NCST': 'N.C..State', 'WAKE': 'Wake.Forest',
    # San Jose State
    'San Jose State': 'San.Jose.St.', 'San Jose St': 'San.Jose.St.',
    'San José State': 'San.Jose.St.', 'San José St': 'San.Jose.St.',
    'SJSU': 'San.Jose.St.', 'SJ State': 'San.Jose.St.',
    # More team slugs
    'New Mexico': 'New.Mexico', 'UNM': 'New.Mexico', 'Air Force': 'Air.Force',
    'Sacramento State': 'Sacramento.St.', 'Sacramento St': 'Sacramento.St.',
    'SAC': 'Sacramento.St.', 'Sac State': 'Sacramento.St.',
    'Montana': 'Montana', 'MONT': 'Montana',
    "Saint Mary's": 'Saint.Marys', 'SMC': 'Saint.Marys', "St Mary's": 'Saint.Marys',
    'Gonzaga': 'Gonzaga', 'GONZ': 'Gonzaga', 'Zags': 'Gonzaga',
    'Morehead State': 'Morehead.St.', 'Morehead St': 'Morehead.St.',
    'Eastern Kentucky': 'Eastern.Kentucky', 'EKU': 'Eastern.Kentucky',
    'Western Kentucky': 'Western.Kentucky', 'WKU': 'Western.Kentucky',
    'Middle Tennessee': 'Middle.Tennessee', 'MTSU': 'Middle.Tennessee',
    'North Texas': 'North.Texas', 'UNT': 'North.Texas',
    'Texas State': 'Texas.St.', 'TXST': 'Texas.St.',
    'Georgia State': 'Georgia.St.', 'GSU': 'Georgia.St.',
    'Appalachian State': 'Appalachian.St.', 'App State': 'Appalachian.St.',
    'Coastal Carolina': 'Coastal.Carolina',
    'James Madison': 'James.Madison', 'JMU': 'James.Madison',
    'Old Dominion': 'Old.Dominion', 'ODU': 'Old.Dominion',
    'Southern Miss': 'Southern.Miss', 'USM': 'Southern.Miss',
    'UT Arlington': 'UT.Arlington', 'UTA': 'UT.Arlington',
    'Maine': 'Maine', 'Vermont': 'Vermont', 'Albany': 'Albany',
    'UMass Lowell': 'UMass.Lowell', 'UMBC': 'UMBC',
    'New Hampshire': 'New.Hampshire', 'UNH': 'New.Hampshire',
    'Hartford': 'Hartford', 'Binghamton': 'Binghamton',
    'Bryant': 'Bryant', 'NJIT': 'NJIT', 'Fairleigh Dickinson': 'Fairleigh.Dickinson',
    'Central Connecticut': 'Central.Connecticut', 'CCSU': 'Central.Connecticut',
    'Sacred Heart': 'Sacred.Heart', 'St. Francis (PA)': 'St..Francis.PA',
    'Mount St. Mary\'s': 'Mount.St..Mary\'s', 'LIU': 'LIU', 'Wagner': 'Wagner',
    'Merrimack': 'Merrimack', 'Stonehill': 'Stonehill',
    
    # Additional State schools with KenPom periods
    'Mississippi St.': 'Mississippi.St.', 'Mississippi State': 'Mississippi.St.',
    'Illinois St.': 'Illinois.St.', 'Illinois State': 'Illinois.St.',
    'Arkansas St.': 'Arkansas.St.', 'Arkansas State': 'Arkansas.St.',
    'Indiana St.': 'Indiana.St.', 'Indiana State': 'Indiana.St.',
    'Missouri St.': 'Missouri.St.', 'Missouri State': 'Missouri.St.',
    'Stephen F. Austin': 'Stephen.F..Austin', 'SFA': 'Stephen.F..Austin',
    'Cal St. Fullerton': 'Cal.St..Fullerton', 'CSF': 'Cal.St..Fullerton', 'CSUF': 'Cal.St..Fullerton',
    'Cal St. Bakersfield': 'Cal.St..Bakersfield', 'CSUB': 'Cal.St..Bakersfield',
    'Long Beach St.': 'Long.Beach.St.', 'Long Beach State': 'Long.Beach.St.', 'LBSU': 'Long.Beach.St.',
    'Portland St.': 'Portland.St.', 'Portland State': 'Portland.St.',
    'Wright St.': 'Wright.St.', 'Wright State': 'Wright.St.',
    'Kennesaw St.': 'Kennesaw.St.', 'Kennesaw State': 'Kennesaw.St.',
    'Jacksonville St.': 'Jacksonville.St.', 'Jacksonville State': 'Jacksonville.St.',
    'Weber St.': 'Weber.St.', 'Weber State': 'Weber.St.',
    'North Dakota St.': 'North.Dakota.St.', 'North Dakota State': 'North.Dakota.St.', 'NDSU': 'North.Dakota.St.',
    'South Dakota St.': 'South.Dakota.St.', 'South Dakota State': 'South.Dakota.St.', 'SDSU': 'South.Dakota.St.',
    'Tarleton St.': 'Tarleton.St.', 'Tarleton State': 'Tarleton.St.',
    'East Tennessee St.': 'East.Tennessee.St.', 'East Tennessee State': 'East.Tennessee.St.', 'ETSU': 'East.Tennessee.St.',
    'Youngstown St.': 'Youngstown.St.', 'Youngstown State': 'Youngstown.St.', 'YSU': 'Youngstown.St.',
    'Tennessee St.': 'Tennessee.St.', 'Tennessee State': 'Tennessee.St.', 'TSU': 'Tennessee.St.',
    'Cleveland St.': 'Cleveland.St.', 'Cleveland State': 'Cleveland.St.',
    'San Jose St.': 'San.Jose.St.',
    'Fresno St.': 'Fresno.St.', 'Fresno State': 'Fresno.St.',
    'Idaho St.': 'Idaho.St.', 'Idaho State': 'Idaho.St.',
    'Montana St.': 'Montana.St.', 'Montana State': 'Montana.St.',
    'Northwestern St.': 'Northwestern.St.', 'Northwestern State': 'Northwestern.St.',
    'Grambling St.': 'Grambling.St.', 'Grambling State': 'Grambling.St.',
    'Norfolk St.': 'Norfolk.St.', 'Norfolk State': 'Norfolk.St.',
    'Alabama St.': 'Alabama.St.', 'Alabama State': 'Alabama.St.',
    'Ball St.': 'Ball.St.', 'Ball State': 'Ball.St.',
    'Kent St.': 'Kent.St.', 'Kent State': 'Kent.St.',
    'St. Thomas': 'St..Thomas',
    
    # Miami disambiguation
    'Miami FL': 'Miami.FL', 'Miami (FL)': 'Miami.FL', 'Miami Florida': 'Miami.FL',
    'Miami OH': 'Miami.OH', 'Miami (OH)': 'Miami.OH', 'Miami Ohio': 'Miami.OH',
    
    # N Illinois / Northern Illinois
    'N Illinois': 'Northern.Illinois', 'Northern Illinois': 'Northern.Illinois', 'NIU': 'Northern.Illinois',
    
    # UCF / Central Florida
    'UCF': 'UCF', 'Central Florida': 'UCF',
    
    # Florida Atlantic
    'Florida Atlantic': 'Florida.Atlantic', 'FAU': 'Florida.Atlantic',
    
    # Santa Clara
    'Santa Clara': 'Santa.Clara',
}


def get_kenpom_slug(team_name: str) -> str:
    """Convert team name to KenPom URL slug."""
    # Try exact match first
    if team_name in KENPOM_TEAM_SLUGS:
        return KENPOM_TEAM_SLUGS[team_name]
    
    # Try case-insensitive match
    for key, slug in KENPOM_TEAM_SLUGS.items():
        if key.lower() == team_name.lower():
            return slug
    
    # Default: replace spaces with dots
    return team_name.replace(' ', '.')


def scrape_covers_cbb_slate(date_str: str = None) -> List[Dict]:
    """
    Scrape today's CBB matchups from Covers.com.
    
    Returns list of games with:
    - away_team, home_team
    - away_record, home_record
    - away_ats, home_ats
    - away_l10, home_l10
    - spread, total
    - game_time
    """
    from datetime import date, datetime
    
    if not date_str:
        date_str = date.today().strftime('%Y-%m-%d')
    
    games = []
    try:
        url = f"https://www.covers.com/sports/ncaab/matchups?selectedDate={date_str}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
        }
        
        logger.info(f"Fetching CBB matchups from Covers.com for {date_str}")
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"Covers CBB fetch failed: {resp.status_code}")
            return games
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Find all game cards
        game_cards = soup.find_all('div', class_=re.compile(r'cmg_matchup_game_box|matchup-card', re.I))
        if not game_cards:
            # Try alternate selectors
            game_cards = soup.find_all('article', class_=re.compile(r'game-card', re.I))
        if not game_cards:
            game_cards = soup.select('[data-game-id], .covers-MatchupCard, .matchup-container')
        
        logger.info(f"Found {len(game_cards)} CBB game cards on Covers")
        
        for card in game_cards:
            try:
                game = {}
                
                # Extract team names
                team_elements = card.find_all(['span', 'div', 'a'], class_=re.compile(r'team.*name|name', re.I))
                if len(team_elements) >= 2:
                    game['away_team'] = team_elements[0].get_text(strip=True)
                    game['home_team'] = team_elements[1].get_text(strip=True)
                
                # Extract records (Win/Loss)
                record_elements = card.find_all(string=re.compile(r'\d+-\d+'))
                if record_elements:
                    for i, rec in enumerate(record_elements[:4]):
                        rec_text = rec.strip() if hasattr(rec, 'strip') else str(rec)
                        match = re.search(r'(\d+-\d+)', rec_text)
                        if match:
                            if i == 0:
                                game['away_record'] = match.group(1)
                            elif i == 1:
                                game['home_record'] = match.group(1)
                
                # Extract ATS records
                ats_elements = card.find_all(string=re.compile(r'\d+-\d+-\d+\s*ATS|\(\d+-\d+-\d+\s*ATS\)'))
                for i, ats in enumerate(ats_elements[:2]):
                    ats_match = re.search(r'(\d+-\d+-\d+)', str(ats))
                    if ats_match:
                        if i == 0:
                            game['away_ats'] = ats_match.group(1)
                        else:
                            game['home_ats'] = ats_match.group(1)
                
                # Extract Last 10
                l10_section = card.find(string=re.compile(r'Last\s*10', re.I))
                if l10_section:
                    parent = l10_section.find_parent()
                    if parent:
                        l10_values = parent.find_all(string=re.compile(r'\d+-\d+'))
                        for i, val in enumerate(l10_values[:2]):
                            l10_match = re.search(r'(\d+-\d+)', str(val))
                            if l10_match:
                                if i == 0:
                                    game['away_l10'] = l10_match.group(1)
                                else:
                                    game['home_l10'] = l10_match.group(1)
                
                # Extract spread and total
                spread_elements = card.find_all(string=re.compile(r'[+-]?\d+\.?\d*'))
                for el in spread_elements:
                    text = str(el).strip()
                    if 'o/u' in text.lower() or text.startswith('o/u'):
                        total_match = re.search(r'(\d+\.?\d*)', text)
                        if total_match:
                            game['total'] = float(total_match.group(1))
                    elif text.startswith('+') or text.startswith('-'):
                        try:
                            game['spread'] = float(text)
                        except:
                            pass
                
                if game.get('away_team') and game.get('home_team'):
                    games.append(game)
                    
            except Exception as e:
                logger.debug(f"Error parsing CBB game card: {e}")
                continue
        
        logger.info(f"Parsed {len(games)} CBB games from Covers.com")
        return games
        
    except Exception as e:
        logger.error(f"Error scraping Covers CBB slate: {e}")
        return games


def scrape_kenpom_team_metrics(team_name: str) -> Dict:
    """
    Scrape key metrics from a team's KenPom page.
    
    Returns:
    - adj_o (Adjusted Offensive Efficiency)
    - adj_d (Adjusted Defensive Efficiency)
    - adj_em (Adjusted Efficiency Margin)
    - adj_tempo (Adjusted Tempo)
    - four_factors (eFG%, TO%, OR%, FT Rate for Off/Def)
    - sos (Strength of Schedule)
    - rank (Overall ranking)
    """
    metrics = {'team': team_name}
    
    try:
        slug = get_kenpom_slug(team_name)
        url = f"https://kenpom.com/team.php?team={slug}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Referer': 'https://kenpom.com/',
        }
        
        logger.info(f"Fetching KenPom metrics for {team_name} ({slug})")
        resp = requests.get(url, headers=headers, timeout=15)
        
        if resp.status_code != 200:
            logger.warning(f"KenPom fetch failed for {team_name}: {resp.status_code}")
            return metrics
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Look for the Scouting Report table
        report_table = soup.find('table', {'id': 'scout-table'})
        if not report_table:
            # Try finding by class or nearby text
            tables = soup.find_all('table')
            for t in tables:
                if 'Adj. Efficiency' in t.get_text() or 'Four Factors' in t.get_text():
                    report_table = t
                    break
        
        if report_table:
            rows = report_table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    label = cells[0].get_text(strip=True).lower()
                    off_val = cells[1].get_text(strip=True)
                    def_val = cells[2].get_text(strip=True)
                    
                    try:
                        if 'adj. efficiency' in label or 'adj efficiency' in label:
                            metrics['adj_o'] = float(re.sub(r'[^\d.]', '', off_val)) if off_val else None
                            metrics['adj_d'] = float(re.sub(r'[^\d.]', '', def_val)) if def_val else None
                        elif 'adj. tempo' in label or 'tempo' in label:
                            metrics['adj_tempo'] = float(re.sub(r'[^\d.]', '', off_val)) if off_val else None
                        elif 'effective fg' in label or 'efg' in label:
                            metrics['efg_off'] = float(re.sub(r'[^\d.]', '', off_val)) if off_val else None
                            metrics['efg_def'] = float(re.sub(r'[^\d.]', '', def_val)) if def_val else None
                        elif 'turnover' in label or 'to%' in label:
                            metrics['to_off'] = float(re.sub(r'[^\d.]', '', off_val)) if off_val else None
                            metrics['to_def'] = float(re.sub(r'[^\d.]', '', def_val)) if def_val else None
                        elif 'off. reb' in label or 'or%' in label:
                            metrics['or_off'] = float(re.sub(r'[^\d.]', '', off_val)) if off_val else None
                            metrics['or_def'] = float(re.sub(r'[^\d.]', '', def_val)) if def_val else None
                        elif 'fta/fga' in label or 'ft rate' in label:
                            metrics['ftr_off'] = float(re.sub(r'[^\d.]', '', off_val)) if off_val else None
                            metrics['ftr_def'] = float(re.sub(r'[^\d.]', '', def_val)) if def_val else None
                    except (ValueError, TypeError):
                        pass
        
        # Calculate adj_em if we have both
        if metrics.get('adj_o') and metrics.get('adj_d'):
            metrics['adj_em'] = round(metrics['adj_o'] - metrics['adj_d'], 1)
        
        # Try to find SOS
        sos_section = soup.find(string=re.compile(r'Strength of Schedule', re.I))
        if sos_section:
            parent = sos_section.find_parent('table') or sos_section.find_parent()
            if parent:
                sos_vals = re.findall(r'([+-]?\d+\.?\d*)', parent.get_text())
                if sos_vals:
                    metrics['sos'] = float(sos_vals[0])
        
        # Try to find overall rank
        rank_match = re.search(r'#(\d+)\s+in', resp.text) or re.search(r'Rank:\s*(\d+)', resp.text)
        if rank_match:
            metrics['rank'] = int(rank_match.group(1))
        
        logger.info(f"KenPom metrics for {team_name}: adj_o={metrics.get('adj_o')}, adj_d={metrics.get('adj_d')}")
        return metrics
        
    except Exception as e:
        logger.error(f"Error scraping KenPom for {team_name}: {e}")
        return metrics


def get_cbb_slate_with_kenpom(date_str: str = None) -> List[Dict]:
    """
    Get today's CBB slate from Covers.com enriched with KenPom metrics.
    
    This is the main function that:
    1. Scrapes Covers.com for today's matchups (records, ATS, L10, spreads)
    2. For each team, scrapes KenPom for key metrics
    3. Returns combined data
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    # Step 1: Get today's games from Covers
    games = scrape_covers_cbb_slate(date_str)
    if not games:
        logger.warning("No CBB games found on Covers.com")
        return []
    
    # Step 2: Get all unique team names
    all_teams = set()
    for g in games:
        if g.get('away_team'):
            all_teams.add(g['away_team'])
        if g.get('home_team'):
            all_teams.add(g['home_team'])
    
    logger.info(f"Fetching KenPom metrics for {len(all_teams)} teams...")
    
    # Step 3: Fetch KenPom metrics in parallel
    team_metrics = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_team = {executor.submit(scrape_kenpom_team_metrics, team): team for team in all_teams}
        for future in as_completed(future_to_team):
            team = future_to_team[future]
            try:
                metrics = future.result()
                team_metrics[team] = metrics
            except Exception as e:
                logger.warning(f"Error fetching KenPom for {team}: {e}")
                team_metrics[team] = {'team': team}
    
    # Step 4: Enrich games with KenPom metrics
    for game in games:
        away = game.get('away_team')
        home = game.get('home_team')
        
        if away and away in team_metrics:
            for key, val in team_metrics[away].items():
                if key != 'team':
                    game[f'away_{key}'] = val
        
        if home and home in team_metrics:
            for key, val in team_metrics[home].items():
                if key != 'team':
                    game[f'home_{key}'] = val
    
    logger.info(f"Enriched {len(games)} CBB games with KenPom metrics")
    return games
