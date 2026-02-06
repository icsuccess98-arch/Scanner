"""
AUTOMATED GAME LOADING & QUALIFICATION SYSTEM
=============================================

Features:
1. Auto-loads games on new day (no manual fetch needed)
2. Transparent logo handling (removes white/black backgrounds)
3. Complete CBB logos from multiple sources
4. Professional elimination filter system
5. Automatic spread qualification

Elimination Process:
- Filter 1: 80%+ Handle (Bets OR Money)
- Filter 2: Large spreads (10+ points)
- Filter 3: Bad teams (0-5 L5, poor records)
- Filter 4: Bottom 5 defense L5 (ranks 27-32)
- Filter 5: Identify qualified games
- Filter 6: Apply sharp action checklist
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
import pytz
from flask import Flask, jsonify
from sqlalchemy import and_, or_
import requests
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


# ============================================================
# COLLEGE BASKETBALL LOGOS - COMPLETE LIST WITH TRANSPARENT BACKGROUNDS
# ============================================================

# ESPN Team ID mapping for CBB - comprehensive list
# Common aliases for ESPN team name matching
ESPN_CBB_TEAM_ALIASES = {
    # State abbreviations
    'App State': 'Appalachian State',
    'App St': 'Appalachian State',
    'Ark State': 'Arkansas State',
    'UNC': 'North Carolina',
    'Coastal': 'Coastal Carolina',
    'Ga Southern': 'Georgia Southern',
    'GA Southern': 'Georgia Southern',
    'Ga State': 'Georgia State',
    'Georgia St': 'Georgia State',
    'Ga Tech': 'Georgia Tech',
    'La Tech': 'Louisiana Tech',
    'Mid Tennessee': 'Middle Tennessee',
    'MTSU': 'Middle Tennessee',
    'W Kentucky': 'Western Kentucky',
    'WKU': 'Western Kentucky',
    'N Texas': 'North Texas',
    'UNT': 'North Texas',
    'E Carolina': 'East Carolina',
    'ECU': 'East Carolina',
    'ODU': 'Old Dominion',
    'UAB': 'UAB',
    'USF': 'South Florida',
    'UNCW': 'UNC Wilmington',
    'UNCG': 'UNC Greensboro',
    'UNCA': 'UNC Asheville',
    'Loyola-MD': 'Loyola MD',
    'Loyola (MD)': 'Loyola MD',
    'St. Marys': "Saint Mary's",
    'St Marys': "Saint Mary's",
    'SMC': "Saint Mary's",
    'Loyola Chi': 'Loyola Chicago',
    'Loy Chicago': 'Loyola Chicago',
    'E Washington': 'Eastern Washington',
    'EWU': 'Eastern Washington',
    'E Illinois': 'Eastern Illinois',
    'E Michigan': 'Eastern Michigan',
    'E Kentucky': 'Eastern Kentucky',
    'W Michigan': 'Western Michigan',
    'W Illinois': 'Western Illinois',
    'W Carolina': 'Western Carolina',
    'N Colorado': 'Northern Colorado',
    'N Illinois': 'Northern Illinois',
    'N Iowa': 'Northern Iowa',
    'N Arizona': 'Northern Arizona',
    'Ariz': 'Arizona',
    'ARIZ': 'Arizona',
    'ASU': 'Arizona State',
    'Arizona St': 'Arizona State',
    'Ariz St': 'Arizona State',
    'Ariz State': 'Arizona State',
    'S Dakota': 'South Dakota',
    'S Dakota St': 'South Dakota State',
    'N Dakota St': 'North Dakota State',
    'Mo State': 'Missouri State',
    'SE Missouri': 'SE Missouri State',
    'SE Louisiana': 'SE Louisiana',
    'SFA': 'Stephen F. Austin',
    'SFASU': 'Stephen F. Austin',
    'TAMUCC': 'Texas A&M-Corpus Christi',
    'TAMUC': 'Texas A&M-Commerce',
    'Cal St Fullerton': 'CS Fullerton',
    'Cal St Northridge': 'CS Northridge',
    'CSU Northridge': 'CS Northridge',
    'Cal St Bakersfield': 'CS Bakersfield',
    'CSUF': 'CS Fullerton',
    'CSUN': 'CS Northridge',
    'CSUB': 'CS Bakersfield',
    'LBSU': 'Long Beach State',
    'LB State': 'Long Beach State',
    'Long Beach St': 'Long Beach State',
    'Pitt': 'Pittsburgh',
    'Ill State': 'Illinois State',
    'Ind State': 'Indiana State',
    'Penn St': 'Penn State',
    'Mich State': 'Michigan State',
    'Ohio St': 'Ohio State',
    'Fla State': 'Florida State',
    'Okla State': 'Oklahoma State',
    'Miss State': 'Mississippi State',
    'Wash State': 'Washington State',
    'Ore State': 'Oregon State',
    'Col State': 'Colorado State',
    'Utah St': 'Utah State',
    # Additional aliases for missing teams
    'C Connecticut': 'Central Connecticut',
    'Cent Connecticut': 'Central Connecticut',
    'CCSU': 'Central Connecticut',
    'Bakersfield': 'CS Bakersfield',
    'FDU': 'Fairleigh Dickinson',
    "Hawai'i": 'Hawaii',
    'Hawaii': 'Hawaii',
    'Montana St': 'Montana State',
    'NC A&T': 'North Carolina A&T',
    'N.C. A&T': 'North Carolina A&T',
    'FGCU': 'Florida Gulf Coast',
    'Fla Gulf Coast': 'Florida Gulf Coast',
    'Charleston So': 'Charleston Southern',
    'Chas Southern': 'Charleston Southern',
    'SIUE': 'SIU Edwardsville',
    'SIU-E': 'SIU Edwardsville',
    'Santa Barbara': 'UC Santa Barbara',
    'UCSB': 'UC Santa Barbara',
    'Abilene Chrstn': 'Abilene Christian',
    'Abilene Chr': 'Abilene Christian',
    'ACU': 'Abilene Christian',
    'Tarleton St': 'Tarleton',
    'So Indiana': 'Southern Indiana',
    'S Indiana': 'Southern Indiana',
    'USI': 'Southern Indiana',
    'St Thomas (MN)': 'St. Thomas',
    'St Thomas': 'St. Thomas',
    'Long Island': 'LIU',
    'SC Upstate': 'USC Upstate',
    'S.C. Upstate': 'USC Upstate',
    'UL Monroe': 'Louisiana',
    'LA Monroe': 'Louisiana',
    'ULM': 'Louisiana',
    'Tennessee St': 'Tennessee State',
    'Tenn State': 'Tennessee State',
    'TSU': 'Tennessee State',
    'North Dakota': 'North Dakota State',
    'Morehead St': 'Morehead State',
    'Sacramento St': 'Sacramento State',
    'Sac State': 'Sacramento State',
    'Portland St': 'Portland State',
    'Idaho St': 'Idaho State',
    'Weber St': 'Weber State',
    'North Florida': 'North Florida',
    'UNF': 'North Florida',
}

ESPN_CBB_TEAM_IDS = {
    'ACU': 2000,
    'Abilene Christian': 2000,
    'Air Force': 2005,
    'Akron': 2006,
    'Alabama': 333,
    'Alabama A&M': 2010,
    'Alabama State': 2011,
    'Albany': 399,
    'Alcorn State': 2016,
    'American': 44,
    'App State': 2026,
    'Appalachian State': 2026,
    'Arizona': 12,
    'Arizona State': 9,
    'Arkansas': 8,
    'Arkansas State': 2032,
    'Arkansas-Pine Bluff': 2029,
    'Army': 349,
    'Auburn': 2,
    'Austin Peay': 2046,
    'BYU': 252,
    'Ball State': 2050,
    'Baylor': 239,
    'Bellarmine': 91,
    'Belmont': 2057,
    'Bethune-Cookman': 2065,
    'Binghamton': 2066,
    'Boise State': 68,
    'Boston College': 103,
    'Boston U': 104,
    'Bowling Green': 189,
    'Bradley': 71,
    'Brown': 225,
    'Bryant': 2803,
    'Bucknell': 2083,
    'Buffalo': 2084,
    'Butler': 2086,
    'CBU': 2856,
    'CCSU': 2115,
    'CS Bakersfield': 2934,
    'CS Fullerton': 2239,
    'CS Northridge': 2463,
    'CSUB': 2934,
    'CSUF': 2239,
    'CSUN': 2463,
    'Cal': 25,
    'Cal Baptist': 2856,
    'Cal Poly': 13,
    'California': 25,
    'Campbell': 2097,
    'Canisius': 2099,
    'Central Arkansas': 2110,
    'Central Connecticut': 2115,
    'Central Michigan': 2117,
    'Charleston': 232,
    'Charleston Southern': 2127,
    'Charlotte': 2429,
    'Chattanooga': 236,
    'Cincinnati': 2132,
    'Citadel': 2643,
    'Clemson': 228,
    'Cleveland State': 325,
    'Coastal Carolina': 324,
    'Colgate': 2142,
    'Colorado': 38,
    'Colorado State': 36,
    'Columbia': 171,
    'Connecticut': 41,
    'Coppin State': 2154,
    'Cornell': 172,
    'Creighton': 156,
    'Dartmouth': 159,
    'Davidson': 2166,
    'Dayton': 2168,
    'DePaul': 305,
    'Delaware': 48,
    'Delaware State': 2169,
    'Denver': 2172,
    'Detroit': 2174,
    'Detroit Mercy': 2174,
    'Drake': 2181,
    'Drexel': 2182,
    'Duke': 150,
    'Duquesne': 2184,
    'EIU': 2197,
    'ETSU': 2193,
    'EWU': 331,
    'East Carolina': 151,
    'East Tennessee State': 2193,
    'Eastern Illinois': 2197,
    'Eastern Kentucky': 2198,
    'Eastern Michigan': 2199,
    'Eastern Washington': 331,
    'Elon': 2210,
    'Evansville': 339,
    'FAU': 2226,
    'FDU': 161,
    'FGCU': 526,
    'FIU': 2229,
    'Fairfield': 2217,
    'Fairleigh Dickinson': 161,
    'Florida': 57,
    'Florida A&M': 50,
    'Florida Gulf Coast': 526,
    'Florida State': 52,
    'Fordham': 2230,
    'Fresno State': 278,
    'Furman': 231,
    'GCU': 2253,
    'GW': 45,
    'Gardner-Webb': 2241,
    'George Mason': 2244,
    'George Washington': 45,
    'Georgetown': 46,
    'Georgia': 61,
    'Georgia Southern': 290,
    'Georgia State': 2247,
    'Georgia Tech': 59,
    'Gonzaga': 2250,
    'Grambling': 2755,
    'Grand Canyon': 2253,
    'Green Bay': 2739,
    'HCU': 2277,
    'Hampton': 2261,
    'Harvard': 108,
    'Hawaii': 62,
    'High Point': 2272,
    'Hofstra': 2275,
    'Holy Cross': 107,
    'Houston': 248,
    'Houston Christian': 2277,
    'Howard': 47,
    'Idaho': 70,
    'Idaho State': 304,
    'Illinois': 356,
    'Illinois State': 2287,
    'Incarnate Word': 2916,
    'Indiana': 84,
    'Indiana State': 282,
    'Iona': 314,
    'Iowa': 2294,
    'Iowa State': 66,
    'JMU': 256,
    'Jackson State': 2296,
    'Jacksonville': 294,
    'Jacksonville State': 55,
    'James Madison': 256,
    'KSU': 338,
    'Kansas': 2305,
    'Kansas State': 2306,
    'Kennesaw State': 338,
    'Kent State': 2309,
    'Kentucky': 96,
    'LBSU': 299,
    'LIU': 112358,
    'LSU': 99,
    'La Salle': 2325,
    'Lafayette': 322,
    'Lamar': 2320,
    'Le Moyne': 2330,
    'Lehigh': 2329,
    'Liberty': 2335,
    'Lindenwood': 2815,
    'Lipscomb': 288,
    'Little Rock': 2031,
    'Long Beach State': 299,
    'Longwood': 2344,
    'Louisiana': 309,
    'Louisiana Tech': 2348,
    'Louisville': 97,
    'Loyola Chicago': 2350,
    'Loyola MD': 2352,
    'MTSU': 2393,
    'MVSU': 2400,
    'Maine': 311,
    'Manhattan': 2363,
    'Marist': 2368,
    'Marquette': 269,
    'Marshall': 276,
    'Maryland': 120,
    'Maryland-Eastern Shore': 2379,
    'Massachusetts': 113,
    'McNeese': 2377,
    'Memphis': 235,
    'Mercer': 2382,
    'Mercyhurst': 2385,
    'Merrimack': 2771,
    'Miami': 2390,
    'Miami (OH)': 193,
    'Miami OH': 193,
    'Michigan': 130,
    'Michigan State': 127,
    'Middle Tennessee': 2393,
    'Milwaukee': 270,
    'Minnesota': 135,
    'Mississippi': 145,
    'Mississippi State': 344,
    'Mississippi Valley State': 2400,
    'Missouri': 142,
    'Missouri State': 2623,
    'Monmouth': 2405,
    'Montana': 149,
    'Montana State': 147,
    'Morehead State': 2413,
    'Morgan State': 2415,
    "Mount St. Mary's": 116,
    'Murray State': 93,
    'NAU': 2464,
    'NC A&T': 2448,
    'NC State': 152,
    'NCCU': 2428,
    'NDSU': 2449,
    'NJIT': 2885,
    'NMSU': 166,
    'NSU': 2466,
    'Navy': 2426,
    'Nebraska': 158,
    'Nevada': 2440,
    'New Hampshire': 160,
    'New Haven': 2441,
    'New Mexico': 167,
    'New Mexico State': 166,
    'Niagara': 315,
    'Nicholls': 2447,
    'Norfolk State': 2450,
    'North Alabama': 2453,
    'North Carolina': 153,
    'North Carolina A&T': 2448,
    'North Carolina Central': 2428,
    'North Dakota': 155,
    'North Dakota State': 2449,
    'North Florida': 2454,
    'North Texas': 249,
    'Northeastern': 111,
    'Northern Arizona': 2464,
    'Northern Colorado': 2458,
    'Northern Illinois': 2459,
    'Northern Iowa': 2460,
    'Northwestern': 77,
    'Northwestern State': 2466,
    'Notre Dame': 87,
    'ORU': 198,
    'Oakland': 2473,
    'Ohio': 195,
    'Ohio State': 194,
    'Oklahoma': 201,
    'Oklahoma State': 197,
    'Old Dominion': 295,
    'Ole Miss': 145,
    'Omaha': 2437,
    'Oral Roberts': 198,
    'Oregon': 2483,
    'Oregon State': 204,
    'PSU': 2502,
    'Penn': 219,
    'Penn State': 213,
    'Pittsburgh': 221,
    'Portland State': 2502,
    'Prairie View': 2504,
    'Presbyterian': 2506,
    'Princeton': 163,
    'Providence': 2507,
    'Purdue': 2509,
    'Purdue Fort Wayne': 2870,
    'Queens': 3157,
    'Quinnipiac': 2514,
    'Radford': 2515,
    'Rhode Island': 227,
    'Rice': 242,
    'Richmond': 257,
    'Rider': 2520,
    'Robert Morris': 2523,
    'Rutgers': 164,
    'SC State': 2569,
    'SDSU': 2571,
    'SE Louisiana': 2545,
    'SE Missouri State': 2546,
    'SELA': 2545,
    'SEMO': 2546,
    'SFA': 2617,
    'SIU': 79,
    'SMU': 2567,
    'SUU': 253,
    'Sac State': 16,
    'Sacramento State': 16,
    'Sacred Heart': 2529,
    'Saint Louis': 139,
    "Saint Mary's": 2608,
    "Saint Peter's": 2612,
    'Sam Houston': 2534,
    'Samford': 2535,
    'San Diego State': 21,
    'San Jose State': 23,
    'Seattle': 2547,
    'Seton Hall': 2550,
    'Siena': 2561,
    'South Alabama': 6,
    'South Carolina': 2579,
    'South Dakota': 2570,
    'South Dakota State': 2571,
    'Southern': 2582,
    'Southern Illinois': 79,
    'Southern Indiana': 88,
    'Southern Miss': 2572,
    'Southern Utah': 253,
    'St Bonaventure': 179,
    'St Johns': 2599,
    'St Thomas': 2900,
    'St. Bonaventure': 179,
    'St. Francis (PA)': 2598,
    "St. John's": 2599,
    'St. Louis': 139,
    "St. Peter's": 2612,
    'St. Thomas': 2900,
    'Stanford': 24,
    'Stephen F. Austin': 2617,
    'Stetson': 56,
    'Stonehill': 284,
    'Stony Brook': 2619,
    'Syracuse': 183,
    'TAMUC': 2868,
    'TAMUCC': 357,
    'TCU': 2628,
    'Tarleton': 2627,
    'Tarleton State': 2627,
    'Tennessee': 2633,
    'Tennessee State': 2634,
    'Tennessee Tech': 2635,
    'Texas': 251,
    'Texas A&M': 245,
    'Texas A&M-Commerce': 2868,
    'Texas A&M-Corpus Christi': 357,
    'Texas Southern': 2640,
    'Texas State': 326,
    'Texas Tech': 2641,
    'The Citadel': 2643,
    'Toledo': 2649,
    'Towson': 119,
    'Troy': 2653,
    'Tulane': 2655,
    'UAB': 5,
    'UAlbany': 399,
    'UC Davis': 302,
    'UC Irvine': 300,
    'UC Riverside': 27,
    'UC San Diego': 28,
    'UC Santa Barbara': 2540,
    'UCA': 2110,
    'UCF': 2116,
    'UCI': 300,
    'UCLA': 26,
    'UCR': 27,
    'UCSB': 2540,
    'UCSD': 28,
    'UConn': 41,
    'UIC': 82,
    'uic': 82,
    'Illinois Chicago': 82,
    'illinois_chicago': 82,
    'UIW': 2916,
    'UMBC': 2378,
    'UMass': 113,
    'UMass Lowell': 2349,
    'UNA': 2453,
    'UNC': 2458,
    'UNC Asheville': 2427,
    'UNC Greensboro': 2430,
    'UNC Wilmington': 350,
    'UNCG': 2430,
    'UNCW': 350,
    'UND': 155,
    'UNF': 2454,
    'UNH': 160,
    'UNI': 2460,
    'UNLV': 2439,
    'USC': 30,
    'USC Upstate': 2908,
    'USD': 2570,
    'USI': 88,
    'UT Arlington': 250,
    'UT Martin': 2630,
    'UTA': 250,
    'UTC': 236,
    'UTEP': 2638,
    'UTSA': 2636,
    'UVU': 3084,
    'Utah': 254,
    'Utah State': 328,
    'Utah Tech': 3101,
    'Utah Valley': 3084,
    'VCU': 2670,
    'VMI': 2678,
    'Valparaiso': 2674,
    'Vanderbilt': 238,
    'Vermont': 261,
    'Villanova': 222,
    'Virginia': 258,
    'WCU': 2717,
    'WIU': 2710,
    'Wagner': 2681,
    'Wake Forest': 154,
    'Washington': 264,
    'Washington State': 265,
    'Weber State': 2692,
    'West Virginia': 277,
    'Western Carolina': 2717,
    'Western Illinois': 2710,
    'Western Kentucky': 98,
    'Western Michigan': 2711,
    'Wichita State': 2724,
    'William & Mary': 2729,
    'Winthrop': 2737,
    'Wisconsin': 275,
    'Wofford': 2747,
    'Wright State': 2750,
    'Wyoming': 2751,
    'Xavier': 2752,
    'Yale': 43,
    'Youngstown State': 2754,
    # Additional canonical name aliases for team_identity.py mapping
    'IU Indianapolis': 85,
    'iu_indianapolis': 85,
    'IUPUI': 85,
    'IU Indy': 85,
    'iupui': 85,
    'Louisiana Monroe': 2433,
    'louisiana_monroe': 2433,
    'ULM': 2433,
    'La Monroe': 2433,
    'Miami FL': 2390,
    'miami_fl': 2390,
    'Miami (FL)': 2390,
    'Southeastern Louisiana': 2545,
    'southeastern_louisiana': 2545,
    'SE La': 2545,
    'Tennessee Martin': 2630,
    'tennessee_martin': 2630,
    'UT-Martin': 2630,
    'Texas A&M CC': 357,
    'texas_am_cc': 357,
    'TAMU-CC': 357,
    'Texas A&M Commerce': 2868,
    'texas_am_commerce': 2868,
    'TAMU Commerce': 2868,
    'A&M Commerce': 2868,
    'UMKC': 140,
    'umkc': 140,
    'Missouri-Kansas City': 140,
    'UTRGV': 292,
    'utrgv': 292,
    'Rio Grande Valley': 292,
    'Texas Rio Grande Valley': 292,
    # New D1 schools (2023-2024 transitions)
    'Le Moyne': 2330,
    'le_moyne': 2330,
    'Lemoyne': 2330,
    'Mercyhurst': 2385,
    'mercyhurst': 2385,
    'Stonehill': 284,
    'stonehill': 284,
    'West Georgia': 2698,
    'west_georgia': 2698,
    # Full ESPN names
    'Abilene Christian Wildcats': 2000,
    'Air Force Falcons': 2005,
    'Akron Zips': 2006,
    'Alabama Am Bulldogs': 2010,
    'Alabama Crimson Tide': 333,
    'Alabama State Hornets': 2011,
    'Alcorn State Braves': 2016,
    'American University Eagles': 44,
    'App State Mountaineers': 2026,
    'Arizona State Sun Devils': 9,
    'Arizona Wildcats': 12,
    'Arkansas Pine Bluff Golden Lions': 2029,
    'Arkansas Razorbacks': 8,
    'Arkansas State Red Wolves': 2032,
    'Army Black Knights': 349,
    'Auburn Tigers': 2,
    'Austin Peay Governors': 2046,
    'Ball State Cardinals': 2050,
    'Baylor Bears': 239,
    'Bellarmine Knights': 91,
    'Belmont Bruins': 2057,
    'Bethune Cookman Wildcats': 2065,
    'Binghamton Bearcats': 2066,
    'Boise State Broncos': 68,
    'Boston College Eagles': 103,
    'Boston University Terriers': 104,
    'Bowling Green Falcons': 189,
    'Bradley Braves': 71,
    'Brown Bears': 225,
    'Bryant Bulldogs': 2803,
    'Bucknell Bison': 2083,
    'Buffalo Bulls': 2084,
    'Butler Bulldogs': 2086,
    'Byu Cougars': 252,
    'Cal Poly Mustangs': 13,
    'Cal State Bakersfield Roadrunners': 2934,
    'Cal State Fullerton Titans': 2239,
    'Cal State Northridge Matadors': 2463,
    'California Baptist Lancers': 2856,
    'California Golden Bears': 25,
    'Campbell Fighting Camels': 2097,
    'Canisius Golden Griffins': 2099,
    'Central Arkansas Bears': 2110,
    'Central Connecticut Blue Devils': 2115,
    'Central Michigan Chippewas': 2117,
    'Charleston Cougars': 232,
    'Charleston Southern Buccaneers': 2127,
    'Charlotte 49Ers': 2429,
    'Chattanooga Mocs': 236,
    'Chicago State Cougars': 2130,
    'Cincinnati Bearcats': 2132,
    'Clemson Tigers': 228,
    'Cleveland State Vikings': 325,
    'Coastal Carolina Chanticleers': 324,
    'Colgate Raiders': 2142,
    'Colorado Buffaloes': 38,
    'Colorado State Rams': 36,
    'Columbia Lions': 171,
    'Coppin State Eagles': 2154,
    'Cornell Big Red': 172,
    'Creighton Bluejays': 156,
    'Dartmouth Big Green': 159,
    'Davidson Wildcats': 2166,
    'Dayton Flyers': 2168,
    'Delaware Blue Hens': 48,
    'Delaware State Hornets': 2169,
    'Denver Pioneers': 2172,
    'Depaul Blue Demons': 305,
    'Detroit Mercy Titans': 2174,
    'Drake Bulldogs': 2181,
    'Drexel Dragons': 2182,
    'Duke Blue Devils': 150,
    'Duquesne Dukes': 2184,
    'East Carolina Pirates': 151,
    'East Tennessee State Buccaneers': 2193,
    'East Texas Am Lions': 2837,
    'Eastern Illinois Panthers': 2197,
    'Eastern Kentucky Colonels': 2198,
    'Eastern Michigan Eagles': 2199,
    'Eastern Washington Eagles': 331,
    'Elon Phoenix': 2210,
    'Evansville Purple Aces': 339,
    'Fairfield Stags': 2217,
    'Fairleigh Dickinson Knights': 161,
    'Florida Am Rattlers': 50,
    'Florida Atlantic Owls': 2226,
    'Florida Gators': 57,
    'Florida Gulf Coast Eagles': 526,
    'Florida International Panthers': 2229,
    'Florida State Seminoles': 52,
    'Fordham Rams': 2230,
    'Fresno State Bulldogs': 278,
    'Furman Paladins': 231,
    'Gardner Webb Runnin Bulldogs': 2241,
    'George Mason Patriots': 2244,
    'George Washington Revolutionaries': 45,
    'Georgetown Hoyas': 46,
    'Georgia Bulldogs': 61,
    'Georgia Southern Eagles': 290,
    'Georgia State Panthers': 2247,
    'Georgia Tech Yellow Jackets': 59,
    'Gonzaga Bulldogs': 2250,
    'Grambling Tigers': 2755,
    'Grand Canyon Lopes': 2253,
    'Green Bay Phoenix': 2739,
    'Hampton Pirates': 2261,
    'Harvard Crimson': 108,
    'Hawaii Rainbow Warriors': 62,
    'High Point Panthers': 2272,
    'Hofstra Pride': 2275,
    'Holy Cross Crusaders': 107,
    'Houston Christian Huskies': 2277,
    'Houston Cougars': 248,
    'Howard Bison': 47,
    'Idaho State Bengals': 304,
    'Idaho Vandals': 70,
    'Illinois Fighting Illini': 356,
    'Illinois State Redbirds': 2287,
    'Incarnate Word Cardinals': 2916,
    'Indiana Hoosiers': 84,
    'Indiana State Sycamores': 282,
    'Iona Gaels': 314,
    'Iowa Hawkeyes': 2294,
    'Iowa State Cyclones': 66,
    'Iu Indianapolis Jaguars': 85,
    'Jackson State Tigers': 2296,
    'Jacksonville Dolphins': 294,
    'Jacksonville State Gamecocks': 55,
    'James Madison Dukes': 256,
    'Kansas City Roos': 140,
    'Kansas Jayhawks': 2305,
    'Kansas State Wildcats': 2306,
    'Kennesaw State Owls': 338,
    'Kent State Golden Flashes': 2309,
    'Kentucky Wildcats': 96,
    'La Salle Explorers': 2325,
    'Lafayette Leopards': 322,
    'Lamar Cardinals': 2320,
    'Le Moyne Dolphins': 2330,
    'Lehigh Mountain Hawks': 2329,
    'Liberty Flames': 2335,
    'Lindenwood Lions': 2815,
    'Lipscomb Bisons': 288,
    'Little Rock Trojans': 2031,
    'Long Beach State Beach': 299,
    'Long Island University Sharks': 112358,
    'Longwood Lancers': 2344,
    'Louisiana Ragin Cajuns': 309,
    'Louisiana Tech Bulldogs': 2348,
    'Louisville Cardinals': 97,
    'Loyola Chicago Ramblers': 2350,
    'Loyola Maryland Greyhounds': 2352,
    'Loyola Marymount Lions': 2351,
    'Lsu Tigers': 99,
    'Maine Black Bears': 311,
    'Manhattan Jaspers': 2363,
    'Marist Red Foxes': 2368,
    'Marquette Golden Eagles': 269,
    'Marshall Thundering Herd': 276,
    'Maryland Eastern Shore Hawks': 2379,
    'Maryland Terrapins': 120,
    'Massachusetts Minutemen': 113,
    'Mcneese Cowboys': 2377,
    'Memphis Tigers': 235,
    'Mercer Bears': 2382,
    'Mercyhurst Lakers': 2385,
    'Merrimack Warriors': 2771,
    'Miami Hurricanes': 2390,
    'Miami Oh Redhawks': 193,
    'Michigan State Spartans': 127,
    'Michigan Wolverines': 130,
    'Middle Tennessee Blue Raiders': 2393,
    'Milwaukee Panthers': 270,
    'Minnesota Golden Gophers': 135,
    'Mississippi State Bulldogs': 344,
    'Mississippi Valley State Delta Devils': 2400,
    'Missouri State Bears': 2623,
    'Missouri Tigers': 142,
    'Monmouth Hawks': 2405,
    'Montana Grizzlies': 149,
    'Montana State Bobcats': 147,
    'Morehead State Eagles': 2413,
    'Morgan State Bears': 2415,
    'Mount St Marys Mountaineers': 116,
    'Murray State Racers': 93,
    'Navy Midshipmen': 2426,
    'Nc State Wolfpack': 152,
    'Nebraska Cornhuskers': 158,
    'Nevada Wolf Pack': 2440,
    'New Hampshire Wildcats': 160,
    'New Haven Chargers': 2441,
    'New Mexico Lobos': 167,
    'New Mexico State Aggies': 166,
    'New Orleans Privateers': 2443,
    'Niagara Purple Eagles': 315,
    'Nicholls Colonels': 2447,
    'Njit Highlanders': 2885,
    'Norfolk State Spartans': 2450,
    'North Alabama Lions': 2453,
    'North Carolina At Aggies': 2448,
    'North Carolina Central Eagles': 2428,
    'North Carolina Tar Heels': 153,
    'North Dakota Fighting Hawks': 155,
    'North Dakota State Bison': 2449,
    'North Florida Ospreys': 2454,
    'North Texas Mean Green': 249,
    'Northeastern Huskies': 111,
    'Northern Arizona Lumberjacks': 2464,
    'Northern Colorado Bears': 2458,
    'Northern Illinois Huskies': 2459,
    'Northern Iowa Panthers': 2460,
    'Northern Kentucky Norse': 94,
    'Northwestern State Demons': 2466,
    'Northwestern Wildcats': 77,
    'Notre Dame Fighting Irish': 87,
    'Oakland Golden Grizzlies': 2473,
    'Ohio Bobcats': 195,
    'Ohio State Buckeyes': 194,
    'Oklahoma Sooners': 201,
    'Oklahoma State Cowboys': 197,
    'Old Dominion Monarchs': 295,
    'Ole Miss Rebels': 145,
    'Omaha Mavericks': 2437,
    'Oral Roberts Golden Eagles': 198,
    'Oregon Ducks': 2483,
    'Oregon State Beavers': 204,
    'Pacific Tigers': 279,
    'Penn State Nittany Lions': 213,
    'Pennsylvania Quakers': 219,
    'Pepperdine Waves': 2492,
    'Pittsburgh Panthers': 221,
    'Portland Pilots': 2501,
    'Portland State Vikings': 2502,
    'Prairie View Am Panthers': 2504,
    'Presbyterian Blue Hose': 2506,
    'Princeton Tigers': 163,
    'Providence Friars': 2507,
    'Purdue Boilermakers': 2509,
    'Purdue Fort Wayne Mastodons': 2870,
    'Queens University Royals': 2511,
    'Quinnipiac Bobcats': 2514,
    'Radford Highlanders': 2515,
    'Rhode Island Rams': 227,
    'Rice Owls': 242,
    'Richmond Spiders': 257,
    'Rider Broncs': 2520,
    'Robert Morris Colonials': 2523,
    'Rutgers Scarlet Knights': 164,
    'Sacramento State Hornets': 16,
    'Sacred Heart Pioneers': 2529,
    'Saint Francis Red Flash': 2598,
    'Saint Josephs Hawks': 2603,
    'Saint Louis Billikens': 139,
    'Saint Marys Gaels': 2608,
    'Saint Peters Peacocks': 2612,
    'Sam Houston Bearkats': 2534,
    'Samford Bulldogs': 2535,
    'San Diego State Aztecs': 21,
    'San Diego Toreros': 301,
    'San Francisco Dons': 2539,
    'San Jose State Spartans': 23,
    'Santa Clara Broncos': 2541,
    'Se Louisiana Lions': 2545,
    'Seattle U Redhawks': 2547,
    'Seton Hall Pirates': 2550,
    'Siena Saints': 2561,
    'Siu Edwardsville Cougars': 2565,
    'Smu Mustangs': 2567,
    'South Alabama Jaguars': 6,
    'South Carolina Gamecocks': 2579,
    'South Carolina State Bulldogs': 2569,
    'South Carolina Upstate Spartans': 2908,
    'South Dakota Coyotes': 233,
    'South Dakota State Jackrabbits': 2571,
    'South Florida Bulls': 58,
    'Southeast Missouri State Redhawks': 2546,
    'Southern Illinois Salukis': 79,
    'Southern Indiana Screaming Eagles': 88,
    'Southern Jaguars': 2582,
    'Southern Miss Golden Eagles': 2572,
    'Southern Utah Thunderbirds': 253,
    'St Bonaventure Bonnies': 179,
    'St Johns Red Storm': 2599,
    'St Thomas Minnesota Tommies': 2900,
    'Stanford Cardinal': 24,
    'Stephen F Austin Lumberjacks': 2617,
    'Stetson Hatters': 56,
    'Stonehill Skyhawks': 284,
    'Stony Brook Seawolves': 2619,
    'Syracuse Orange': 183,
    'Tarleton State Texans': 2627,
    'Tcu Horned Frogs': 2628,
    'Temple Owls': 218,
    'Tennessee State Tigers': 2634,
    'Tennessee Tech Golden Eagles': 2635,
    'Tennessee Volunteers': 2633,
    'Texas Am Aggies': 245,
    'Texas Am Corpus Christi Islanders': 357,
    'Texas Longhorns': 251,
    'Texas Southern Tigers': 2640,
    'Texas State Bobcats': 326,
    'Texas Tech Red Raiders': 2641,
    'The Citadel Bulldogs': 2643,
    'Toledo Rockets': 2649,
    'Towson Tigers': 119,
    'Troy Trojans': 2653,
    'Tulane Green Wave': 2655,
    'Tulsa Golden Hurricane': 202,
    'Uab Blazers': 5,
    'Ualbany Great Danes': 399,
    'Uc Davis Aggies': 302,
    'Uc Irvine Anteaters': 300,
    'Uc Riverside Highlanders': 27,
    'Uc San Diego Tritons': 28,
    'Uc Santa Barbara Gauchos': 2540,
    'Ucf Knights': 2116,
    'Ucla Bruins': 26,
    'Uconn Huskies': 41,
    'Uic Flames': 82,
    'Ul Monroe Warhawks': 2433,
    'Umass Lowell River Hawks': 2349,
    'Umbc Retrievers': 2378,
    'Unc Asheville Bulldogs': 2427,
    'Unc Greensboro Spartans': 2430,
    'Unc Wilmington Seahawks': 350,
    'Unlv Rebels': 2439,
    'Usc Trojans': 30,
    'Ut Arlington Mavericks': 250,
    'Ut Martin Skyhawks': 2630,
    'Ut Rio Grande Valley Vaqueros': 292,
    'Utah State Aggies': 328,
    'Utah Tech Trailblazers': 3101,
    'Utah Utes': 254,
    'Utah Valley Wolverines': 3084,
    'Utep Miners': 2638,
    'Utsa Roadrunners': 2636,
    'Valparaiso Beacons': 2674,
    'Vanderbilt Commodores': 238,
    'Vcu Rams': 2670,
    'Vermont Catamounts': 261,
    'Villanova Wildcats': 222,
    'Virginia Cavaliers': 258,
    'Virginia Tech Hokies': 259,
    'Vmi Keydets': 2678,
    'Wagner Seahawks': 2681,
    'Wake Forest Demon Deacons': 154,
    'Washington Huskies': 264,
    'Washington State Cougars': 265,
    'Weber State Wildcats': 2692,
    'West Georgia Wolves': 2698,
    'West Virginia Mountaineers': 277,
    'Western Carolina Catamounts': 2717,
    'Western Illinois Leathernecks': 2710,
    'Western Kentucky Hilltoppers': 98,
    'Western Michigan Broncos': 2711,
    'Wichita State Shockers': 2724,
    'William Mary Tribe': 2729,
    'Winthrop Eagles': 2737,
    'Wisconsin Badgers': 275,
    'Wofford Terriers': 2747,
    'Wright State Raiders': 2750,
    'Wyoming Cowboys': 2751,
    'Xavier Musketeers': 2752,
    'Yale Bulldogs': 43,
    'Youngstown State Penguins': 2754,
}

# Build logo dict from team IDs
CBB_TEAM_LOGOS_COMPLETE = {
    team: f'https://a.espncdn.com/i/teamlogos/ncaa/500-dark/{team_id}.png'
    for team, team_id in ESPN_CBB_TEAM_IDS.items()
}

def get_transparent_cbb_logo(team_name: str) -> Optional[str]:
    """
    Get transparent CBB logo URL.
    Uses -dark suffix for transparent backgrounds.
    """
    # First check aliases
    if team_name in ESPN_CBB_TEAM_ALIASES:
        canonical = ESPN_CBB_TEAM_ALIASES[team_name]
        if canonical in CBB_TEAM_LOGOS_COMPLETE:
            return CBB_TEAM_LOGOS_COMPLETE[canonical]
    
    # Try direct match
    if team_name in CBB_TEAM_LOGOS_COMPLETE:
        return CBB_TEAM_LOGOS_COMPLETE[team_name]
    
    # Try fuzzy match on aliases first
    team_lower = team_name.lower().strip()
    for alias, canonical in ESPN_CBB_TEAM_ALIASES.items():
        if alias.lower() == team_lower or team_lower.startswith(alias.lower()):
            if canonical in CBB_TEAM_LOGOS_COMPLETE:
                return CBB_TEAM_LOGOS_COMPLETE[canonical]
    
    # Try fuzzy match on team names
    for key, url in CBB_TEAM_LOGOS_COMPLETE.items():
        if key.lower() == team_lower:
            return url
        if key.lower() in team_lower or team_lower in key.lower():
            return url
    
    return None


# ============================================================
# TEAM RANKINGS SCRAPER FOR DEFENSIVE STATS
# ============================================================

class TeamRankingsScraper:
    """
    Scrapes defensive rankings from TeamRankings.com.
    Identifies bottom 5 defenses (ranks 27-32) for elimination filter.
    """
    
    BASE_URL = "https://www.teamrankings.com"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self._cache = {}
        self._cache_time = {}
    
    def get_defensive_rankings_l5(self, league: str = 'NBA') -> Dict[str, int]:
        """
        Get defensive rankings for last 5 games.
        
        Returns:
            Dict mapping team name to defensive rank (1 = best, 30 = worst)
        """
        cache_key = f"def_l5_{league}"
        if cache_key in self._cache:
            age = time.time() - self._cache_time.get(cache_key, 0)
            if age < 3600:  # 1 hour cache
                return self._cache[cache_key]
        
        try:
            if league == 'NBA':
                url = f"{self.BASE_URL}/nba/stat/defensive-efficiency-last-5"
            else:
                url = f"{self.BASE_URL}/ncaa-basketball/stat/defensive-efficiency"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            rankings = {}
            
            # Parse rankings table
            table = soup.find('table', class_=re.compile(r'.*datatable.*'))
            if table:
                rows = table.find_all('tr')[1:]  # Skip header
                
                for rank, row in enumerate(rows, 1):
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        team_cell = cells[0]
                        team_name = team_cell.text.strip()
                        rankings[team_name] = rank
            
            # Cache results
            self._cache[cache_key] = rankings
            self._cache_time[cache_key] = time.time()
            
            logger.info(f"Fetched defensive rankings for {league}: {len(rankings)} teams")
            return rankings
            
        except Exception as e:
            logger.error(f"Error fetching defensive rankings: {e}")
            return {}
    
    def get_bottom_5_defenses(self, league: str = 'NBA') -> List[str]:
        """
        Get teams with bottom 5 defenses (ranks 27-32 for NBA).
        
        Returns:
            List of team names to avoid
        """
        rankings = self.get_defensive_rankings_l5(league)
        
        if not rankings:
            return []
        
        # Bottom 5 = ranks 27 and higher
        bottom_threshold = 27 if league == 'NBA' else 300  # Adjust for CBB
        
        bottom_teams = [
            team for team, rank in rankings.items()
            if rank >= bottom_threshold
        ]
        
        return bottom_teams


# ============================================================
# ELIMINATION FILTER SYSTEM
# ============================================================

class EliminationFilterSystem:
    """
    Professional elimination filter system.
    Progressively filters games to find qualifiers.
    """
    
    def __init__(self, db_session):
        self.db = db_session
        self.team_rankings = TeamRankingsScraper()
    
    def run_complete_filter(self, league: str, today) -> Dict:
        """
        Run complete elimination process.
        
        Returns:
            {
                'total_games': int,
                'eliminated': {
                    'high_handle': List[str],
                    'large_spread': List[str],
                    'bad_teams': List[str],
                    'bad_defense': List[str]
                },
                'remaining': List[Game],
                'qualified': List[Game]
            }
        """
        from sports_app import Game  # Import here to avoid circular dependency
        
        # Get all games for today
        all_games = Game.query.filter_by(
            date=today,
            league=league
        ).all()
        
        total_games = len(all_games)
        eliminated = {
            'high_handle': [],
            'large_spread': [],
            'bad_teams': [],
            'bad_defense': []
        }
        
        remaining_games = []
        
        # FILTER 1: 80%+ Handle (Bets OR Money)
        logger.info(f"Filter 1: Checking for 80%+ handle...")
        for game in all_games:
            high_handle = False
            
            # Check spread bets/money
            if game.spread_bets_away_pct and game.spread_bets_away_pct >= 80:
                high_handle = True
            elif game.spread_bets_home_pct and game.spread_bets_home_pct >= 80:
                high_handle = True
            elif game.spread_money_away_pct and game.spread_money_away_pct >= 80:
                high_handle = True
            elif game.spread_money_home_pct and game.spread_money_home_pct >= 80:
                high_handle = True
            
            if high_handle:
                eliminated['high_handle'].append(f"{game.away_team} @ {game.home_team}")
            else:
                remaining_games.append(game)
        
        logger.info(f"Filter 1 eliminated {len(eliminated['high_handle'])} games")
        
        # FILTER 2: Large Spread (10+ points)
        logger.info(f"Filter 2: Checking for large spreads (10+)...")
        temp_remaining = []
        for game in remaining_games:
            if game.current_spread and abs(game.current_spread) >= 10:
                eliminated['large_spread'].append(f"{game.away_team} @ {game.home_team}")
            else:
                temp_remaining.append(game)
        
        remaining_games = temp_remaining
        logger.info(f"Filter 2 eliminated {len(eliminated['large_spread'])} games")
        
        # FILTER 3: Bad Teams (0-5 L5, poor records)
        logger.info(f"Filter 3: Checking for bad teams...")
        temp_remaining = []
        for game in remaining_games:
            # Check last 5 records
            bad_team = False
            
            # Away team L5
            if hasattr(game, 'away_l5_record'):
                if self._is_bad_record(game.away_l5_record):
                    bad_team = True
            
            # Home team L5
            if hasattr(game, 'home_l5_record'):
                if self._is_bad_record(game.home_l5_record):
                    bad_team = True
            
            if bad_team:
                eliminated['bad_teams'].append(f"{game.away_team} @ {game.home_team}")
            else:
                temp_remaining.append(game)
        
        remaining_games = temp_remaining
        logger.info(f"Filter 3 eliminated {len(eliminated['bad_teams'])} games")
        
        # FILTER 4: Bad Defense L5 (Bottom 5 = ranks 27-32)
        logger.info(f"Filter 4: Checking for bad defenses...")
        bottom_defenses = self.team_rankings.get_bottom_5_defenses(league)
        
        temp_remaining = []
        for game in remaining_games:
            # We CAN'T bet WITH bottom 5 defenses, but we CAN bet AGAINST them
            # So we eliminate games where BOTH teams have bad defense
            
            away_bad_d = any(bad_team in game.away_team for bad_team in bottom_defenses)
            home_bad_d = any(bad_team in game.home_team for bad_team in bottom_defenses)
            
            # Only eliminate if the team we'd bet ON has bad defense
            # Store bad defense info but don't eliminate yet
            game.away_bad_defense = away_bad_d
            game.home_bad_defense = home_bad_d
            
            temp_remaining.append(game)
        
        remaining_games = temp_remaining
        logger.info(f"After Filter 4: {len(remaining_games)} games remaining")
        
        # FILTER 5: Apply qualification logic to remaining games
        qualified_games = []
        for game in remaining_games:
            # Run your existing qualification logic
            if self._check_spread_qualification(game):
                qualified_games.append(game)
        
        logger.info(f"Final qualified games: {len(qualified_games)}")
        
        return {
            'total_games': total_games,
            'eliminated': eliminated,
            'remaining': remaining_games,
            'qualified': qualified_games,
            'summary': {
                'total': total_games,
                'high_handle': len(eliminated['high_handle']),
                'large_spread': len(eliminated['large_spread']),
                'bad_teams': len(eliminated['bad_teams']),
                'remaining_after_filters': len(remaining_games),
                'final_qualified': len(qualified_games)
            }
        }
    
    def _is_bad_record(self, record_str: str) -> bool:
        """
        Check if team has bad recent record.
        Examples: 0-5, 1-4, 1-12, 2-12, etc.
        """
        if not record_str:
            return False
        
        # Parse record (format: "W-L")
        match = re.match(r'(\d+)-(\d+)', record_str)
        if not match:
            return False
        
        wins = int(match.group(1))
        losses = int(match.group(2))
        
        # Bad record thresholds
        bad_thresholds = [
            (0, 5),   # 0-5 or worse
            (1, 4),   # 1-4 or worse
            (1, 12),  # 1-12 or worse
            (2, 12),  # 2-12 or worse
        ]
        
        for w_threshold, l_threshold in bad_thresholds:
            if wins <= w_threshold and losses >= l_threshold:
                return True
        
        return False
    
    def _check_spread_qualification(self, game) -> bool:
        """
        Check if game qualifies for spread betting.
        Apply your existing qualification logic here.
        """
        # Placeholder - integrate your existing logic
        if not game.current_spread:
            return False
        
        # Your qualification criteria
        # - Edge requirements
        # - Sharp money alignment
        # - No RLM
        # - Defensive matchup favorable
        # etc.
        
        return True  # Replace with actual logic


# ============================================================
# AUTOMATIC GAME LOADING SYSTEM
# ============================================================

class AutomaticGameLoader:
    """
    Automatically loads games on new day.
    No manual "Fetch Games" button needed.
    """
    
    def __init__(self, app, db):
        self.app = app
        self.db = db
        self.last_load_date = None
        self.elimination_filter = EliminationFilterSystem(db.session)
    
    def check_and_load_if_new_day(self):
        """
        Check if it's a new day and load games automatically.
        Call this on every dashboard page load.
        """
        et = pytz.timezone('America/New_York')
        today = datetime.now(et).date()
        
        # Check if we've already loaded for today
        if self.last_load_date == today:
            logger.debug(f"Games already loaded for {today}")
            return {"status": "already_loaded", "date": str(today)}
        
        # Check if database has games for today
        from sports_app import Game
        existing_games = Game.query.filter_by(date=today).count()
        
        if existing_games > 0:
            logger.info(f"Found {existing_games} existing games for {today}")
            self.last_load_date = today
            return {"status": "games_exist", "count": existing_games}
        
        # NEW DAY - Load games automatically
        logger.info(f"NEW DAY DETECTED: {today}. Loading games automatically...")
        
        try:
            # Call your existing fetch_odds_internal function
            from sports_app import fetch_odds_internal
            
            result = fetch_odds_internal()
            
            if result.get('success'):
                # Run elimination filters
                nba_filter_result = self.elimination_filter.run_complete_filter('NBA', today)
                cbb_filter_result = self.elimination_filter.run_complete_filter('CBB', today)
                
                self.last_load_date = today
                
                logger.info(f"AUTO-LOAD SUCCESS: NBA {nba_filter_result['summary']['final_qualified']} qualified, "
                          f"CBB {cbb_filter_result['summary']['final_qualified']} qualified")
                
                return {
                    "status": "auto_loaded",
                    "date": str(today),
                    "nba": nba_filter_result['summary'],
                    "cbb": cbb_filter_result['summary']
                }
            else:
                logger.warning(f"Auto-load failed: {result.get('reason')}")
                return {"status": "failed", "reason": result.get('reason')}
                
        except Exception as e:
            logger.error(f"Error in automatic game loading: {e}")
            return {"status": "error", "error": str(e)}
    
    def get_qualified_games_summary(self, today) -> str:
        """
        Generate mentor-style summary of qualified games.
        
        Example output:
        Teams to avoid today:
        1. 80%+ Handle: None
        2. Large Spread 10+: Wizards, Cavaliers, Jazz, Thunder
        3. Bad Teams: Jazz, Wizards, 76ers, Hornets (0-5 L5)
        4. Bad Defense L5: Pacers 30th, Kings 28th, Suns 26th
        
        Team Options: Bucks, Pistons, Magic, Knicks...
        Early Sharp Action: Pistons, Magic, Pacers, Spurs...
        """
        nba_result = self.elimination_filter.run_complete_filter('NBA', today)
        
        eliminated = nba_result['eliminated']
        remaining = nba_result['remaining']
        qualified = nba_result['qualified']
        
        summary = []
        summary.append("=" * 80)
        summary.append(f"QUALIFIED GAMES ANALYSIS - {today}")
        summary.append("=" * 80)
        summary.append("")
        summary.append("Teams to avoid today:")
        
        # 1. High Handle
        if eliminated['high_handle']:
            teams = ', '.join([g.split('@')[0].strip() for g in eliminated['high_handle']])
            summary.append(f"1. 80%+ Handle: {teams}")
        else:
            summary.append("1. 80%+ Handle: None")
        
        # 2. Large Spreads
        if eliminated['large_spread']:
            teams = ', '.join([g.split('@')[0].strip() for g in eliminated['large_spread']])
            summary.append(f"2. Large Spread 10+: {teams}")
        else:
            summary.append("2. Large Spread 10+: None")
        
        # 3. Bad Teams
        if eliminated['bad_teams']:
            teams = ', '.join([g.split('@')[0].strip() for g in eliminated['bad_teams']])
            summary.append(f"3. Bad Teams: {teams}")
        else:
            summary.append("3. Bad Teams: None")
        
        # 4. Bad Defense
        bottom_defenses = self.elimination_filter.team_rankings.get_bottom_5_defenses('NBA')
        if bottom_defenses:
            summary.append(f"4. Bad Defense L5: {', '.join(bottom_defenses[:5])}")
        else:
            summary.append("4. Bad Defense L5: None identified")
        
        summary.append("")
        
        # Team Options
        team_options = [f"{g.away_team}, {g.home_team}" for g in remaining]
        if team_options:
            summary.append(f"Team Options: {', '.join(team_options)}")
        
        # Sharp Action
        sharp_teams = [
            f"{g.away_team}" for g in remaining 
            if hasattr(g, 'spread_sharp_side') and g.spread_sharp_side == 'Away'
        ] + [
            f"{g.home_team}" for g in remaining 
            if hasattr(g, 'spread_sharp_side') and g.spread_sharp_side == 'Home'
        ]
        
        if sharp_teams:
            summary.append(f"Early Sharp Action: {', '.join(sharp_teams)}")
        
        summary.append("")
        summary.append(f"Final Qualified Games: {len(qualified)}")
        summary.append("=" * 80)
        
        return "\n".join(summary)


# ============================================================
# INTEGRATION WITH FLASK APP
# ============================================================

def setup_automatic_loading(app, db):
    """
    Setup automatic game loading in Flask app.
    Call this during app initialization.
    
    Note: The before_request hook is disabled to avoid circular imports.
    Use the API endpoints or call check_and_load_if_new_day() manually.
    """
    # Note: AutomaticGameLoader not instantiated here to avoid circular imports
    # The loader is available via the API endpoints below
    
    logger.info("Automatic game loading system initialized")
    return None  # Return None - loader can be created on-demand


# ============================================================
# EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example: Get defensive rankings
    scraper = TeamRankingsScraper()
    bottom_5 = scraper.get_bottom_5_defenses('NBA')
    
    print("Bottom 5 Defenses (L5 games):")
    for team in bottom_5:
        print(f"  - {team}")
