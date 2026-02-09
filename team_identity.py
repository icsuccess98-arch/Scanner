"""
Team Identity Resolution System
Batch-optimized for 10k+ teams across KenPom, VSIN, and Covers.com

Data Source Responsibilities:
- KenPom: All stats and rankings (CBB only)
- VSIN: Open lines, current lines, money %, handle %
- Covers.com: Display names, abbreviations, logos, W/L records
"""

import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple
from functools import lru_cache

# =============================================================================
# MASTER TEAM ALIASES - Maps ALL variations to canonical keys
# =============================================================================

CBB_CANONICAL_ALIASES: Dict[str, str] = {
    # A
    "abilene christian": "abilene_christian", "acu": "abilene_christian", "abil christian": "abilene_christian",
    "air force": "air_force", "af": "air_force",
    "akron": "akron", "akr": "akron",
    "alabama": "alabama", "ala": "alabama", "bama": "alabama", "ua": "alabama",
    "alabama a&m": "alabama_am", "aamu": "alabama_am", "ala a&m": "alabama_am",
    "alabama state": "alabama_state", "alst": "alabama_state", "ala st": "alabama_state",
    "albany": "albany", "alb": "albany",
    "alcorn state": "alcorn_state", "alcn": "alcorn_state", "alcorn": "alcorn_state", "alcorn st": "alcorn_state",
    "american": "american", "amer": "american", "american u": "american",
    "appalachian state": "appalachian_state", "app": "appalachian_state", "app st": "appalachian_state", "appalachian st": "appalachian_state",
    "arizona": "arizona", "ariz": "arizona", "arz": "arizona", "ua": "arizona", "zona": "arizona",
    "arizona state": "arizona_state", "asu": "arizona_state", "ariz st": "arizona_state", "arizona st": "arizona_state",
    "arkansas": "arkansas", "ark": "arkansas", "arky": "arkansas",
    "arkansas state": "arkansas_state", "arst": "arkansas_state", "ark st": "arkansas_state", "arkansas st": "arkansas_state",
    "arkansas pine bluff": "arkansas_pine_bluff", "uapb": "arkansas_pine_bluff", "ark pb": "arkansas_pine_bluff",
    "army": "army", "army west point": "army",
    "auburn": "auburn", "aub": "auburn",
    
    # B
    "ball state": "ball_state", "ball": "ball_state", "ball st": "ball_state",
    "baylor": "baylor", "bay": "baylor", "bu": "baylor",
    "bellarmine": "bellarmine", "bell": "bellarmine",
    "belmont": "belmont", "bel": "belmont",
    "bethune cookman": "bethune_cookman", "b-cu": "bethune_cookman", "bethune-cookman": "bethune_cookman", "bcu": "bethune_cookman",
    "binghamton": "binghamton", "bing": "binghamton",
    "boise state": "boise_state", "boise": "boise_state", "boise st": "boise_state",
    "boston college": "boston_college", "bc": "boston_college",
    "boston university": "boston_university", "bu": "boston_university", "boston u": "boston_university",
    "bowling green": "bowling_green", "bgsu": "bowling_green", "bg": "bowling_green",
    "bradley": "bradley", "brad": "bradley",
    "brigham young": "byu", "byu": "byu", "brigham young university": "byu",
    "brown": "brown", "brwn": "brown",
    "bryant": "bryant", "bry": "bryant",
    "bucknell": "bucknell", "buck": "bucknell",
    "buffalo": "buffalo", "buff": "buffalo", "ub": "buffalo",
    "butler": "butler", "but": "butler",
    
    # C
    "cal": "california", "california": "california", "cal bears": "california",
    "cal baptist": "cal_baptist", "cbu": "cal_baptist", "california baptist": "cal_baptist",
    "cal poly": "cal_poly", "cp": "cal_poly", "california polytechnic": "cal_poly",
    "cal state bakersfield": "cs_bakersfield", "csub": "cs_bakersfield", "bakersfield": "cs_bakersfield",
    "cal state fullerton": "cs_fullerton", "csuf": "cs_fullerton", "fullerton": "cs_fullerton",
    "cal state northridge": "cs_northridge", "csun": "cs_northridge", "northridge": "cs_northridge",
    "campbell": "campbell", "camp": "campbell",
    "canisius": "canisius", "can": "canisius",
    "central arkansas": "central_arkansas", "uca": "central_arkansas", "cent ark": "central_arkansas",
    "central connecticut": "central_connecticut", "ccsu": "central_connecticut", "cent conn": "central_connecticut",
    "central florida": "ucf", "ucf": "ucf", "cf": "ucf",
    "central michigan": "central_michigan", "cmu": "central_michigan", "cent mich": "central_michigan",
    "charleston": "charleston", "cofc": "charleston", "college of charleston": "charleston",
    "charleston southern": "charleston_southern", "csu": "charleston_southern", "charl so": "charleston_southern",
    "charlotte": "charlotte", "char": "charlotte", "uncc": "charlotte",
    "chattanooga": "chattanooga", "chat": "chattanooga", "utc": "chattanooga",
    "chicago state": "chicago_state", "chst": "chicago_state", "chi st": "chicago_state",
    "cincinnati": "cincinnati", "cin": "cincinnati", "cincy": "cincinnati", "uc": "cincinnati",
    "citadel": "citadel", "cit": "citadel", "the citadel": "citadel",
    "clemson": "clemson", "clem": "clemson",
    "cleveland state": "cleveland_state", "clev": "cleveland_state", "clev st": "cleveland_state",
    "coastal carolina": "coastal_carolina", "ccu": "coastal_carolina", "coastal": "coastal_carolina",
    "colgate": "colgate", "colg": "colgate",
    "colorado": "colorado", "col": "colorado", "colo": "colorado", "cu": "colorado",
    "colorado state": "colorado_state", "csu": "colorado_state", "colo st": "colorado_state", "colorado st": "colorado_state",
    "columbia": "columbia", "colu": "columbia",
    "connecticut": "connecticut", "uconn": "connecticut", "conn": "connecticut",
    "coppin state": "coppin_state", "copp": "coppin_state", "coppin st": "coppin_state",
    "cornell": "cornell", "corn": "cornell",
    "creighton": "creighton", "crei": "creighton",
    
    # D
    "dartmouth": "dartmouth", "dart": "dartmouth",
    "davidson": "davidson", "dav": "davidson",
    "dayton": "dayton", "day": "dayton",
    "delaware": "delaware", "del": "delaware", "ud": "delaware",
    "delaware state": "delaware_state", "dest": "delaware_state", "del st": "delaware_state",
    "denver": "denver", "den": "denver",
    "depaul": "depaul", "dep": "depaul",
    "detroit mercy": "detroit_mercy", "det": "detroit_mercy", "detroit": "detroit_mercy",
    "drake": "drake", "dra": "drake",
    "drexel": "drexel", "dre": "drexel",
    "duke": "duke", "duk": "duke",
    "duquesne": "duquesne", "duq": "duquesne",
    
    # E
    "east carolina": "east_carolina", "ecu": "east_carolina", "e carolina": "east_carolina",
    "east tennessee state": "east_tennessee_state", "etsu": "east_tennessee_state", "e tenn st": "east_tennessee_state",
    "eastern illinois": "eastern_illinois", "eiu": "eastern_illinois", "e illinois": "eastern_illinois",
    "eastern kentucky": "eastern_kentucky", "eku": "eastern_kentucky", "e kentucky": "eastern_kentucky",
    "eastern michigan": "eastern_michigan", "emu": "eastern_michigan", "e michigan": "eastern_michigan",
    "eastern washington": "eastern_washington", "ewu": "eastern_washington", "e washington": "eastern_washington",
    "elon": "elon",
    "evansville": "evansville", "evan": "evansville",
    
    # F
    "fairfield": "fairfield", "fair": "fairfield",
    "fairleigh dickinson": "fairleigh_dickinson", "fdu": "fairleigh_dickinson",
    "fiu": "fiu", "florida international": "fiu", "florida intl": "fiu",
    "florida": "florida", "fla": "florida", "fl": "florida", "uf": "florida",
    "florida a&m": "florida_am", "famu": "florida_am", "fla a&m": "florida_am",
    "florida atlantic": "florida_atlantic", "fau": "florida_atlantic", "fla atl": "florida_atlantic",
    "florida gulf coast": "florida_gulf_coast", "fgcu": "florida_gulf_coast",
    "florida state": "florida_state", "fsu": "florida_state", "fla st": "florida_state", "florida st": "florida_state",
    "fordham": "fordham", "ford": "fordham",
    "fresno state": "fresno_state", "fres": "fresno_state", "fresno": "fresno_state", "fresno st": "fresno_state",
    "furman": "furman", "fur": "furman",
    
    # G
    "gardner webb": "gardner_webb", "gard": "gardner_webb", "gardner-webb": "gardner_webb",
    "george mason": "george_mason", "gmu": "george_mason", "gm": "george_mason",
    "george washington": "george_washington", "gwu": "george_washington", "gw": "george_washington",
    "georgetown": "georgetown", "gtown": "georgetown", "gt": "georgetown",
    "georgia": "georgia", "uga": "georgia", "ga": "georgia",
    "georgia southern": "georgia_southern", "gaso": "georgia_southern", "ga so": "georgia_southern", "ga southern": "georgia_southern",
    "georgia state": "georgia_state", "gast": "georgia_state", "ga st": "georgia_state", "ga state": "georgia_state",
    "georgia tech": "georgia_tech", "gt": "georgia_tech", "ga tech": "georgia_tech",
    "gonzaga": "gonzaga", "gonz": "gonzaga", "zags": "gonzaga",
    "grambling": "grambling", "gram": "grambling", "grambling state": "grambling", "grambling st": "grambling",
    "grand canyon": "grand_canyon", "gcu": "grand_canyon",
    "green bay": "green_bay", "gb": "green_bay", "uw green bay": "green_bay",
    
    # H
    "hampton": "hampton", "hamp": "hampton",
    # Hartford reclassified to D3 in 2024 - removed from D1
    "harvard": "harvard", "harv": "harvard",
    "hawaii": "hawaii", "haw": "hawaii", "hawai'i": "hawaii",
    "high point": "high_point", "hpu": "high_point",
    "hofstra": "hofstra", "hof": "hofstra",
    "holy cross": "holy_cross", "hc": "holy_cross",
    "houston": "houston", "hou": "houston", "uh": "houston",
    "houston baptist": "houston_christian", "hbu": "houston_christian", "houston christian": "houston_christian",
    "howard": "howard", "how": "howard",
    
    # I
    "idaho": "idaho", "ida": "idaho",
    "idaho state": "idaho_state", "idst": "idaho_state", "idaho st": "idaho_state",
    "illinois": "illinois", "ill": "illinois", "uiuc": "illinois",
    "illinois state": "illinois_state", "ilst": "illinois_state", "ill st": "illinois_state", "illinois st": "illinois_state",
    "illinois chicago": "uic", "uic": "uic", "illinois-chicago": "uic", "uic flames": "uic",
    "incarnate word": "incarnate_word", "uiw": "incarnate_word",
    "indiana": "indiana", "ind": "indiana", "iu": "indiana",
    "indiana state": "indiana_state", "inst": "indiana_state", "ind st": "indiana_state", "indiana st": "indiana_state",
    "iona": "iona",
    "iowa": "iowa", "iow": "iowa",
    "iowa state": "iowa_state", "iast": "iowa_state", "iowa st": "iowa_state",
    "iu indianapolis": "iu_indianapolis", "iupui": "iu_indianapolis", "iu indy": "iu_indianapolis",
    
    # J
    "jackson state": "jackson_state", "jkst": "jackson_state", "jackson st": "jackson_state",
    "jacksonville": "jacksonville", "jax": "jacksonville", "ju": "jacksonville",
    "jacksonville state": "jacksonville_state", "jvst": "jacksonville_state", "jax st": "jacksonville_state",
    "james madison": "james_madison", "jmu": "james_madison", "jm": "james_madison",
    
    # K
    "kansas": "kansas", "kan": "kansas", "ku": "kansas", "kans": "kansas",
    "kansas state": "kansas_state", "ksu": "kansas_state", "k-state": "kansas_state", "kansas st": "kansas_state", "kst": "kansas_state",
    "kennesaw state": "kennesaw_state", "kenn": "kennesaw_state", "kennesaw st": "kennesaw_state", "kennesaw": "kennesaw_state",
    "kent state": "kent_state", "kent": "kent_state", "kent st": "kent_state",
    "kentucky": "kentucky", "ken": "kentucky", "uk": "kentucky",
    
    # L
    "la salle": "la_salle", "las": "la_salle", "lasalle": "la_salle",
    "lafayette": "lafayette", "laf": "lafayette",
    "lamar": "lamar", "lam": "lamar",
    "le moyne": "le_moyne", "lemoyne": "le_moyne", "le moyne dolphins": "le_moyne",
    "lehigh": "lehigh", "leh": "lehigh",
    "liberty": "liberty", "lib": "liberty",
    "lindenwood": "lindenwood", "lind": "lindenwood",
    "lipscomb": "lipscomb", "lip": "lipscomb",
    "little rock": "little_rock", "ualr": "little_rock", "arkansas little rock": "little_rock",
    "long beach state": "long_beach_state", "lbsu": "long_beach_state", "long beach st": "long_beach_state", "long beach": "long_beach_state",
    "long island": "long_island", "liu": "long_island", "long island u": "long_island",
    "longwood": "longwood", "long": "longwood",
    "louisiana": "louisiana", "ul": "louisiana", "louisiana lafayette": "louisiana", "la lafayette": "louisiana",
    "louisiana monroe": "louisiana_monroe", "ulm": "louisiana_monroe", "la monroe": "louisiana_monroe",
    "louisiana tech": "louisiana_tech", "lat": "louisiana_tech", "la tech": "louisiana_tech",
    "louisville": "louisville", "lou": "louisville", "uofl": "louisville", "l'ville": "louisville",
    "loyola chicago": "loyola_chicago", "loychi": "loyola_chicago", "luc": "loyola_chicago", "loyola il": "loyola_chicago",
    "loyola marymount": "loyola_marymount", "lmu": "loyola_marymount",
    "loyola md": "loyola_maryland", "loyola maryland": "loyola_maryland",
    "lsu": "lsu", "louisiana state": "lsu",
    
    # M
    "maine": "maine", "me": "maine",
    "manhattan": "manhattan", "manh": "manhattan",
    "marist": "marist", "mar": "marist",
    "marquette": "marquette", "marq": "marquette",
    "marshall": "marshall", "mrsh": "marshall",
    "maryland": "maryland", "md": "maryland", "umd": "maryland",
    "maryland eastern shore": "maryland_eastern_shore", "umes": "maryland_eastern_shore", "md es": "maryland_eastern_shore",
    "massachusetts": "massachusetts", "mass": "massachusetts", "umass": "massachusetts",
    "mcneese": "mcneese", "mcn": "mcneese", "mcneese state": "mcneese", "mcneese st": "mcneese",
    "memphis": "memphis", "mem": "memphis",
    "mercer": "mercer", "merc": "mercer",
    "mercyhurst": "mercyhurst", "mercy": "mercyhurst", "mercyhurst lakers": "mercyhurst",
    "merrimack": "merrimack", "merr": "merrimack",
    "miami fl": "miami_fl", "miami": "miami_fl", "mia": "miami_fl", "miami florida": "miami_fl",
    "miami oh": "miami_oh", "mioh": "miami_oh", "miami ohio": "miami_oh",
    "michigan": "michigan", "mich": "michigan", "um": "michigan",
    "michigan state": "michigan_state", "msu": "michigan_state", "mich st": "michigan_state", "michigan st": "michigan_state",
    "middle tennessee": "middle_tennessee", "mtsu": "middle_tennessee", "mid tenn": "middle_tennessee", "middle tennessee state": "middle_tennessee",
    "milwaukee": "milwaukee", "milw": "milwaukee", "uw milwaukee": "milwaukee",
    "minnesota": "minnesota", "minn": "minnesota",
    "mississippi": "mississippi", "miss": "mississippi", "ole miss": "mississippi",
    "mississippi state": "mississippi_state", "msst": "mississippi_state", "miss st": "mississippi_state", "mississippi st": "mississippi_state",
    "mississippi valley state": "mississippi_valley_state", "mvsu": "mississippi_valley_state", "miss valley st": "mississippi_valley_state",
    "missouri": "missouri", "miz": "missouri", "mizzou": "missouri", "mo": "missouri",
    "missouri kansas city": "umkc", "umkc": "umkc", "mo kc": "umkc",
    "missouri state": "missouri_state", "most": "missouri_state", "mo st": "missouri_state", "missouri st": "missouri_state",
    "monmouth": "monmouth", "monm": "monmouth",
    "montana": "montana", "mont": "montana",
    "montana state": "montana_state", "mtst": "montana_state", "mont st": "montana_state", "montana st": "montana_state",
    "morehead state": "morehead_state", "more": "morehead_state", "morehead st": "morehead_state",
    "morgan state": "morgan_state", "morg": "morgan_state", "morgan st": "morgan_state",
    "mount st marys": "mount_st_marys", "msm": "mount_st_marys", "mt st marys": "mount_st_marys", "mount st. mary's": "mount_st_marys",
    "murray state": "murray_state", "murr": "murray_state", "murray st": "murray_state",
    
    # N
    "navy": "navy", "nav": "navy",
    "nc state": "nc_state", "ncst": "nc_state", "north carolina state": "nc_state", "n carolina st": "nc_state",
    "nebraska": "nebraska", "neb": "nebraska",
    "nevada": "nevada", "nev": "nevada", "unr": "nevada",
    "new hampshire": "new_hampshire", "nh": "new_hampshire",
    "new mexico": "new_mexico", "nm": "new_mexico", "unm": "new_mexico",
    "new mexico state": "new_mexico_state", "nmsu": "new_mexico_state", "nm st": "new_mexico_state", "new mexico st": "new_mexico_state",
    "new orleans": "new_orleans", "uno": "new_orleans",
    "niagara": "niagara", "niag": "niagara",
    "nicholls": "nicholls", "nich": "nicholls", "nicholls state": "nicholls", "nicholls st": "nicholls",
    "njit": "njit",
    "norfolk state": "norfolk_state", "norf": "norfolk_state", "norfolk st": "norfolk_state",
    "north alabama": "north_alabama", "una": "north_alabama", "n alabama": "north_alabama",
    "north carolina": "north_carolina", "unc": "north_carolina", "n carolina": "north_carolina", "carolina": "north_carolina",
    "north carolina a&t": "north_carolina_at", "ncat": "north_carolina_at", "nc a&t": "north_carolina_at",
    "north carolina central": "north_carolina_central", "nccu": "north_carolina_central", "nc central": "north_carolina_central",
    "north dakota": "north_dakota", "und": "north_dakota", "n dakota": "north_dakota",
    "north dakota state": "north_dakota_state", "ndsu": "north_dakota_state", "n dakota st": "north_dakota_state",
    "north florida": "north_florida", "unf": "north_florida", "n florida": "north_florida",
    "north texas": "north_texas", "unt": "north_texas", "n texas": "north_texas",
    "northeastern": "northeastern", "neu": "northeastern", "ne": "northeastern",
    "northern arizona": "northern_arizona", "nau": "northern_arizona", "n arizona": "northern_arizona",
    "northern colorado": "northern_colorado", "noco": "northern_colorado", "n colorado": "northern_colorado",
    "northern illinois": "northern_illinois", "niu": "northern_illinois", "n illinois": "northern_illinois",
    "northern iowa": "northern_iowa", "uni": "northern_iowa", "n iowa": "northern_iowa",
    "northern kentucky": "northern_kentucky", "nku": "northern_kentucky", "n kentucky": "northern_kentucky",
    "northwestern": "northwestern", "nw": "northwestern",
    "northwestern state": "northwestern_state", "nwst": "northwestern_state", "nw st": "northwestern_state", "northwestern st": "northwestern_state",
    "notre dame": "notre_dame", "nd": "notre_dame",
    
    # O
    "oakland": "oakland", "oak": "oakland",
    "ohio": "ohio", "ohi": "ohio", "ohio bobcats": "ohio",
    "ohio state": "ohio_state", "osu": "ohio_state", "ohio st": "ohio_state",
    "oklahoma": "oklahoma", "okla": "oklahoma", "ou": "oklahoma",
    "oklahoma state": "oklahoma_state", "okst": "oklahoma_state", "okla st": "oklahoma_state", "oklahoma st": "oklahoma_state",
    "old dominion": "old_dominion", "odu": "old_dominion",
    "omaha": "omaha", "uno": "omaha", "nebraska omaha": "omaha",
    "oral roberts": "oral_roberts", "oru": "oral_roberts",
    "oregon": "oregon", "ore": "oregon", "uo": "oregon",
    "oregon state": "oregon_state", "orst": "oregon_state", "ore st": "oregon_state", "oregon st": "oregon_state",
    
    # P
    "pacific": "pacific", "pac": "pacific",
    "penn": "penn", "pennsylvania": "penn", "upenn": "penn",
    "penn state": "penn_state", "psu": "penn_state", "penn st": "penn_state",
    "pepperdine": "pepperdine", "pepp": "pepperdine",
    "pittsburgh": "pittsburgh", "pitt": "pittsburgh",
    "portland": "portland", "port": "portland",
    "portland state": "portland_state", "pst": "portland_state", "portland st": "portland_state",
    "prairie view a&m": "prairie_view", "pvam": "prairie_view", "prairie view": "prairie_view",
    "presbyterian": "presbyterian", "pres": "presbyterian",
    "princeton": "princeton", "prin": "princeton",
    "providence": "providence", "prov": "providence",
    "purdue": "purdue", "pur": "purdue",
    "purdue fort wayne": "purdue_fort_wayne", "pfw": "purdue_fort_wayne",
    
    # Q
    "queens": "queens", "quns": "queens", "queens university": "queens",
    "quinnipiac": "quinnipiac", "quin": "quinnipiac",
    
    # R
    "radford": "radford", "rad": "radford",
    "rhode island": "rhode_island", "ri": "rhode_island", "uri": "rhode_island",
    "rice": "rice",
    "richmond": "richmond", "rich": "richmond",
    "rider": "rider", "rid": "rider",
    "robert morris": "robert_morris", "rmu": "robert_morris",
    "rutgers": "rutgers", "rutg": "rutgers",
    
    # S
    "sacramento state": "sacramento_state", "sac": "sacramento_state", "sac st": "sacramento_state", "sacramento st": "sacramento_state",
    "sacred heart": "sacred_heart", "shu": "sacred_heart",
    "saint francis": "saint_francis", "sfpa": "saint_francis", "st francis pa": "saint_francis",
    "saint josephs": "saint_josephs", "stjo": "saint_josephs", "st josephs": "saint_josephs", "saint joseph's": "saint_josephs", "st joseph's": "saint_josephs",
    "saint louis": "saint_louis", "slu": "saint_louis", "st louis": "saint_louis",
    "saint marys": "saint_marys", "smc": "saint_marys", "st marys": "saint_marys", "saint mary's": "saint_marys", "st mary's": "saint_marys",
    "saint peters": "saint_peters", "stpe": "saint_peters", "st peters": "saint_peters", "saint peter's": "saint_peters", "st peter's": "saint_peters",
    "sam houston": "sam_houston", "shsu": "sam_houston", "sam houston state": "sam_houston", "sam houston st": "sam_houston",
    "samford": "samford", "sam": "samford",
    "san diego": "san_diego", "sd": "san_diego",
    "san diego state": "san_diego_state", "sdsu": "san_diego_state", "sd st": "san_diego_state", "san diego st": "san_diego_state",
    "san francisco": "san_francisco", "sf": "san_francisco", "usf": "san_francisco",
    "san jose state": "san_jose_state", "sjsu": "san_jose_state", "san jose st": "san_jose_state", "sj st": "san_jose_state",
    "santa clara": "santa_clara", "scla": "santa_clara",
    "seattle": "seattle", "sea": "seattle", "seattle u": "seattle",
    "seton hall": "seton_hall", "sh": "seton_hall",
    "siena": "siena", "sie": "siena",
    "siu edwardsville": "siu_edwardsville", "siue": "siu_edwardsville",
    "south alabama": "south_alabama", "usa": "south_alabama", "s alabama": "south_alabama",
    "south carolina": "south_carolina", "sc": "south_carolina", "s carolina": "south_carolina",
    "south carolina state": "south_carolina_state", "scst": "south_carolina_state", "sc st": "south_carolina_state", "sc state": "south_carolina_state",
    "south carolina upstate": "south_carolina_upstate", "upst": "south_carolina_upstate", "sc upstate": "south_carolina_upstate",
    "south dakota": "south_dakota", "sdak": "south_dakota", "s dakota": "south_dakota",
    "south dakota state": "south_dakota_state", "sdst": "south_dakota_state", "s dakota st": "south_dakota_state",
    "south florida": "south_florida", "usf": "south_florida", "s florida": "south_florida",
    "southeast missouri state": "southeast_missouri_state", "semo": "southeast_missouri_state", "se missouri st": "southeast_missouri_state", "se missouri state": "southeast_missouri_state",
    "southeastern louisiana": "southeastern_louisiana", "sela": "southeastern_louisiana", "se louisiana": "southeastern_louisiana",
    "southern": "southern", "sou": "southern", "southern university": "southern",
    "southern illinois": "southern_illinois", "siu": "southern_illinois", "s illinois": "southern_illinois",
    "southern indiana": "southern_indiana", "usi": "southern_indiana", "s indiana": "southern_indiana",
    "southern methodist": "smu", "smu": "smu",
    "southern miss": "southern_miss", "usm": "southern_miss", "southern mississippi": "southern_miss",
    "southern utah": "southern_utah", "suu": "southern_utah", "s utah": "southern_utah",
    "st bonaventure": "st_bonaventure", "stbn": "st_bonaventure", "saint bonaventure": "st_bonaventure",
    "st johns": "st_johns", "stjo": "st_johns", "saint johns": "st_johns", "st john's": "st_johns", "saint john's": "st_johns",
    "sju": "st_johns", "st. john's": "st_johns", "st. john's red storm": "st_johns", "st johns red storm": "st_johns",
    "st thomas": "st_thomas", "stmn": "st_thomas", "saint thomas": "st_thomas",
    "stanford": "stanford", "stan": "stanford",
    "stephen f austin": "stephen_f_austin", "sfa": "stephen_f_austin",
    "stetson": "stetson", "stet": "stetson",
    "stonehill": "stonehill", "shill": "stonehill", "stonehill skyhawks": "stonehill",
    "stony brook": "stony_brook", "ston": "stony_brook",
    "syracuse": "syracuse", "syr": "syracuse", "cuse": "syracuse",
    
    # T
    "tarleton state": "tarleton_state", "tarl": "tarleton_state", "tarleton st": "tarleton_state", "tarleton": "tarleton_state",
    "tcu": "tcu", "texas christian": "tcu",
    "temple": "temple", "tem": "temple",
    "tennessee": "tennessee", "tenn": "tennessee", "ut": "tennessee",
    "tennessee martin": "tennessee_martin", "utm": "tennessee_martin", "ut martin": "tennessee_martin",
    "tennessee state": "tennessee_state", "tnst": "tennessee_state", "tenn st": "tennessee_state", "tennessee st": "tennessee_state",
    "tennessee tech": "tennessee_tech", "ttu": "tennessee_tech", "tenn tech": "tennessee_tech",
    "texas": "texas", "tex": "texas", "ut": "texas",
    "texas a&m": "texas_am", "tamu": "texas_am", "tex a&m": "texas_am",
    "texas a&m commerce": "texas_am_commerce", "tamc": "texas_am_commerce",
    "texas a&m corpus christi": "texas_am_cc", "amcc": "texas_am_cc", "tex a&m cc": "texas_am_cc",
    "texas southern": "texas_southern", "txso": "texas_southern", "tex so": "texas_southern",
    "texas state": "texas_state", "txst": "texas_state", "tex st": "texas_state", "texas st": "texas_state",
    "texas tech": "texas_tech", "ttu": "texas_tech", "tex tech": "texas_tech",
    "toledo": "toledo", "tol": "toledo",
    "towson": "towson", "tow": "towson",
    "troy": "troy", "tro": "troy",
    "tulane": "tulane", "tul": "tulane",
    "tulsa": "tulsa", "tlsa": "tulsa",
    
    # U
    "uab": "uab", "alabama birmingham": "uab",
    "uc davis": "uc_davis", "ucd": "uc_davis",
    "uc irvine": "uc_irvine", "uci": "uc_irvine",
    "uc riverside": "uc_riverside", "ucr": "uc_riverside",
    "uc san diego": "uc_san_diego", "ucsd": "uc_san_diego",
    "uc santa barbara": "uc_santa_barbara", "ucsb": "uc_santa_barbara",
    "ucla": "ucla",
    "umbc": "umbc", "maryland baltimore county": "umbc",
    "unc asheville": "unc_asheville", "unca": "unc_asheville", "nc asheville": "unc_asheville",
    "unc greensboro": "unc_greensboro", "uncg": "unc_greensboro", "nc greensboro": "unc_greensboro",
    "unc wilmington": "unc_wilmington", "uncw": "unc_wilmington", "nc wilmington": "unc_wilmington",
    "unlv": "unlv", "nevada las vegas": "unlv",
    "usc": "usc", "southern california": "usc", "southern cal": "usc",
    "usc upstate": "south_carolina_upstate",
    "ut arlington": "ut_arlington", "uta": "ut_arlington", "texas arlington": "ut_arlington",
    "ut rio grande valley": "utrgv", "utrgv": "utrgv",
    "utah": "utah", "uta": "utah",
    "utah state": "utah_state", "utst": "utah_state", "utah st": "utah_state",
    "utah tech": "utah_tech", "utch": "utah_tech",
    "utah valley": "utah_valley", "uvu": "utah_valley",
    "utep": "utep", "texas el paso": "utep",
    "utsa": "utsa", "texas san antonio": "utsa",
    
    # V
    "valparaiso": "valparaiso", "valpo": "valparaiso", "valp": "valparaiso",
    "vanderbilt": "vanderbilt", "vandy": "vanderbilt", "vand": "vanderbilt",
    "vermont": "vermont", "vt": "vermont", "uvm": "vermont",
    "villanova": "villanova", "nova": "villanova", "vill": "villanova",
    "virginia": "virginia", "uva": "virginia", "va": "virginia",
    "virginia commonwealth": "vcu", "vcu": "vcu",
    "virginia tech": "virginia_tech", "vt": "virginia_tech", "va tech": "virginia_tech",
    "vmi": "vmi", "virginia military": "vmi",
    
    # W
    "wagner": "wagner", "wag": "wagner",
    "wake forest": "wake_forest", "wake": "wake_forest", "wf": "wake_forest",
    "washington": "washington", "wash": "washington", "uw": "washington",
    "washington state": "washington_state", "wsu": "washington_state", "wash st": "washington_state", "washington st": "washington_state",
    "weber state": "weber_state", "web": "weber_state", "weber st": "weber_state",
    "west georgia": "west_georgia", "uwg": "west_georgia", "west ga": "west_georgia",
    "west virginia": "west_virginia", "wvu": "west_virginia", "wv": "west_virginia",
    "western carolina": "western_carolina", "wcu": "western_carolina", "w carolina": "western_carolina",
    "western illinois": "western_illinois", "wiu": "western_illinois", "w illinois": "western_illinois",
    "western kentucky": "western_kentucky", "wku": "western_kentucky", "w kentucky": "western_kentucky",
    "western michigan": "western_michigan", "wmu": "western_michigan", "w michigan": "western_michigan",
    "wichita state": "wichita_state", "wich": "wichita_state", "wichita st": "wichita_state",
    "william mary": "william_mary", "wm": "william_mary", "william & mary": "william_mary",
    "winthrop": "winthrop", "win": "winthrop",
    "wisconsin": "wisconsin", "wisc": "wisconsin", "wis": "wisconsin",
    "wofford": "wofford", "wof": "wofford",
    "wright state": "wright_state", "wrst": "wright_state", "wright st": "wright_state",
    "wyoming": "wyoming", "wyo": "wyoming",
    
    # X-Y-Z
    "xavier": "xavier", "xav": "xavier",
    "yale": "yale",
    "youngstown state": "youngstown_state", "ysu": "youngstown_state", "youngstown st": "youngstown_state",
}

NBA_CANONICAL_ALIASES: Dict[str, str] = {
    # Eastern Conference - Atlantic
    "boston": "boston", "boston celtics": "boston", "celtics": "boston", "bos": "boston",
    "brooklyn": "brooklyn", "brooklyn nets": "brooklyn", "nets": "brooklyn", "bkn": "brooklyn", "brk": "brooklyn",
    "new york": "new_york", "new york knicks": "new_york", "knicks": "new_york", "nyk": "new_york", "ny knicks": "new_york",
    "philadelphia": "philadelphia", "philadelphia 76ers": "philadelphia", "76ers": "philadelphia", "sixers": "philadelphia", "phi": "philadelphia", "philly": "philadelphia",
    "toronto": "toronto", "toronto raptors": "toronto", "raptors": "toronto", "tor": "toronto",
    
    # Eastern Conference - Central
    "chicago": "chicago", "chicago bulls": "chicago", "bulls": "chicago", "chi": "chicago",
    "cleveland": "cleveland", "cleveland cavaliers": "cleveland", "cavaliers": "cleveland", "cavs": "cleveland", "cle": "cleveland",
    "detroit": "detroit", "detroit pistons": "detroit", "pistons": "detroit", "det": "detroit",
    "indiana": "indiana", "indiana pacers": "indiana", "pacers": "indiana", "ind": "indiana",
    "milwaukee": "milwaukee", "milwaukee bucks": "milwaukee", "bucks": "milwaukee", "mil": "milwaukee",
    
    # Eastern Conference - Southeast
    "atlanta": "atlanta", "atlanta hawks": "atlanta", "hawks": "atlanta", "atl": "atlanta",
    "charlotte": "charlotte", "charlotte hornets": "charlotte", "hornets": "charlotte", "cha": "charlotte",
    "miami": "miami", "miami heat": "miami", "heat": "miami", "mia": "miami",
    "orlando": "orlando", "orlando magic": "orlando", "magic": "orlando", "orl": "orlando",
    "washington": "washington", "washington wizards": "washington", "wizards": "washington", "was": "washington", "wsh": "washington",
    
    # Western Conference - Northwest
    "denver": "denver", "denver nuggets": "denver", "nuggets": "denver", "den": "denver",
    "minnesota": "minnesota", "minnesota timberwolves": "minnesota", "timberwolves": "minnesota", "wolves": "minnesota", "min": "minnesota",
    "oklahoma city": "oklahoma_city", "oklahoma city thunder": "oklahoma_city", "thunder": "oklahoma_city", "okc": "oklahoma_city", "okla city": "oklahoma_city",
    "portland": "portland", "portland trail blazers": "portland", "trail blazers": "portland", "blazers": "portland", "por": "portland",
    "utah": "utah", "utah jazz": "utah", "jazz": "utah", "uta": "utah",
    
    # Western Conference - Pacific
    "golden state": "golden_state", "golden state warriors": "golden_state", "warriors": "golden_state", "gsw": "golden_state", "gs": "golden_state",
    "la clippers": "la_clippers", "los angeles clippers": "la_clippers", "clippers": "la_clippers", "lac": "la_clippers",
    "la lakers": "la_lakers", "los angeles lakers": "la_lakers", "lakers": "la_lakers", "lal": "la_lakers",
    "phoenix": "phoenix", "phoenix suns": "phoenix", "suns": "phoenix", "phx": "phoenix",
    "sacramento": "sacramento", "sacramento kings": "sacramento", "kings": "sacramento", "sac": "sacramento",
    
    # Western Conference - Southwest
    "dallas": "dallas", "dallas mavericks": "dallas", "mavericks": "dallas", "mavs": "dallas", "dal": "dallas",
    "houston": "houston", "houston rockets": "houston", "rockets": "houston", "hou": "houston",
    "memphis": "memphis", "memphis grizzlies": "memphis", "grizzlies": "memphis", "mem": "memphis",
    "new orleans": "new_orleans", "new orleans pelicans": "new_orleans", "pelicans": "new_orleans", "nop": "new_orleans",
    "san antonio": "san_antonio", "san antonio spurs": "san_antonio", "spurs": "san_antonio", "sas": "san_antonio", "sa": "san_antonio",
}

# =============================================================================
# COVERS.COM DISPLAY MAPPINGS
# =============================================================================

CBB_COVERS_DISPLAY: Dict[str, Dict] = {
    # === A ===
    "abilene_christian": {"abbrev": "ACU", "logo_key": "abilene-christian"},
    "air_force": {"abbrev": "AF", "logo_key": "air-force"},
    "akron": {"abbrev": "AKR", "logo_key": "akron"},
    "alabama": {"abbrev": "ALA", "logo_key": "alabama"},
    "alabama_am": {"abbrev": "AAMU", "logo_key": "alabama-am"},
    "alabama_state": {"abbrev": "ALST", "logo_key": "alabama-state"},
    "albany": {"abbrev": "ALB", "logo_key": "albany"},
    "alcorn_state": {"abbrev": "ALCN", "logo_key": "alcorn-state"},
    "american": {"abbrev": "AMER", "logo_key": "american"},
    "appalachian_state": {"abbrev": "APP", "logo_key": "appalachian-state"},
    "arizona": {"abbrev": "ARIZ", "logo_key": "arizona"},
    "arizona_state": {"abbrev": "ASU", "logo_key": "arizona-state"},
    "arkansas": {"abbrev": "ARK", "logo_key": "arkansas"},
    "arkansas_pine_bluff": {"abbrev": "UAPB", "logo_key": "arkansas-pine-bluff"},
    "arkansas_state": {"abbrev": "ARST", "logo_key": "arkansas-state"},
    "army": {"abbrev": "ARMY", "logo_key": "army"},
    "auburn": {"abbrev": "AUB", "logo_key": "auburn"},
    # === B ===
    "ball_state": {"abbrev": "BALL", "logo_key": "ball-state"},
    "baylor": {"abbrev": "BAY", "logo_key": "baylor"},
    "bellarmine": {"abbrev": "BELL", "logo_key": "bellarmine"},
    "belmont": {"abbrev": "BEL", "logo_key": "belmont"},
    "bethune_cookman": {"abbrev": "BCU", "logo_key": "bethune-cookman"},
    "binghamton": {"abbrev": "BING", "logo_key": "binghamton"},
    "boise_state": {"abbrev": "BSU", "logo_key": "boise-state"},
    "boston_college": {"abbrev": "BC", "logo_key": "boston-college"},
    "boston_university": {"abbrev": "BU", "logo_key": "boston-university"},
    "bowling_green": {"abbrev": "BGSU", "logo_key": "bowling-green"},
    "bradley": {"abbrev": "BRAD", "logo_key": "bradley"},
    "brown": {"abbrev": "BRWN", "logo_key": "brown"},
    "bryant": {"abbrev": "BRY", "logo_key": "bryant"},
    "bucknell": {"abbrev": "BUCK", "logo_key": "bucknell"},
    "buffalo": {"abbrev": "BUFF", "logo_key": "buffalo"},
    "butler": {"abbrev": "BUT", "logo_key": "butler"},
    "byu": {"abbrev": "BYU", "logo_key": "byu"},
    # === C ===
    "cal_baptist": {"abbrev": "CBU", "logo_key": "cal-baptist"},
    "cal_poly": {"abbrev": "CP", "logo_key": "cal-poly"},
    "california": {"abbrev": "CAL", "logo_key": "california"},
    "campbell": {"abbrev": "CAMP", "logo_key": "campbell"},
    "canisius": {"abbrev": "CAN", "logo_key": "canisius"},
    "central_arkansas": {"abbrev": "UCA", "logo_key": "central-arkansas"},
    "central_connecticut": {"abbrev": "CCSU", "logo_key": "central-connecticut"},
    "central_michigan": {"abbrev": "CMU", "logo_key": "central-michigan"},
    "charleston": {"abbrev": "COFC", "logo_key": "charleston"},
    "charleston_southern": {"abbrev": "CSU", "logo_key": "charleston-southern"},
    "charlotte": {"abbrev": "CHAR", "logo_key": "charlotte"},
    "chattanooga": {"abbrev": "CHAT", "logo_key": "chattanooga"},
    "chicago_state": {"abbrev": "CHST", "logo_key": "chicago-state"},
    "cincinnati": {"abbrev": "CIN", "logo_key": "cincinnati"},
    "citadel": {"abbrev": "CIT", "logo_key": "citadel"},
    "clemson": {"abbrev": "CLEM", "logo_key": "clemson"},
    "cleveland_state": {"abbrev": "CLEV", "logo_key": "cleveland-state"},
    "coastal_carolina": {"abbrev": "CCU", "logo_key": "coastal-carolina"},
    "colgate": {"abbrev": "COLG", "logo_key": "colgate"},
    "colorado": {"abbrev": "COLO", "logo_key": "colorado"},
    "colorado_state": {"abbrev": "CSU", "logo_key": "colorado-state"},
    "columbia": {"abbrev": "COLU", "logo_key": "columbia"},
    "connecticut": {"abbrev": "UCONN", "logo_key": "connecticut"},
    "coppin_state": {"abbrev": "COPP", "logo_key": "coppin-state"},
    "cornell": {"abbrev": "CORN", "logo_key": "cornell"},
    "creighton": {"abbrev": "CREI", "logo_key": "creighton"},
    "cs_bakersfield": {"abbrev": "CSUB", "logo_key": "cs-bakersfield"},
    "cs_fullerton": {"abbrev": "CSUF", "logo_key": "cs-fullerton"},
    "cs_northridge": {"abbrev": "CSUN", "logo_key": "cs-northridge"},
    # === D ===
    "dartmouth": {"abbrev": "DART", "logo_key": "dartmouth"},
    "davidson": {"abbrev": "DAV", "logo_key": "davidson"},
    "dayton": {"abbrev": "DAY", "logo_key": "dayton"},
    "delaware": {"abbrev": "DEL", "logo_key": "delaware"},
    "delaware_state": {"abbrev": "DEST", "logo_key": "delaware-state"},
    "denver": {"abbrev": "DEN", "logo_key": "denver"},
    "depaul": {"abbrev": "DEP", "logo_key": "depaul"},
    "detroit_mercy": {"abbrev": "DET", "logo_key": "detroit-mercy"},
    "drake": {"abbrev": "DRA", "logo_key": "drake"},
    "drexel": {"abbrev": "DRE", "logo_key": "drexel"},
    "duke": {"abbrev": "DUKE", "logo_key": "duke"},
    "duquesne": {"abbrev": "DUQ", "logo_key": "duquesne"},
    # === E ===
    "east_carolina": {"abbrev": "ECU", "logo_key": "east-carolina"},
    "east_tennessee_state": {"abbrev": "ETSU", "logo_key": "east-tennessee-state"},
    "eastern_illinois": {"abbrev": "EIU", "logo_key": "eastern-illinois"},
    "eastern_kentucky": {"abbrev": "EKU", "logo_key": "eastern-kentucky"},
    "eastern_michigan": {"abbrev": "EMU", "logo_key": "eastern-michigan"},
    "eastern_washington": {"abbrev": "EWU", "logo_key": "eastern-washington"},
    "elon": {"abbrev": "ELON", "logo_key": "elon"},
    "evansville": {"abbrev": "EVAN", "logo_key": "evansville"},
    # === F ===
    "fairfield": {"abbrev": "FAIR", "logo_key": "fairfield"},
    "fairleigh_dickinson": {"abbrev": "FDU", "logo_key": "fairleigh-dickinson"},
    "fiu": {"abbrev": "FIU", "logo_key": "fiu"},
    "florida": {"abbrev": "FLA", "logo_key": "florida"},
    "florida_am": {"abbrev": "FAMU", "logo_key": "florida-am"},
    "florida_atlantic": {"abbrev": "FAU", "logo_key": "florida-atlantic"},
    "florida_gulf_coast": {"abbrev": "FGCU", "logo_key": "florida-gulf-coast"},
    "florida_state": {"abbrev": "FSU", "logo_key": "florida-state"},
    "fordham": {"abbrev": "FORD", "logo_key": "fordham"},
    "fresno_state": {"abbrev": "FRES", "logo_key": "fresno-state"},
    "furman": {"abbrev": "FUR", "logo_key": "furman"},
    # === G ===
    "gardner_webb": {"abbrev": "GARD", "logo_key": "gardner-webb"},
    "george_mason": {"abbrev": "GMU", "logo_key": "george-mason"},
    "george_washington": {"abbrev": "GW", "logo_key": "george-washington"},
    "georgetown": {"abbrev": "GTWN", "logo_key": "georgetown"},
    "georgia": {"abbrev": "UGA", "logo_key": "georgia"},
    "georgia_southern": {"abbrev": "GASO", "logo_key": "georgia-southern"},
    "georgia_state": {"abbrev": "GAST", "logo_key": "georgia-state"},
    "georgia_tech": {"abbrev": "GT", "logo_key": "georgia-tech"},
    "gonzaga": {"abbrev": "GONZ", "logo_key": "gonzaga"},
    "grambling": {"abbrev": "GRAM", "logo_key": "grambling"},
    "grand_canyon": {"abbrev": "GCU", "logo_key": "grand-canyon"},
    "green_bay": {"abbrev": "GB", "logo_key": "green-bay"},
    # === H ===
    "hampton": {"abbrev": "HAMP", "logo_key": "hampton"},
    "harvard": {"abbrev": "HARV", "logo_key": "harvard"},
    "hawaii": {"abbrev": "HAW", "logo_key": "hawaii"},
    "high_point": {"abbrev": "HPU", "logo_key": "high-point"},
    "hofstra": {"abbrev": "HOF", "logo_key": "hofstra"},
    "holy_cross": {"abbrev": "HC", "logo_key": "holy-cross"},
    "houston": {"abbrev": "HOU", "logo_key": "houston"},
    "houston_christian": {"abbrev": "HCU", "logo_key": "houston-christian"},
    "howard": {"abbrev": "HOW", "logo_key": "howard"},
    # === I ===
    "idaho": {"abbrev": "IDA", "logo_key": "idaho"},
    "idaho_state": {"abbrev": "IDST", "logo_key": "idaho-state"},
    "illinois": {"abbrev": "ILL", "logo_key": "illinois"},
    "illinois_state": {"abbrev": "ILST", "logo_key": "illinois-state"},
    "incarnate_word": {"abbrev": "UIW", "logo_key": "incarnate-word"},
    "indiana": {"abbrev": "IND", "logo_key": "indiana"},
    "indiana_state": {"abbrev": "INST", "logo_key": "indiana-state"},
    "iona": {"abbrev": "IONA", "logo_key": "iona"},
    "iowa": {"abbrev": "IOWA", "logo_key": "iowa"},
    "iowa_state": {"abbrev": "ISU", "logo_key": "iowa-state"},
    "iu_indianapolis": {"abbrev": "IUPU", "logo_key": "iu-indianapolis"},
    # === J ===
    "jackson_state": {"abbrev": "JKST", "logo_key": "jackson-state"},
    "jacksonville": {"abbrev": "JAX", "logo_key": "jacksonville"},
    "jacksonville_state": {"abbrev": "JVST", "logo_key": "jacksonville-state"},
    "james_madison": {"abbrev": "JMU", "logo_key": "james-madison"},
    # === K ===
    "kansas": {"abbrev": "KU", "logo_key": "kansas"},
    "kansas_state": {"abbrev": "KSU", "logo_key": "kansas-state"},
    "kennesaw_state": {"abbrev": "KENN", "logo_key": "kennesaw-state"},
    "kent_state": {"abbrev": "KENT", "logo_key": "kent-state"},
    "kentucky": {"abbrev": "UK", "logo_key": "kentucky"},
    # === L ===
    "la_salle": {"abbrev": "LAS", "logo_key": "la-salle"},
    "lafayette": {"abbrev": "LAF", "logo_key": "lafayette"},
    "lamar": {"abbrev": "LAM", "logo_key": "lamar"},
    "le_moyne": {"abbrev": "LEM", "logo_key": "le-moyne"},
    "lehigh": {"abbrev": "LEH", "logo_key": "lehigh"},
    "liberty": {"abbrev": "LIB", "logo_key": "liberty"},
    "lindenwood": {"abbrev": "LIND", "logo_key": "lindenwood"},
    "lipscomb": {"abbrev": "LIP", "logo_key": "lipscomb"},
    "little_rock": {"abbrev": "UALR", "logo_key": "little-rock"},
    "long_beach_state": {"abbrev": "LBSU", "logo_key": "long-beach-state"},
    "long_island": {"abbrev": "LIU", "logo_key": "long-island"},
    "longwood": {"abbrev": "LONG", "logo_key": "longwood"},
    "louisiana": {"abbrev": "UL", "logo_key": "louisiana"},
    "louisiana_monroe": {"abbrev": "ULM", "logo_key": "louisiana-monroe"},
    "louisiana_tech": {"abbrev": "LAT", "logo_key": "louisiana-tech"},
    "louisville": {"abbrev": "LOU", "logo_key": "louisville"},
    "loyola_chicago": {"abbrev": "LUC", "logo_key": "loyola-chicago"},
    "loyola_maryland": {"abbrev": "LOMD", "logo_key": "loyola-maryland"},
    "loyola_marymount": {"abbrev": "LMU", "logo_key": "loyola-marymount"},
    "lsu": {"abbrev": "LSU", "logo_key": "lsu"},
    # === M ===
    "maine": {"abbrev": "ME", "logo_key": "maine"},
    "manhattan": {"abbrev": "MANH", "logo_key": "manhattan"},
    "marist": {"abbrev": "MAR", "logo_key": "marist"},
    "marquette": {"abbrev": "MARQ", "logo_key": "marquette"},
    "marshall": {"abbrev": "MRSH", "logo_key": "marshall"},
    "maryland": {"abbrev": "MD", "logo_key": "maryland"},
    "maryland_eastern_shore": {"abbrev": "UMES", "logo_key": "maryland-eastern-shore"},
    "massachusetts": {"abbrev": "MASS", "logo_key": "massachusetts"},
    "mcneese": {"abbrev": "MCN", "logo_key": "mcneese"},
    "memphis": {"abbrev": "MEM", "logo_key": "memphis"},
    "mercer": {"abbrev": "MERC", "logo_key": "mercer"},
    "mercyhurst": {"abbrev": "MRCY", "logo_key": "mercyhurst"},
    "merrimack": {"abbrev": "MERR", "logo_key": "merrimack"},
    "miami_fl": {"abbrev": "MIA", "logo_key": "miami"},
    "miami_oh": {"abbrev": "MIOH", "logo_key": "miami-oh"},
    "michigan": {"abbrev": "MICH", "logo_key": "michigan"},
    "michigan_state": {"abbrev": "MSU", "logo_key": "michigan-state"},
    "middle_tennessee": {"abbrev": "MTSU", "logo_key": "middle-tennessee"},
    "milwaukee": {"abbrev": "MILW", "logo_key": "milwaukee"},
    "minnesota": {"abbrev": "MINN", "logo_key": "minnesota"},
    "mississippi": {"abbrev": "MISS", "logo_key": "mississippi"},
    "mississippi_state": {"abbrev": "MSST", "logo_key": "mississippi-state"},
    "mississippi_valley_state": {"abbrev": "MVSU", "logo_key": "mississippi-valley-state"},
    "missouri": {"abbrev": "MIZ", "logo_key": "missouri"},
    "missouri_state": {"abbrev": "MOST", "logo_key": "missouri-state"},
    "monmouth": {"abbrev": "MONM", "logo_key": "monmouth"},
    "montana": {"abbrev": "MONT", "logo_key": "montana"},
    "montana_state": {"abbrev": "MTST", "logo_key": "montana-state"},
    "morehead_state": {"abbrev": "MORE", "logo_key": "morehead-state"},
    "morgan_state": {"abbrev": "MORG", "logo_key": "morgan-state"},
    "mount_st_marys": {"abbrev": "MSM", "logo_key": "mount-st-marys"},
    "murray_state": {"abbrev": "MURR", "logo_key": "murray-state"},
    # === N ===
    "navy": {"abbrev": "NAVY", "logo_key": "navy"},
    "nc_state": {"abbrev": "NCST", "logo_key": "nc-state"},
    "nebraska": {"abbrev": "NEB", "logo_key": "nebraska"},
    "nevada": {"abbrev": "NEV", "logo_key": "nevada"},
    "new_hampshire": {"abbrev": "UNH", "logo_key": "new-hampshire"},
    "new_mexico": {"abbrev": "UNM", "logo_key": "new-mexico"},
    "new_mexico_state": {"abbrev": "NMSU", "logo_key": "new-mexico-state"},
    "new_orleans": {"abbrev": "UNO", "logo_key": "new-orleans"},
    "niagara": {"abbrev": "NIAG", "logo_key": "niagara"},
    "nicholls": {"abbrev": "NICH", "logo_key": "nicholls"},
    "njit": {"abbrev": "NJIT", "logo_key": "njit"},
    "norfolk_state": {"abbrev": "NORF", "logo_key": "norfolk-state"},
    "north_alabama": {"abbrev": "UNA", "logo_key": "north-alabama"},
    "north_carolina": {"abbrev": "UNC", "logo_key": "north-carolina"},
    "north_carolina_at": {"abbrev": "NCAT", "logo_key": "north-carolina-at"},
    "north_carolina_central": {"abbrev": "NCCU", "logo_key": "north-carolina-central"},
    "north_dakota": {"abbrev": "UND", "logo_key": "north-dakota"},
    "north_dakota_state": {"abbrev": "NDSU", "logo_key": "north-dakota-state"},
    "north_florida": {"abbrev": "UNF", "logo_key": "north-florida"},
    "north_texas": {"abbrev": "UNT", "logo_key": "north-texas"},
    "northeastern": {"abbrev": "NEU", "logo_key": "northeastern"},
    "northern_arizona": {"abbrev": "NAU", "logo_key": "northern-arizona"},
    "northern_colorado": {"abbrev": "NOCO", "logo_key": "northern-colorado"},
    "northern_illinois": {"abbrev": "NIU", "logo_key": "northern-illinois"},
    "northern_iowa": {"abbrev": "UNI", "logo_key": "northern-iowa"},
    "northern_kentucky": {"abbrev": "NKU", "logo_key": "northern-kentucky"},
    "northwestern": {"abbrev": "NW", "logo_key": "northwestern"},
    "northwestern_state": {"abbrev": "NWST", "logo_key": "northwestern-state"},
    "notre_dame": {"abbrev": "ND", "logo_key": "notre-dame"},
    # === O ===
    "oakland": {"abbrev": "OAK", "logo_key": "oakland"},
    "ohio": {"abbrev": "OHIO", "logo_key": "ohio"},
    "ohio_state": {"abbrev": "OSU", "logo_key": "ohio-state"},
    "oklahoma": {"abbrev": "OKLA", "logo_key": "oklahoma"},
    "oklahoma_state": {"abbrev": "OKST", "logo_key": "oklahoma-state"},
    "old_dominion": {"abbrev": "ODU", "logo_key": "old-dominion"},
    "omaha": {"abbrev": "UNO", "logo_key": "omaha"},
    "oral_roberts": {"abbrev": "ORU", "logo_key": "oral-roberts"},
    "oregon": {"abbrev": "ORE", "logo_key": "oregon"},
    "oregon_state": {"abbrev": "ORST", "logo_key": "oregon-state"},
    # === P ===
    "pacific": {"abbrev": "PAC", "logo_key": "pacific"},
    "penn": {"abbrev": "PENN", "logo_key": "penn"},
    "penn_state": {"abbrev": "PSU", "logo_key": "penn-state"},
    "pepperdine": {"abbrev": "PEPP", "logo_key": "pepperdine"},
    "pittsburgh": {"abbrev": "PITT", "logo_key": "pittsburgh"},
    "portland": {"abbrev": "PORT", "logo_key": "portland"},
    "portland_state": {"abbrev": "PSU", "logo_key": "portland-state"},
    "prairie_view": {"abbrev": "PVAM", "logo_key": "prairie-view"},
    "presbyterian": {"abbrev": "PRES", "logo_key": "presbyterian"},
    "princeton": {"abbrev": "PRIN", "logo_key": "princeton"},
    "providence": {"abbrev": "PROV", "logo_key": "providence"},
    "purdue": {"abbrev": "PUR", "logo_key": "purdue"},
    "purdue_fort_wayne": {"abbrev": "PFW", "logo_key": "purdue-fort-wayne"},
    # === Q ===
    "queens": {"abbrev": "QUNS", "logo_key": "queens"},
    "quinnipiac": {"abbrev": "QUIN", "logo_key": "quinnipiac"},
    # === R ===
    "radford": {"abbrev": "RAD", "logo_key": "radford"},
    "rhode_island": {"abbrev": "URI", "logo_key": "rhode-island"},
    "rice": {"abbrev": "RICE", "logo_key": "rice"},
    "richmond": {"abbrev": "RICH", "logo_key": "richmond"},
    "rider": {"abbrev": "RID", "logo_key": "rider"},
    "robert_morris": {"abbrev": "RMU", "logo_key": "robert-morris"},
    "rutgers": {"abbrev": "RUTG", "logo_key": "rutgers"},
    # === S ===
    "sacramento_state": {"abbrev": "SAC", "logo_key": "sacramento-state"},
    "sacred_heart": {"abbrev": "SHU", "logo_key": "sacred-heart"},
    "saint_francis": {"abbrev": "SFPA", "logo_key": "saint-francis"},
    "saint_josephs": {"abbrev": "STJO", "logo_key": "saint-josephs"},
    "saint_louis": {"abbrev": "SLU", "logo_key": "saint-louis"},
    "saint_marys": {"abbrev": "SMC", "logo_key": "saint-marys"},
    "saint_peters": {"abbrev": "STPE", "logo_key": "saint-peters"},
    "sam_houston": {"abbrev": "SHSU", "logo_key": "sam-houston"},
    "samford": {"abbrev": "SAM", "logo_key": "samford"},
    "san_diego": {"abbrev": "SD", "logo_key": "san-diego"},
    "san_diego_state": {"abbrev": "SDSU", "logo_key": "san-diego-state"},
    "san_francisco": {"abbrev": "USF", "logo_key": "san-francisco"},
    "san_jose_state": {"abbrev": "SJSU", "logo_key": "san-jose-state"},
    "santa_clara": {"abbrev": "SCU", "logo_key": "santa-clara"},
    "seattle": {"abbrev": "SEA", "logo_key": "seattle"},
    "seton_hall": {"abbrev": "SH", "logo_key": "seton-hall"},
    "siena": {"abbrev": "SIE", "logo_key": "siena"},
    "siu_edwardsville": {"abbrev": "SIUE", "logo_key": "siu-edwardsville"},
    "smu": {"abbrev": "SMU", "logo_key": "smu"},
    "south_alabama": {"abbrev": "USA", "logo_key": "south-alabama"},
    "south_carolina": {"abbrev": "SC", "logo_key": "south-carolina"},
    "south_carolina_state": {"abbrev": "SCST", "logo_key": "south-carolina-state"},
    "south_carolina_upstate": {"abbrev": "UPST", "logo_key": "south-carolina-upstate"},
    "south_dakota": {"abbrev": "SDAK", "logo_key": "south-dakota"},
    "south_dakota_state": {"abbrev": "SDST", "logo_key": "south-dakota-state"},
    "south_florida": {"abbrev": "USF", "logo_key": "south-florida"},
    "southeast_missouri_state": {"abbrev": "SEMO", "logo_key": "southeast-missouri-state"},
    "southeastern_louisiana": {"abbrev": "SELA", "logo_key": "southeastern-louisiana"},
    "southern": {"abbrev": "SOU", "logo_key": "southern"},
    "southern_illinois": {"abbrev": "SIU", "logo_key": "southern-illinois"},
    "southern_indiana": {"abbrev": "USI", "logo_key": "southern-indiana"},
    "southern_miss": {"abbrev": "USM", "logo_key": "southern-miss"},
    "southern_utah": {"abbrev": "SUU", "logo_key": "southern-utah"},
    "st_bonaventure": {"abbrev": "STBN", "logo_key": "st-bonaventure"},
    "st_johns": {"abbrev": "STJO", "logo_key": "st-johns"},
    "st_thomas": {"abbrev": "STMN", "logo_key": "st-thomas"},
    "stanford": {"abbrev": "STAN", "logo_key": "stanford"},
    "stephen_f_austin": {"abbrev": "SFA", "logo_key": "stephen-f-austin"},
    "stetson": {"abbrev": "STET", "logo_key": "stetson"},
    "stonehill": {"abbrev": "SHILL", "logo_key": "stonehill"},
    "stony_brook": {"abbrev": "STON", "logo_key": "stony-brook"},
    "syracuse": {"abbrev": "SYR", "logo_key": "syracuse"},
    # === T ===
    "tarleton_state": {"abbrev": "TARL", "logo_key": "tarleton-state"},
    "tcu": {"abbrev": "TCU", "logo_key": "tcu"},
    "temple": {"abbrev": "TEM", "logo_key": "temple"},
    "tennessee": {"abbrev": "TENN", "logo_key": "tennessee"},
    "tennessee_martin": {"abbrev": "UTM", "logo_key": "tennessee-martin"},
    "tennessee_state": {"abbrev": "TNST", "logo_key": "tennessee-state"},
    "tennessee_tech": {"abbrev": "TTU", "logo_key": "tennessee-tech"},
    "texas": {"abbrev": "TEX", "logo_key": "texas"},
    "texas_am": {"abbrev": "TAMU", "logo_key": "texas-am"},
    "texas_am_cc": {"abbrev": "AMCC", "logo_key": "texas-am-cc"},
    "texas_am_commerce": {"abbrev": "TAMC", "logo_key": "texas-am-commerce"},
    "texas_southern": {"abbrev": "TXSO", "logo_key": "texas-southern"},
    "texas_state": {"abbrev": "TXST", "logo_key": "texas-state"},
    "texas_tech": {"abbrev": "TTU", "logo_key": "texas-tech"},
    "toledo": {"abbrev": "TOL", "logo_key": "toledo"},
    "towson": {"abbrev": "TOW", "logo_key": "towson"},
    "troy": {"abbrev": "TROY", "logo_key": "troy"},
    "tulane": {"abbrev": "TUL", "logo_key": "tulane"},
    "tulsa": {"abbrev": "TLSA", "logo_key": "tulsa"},
    # === U ===
    "uab": {"abbrev": "UAB", "logo_key": "uab"},
    "uc_davis": {"abbrev": "UCD", "logo_key": "uc-davis"},
    "uc_irvine": {"abbrev": "UCI", "logo_key": "uc-irvine"},
    "uc_riverside": {"abbrev": "UCR", "logo_key": "uc-riverside"},
    "uc_san_diego": {"abbrev": "UCSD", "logo_key": "uc-san-diego"},
    "uc_santa_barbara": {"abbrev": "UCSB", "logo_key": "uc-santa-barbara"},
    "ucf": {"abbrev": "UCF", "logo_key": "ucf"},
    "ucla": {"abbrev": "UCLA", "logo_key": "ucla"},
    "uic": {"abbrev": "UIC", "logo_key": "uic"},
    "umbc": {"abbrev": "UMBC", "logo_key": "umbc"},
    "umkc": {"abbrev": "UMKC", "logo_key": "umkc"},
    "unc_asheville": {"abbrev": "UNCA", "logo_key": "unc-asheville"},
    "unc_greensboro": {"abbrev": "UNCG", "logo_key": "unc-greensboro"},
    "unc_wilmington": {"abbrev": "UNCW", "logo_key": "unc-wilmington"},
    "unlv": {"abbrev": "UNLV", "logo_key": "unlv"},
    "usc": {"abbrev": "USC", "logo_key": "usc"},
    "ut_arlington": {"abbrev": "UTA", "logo_key": "ut-arlington"},
    "utah": {"abbrev": "UTAH", "logo_key": "utah"},
    "utah_state": {"abbrev": "USU", "logo_key": "utah-state"},
    "utah_tech": {"abbrev": "UTCH", "logo_key": "utah-tech"},
    "utah_valley": {"abbrev": "UVU", "logo_key": "utah-valley"},
    "utep": {"abbrev": "UTEP", "logo_key": "utep"},
    "utrgv": {"abbrev": "UTRGV", "logo_key": "utrgv"},
    "utsa": {"abbrev": "UTSA", "logo_key": "utsa"},
    # === V ===
    "valparaiso": {"abbrev": "VALP", "logo_key": "valparaiso"},
    "vanderbilt": {"abbrev": "VAND", "logo_key": "vanderbilt"},
    "vcu": {"abbrev": "VCU", "logo_key": "vcu"},
    "vermont": {"abbrev": "UVM", "logo_key": "vermont"},
    "villanova": {"abbrev": "NOVA", "logo_key": "villanova"},
    "virginia": {"abbrev": "UVA", "logo_key": "virginia"},
    "virginia_tech": {"abbrev": "VT", "logo_key": "virginia-tech"},
    "vmi": {"abbrev": "VMI", "logo_key": "vmi"},
    # === W ===
    "wagner": {"abbrev": "WAG", "logo_key": "wagner"},
    "wake_forest": {"abbrev": "WAKE", "logo_key": "wake-forest"},
    "washington": {"abbrev": "WASH", "logo_key": "washington"},
    "washington_state": {"abbrev": "WSU", "logo_key": "washington-state"},
    "weber_state": {"abbrev": "WEB", "logo_key": "weber-state"},
    "west_georgia": {"abbrev": "UWG", "logo_key": "west-georgia"},
    "west_virginia": {"abbrev": "WVU", "logo_key": "west-virginia"},
    "western_carolina": {"abbrev": "WCU", "logo_key": "western-carolina"},
    "western_illinois": {"abbrev": "WIU", "logo_key": "western-illinois"},
    "western_kentucky": {"abbrev": "WKU", "logo_key": "western-kentucky"},
    "western_michigan": {"abbrev": "WMU", "logo_key": "western-michigan"},
    "wichita_state": {"abbrev": "WICH", "logo_key": "wichita-state"},
    "william_mary": {"abbrev": "WM", "logo_key": "william-mary"},
    "winthrop": {"abbrev": "WIN", "logo_key": "winthrop"},
    "wisconsin": {"abbrev": "WIS", "logo_key": "wisconsin"},
    "wofford": {"abbrev": "WOF", "logo_key": "wofford"},
    "wright_state": {"abbrev": "WRST", "logo_key": "wright-state"},
    "wyoming": {"abbrev": "WYO", "logo_key": "wyoming"},
    # === X-Y-Z ===
    "xavier": {"abbrev": "XAV", "logo_key": "xavier"},
    "yale": {"abbrev": "YALE", "logo_key": "yale"},
    "youngstown_state": {"abbrev": "YSU", "logo_key": "youngstown-state"},
}

NBA_COVERS_DISPLAY: Dict[str, Dict] = {
    "boston": {"abbrev": "BOS", "logo_key": "celtics"},
    "brooklyn": {"abbrev": "BKN", "logo_key": "nets"},
    "new_york": {"abbrev": "NYK", "logo_key": "knicks"},
    "philadelphia": {"abbrev": "PHI", "logo_key": "76ers"},
    "toronto": {"abbrev": "TOR", "logo_key": "raptors"},
    "chicago": {"abbrev": "CHI", "logo_key": "bulls"},
    "cleveland": {"abbrev": "CLE", "logo_key": "cavaliers"},
    "detroit": {"abbrev": "DET", "logo_key": "pistons"},
    "indiana": {"abbrev": "IND", "logo_key": "pacers"},
    "milwaukee": {"abbrev": "MIL", "logo_key": "bucks"},
    "atlanta": {"abbrev": "ATL", "logo_key": "hawks"},
    "charlotte": {"abbrev": "CHA", "logo_key": "hornets"},
    "miami": {"abbrev": "MIA", "logo_key": "heat"},
    "orlando": {"abbrev": "ORL", "logo_key": "magic"},
    "washington": {"abbrev": "WAS", "logo_key": "wizards"},
    "denver": {"abbrev": "DEN", "logo_key": "nuggets"},
    "minnesota": {"abbrev": "MIN", "logo_key": "timberwolves"},
    "oklahoma_city": {"abbrev": "OKC", "logo_key": "thunder"},
    "portland": {"abbrev": "POR", "logo_key": "trail-blazers"},
    "utah": {"abbrev": "UTA", "logo_key": "jazz"},
    "golden_state": {"abbrev": "GSW", "logo_key": "warriors"},
    "la_clippers": {"abbrev": "LAC", "logo_key": "clippers"},
    "la_lakers": {"abbrev": "LAL", "logo_key": "lakers"},
    "phoenix": {"abbrev": "PHX", "logo_key": "suns"},
    "sacramento": {"abbrev": "SAC", "logo_key": "kings"},
    "dallas": {"abbrev": "DAL", "logo_key": "mavericks"},
    "houston": {"abbrev": "HOU", "logo_key": "rockets"},
    "memphis": {"abbrev": "MEM", "logo_key": "grizzlies"},
    "new_orleans": {"abbrev": "NOP", "logo_key": "pelicans"},
    "san_antonio": {"abbrev": "SAS", "logo_key": "spurs"},
}

# =============================================================================
# KENPOM SLUG MAPPINGS (for API calls)
# =============================================================================

KENPOM_SLUGS: Dict[str, str] = {
    # Default fallback is canonical_key.replace("_", ".")
    # Only list teams that deviate from that pattern

    # --- State schools (KenPom uses .st suffix) ---
    "nc_state": "north.carolina.st",
    "michigan_state": "michigan.st",
    "florida_state": "florida.st",
    "ohio_state": "ohio.st",
    "iowa_state": "iowa.st",
    "oklahoma_state": "oklahoma.st",
    "kansas_state": "kansas.st",
    "mississippi_state": "mississippi.st",
    "penn_state": "penn.st",
    "oregon_state": "oregon.st",
    "washington_state": "washington.st",
    "boise_state": "boise.st",
    "san_diego_state": "san.diego.st",
    "fresno_state": "fresno.st",
    "colorado_state": "colorado.st",
    "utah_state": "utah.st",
    "new_mexico_state": "new.mexico.st",
    "arizona_state": "arizona.st",
    "texas_tech": "texas.tech",
    "wake_forest": "wake.forest",
    "alabama_state": "alabama.st",
    "alcorn_state": "alcorn.st",
    "appalachian_state": "appalachian.st",
    "arkansas_state": "arkansas.st",
    "arkansas_pine_bluff": "arkansas.pine.bluff",
    "ball_state": "ball.st",
    "coppin_state": "coppin.st",
    "delaware_state": "delaware.st",
    "georgia_state": "georgia.st",
    "idaho_state": "idaho.st",
    "illinois_state": "illinois.st",
    "indiana_state": "indiana.st",
    "jackson_state": "jackson.st",
    "jacksonville_state": "jacksonville.st",
    "kennesaw_state": "kennesaw.st",
    "kent_state": "kent.st",
    "long_beach_state": "long.beach.st",
    "missouri_state": "missouri.st",
    "montana_state": "montana.st",
    "morehead_state": "morehead.st",
    "morgan_state": "morgan.st",
    "murray_state": "murray.st",
    "norfolk_state": "norfolk.st",
    "north_dakota_state": "north.dakota.st",
    "northwestern_state": "northwestern.st",
    "portland_state": "portland.st",
    "sacramento_state": "sacramento.st",
    "south_carolina_state": "south.carolina.st",
    "south_dakota_state": "south.dakota.st",
    "southeast_missouri_state": "southeast.missouri.st",
    "tarleton_state": "tarleton.st",
    "tennessee_state": "tennessee.st",
    "tennessee_tech": "tennessee.tech",
    "texas_state": "texas.st",
    "weber_state": "weber.st",
    "wichita_state": "wichita.st",
    "wright_state": "wright.st",
    "youngstown_state": "youngstown.st",

    # --- Saint/St. schools (KenPom uses st. with apostrophes) ---
    "st_johns": "st.john's",
    "saint_marys": "st.mary's",
    "saint_josephs": "st.joseph's",
    "saint_louis": "saint.louis",
    "saint_peters": "st.peter's",
    "st_bonaventure": "st.bonaventure",
    "st_thomas": "st.thomas.mn",
    "mount_st_marys": "mount.st.mary's",
    "saint_francis": "st.francis.pa",

    # --- A&M / special characters ---
    "texas_am": "texas.a.m",
    "texas_am_cc": "texas.a.m.corpus.christi",
    "texas_am_commerce": "texas.a.m.commerce",
    "alabama_am": "alabama.a.m",
    "florida_am": "florida.a.m",
    "north_carolina_at": "north.carolina.a.t",
    "prairie_view": "prairie.view.a.m",

    # --- UConn / UCF / UCLA etc (abbreviation schools) ---
    "connecticut": "connecticut",
    "ucf": "central.florida",
    "uab": "uab",
    "ucla": "ucla",
    "usc": "usc",
    "umbc": "umbc",
    "umkc": "umkc",
    "unlv": "unlv",
    "utep": "utep",
    "utsa": "utsa",
    "utrgv": "ut.rio.grande.valley",
    "uic": "illinois.chicago",
    "fiu": "fiu",
    "smu": "smu",
    "tcu": "tcu",
    "lsu": "lsu",
    "byu": "byu",
    "vcu": "vcu",
    "njit": "njit",
    "vmi": "vmi",

    # --- Hyphenated / multi-word special cases ---
    "bethune_cookman": "bethune.cookman",
    "boston_college": "boston.college",
    "boston_university": "boston.u.",
    "cal_baptist": "cal.baptist",
    "cal_poly": "cal.poly",
    "central_arkansas": "central.arkansas",
    "central_connecticut": "central.connecticut",
    "central_michigan": "central.michigan",
    "charleston_southern": "charleston.southern",
    "cleveland_state": "cleveland.st",
    "coastal_carolina": "coastal.carolina",
    "cs_bakersfield": "cal.st..bakersfield",
    "cs_fullerton": "cal.st..fullerton",
    "cs_northridge": "cal.st..northridge",
    "detroit_mercy": "detroit.mercy",
    "east_carolina": "east.carolina",
    "east_tennessee_state": "east.tennessee.st",
    "eastern_illinois": "eastern.illinois",
    "eastern_kentucky": "eastern.kentucky",
    "eastern_michigan": "eastern.michigan",
    "eastern_washington": "eastern.washington",
    "fairleigh_dickinson": "fairleigh.dickinson",
    "florida_atlantic": "florida.atlantic",
    "florida_gulf_coast": "florida.gulf.coast",
    "gardner_webb": "gardner.webb",
    "george_mason": "george.mason",
    "george_washington": "george.washington",
    "georgia_southern": "georgia.southern",
    "georgia_tech": "georgia.tech",
    "grand_canyon": "grand.canyon",
    "green_bay": "green.bay",
    "high_point": "high.point",
    "holy_cross": "holy.cross",
    "houston_christian": "houston.christian",
    "incarnate_word": "incarnate.word",
    "iu_indianapolis": "iu.indianapolis",
    "james_madison": "james.madison",
    "la_salle": "la.salle",
    "le_moyne": "le.moyne",
    "little_rock": "little.rock",
    "long_island": "liu",
    "louisiana_monroe": "louisiana.monroe",
    "louisiana_tech": "louisiana.tech",
    "loyola_chicago": "loyola.chicago",
    "loyola_maryland": "loyola.md",
    "loyola_marymount": "loyola.marymount",
    "maryland_eastern_shore": "maryland.eastern.shore",
    "miami_fl": "miami.fl",
    "miami_oh": "miami.oh",
    "middle_tennessee": "middle.tennessee",
    "mississippi_valley_state": "mississippi.valley.st",
    "new_hampshire": "new.hampshire",
    "new_mexico": "new.mexico",
    "new_orleans": "new.orleans",
    "north_alabama": "north.alabama",
    "north_carolina": "north.carolina",
    "north_carolina_central": "north.carolina.central",
    "north_dakota": "north.dakota",
    "north_florida": "north.florida",
    "north_texas": "north.texas",
    "northern_arizona": "northern.arizona",
    "northern_colorado": "northern.colorado",
    "northern_illinois": "northern.illinois",
    "northern_iowa": "northern.iowa",
    "northern_kentucky": "northern.kentucky",
    "notre_dame": "notre.dame",
    "old_dominion": "old.dominion",
    "oral_roberts": "oral.roberts",
    "purdue_fort_wayne": "purdue.fort.wayne",
    "rhode_island": "rhode.island",
    "robert_morris": "robert.morris",
    "sacred_heart": "sacred.heart",
    "sam_houston": "sam.houston.st",
    "san_diego": "san.diego",
    "san_diego_state": "san.diego.st",
    "san_francisco": "san.francisco",
    "san_jose_state": "san.jose.st",
    "santa_clara": "santa.clara",
    "seton_hall": "seton.hall",
    "siu_edwardsville": "siu.edwardsville",
    "south_alabama": "south.alabama",
    "south_carolina": "south.carolina",
    "south_carolina_upstate": "south.carolina.upstate",
    "south_dakota": "south.dakota",
    "south_florida": "south.florida",
    "southeastern_louisiana": "southeastern.louisiana",
    "southern_illinois": "southern.illinois",
    "southern_indiana": "southern.indiana",
    "southern_miss": "southern.miss",
    "southern_utah": "southern.utah",
    "stephen_f_austin": "stephen.f..austin",
    "stony_brook": "stony.brook",
    "tennessee_martin": "tennessee.martin",
    "texas_southern": "texas.southern",
    "uc_davis": "uc.davis",
    "uc_irvine": "uc.irvine",
    "uc_riverside": "uc.riverside",
    "uc_san_diego": "uc.san.diego",
    "uc_santa_barbara": "uc.santa.barbara",
    "unc_asheville": "unc.asheville",
    "unc_greensboro": "unc.greensboro",
    "unc_wilmington": "unc.wilmington",
    "ut_arlington": "ut.arlington",
    "utah_tech": "utah.tech",
    "utah_valley": "utah.valley",
    "virginia_tech": "virginia.tech",
    "wake_forest": "wake.forest",
    "west_georgia": "west.georgia",
    "west_virginia": "west.virginia",
    "western_carolina": "western.carolina",
    "western_illinois": "western.illinois",
    "western_kentucky": "western.kentucky",
    "western_michigan": "western.michigan",
    "william_mary": "william.and.mary",
    "bowling_green": "bowling.green",
}

# =============================================================================
# VSIN NAME VARIATIONS (how VSIN displays teams)
# =============================================================================

VSIN_NAME_VARIATIONS: Dict[str, str] = {
    "north carolina": "north_carolina",
    "nc state": "nc_state",
    "michigan st": "michigan_state",
    "michigan state": "michigan_state",
    "ohio st": "ohio_state",
    "ohio state": "ohio_state",
    "florida st": "florida_state",
    "penn st": "penn_state",
    "iowa st": "iowa_state",
    "kansas st": "kansas_state",
    "oklahoma st": "oklahoma_state",
    "miss st": "mississippi_state",
    "mississippi st": "mississippi_state",
    "oregon st": "oregon_state",
    "washington st": "washington_state",
    "boise st": "boise_state",
    "san diego st": "san_diego_state",
    "fresno st": "fresno_state",
    "colorado st": "colorado_state",
    "utah st": "utah_state",
    "new mexico st": "new_mexico_state",
    "arizona st": "arizona_state",
    "texas a&m": "texas_am",
    "texas am": "texas_am",
    "st johns": "st_johns",
    "st. johns": "st_johns",
    "st john's": "st_johns",
    "st. john's": "st_johns",
    "st marys": "saint_marys",
    "st. marys": "saint_marys",
    "st mary's": "saint_marys",
    "st. mary's": "saint_marys",
    "st josephs": "saint_josephs",
    "st. josephs": "saint_josephs",
    "st joseph's": "saint_josephs",
    "st. joseph's": "saint_josephs",
    "usc": "usc",
    "ucla": "ucla",
    "unc": "north_carolina",
    "uconn": "connecticut",
    "ole miss": "mississippi",
}

# =============================================================================
# CORE NORMALIZATION FUNCTIONS
# =============================================================================

def strip_accents(text: str) -> str:
    """Remove diacritics (San José → San Jose)"""
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def clean_team_name(name: str) -> str:
    """Basic cleaning: lowercase, strip, remove punctuation"""
    if not name:
        return ""
    name = strip_accents(name.lower().strip())
    name = re.sub(r"[''`]", "", name)
    name = re.sub(r"[^\w\s&]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

@lru_cache(maxsize=10000)
def normalize_team_name(name: str, league: str = "CBB") -> Optional[str]:
    """
    Normalize any team name to its canonical key.
    Cached for batch performance (10k+ teams).
    
    Args:
        name: Raw team name from any source
        league: "CBB" or "NBA"
    
    Returns:
        Canonical key or None if not found
    """
    if not name:
        return None
    
    cleaned = clean_team_name(name)
    
    aliases = CBB_CANONICAL_ALIASES if league.upper() == "CBB" else NBA_CANONICAL_ALIASES
    
    if cleaned in aliases:
        return aliases[cleaned]
    
    if league.upper() == "CBB":
        if cleaned in VSIN_NAME_VARIATIONS:
            return VSIN_NAME_VARIATIONS[cleaned]
    
    for alias, canonical in aliases.items():
        if cleaned == alias or cleaned.replace(" ", "_") == canonical:
            return canonical
    
    if cleaned.replace(" ", "_") in aliases.values():
        return cleaned.replace(" ", "_")
    
    return None

def get_covers_display(canonical_key: str, league: str = "CBB") -> Optional[Dict]:
    """Get Covers.com display info (abbreviation + logo key)"""
    display_map = CBB_COVERS_DISPLAY if league.upper() == "CBB" else NBA_COVERS_DISPLAY
    return display_map.get(canonical_key)

def get_kenpom_slug(canonical_key: str) -> Optional[str]:
    """Get KenPom API slug for a canonical key"""
    if canonical_key in KENPOM_SLUGS:
        return KENPOM_SLUGS[canonical_key]
    return canonical_key.replace("_", ".")

# =============================================================================
# BATCH RESOLUTION SYSTEM
# =============================================================================

def batch_resolve_teams(
    rows: List[Dict],
    league: str
) -> Dict:
    """
    Batch resolve team identities across KenPom, VSIN, and Covers.com.
    Optimized for 10k+ teams with caching.
    
    Args:
        rows: List of dicts with keys:
            - kenpom_name: Team name from KenPom
            - vsin_name: Team name from VSIN
            - covers_name: Team name from Covers.com
            - covers_abbrev: (optional) Pre-fetched abbreviation
            - covers_logo_key: (optional) Pre-fetched logo key
        league: "CBB" or "NBA"
    
    Returns:
        {
            "verified": List of verified team records,
            "failed": List of failed team records,
            "stats": Resolution statistics
        }
    """
    
    normalization_cache: Dict[str, Optional[str]] = {}

    def cached_normalize(name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        if name not in normalization_cache:
            result = normalize_team_name(name, league)
            normalization_cache[name] = result
        return normalization_cache[name]

    verified = []
    failed = []

    for row in rows:
        normalized_keys: Set[str] = set()
        source_results = {}

        for source in ("kenpom_name", "vsin_name", "covers_name"):
            raw_name = row.get(source)
            if raw_name:
                key = cached_normalize(raw_name)
                if key:
                    normalized_keys.add(key)
                    source_results[source] = key
                else:
                    source_results[source] = f"UNMATCHED: {raw_name}"

        if len(normalized_keys) == 0:
            failed.append({
                **row,
                "canonical_key": None,
                "league": league,
                "resolution_status": "FAILED",
                "failure_reason": f"No sources matched: {source_results}",
                "source_results": source_results
            })
            continue

        if len(normalized_keys) > 1:
            failed.append({
                **row,
                "canonical_key": None,
                "league": league,
                "resolution_status": "FAILED",
                "failure_reason": f"Key mismatch across sources: {normalized_keys}",
                "source_results": source_results
            })
            continue

        canonical_key = normalized_keys.pop()
        
        covers_info = get_covers_display(canonical_key, league)
        covers_abbrev = row.get("covers_abbrev") or (covers_info.get("abbrev") if covers_info else None)
        covers_logo_key = row.get("covers_logo_key") or (covers_info.get("logo_key") if covers_info else None)

        if not covers_abbrev:
            covers_abbrev = canonical_key.upper().replace("_", "")[:4]
        
        if not covers_logo_key:
            covers_logo_key = canonical_key.replace("_", "-")

        kenpom_slug = get_kenpom_slug(canonical_key)

        verified.append({
            **row,
            "canonical_key": canonical_key,
            "league": league,
            "resolution_status": "VERIFIED",
            "failure_reason": "",
            "covers_abbrev": covers_abbrev,
            "covers_logo_key": covers_logo_key,
            "kenpom_slug": kenpom_slug,
            "source_results": source_results
        })

    return {
        "verified": verified,
        "failed": failed,
        "stats": {
            "total_rows": len(rows),
            "verified": len(verified),
            "failed": len(failed),
            "success_rate": f"{(len(verified)/len(rows)*100):.1f}%" if rows else "0%",
            "unique_names_normalized": len(normalization_cache),
            "cache_efficiency": f"{len(normalization_cache)} unique names cached"
        }
    }

# =============================================================================
# SINGLE TEAM RESOLUTION (for real-time lookups)
# =============================================================================

def resolve_team(
    name: str,
    source: str,
    league: str = "CBB"
) -> Optional[Dict]:
    """
    Resolve a single team name from a specific source.
    
    Args:
        name: Team name
        source: "kenpom", "vsin", or "covers"
        league: "CBB" or "NBA"
    
    Returns:
        Resolved team info or None
    """
    canonical = normalize_team_name(name, league)
    if not canonical:
        return None
    
    covers_info = get_covers_display(canonical, league)
    
    return {
        "canonical_key": canonical,
        "source": source,
        "original_name": name,
        "covers_abbrev": covers_info.get("abbrev") if covers_info else canonical.upper()[:4],
        "covers_logo_key": covers_info.get("logo_key") if covers_info else canonical.replace("_", "-"),
        "kenpom_slug": get_kenpom_slug(canonical) if league == "CBB" else None,
        "league": league
    }

# =============================================================================
# DIAGNOSTIC TOOLS
# =============================================================================

def diagnose_team_match(name1: str, name2: str, league: str = "CBB") -> Dict:
    """Check if two names resolve to the same team"""
    key1 = normalize_team_name(name1, league)
    key2 = normalize_team_name(name2, league)
    
    return {
        "name1": name1,
        "name2": name2,
        "key1": key1,
        "key2": key2,
        "match": key1 == key2 and key1 is not None,
        "both_resolved": key1 is not None and key2 is not None
    }

def find_unmatched_names(names: List[str], league: str = "CBB") -> List[str]:
    """Find names that don't resolve to any canonical key"""
    unmatched = []
    for name in names:
        if normalize_team_name(name, league) is None:
            unmatched.append(name)
    return unmatched
