import os
import requests
from datetime import datetime
import time

DISCORD_WEBHOOK = os.environ.get("Cryptodiscord", "")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

THRESHOLDS = {
    "NBA": 8.0,
    "CBB": 8.0,
    "NFL": 3.5,
    "NHL": 0.5
}

SPORT_KEYS = {
    "NBA": "basketball_nba",
    "NFL": "americanfootball_nfl",
    "NHL": "icehockey_nhl",
    "CBB": "basketball_ncaab"
}

def send_discord(msg):
    if not DISCORD_WEBHOOK:
        return
    payload = {"content": msg}
    requests.post(DISCORD_WEBHOOK, json=payload)

def fetch_espn_team_stats(league):
    teams = {}
    
    if league == "NBA":
        sport_path = "basketball/nba"
        ppg_stat = "avgPoints"
        opp_stat = "avgPointsAgainst"
    elif league == "NFL":
        sport_path = "football/nfl"
        ppg_stat = "avgPoints"
        opp_stat = "avgPointsAgainst"
    elif league == "NHL":
        sport_path = "hockey/nhl"
        ppg_stat = "avgGoals"
        opp_stat = "avgGoalsAgainst"
    elif league == "CBB":
        sport_path = "basketball/mens-college-basketball"
        ppg_stat = "avgPoints"
        opp_stat = "avgPointsAgainst"
    else:
        return teams
    
    try:
        if league == "CBB":
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/teams?limit=500"
        else:
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/teams"
        
        resp = requests.get(url, timeout=30)
        data = resp.json()
        
        team_list = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        
        for team_data in team_list:
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            team_abbr = team.get("abbreviation", "")
            
            try:
                stats_url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/teams/{team_id}/statistics"
                stats_resp = requests.get(stats_url, timeout=10)
                stats_data = stats_resp.json()
                
                ppg = None
                opp_ppg = None
                
                for stat_cat in stats_data.get("results", {}).get("stats", {}).get("categories", []):
                    for stat in stat_cat.get("stats", []):
                        if stat.get("name") == ppg_stat:
                            ppg = float(stat.get("value", 0))
                        if stat.get("name") == opp_stat:
                            opp_ppg = float(stat.get("value", 0))
                
                if ppg and opp_ppg:
                    teams[team_name.lower()] = {
                        "name": team_name,
                        "abbr": team_abbr,
                        "ppg": ppg,
                        "opp_ppg": opp_ppg
                    }
                    teams[team_abbr.lower()] = teams[team_name.lower()]
                    
                    short_name = team_name.split()[-1].lower()
                    teams[short_name] = teams[team_name.lower()]
                    
            except Exception:
                continue
                
    except Exception as e:
        print(f"Error fetching {league} stats: {e}")
    
    return teams

def fetch_odds(league):
    if not ODDS_API_KEY:
        return []
    
    sport_key = SPORT_KEYS.get(league)
    if not sport_key:
        return []
    
    try:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/"
        params = {
            "apiKey": ODDS_API_KEY,
            "regions": "us",
            "markets": "totals",
            "oddsFormat": "american"
        }
        
        resp = requests.get(url, params=params, timeout=30)
        
        if resp.status_code != 200:
            return []
        
        data = resp.json()
        
        games = []
        for event in data:
            home_team = event.get("home_team", "")
            away_team = event.get("away_team", "")
            commence_time = event.get("commence_time", "")
            
            bovada_line = None
            for bookmaker in event.get("bookmakers", []):
                if bookmaker.get("key") == "bovada":
                    for market in bookmaker.get("markets", []):
                        if market.get("key") == "totals":
                            for outcome in market.get("outcomes", []):
                                if outcome.get("name") == "Over":
                                    bovada_line = outcome.get("point")
                                    break
                    break
            
            if not bovada_line:
                for bookmaker in event.get("bookmakers", []):
                    for market in bookmaker.get("markets", []):
                        if market.get("key") == "totals":
                            for outcome in market.get("outcomes", []):
                                if outcome.get("name") == "Over":
                                    bovada_line = outcome.get("point")
                                    break
                            if bovada_line:
                                break
                    if bovada_line:
                        break
            
            if bovada_line:
                games.append({
                    "home_team": home_team,
                    "away_team": away_team,
                    "bovada_line": bovada_line,
                    "commence_time": commence_time
                })
        
        return games
        
    except Exception as e:
        print(f"Error fetching odds for {league}: {e}")
        return []

def find_team_stats(team_name, stats_dict):
    normalized = team_name.lower().strip()
    
    if normalized in stats_dict:
        return stats_dict[normalized]
    
    for key, val in stats_dict.items():
        if normalized in key or key in normalized:
            return val
    
    words = normalized.split()
    for word in words:
        if len(word) > 3 and word in stats_dict:
            return stats_dict[word]
    
    return None

def calculate_bet(team_a_stats, team_b_stats, bovada_line, league):
    if not team_a_stats or not team_b_stats:
        return None
    
    threshold = THRESHOLDS[league]
    
    expected_a = (team_a_stats["ppg"] + team_b_stats["opp_ppg"]) / 2
    expected_b = (team_b_stats["ppg"] + team_a_stats["opp_ppg"]) / 2
    projected_total = expected_a + expected_b
    difference = projected_total - bovada_line
    
    if projected_total >= bovada_line + threshold:
        return {
            "expected_a": expected_a,
            "expected_b": expected_b,
            "projected_total": projected_total,
            "difference": difference,
            "decision": "OVER",
            "threshold": threshold
        }
    elif bovada_line >= projected_total + threshold:
        return {
            "expected_a": expected_a,
            "expected_b": expected_b,
            "projected_total": projected_total,
            "difference": difference,
            "decision": "UNDER",
            "threshold": threshold
        }
    
    return None

def format_output(team_a, team_b, team_a_stats, team_b_stats, bovada_line, league, result):
    msg = f"Game: {team_a} vs {team_b}\n"
    msg += f"League: {league}\n"
    msg += f"• {team_a} PPG: {team_a_stats['ppg']:.1f}\n"
    msg += f"• {team_a} Opp PPG: {team_a_stats['opp_ppg']:.1f}\n"
    msg += f"• {team_b} PPG: {team_b_stats['ppg']:.1f}\n"
    msg += f"• {team_b} Opp PPG: {team_b_stats['opp_ppg']:.1f}\n\n"
    msg += f"Expected {team_a}: {result['expected_a']:.1f}\n"
    msg += f"Expected {team_b}: {result['expected_b']:.1f}\n"
    msg += f"Projected Total: {result['projected_total']:.1f}\n"
    msg += f"Bovada Line: {bovada_line}\n"
    msg += f"Difference: {result['difference']:.1f}\n\n"
    msg += f"Decision: {result['decision']}\n"
    msg += f"Reason: Threshold met ({abs(result['difference']):.1f} >= {result['threshold']})"
    
    return msg

def scan_league(league, team_stats):
    games = fetch_odds(league)
    
    if not games:
        return []
    
    qualified_bets = []
    
    for game in games:
        home_team = game["home_team"]
        away_team = game["away_team"]
        bovada_line = game["bovada_line"]
        
        home_stats = find_team_stats(home_team, team_stats)
        away_stats = find_team_stats(away_team, team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        result = calculate_bet(away_stats, home_stats, bovada_line, league)
        
        if result:
            output = format_output(away_team, home_team, away_stats, home_stats, bovada_line, league, result)
            qualified_bets.append(output)
    
    return qualified_bets

def scan_all_leagues():
    if not ODDS_API_KEY:
        msg = "Insufficient data — no play.\nMissing ODDS_API_KEY."
        print(msg)
        send_discord(msg)
        return
    
    all_bets = []
    
    for league in ["NBA", "NFL", "NHL", "CBB"]:
        print(f"Scanning {league}...")
        
        team_stats = fetch_espn_team_stats(league)
        
        if not team_stats:
            continue
        
        bets = scan_league(league, team_stats)
        
        if bets:
            all_bets.extend(bets)
        
        time.sleep(1)
    
    if all_bets:
        full_msg = "\n\n---\n\n".join(all_bets)
        print(full_msg)
        send_discord(full_msg)
    else:
        print("No qualified bets found.")

if __name__ == "__main__":
    scan_all_leagues()
