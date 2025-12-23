import os
import requests
from datetime import datetime

DISCORD_WEBHOOK = os.environ.get("Cryptodiscord", "")

THRESHOLDS = {
    "NBA": 8.0,
    "CBB": 8.0,
    "NFL": 3.5,
    "NHL": 0.5
}

def send_discord(msg):
    if not DISCORD_WEBHOOK:
        print("No Discord webhook configured")
        return
    discord_msg = msg.replace("<b>", "**").replace("</b>", "**")
    discord_msg = discord_msg.replace("<u>", "__").replace("</u>", "__")
    payload = {"content": discord_msg}
    requests.post(DISCORD_WEBHOOK, json=payload)

def fetch_espn_nba_stats():
    """Fetch NBA team stats from ESPN API"""
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
        resp = requests.get(url)
        data = resp.json()
        
        for team_data in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            team_abbr = team.get("abbreviation", "")
            
            stats_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/statistics"
            stats_resp = requests.get(stats_url)
            stats_data = stats_resp.json()
            
            ppg = None
            opp_ppg = None
            
            for stat_cat in stats_data.get("results", {}).get("stats", {}).get("categories", []):
                for stat in stat_cat.get("stats", []):
                    if stat.get("name") == "avgPoints":
                        ppg = float(stat.get("value", 0))
                    if stat.get("name") == "avgPointsAgainst":
                        opp_ppg = float(stat.get("value", 0))
            
            if ppg and opp_ppg:
                teams[team_abbr] = {
                    "name": team_name,
                    "ppg": ppg,
                    "opp_ppg": opp_ppg
                }
                teams[team_name] = teams[team_abbr]
    except Exception as e:
        print(f"Error fetching NBA stats: {e}")
    
    return teams

def fetch_espn_nfl_stats():
    """Fetch NFL team stats from ESPN API"""
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
        resp = requests.get(url)
        data = resp.json()
        
        for team_data in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            team_abbr = team.get("abbreviation", "")
            
            stats_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/statistics"
            stats_resp = requests.get(stats_url)
            stats_data = stats_resp.json()
            
            ppg = None
            opp_ppg = None
            
            for stat_cat in stats_data.get("results", {}).get("stats", {}).get("categories", []):
                for stat in stat_cat.get("stats", []):
                    if stat.get("name") == "avgPoints":
                        ppg = float(stat.get("value", 0))
                    if stat.get("name") == "avgPointsAgainst":
                        opp_ppg = float(stat.get("value", 0))
            
            if ppg and opp_ppg:
                teams[team_abbr] = {
                    "name": team_name,
                    "ppg": ppg,
                    "opp_ppg": opp_ppg
                }
                teams[team_name] = teams[team_abbr]
    except Exception as e:
        print(f"Error fetching NFL stats: {e}")
    
    return teams

def fetch_espn_nhl_stats():
    """Fetch NHL team stats from ESPN API"""
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams"
        resp = requests.get(url)
        data = resp.json()
        
        for team_data in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            team_abbr = team.get("abbreviation", "")
            
            stats_url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/statistics"
            stats_resp = requests.get(stats_url)
            stats_data = stats_resp.json()
            
            ppg = None
            opp_ppg = None
            
            for stat_cat in stats_data.get("results", {}).get("stats", {}).get("categories", []):
                for stat in stat_cat.get("stats", []):
                    if stat.get("name") == "avgGoals":
                        ppg = float(stat.get("value", 0))
                    if stat.get("name") == "avgGoalsAgainst":
                        opp_ppg = float(stat.get("value", 0))
            
            if ppg and opp_ppg:
                teams[team_abbr] = {
                    "name": team_name,
                    "ppg": ppg,
                    "opp_ppg": opp_ppg
                }
                teams[team_name] = teams[team_abbr]
    except Exception as e:
        print(f"Error fetching NHL stats: {e}")
    
    return teams

def fetch_espn_cbb_stats():
    """Fetch College Basketball team stats from ESPN API"""
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=500"
        resp = requests.get(url)
        data = resp.json()
        
        for team_data in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            team_abbr = team.get("abbreviation", "")
            
            stats_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/statistics"
            stats_resp = requests.get(stats_url)
            stats_data = stats_resp.json()
            
            ppg = None
            opp_ppg = None
            
            for stat_cat in stats_data.get("results", {}).get("stats", {}).get("categories", []):
                for stat in stat_cat.get("stats", []):
                    if stat.get("name") == "avgPoints":
                        ppg = float(stat.get("value", 0))
                    if stat.get("name") == "avgPointsAgainst":
                        opp_ppg = float(stat.get("value", 0))
            
            if ppg and opp_ppg:
                teams[team_abbr] = {
                    "name": team_name,
                    "ppg": ppg,
                    "opp_ppg": opp_ppg
                }
                teams[team_name] = teams[team_abbr]
    except Exception as e:
        print(f"Error fetching CBB stats: {e}")
    
    return teams

def calculate_bet(team_a_stats, team_b_stats, bovada_line, league):
    """Calculate bet using exact formula specified"""
    
    if not team_a_stats or not team_b_stats:
        return None, "Insufficient data — no play."
    
    if league not in THRESHOLDS:
        return None, f"Unknown league: {league}"
    
    threshold = THRESHOLDS[league]
    
    expected_a = (team_a_stats["ppg"] + team_b_stats["opp_ppg"]) / 2
    expected_b = (team_b_stats["ppg"] + team_a_stats["opp_ppg"]) / 2
    projected_total = expected_a + expected_b
    difference = projected_total - bovada_line
    
    decision = "NO BET"
    reason = "EDGE TOO SMALL"
    
    if projected_total >= bovada_line + threshold:
        decision = "OVER"
        reason = f"Threshold met: {abs(difference):.1f} >= {threshold}"
    elif bovada_line >= projected_total + threshold:
        decision = "UNDER"
        reason = f"Threshold met: {abs(difference):.1f} >= {threshold}"
    else:
        reason = f"Edge {abs(difference):.1f} < threshold {threshold}"
    
    return {
        "expected_a": expected_a,
        "expected_b": expected_b,
        "projected_total": projected_total,
        "difference": difference,
        "decision": decision,
        "reason": reason,
        "threshold": threshold
    }, None

def format_output(team_a, team_b, team_a_stats, team_b_stats, bovada_line, league, result):
    """Format output according to specification"""
    
    msg = f"**Game:** {team_a} vs {team_b}\n"
    msg += f"**League:** {league}\n\n"
    msg += f"• {team_a} PPG: {team_a_stats['ppg']:.1f}\n"
    msg += f"• {team_a} Opp PPG: {team_a_stats['opp_ppg']:.1f}\n"
    msg += f"• {team_b} PPG: {team_b_stats['ppg']:.1f}\n"
    msg += f"• {team_b} Opp PPG: {team_b_stats['opp_ppg']:.1f}\n\n"
    msg += f"**Expected {team_a}:** {result['expected_a']:.1f}\n"
    msg += f"**Expected {team_b}:** {result['expected_b']:.1f}\n"
    msg += f"**Projected Total:** {result['projected_total']:.1f}\n"
    msg += f"**Bovada Line:** {bovada_line}\n"
    msg += f"**Difference:** {result['difference']:.1f}\n\n"
    
    if result['decision'] == "OVER":
        msg += f"🟢 **Decision: OVER**\n"
    elif result['decision'] == "UNDER":
        msg += f"🔴 **Decision: UNDER**\n"
    else:
        msg += f"⚪ **Decision: NO BET**\n"
    
    msg += f"**Reason:** {result['reason']}"
    
    return msg

def analyze_game(team_a, team_b, bovada_line, league):
    """Main function to analyze a single game"""
    
    if league == "NBA":
        teams = fetch_espn_nba_stats()
    elif league == "NFL":
        teams = fetch_espn_nfl_stats()
    elif league == "NHL":
        teams = fetch_espn_nhl_stats()
    elif league == "CBB":
        teams = fetch_espn_cbb_stats()
    else:
        return f"Unknown league: {league}"
    
    team_a_stats = teams.get(team_a)
    team_b_stats = teams.get(team_b)
    
    if not team_a_stats:
        return f"Insufficient data — no play. (Could not find stats for {team_a})"
    if not team_b_stats:
        return f"Insufficient data — no play. (Could not find stats for {team_b})"
    
    result, error = calculate_bet(team_a_stats, team_b_stats, bovada_line, league)
    
    if error:
        return error
    
    output = format_output(team_a, team_b, team_a_stats, team_b_stats, bovada_line, league, result)
    
    print(output)
    send_discord(output)
    
    return output

def scan_todays_games():
    """Fetch and analyze today's games from ESPN"""
    
    msg = f"🏀 **Sports Betting Scanner** — {datetime.now().strftime('%b %d, %Y')}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    games_found = False
    
    for league, sport_path in [("NBA", "basketball/nba"), ("NFL", "football/nfl"), ("NHL", "hockey/nhl")]:
        try:
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/scoreboard"
            resp = requests.get(url)
            data = resp.json()
            
            events = data.get("events", [])
            
            if events:
                msg += f"**{league} Games Today:**\n"
                for event in events:
                    name = event.get("name", "")
                    status = event.get("status", {}).get("type", {}).get("description", "")
                    msg += f"• {name} ({status})\n"
                msg += "\n"
                games_found = True
                
        except Exception as e:
            print(f"Error fetching {league} games: {e}")
    
    if not games_found:
        msg += "No games found for today.\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "To analyze a game, provide:\n"
    msg += "• Team A name\n"
    msg += "• Team B name\n"
    msg += "• Bovada O/U line\n"
    msg += "• League (NBA/NFL/NHL/CBB)\n"
    
    print(msg)
    send_discord(msg)
    
    return msg

if __name__ == "__main__":
    scan_todays_games()
