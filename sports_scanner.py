import os
import requests
from datetime import datetime
import time

DISCORD_WEBHOOK = os.environ.get("Cryptodiscord", "")

THRESHOLDS = {
    "NBA": 8.0,
    "CBB": 8.0,
    "NFL": 3.5,
    "NHL": 0.5
}

def send_discord(msg):
    if not DISCORD_WEBHOOK:
        return
    payload = {"content": msg}
    requests.post(DISCORD_WEBHOOK, json=payload)

def fetch_nba_stats():
    from nba_api.stats.endpoints import leaguedashteamstats
    
    teams = {}
    try:
        offense = leaguedashteamstats.LeagueDashTeamStats(
            season='2024-25',
            season_type_all_star='Regular Season',
            measure_type_detailed_defense='Base',
            per_mode_detailed='PerGame'
        )
        off_df = offense.get_data_frames()[0]
        
        defense = leaguedashteamstats.LeagueDashTeamStats(
            season='2024-25',
            season_type_all_star='Regular Season',
            measure_type_detailed_defense='Opponent',
            per_mode_detailed='PerGame'
        )
        def_df = defense.get_data_frames()[0]
        
        opp_dict = {}
        for _, row in def_df.iterrows():
            opp_dict[row['TEAM_ID']] = row['OPP_PTS']
        
        for _, row in off_df.iterrows():
            team_name = row['TEAM_NAME']
            team_id = row['TEAM_ID']
            ppg = row['PTS']
            opp_ppg = opp_dict.get(team_id)
            
            if ppg and opp_ppg:
                teams[team_name.lower()] = {
                    "name": team_name,
                    "ppg": ppg,
                    "opp_ppg": opp_ppg
                }
                teams[str(team_id)] = teams[team_name.lower()]
        
        print(f"Loaded {len(teams)//2} NBA teams with stats")
        
    except Exception as e:
        print(f"Error fetching NBA stats: {e}")
    
    return teams

def fetch_nfl_stats():
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
        resp = requests.get(url, timeout=30)
        data = resp.json()
        
        team_list = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        
        for team_data in team_list:
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            
            try:
                record_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}"
                record_resp = requests.get(record_url, timeout=10)
                record_data = record_resp.json()
                
                team_record = record_data.get("team", {}).get("record", {})
                items = team_record.get("items", [])
                
                ppg = None
                opp_ppg = None
                
                for item in items:
                    for stat in item.get("stats", []):
                        if stat.get("name") == "pointsFor":
                            games = next((s.get("value") for s in item.get("stats", []) if s.get("name") == "gamesPlayed"), 1)
                            ppg = stat.get("value", 0) / max(games, 1)
                        if stat.get("name") == "pointsAgainst":
                            games = next((s.get("value") for s in item.get("stats", []) if s.get("name") == "gamesPlayed"), 1)
                            opp_ppg = stat.get("value", 0) / max(games, 1)
                
                if ppg and opp_ppg:
                    teams[team_name.lower()] = {
                        "name": team_name,
                        "ppg": ppg,
                        "opp_ppg": opp_ppg
                    }
                    teams[str(team_id)] = teams[team_name.lower()]
                    
            except Exception:
                continue
                
    except Exception as e:
        print(f"Error fetching NFL stats: {e}")
    
    print(f"Loaded {len(teams)//2} NFL teams with stats")
    return teams

def fetch_nhl_stats():
    teams = {}
    try:
        url = "https://api.nhle.com/stats/rest/en/team/summary?cayenneExp=seasonId=20242025"
        resp = requests.get(url, timeout=30)
        data = resp.json()
        
        for team in data.get("data", []):
            team_name = team.get("teamFullName", "")
            games = team.get("gamesPlayed", 0)
            
            if games > 0:
                ppg = team.get("goalsFor", 0) / games
                opp_ppg = team.get("goalsAgainst", 0) / games
                
                if ppg and opp_ppg:
                    teams[team_name.lower()] = {
                        "name": team_name,
                        "ppg": ppg,
                        "opp_ppg": opp_ppg
                    }
        
        print(f"Loaded {len(teams)} NHL teams with stats")
        
    except Exception as e:
        print(f"Error fetching NHL stats: {e}")
    
    return teams

def fetch_espn_games_with_odds(sport, league_key):
    games = []
    
    try:
        events_url = f"http://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league_key}/events?limit=50"
        resp = requests.get(events_url, timeout=30)
        data = resp.json()
        
        for item in data.get("items", []):
            event_ref = item.get("$ref", "")
            if not event_ref:
                continue
            
            try:
                event_resp = requests.get(event_ref, timeout=10)
                event_data = event_resp.json()
                
                competitions = event_data.get("competitions", [])
                if not competitions:
                    continue
                
                comp = competitions[0]
                comp_id = comp.get("id")
                
                competitors = []
                comp_ref = comp.get("$ref", "")
                if comp_ref:
                    comp_resp = requests.get(comp_ref, timeout=10)
                    comp_data = comp_resp.json()
                    
                    for competitor in comp_data.get("competitors", []):
                        team_ref = competitor.get("team", {}).get("$ref", "")
                        if team_ref:
                            team_resp = requests.get(team_ref, timeout=10)
                            team_data = team_resp.json()
                            competitors.append({
                                "id": str(team_data.get("id")),
                                "name": team_data.get("displayName", ""),
                                "homeAway": competitor.get("homeAway", "")
                            })
                
                odds_url = f"http://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league_key}/events/{comp_id}/competitions/{comp_id}/odds"
                odds_resp = requests.get(odds_url, timeout=10)
                odds_data = odds_resp.json()
                
                over_under = None
                for odds_item in odds_data.get("items", []):
                    if "overUnder" in odds_item:
                        over_under = odds_item.get("overUnder")
                        break
                
                if over_under and len(competitors) == 2:
                    home_team = next((c for c in competitors if c["homeAway"] == "home"), None)
                    away_team = next((c for c in competitors if c["homeAway"] == "away"), None)
                    
                    if home_team and away_team:
                        games.append({
                            "home_team_id": home_team["id"],
                            "home_team": home_team["name"],
                            "away_team_id": away_team["id"],
                            "away_team": away_team["name"],
                            "over_under": over_under
                        })
                
            except Exception:
                continue
            
            time.sleep(0.1)
        
    except Exception as e:
        print(f"Error fetching games: {e}")
    
    return games

def find_team_stats(team_name, team_id, stats_dict):
    if team_id in stats_dict:
        return stats_dict[team_id]
    
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

def calculate_bet(team_a_stats, team_b_stats, line, league):
    if not team_a_stats or not team_b_stats:
        return None
    
    threshold = THRESHOLDS[league]
    
    expected_a = (team_a_stats["ppg"] + team_b_stats["opp_ppg"]) / 2
    expected_b = (team_b_stats["ppg"] + team_a_stats["opp_ppg"]) / 2
    projected_total = expected_a + expected_b
    difference = projected_total - line
    
    if projected_total >= line + threshold:
        return {
            "expected_a": expected_a,
            "expected_b": expected_b,
            "projected_total": projected_total,
            "difference": difference,
            "decision": "OVER",
            "threshold": threshold
        }
    elif line >= projected_total + threshold:
        return {
            "expected_a": expected_a,
            "expected_b": expected_b,
            "projected_total": projected_total,
            "difference": difference,
            "decision": "UNDER",
            "threshold": threshold
        }
    
    return None

def format_output(team_a, team_b, team_a_stats, team_b_stats, line, league, result):
    msg = f"Game: {team_a} vs {team_b}\n"
    msg += f"League: {league}\n"
    msg += f"• {team_a} PPG: {team_a_stats['ppg']:.1f}\n"
    msg += f"• {team_a} Opp PPG: {team_a_stats['opp_ppg']:.1f}\n"
    msg += f"• {team_b} PPG: {team_b_stats['ppg']:.1f}\n"
    msg += f"• {team_b} Opp PPG: {team_b_stats['opp_ppg']:.1f}\n\n"
    msg += f"Expected {team_a}: {result['expected_a']:.1f}\n"
    msg += f"Expected {team_b}: {result['expected_b']:.1f}\n"
    msg += f"Projected Total: {result['projected_total']:.1f}\n"
    msg += f"Line: {line}\n"
    msg += f"Difference: {result['difference']:.1f}\n\n"
    msg += f"Decision: {result['decision']}\n"
    msg += f"Reason: Threshold met ({abs(result['difference']):.1f} >= {result['threshold']})"
    
    return msg

def scan_nba():
    print("Fetching NBA stats...")
    team_stats = fetch_nba_stats()
    
    if not team_stats:
        return []
    
    print("Fetching NBA games with odds...")
    games = fetch_espn_games_with_odds("basketball", "nba")
    
    if not games:
        print("No NBA games with odds")
        return []
    
    qualified_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        result = calculate_bet(away_stats, home_stats, game["over_under"], "NBA")
        
        if result:
            output = format_output(
                game["away_team"], game["home_team"],
                away_stats, home_stats,
                game["over_under"], "NBA", result
            )
            qualified_bets.append(output)
    
    return qualified_bets

def scan_nfl():
    print("Fetching NFL stats...")
    team_stats = fetch_nfl_stats()
    
    if not team_stats:
        return []
    
    print("Fetching NFL games with odds...")
    games = fetch_espn_games_with_odds("football", "nfl")
    
    if not games:
        print("No NFL games with odds")
        return []
    
    qualified_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        result = calculate_bet(away_stats, home_stats, game["over_under"], "NFL")
        
        if result:
            output = format_output(
                game["away_team"], game["home_team"],
                away_stats, home_stats,
                game["over_under"], "NFL", result
            )
            qualified_bets.append(output)
    
    return qualified_bets

def scan_nhl():
    print("Fetching NHL stats...")
    team_stats = fetch_nhl_stats()
    
    if not team_stats:
        return []
    
    print("Fetching NHL games with odds...")
    games = fetch_espn_games_with_odds("hockey", "nhl")
    
    if not games:
        print("No NHL games with odds")
        return []
    
    qualified_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        result = calculate_bet(away_stats, home_stats, game["over_under"], "NHL")
        
        if result:
            output = format_output(
                game["away_team"], game["home_team"],
                away_stats, home_stats,
                game["over_under"], "NHL", result
            )
            qualified_bets.append(output)
    
    return qualified_bets

def scan_all_leagues():
    all_bets = []
    
    print("\n=== Scanning NBA ===")
    bets = scan_nba()
    if bets:
        all_bets.extend(bets)
        print(f"Found {len(bets)} qualified bet(s)")
    else:
        print("No qualified bets")
    
    time.sleep(1)
    
    print("\n=== Scanning NFL ===")
    bets = scan_nfl()
    if bets:
        all_bets.extend(bets)
        print(f"Found {len(bets)} qualified bet(s)")
    else:
        print("No qualified bets")
    
    time.sleep(1)
    
    print("\n=== Scanning NHL ===")
    bets = scan_nhl()
    if bets:
        all_bets.extend(bets)
        print(f"Found {len(bets)} qualified bet(s)")
    else:
        print("No qualified bets")
    
    if all_bets:
        full_msg = "\n\n---\n\n".join(all_bets)
        print("\n" + "=" * 50)
        print("QUALIFIED BETS:")
        print("=" * 50)
        print(full_msg)
        send_discord(full_msg)
    else:
        print("\nNo qualified bets found across all leagues.")

if __name__ == "__main__":
    scan_all_leagues()
