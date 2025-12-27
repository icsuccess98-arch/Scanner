import os
import requests
from datetime import datetime
import time

DISCORD_WEBHOOK = os.environ.get("SPORTS_DISCORD_WEBHOOK", "")

THRESHOLDS = {
    "NBA": 8.0,
    "CBB": 8.0,
    "NFL": 3.5,
    "CFB": 3.5,
    "NHL": 0.5
}


def send_discord(msg):
    if not DISCORD_WEBHOOK:
        print("WARNING: DISCORD_WEBHOOK not set")
        return
    
    if len(msg) > 2000:
        msg = msg[:1997] + "..."
    
    print(f"Sending Discord message ({len(msg)} chars)...")
    try:
        payload = {"content": msg}
        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=30)
        print(f"Discord response: {resp.status_code}")
        if resp.status_code != 204:
            print(f"Discord error: {resp.text}")
    except Exception as e:
        print(f"Discord send error: {e}")

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

def fetch_cbb_stats():
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams?limit=500"
        resp = requests.get(url, timeout=60)
        data = resp.json()
        
        team_list = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        
        for team_data in team_list:
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            
            try:
                record_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}"
                record_resp = requests.get(record_url, timeout=10)
                record_data = record_resp.json()
                
                team_record = record_data.get("team", {}).get("record", {})
                items = team_record.get("items", [])
                
                ppg = None
                opp_ppg = None
                
                for item in items:
                    if item.get("type") == "total":
                        for stat in item.get("stats", []):
                            if stat.get("name") == "avgPointsFor":
                                ppg = stat.get("value")
                            if stat.get("name") == "avgPointsAgainst":
                                opp_ppg = stat.get("value")
                        break
                
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
        print(f"Error fetching CBB stats: {e}")
    
    print(f"Loaded {len(teams)//2} CBB teams with stats")
    return teams

def fetch_cfb_stats():
    teams = {}
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?limit=500"
        resp = requests.get(url, timeout=60)
        data = resp.json()
        
        team_list = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        
        for team_data in team_list:
            team = team_data.get("team", {})
            team_id = team.get("id")
            team_name = team.get("displayName", "")
            
            try:
                record_url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}"
                record_resp = requests.get(record_url, timeout=10)
                record_data = record_resp.json()
                
                team_record = record_data.get("team", {}).get("record", {})
                items = team_record.get("items", [])
                
                ppg = None
                opp_ppg = None
                
                for item in items:
                    if item.get("type") == "total":
                        for stat in item.get("stats", []):
                            if stat.get("name") == "avgPointsFor":
                                ppg = stat.get("value")
                            if stat.get("name") == "avgPointsAgainst":
                                opp_ppg = stat.get("value")
                        break
                
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
        print(f"Error fetching CFB stats: {e}")
    
    print(f"Loaded {len(teams)//2} CFB teams with stats")
    return teams

def fetch_espn_games_with_odds(sport, league_key):
    from datetime import timezone
    import pytz
    
    games = []
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).strftime("%Y%m%d")
    
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
                
                game_time = ""
                game_date = ""
                date_str = event_data.get("date", "")
                if date_str:
                    try:
                        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        dt_et = dt.astimezone(et)
                        game_time = dt_et.strftime("%-I:%M %p ET")
                        game_date = dt_et.strftime("%Y%m%d")
                    except:
                        pass
                
                if game_date != today:
                    continue
                
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
                spread = None
                spread_team = None
                for odds_item in odds_data.get("items", []):
                    if "overUnder" in odds_item:
                        over_under = odds_item.get("overUnder")
                    if "spread" in odds_item:
                        spread = odds_item.get("spread")
                    if "spreadLeader" in odds_item:
                        leader_ref = odds_item.get("spreadLeader", {}).get("$ref", "")
                        if leader_ref:
                            try:
                                leader_resp = requests.get(leader_ref, timeout=10)
                                leader_data = leader_resp.json()
                                spread_team = leader_data.get("displayName", "")
                            except:
                                pass
                    if over_under and spread:
                        break
                
                if (over_under or spread) and len(competitors) == 2:
                    home_team = next((c for c in competitors if c["homeAway"] == "home"), None)
                    away_team = next((c for c in competitors if c["homeAway"] == "away"), None)
                    
                    game_status = event_data.get("status", {}).get("type", {}).get("name", "")
                    total_score = None
                    
                    if game_status == "STATUS_FINAL":
                        try:
                            home_score = 0
                            away_score = 0
                            for competitor in comp_data.get("competitors", []):
                                score = competitor.get("score", {})
                                if isinstance(score, dict):
                                    score_ref = score.get("$ref", "")
                                    if score_ref:
                                        score_resp = requests.get(score_ref, timeout=10)
                                        score_data = score_resp.json()
                                        pts = score_data.get("value", 0)
                                        if competitor.get("homeAway") == "home":
                                            home_score = pts
                                        else:
                                            away_score = pts
                            total_score = home_score + away_score
                        except:
                            pass
                    
                    if home_team and away_team:
                        games.append({
                            "home_team_id": home_team["id"],
                            "home_team": home_team["name"],
                            "away_team_id": away_team["id"],
                            "away_team": away_team["name"],
                            "over_under": over_under,
                            "spread": spread,
                            "spread_team": spread_team,
                            "game_time": game_time,
                            "game_status": game_status,
                            "total_score": total_score
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

def format_output(away_team, home_team, away_stats, home_stats, line, league, result, game_time=""):
    diff_sign = "+" if result['difference'] > 0 else ""
    time_str = f" ({game_time})" if game_time else ""
    
    msg = f"• {away_team} @ {home_team}{time_str}\n"
    msg += f"  Line: {line} | Proj: {result['projected_total']:.1f} | {diff_sign}{result['difference']:.1f}\n"
    msg += f"  PICK: {result['decision']} {line}"
    
    return msg


def fetch_completed_scores(sport, league):
    scores = {}
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"
        resp = requests.get(url, timeout=30)
        data = resp.json()
        
        for event in data.get("events", []):
            status = event.get("status", {}).get("type", {}).get("name", "")
            if status == "STATUS_FINAL":
                competitions = event.get("competitions", [])
                if competitions:
                    comp = competitions[0]
                    competitors = comp.get("competitors", [])
                    total = 0
                    teams = []
                    for c in competitors:
                        score = c.get("score", "0")
                        total += int(score) if score.isdigit() else 0
                        teams.append(c.get("team", {}).get("displayName", ""))
                    
                    for team in teams:
                        scores[team.lower()] = total
    except Exception as e:
        print(f"Error fetching scores: {e}")
    
    return scores

def get_game_result(game, decision, completed_scores):
    home_team = game.get("home_team", "").lower()
    away_team = game.get("away_team", "").lower()
    
    total_score = completed_scores.get(home_team) or completed_scores.get(away_team)
    
    if total_score is None:
        return None
    
    line = game["over_under"]
    if decision == "OVER":
        return "win" if total_score > line else "loss"
    else:
        return "win" if total_score < line else "loss"

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
    
    total_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        if game.get("over_under"):
            result = calculate_bet(away_stats, home_stats, game["over_under"], "NBA")
            if result:
                output = format_output(
                    game["away_team"], game["home_team"],
                    away_stats, home_stats,
                    game["over_under"], "NBA", result,
                    game.get("game_time", "")
                )
                total_bets.append(output)
    
    return total_bets

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
    
    total_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        if game.get("over_under"):
            result = calculate_bet(away_stats, home_stats, game["over_under"], "NFL")
            if result:
                output = format_output(
                    game["away_team"], game["home_team"],
                    away_stats, home_stats,
                    game["over_under"], "NFL", result,
                    game.get("game_time", "")
                )
                total_bets.append(output)
    
    return total_bets

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
    
    total_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        if game.get("over_under"):
            result = calculate_bet(away_stats, home_stats, game["over_under"], "NHL")
            if result:
                output = format_output(
                    game["away_team"], game["home_team"],
                    away_stats, home_stats,
                    game["over_under"], "NHL", result,
                    game.get("game_time", "")
                )
                total_bets.append(output)
    
    return total_bets

def scan_cbb():
    print("Fetching CBB stats...")
    team_stats = fetch_cbb_stats()
    
    if not team_stats:
        return []
    
    print("Fetching CBB games with odds...")
    games = fetch_espn_games_with_odds("basketball", "mens-college-basketball")
    
    if not games:
        print("No CBB games with odds")
        return []
    
    total_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        if game.get("over_under"):
            result = calculate_bet(away_stats, home_stats, game["over_under"], "CBB")
            if result:
                output = format_output(
                    game["away_team"], game["home_team"],
                    away_stats, home_stats,
                    game["over_under"], "CBB", result,
                    game.get("game_time", "")
                )
                total_bets.append(output)
    
    return total_bets

def scan_cfb():
    print("Fetching CFB stats...")
    team_stats = fetch_cfb_stats()
    
    if not team_stats:
        return []
    
    print("Fetching CFB games with odds...")
    games = fetch_espn_games_with_odds("football", "college-football")
    
    if not games:
        print("No CFB games with odds")
        return []
    
    total_bets = []
    
    for game in games:
        home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
        away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
        
        if not home_stats or not away_stats:
            continue
        
        if game.get("over_under"):
            result = calculate_bet(away_stats, home_stats, game["over_under"], "CFB")
            if result:
                output = format_output(
                    game["away_team"], game["home_team"],
                    away_stats, home_stats,
                    game["over_under"], "CFB", result,
                    game.get("game_time", "")
                )
                total_bets.append(output)
    
    return total_bets

def scan_all_leagues():
    league_totals = {}
    
    print("\n=== Scanning NBA ===")
    bets = scan_nba()
    if bets:
        league_totals["NBA"] = bets
    print(f"Found {len(bets)} O/U bet(s)")
    
    time.sleep(1)
    
    print("\n=== Scanning CBB ===")
    bets = scan_cbb()
    if bets:
        league_totals["CBB"] = bets
    print(f"Found {len(bets)} O/U bet(s)")
    
    time.sleep(1)
    
    print("\n=== Scanning NFL ===")
    bets = scan_nfl()
    if bets:
        league_totals["NFL"] = bets
    print(f"Found {len(bets)} O/U bet(s)")
    
    time.sleep(1)
    
    print("\n=== Scanning CFB ===")
    bets = scan_cfb()
    if bets:
        league_totals["CFB"] = bets
    print(f"Found {len(bets)} O/U bet(s)")
    
    time.sleep(1)
    
    print("\n=== Scanning NHL ===")
    bets = scan_nhl()
    if bets:
        league_totals["NHL"] = bets
    print(f"Found {len(bets)} O/U bet(s)")
    
    headers = {
        "NBA": "🏀 NBA",
        "CBB": "🏀 CBB",
        "NFL": "🏈 NFL",
        "CFB": "🏈 CFB",
        "NHL": "🏒 NHL"
    }
    
    if league_totals:
        print("\n" + "=" * 50)
        print("QUALIFIED BETS:")
        print("=" * 50)
        
        full_msg = "📊 TOTALS (O/U)\n"
        for league in ["NBA", "CBB", "NFL", "CFB", "NHL"]:
            if league in league_totals:
                full_msg += f"\n{headers[league]}\n\n"
                full_msg += "\n\n".join(league_totals[league])
                full_msg += "\n"
        
        print(full_msg)
        send_discord(full_msg)
    else:
        print("\nNo qualified bets found across all leagues.")

def scan_top_picks(top_n=10):
    """Scan all leagues and return only the highest-edge picks sorted by certainty"""
    all_picks = []
    
    headers = {
        "NBA": "🏀 NBA",
        "CBB": "🏀 CBB", 
        "NFL": "🏈 NFL",
        "CFB": "🏈 CFB",
        "NHL": "🏒 NHL"
    }
    
    league_configs = [
        ("NBA", "basketball", "nba", fetch_nba_stats),
        ("CBB", "basketball", "mens-college-basketball", fetch_cbb_stats),
        ("NFL", "football", "nfl", fetch_nfl_stats),
        ("CFB", "football", "college-football", fetch_cfb_stats),
        ("NHL", "hockey", "nhl", fetch_nhl_stats),
    ]
    
    for league, sport, league_key, stats_func in league_configs:
        print(f"\n=== Scanning {league} ===")
        team_stats = stats_func()
        if not team_stats:
            continue
            
        games = fetch_espn_games_with_odds(sport, league_key)
        if not games:
            print(f"No {league} games with odds")
            continue
        
        for game in games:
            home_stats = find_team_stats(game["home_team"], game["home_team_id"], team_stats)
            away_stats = find_team_stats(game["away_team"], game["away_team_id"], team_stats)
            
            if not home_stats or not away_stats:
                continue
            
            if game.get("over_under"):
                line = game["over_under"]
                threshold = THRESHOLDS[league]
                
                expected_a = (away_stats["ppg"] + home_stats["opp_ppg"]) / 2
                expected_b = (home_stats["ppg"] + away_stats["opp_ppg"]) / 2
                projected = expected_a + expected_b
                edge = projected - line
                abs_edge = abs(edge)
                
                if abs_edge >= threshold:
                    decision = "OVER" if edge > 0 else "UNDER"
                    edge_pct = (abs_edge / threshold) * 100
                    
                    all_picks.append({
                        "league": league,
                        "header": headers[league],
                        "away_team": game["away_team"],
                        "home_team": game["home_team"],
                        "game_time": game.get("game_time", ""),
                        "line": line,
                        "projected": projected,
                        "edge": edge,
                        "abs_edge": abs_edge,
                        "edge_pct": edge_pct,
                        "decision": decision,
                        "threshold": threshold
                    })
        
        time.sleep(1)
    
    all_picks.sort(key=lambda x: x["abs_edge"], reverse=True)
    top_picks = all_picks[:top_n]
    
    if top_picks:
        print("\n" + "=" * 60)
        print(f"TOP {len(top_picks)} HIGHEST CERTAINTY PICKS")
        print("=" * 60)
        
        for i, pick in enumerate(top_picks, 1):
            edge_sign = "+" if pick["edge"] > 0 else ""
            edge_over_threshold = pick["abs_edge"] / pick["threshold"]
            confidence = min(edge_over_threshold * 50 + 50, 99)
            
            print(f"\n#{i} {pick['header']} - {pick['away_team']} @ {pick['home_team']}")
            print(f"   Time: {pick['game_time']}")
            print(f"   Line: {pick['line']} | Proj: {pick['projected']:.1f} | Edge: {edge_sign}{pick['edge']:.1f}")
            print(f"   PICK: {pick['decision']} {pick['line']}")
            print(f"   Confidence: {confidence:.0f}% ({pick['abs_edge']:.1f} pts over {pick['threshold']} threshold)")
    else:
        print("\nNo qualified picks found.")
    
    return top_picks

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--top":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        scan_top_picks(n)
    else:
        scan_all_leagues()
