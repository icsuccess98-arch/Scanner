import os
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
import requests
import pytz

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or "sports-model-secret-key"
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
db.init_app(app)

THRESHOLDS = {"NBA": 8.0, "CBB": 8.0, "NFL": 3.5, "CFB": 3.5, "NHL": 0.5}

class Game(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)
    league = db.Column(db.String(10), nullable=False)
    away_team = db.Column(db.String(100), nullable=False)
    home_team = db.Column(db.String(100), nullable=False)
    game_time = db.Column(db.String(20))
    line = db.Column(db.Float)
    away_ppg = db.Column(db.Float)
    away_opp_ppg = db.Column(db.Float)
    home_ppg = db.Column(db.Float)
    home_opp_ppg = db.Column(db.Float)
    projected_total = db.Column(db.Float)
    edge = db.Column(db.Float)
    direction = db.Column(db.String(10))
    is_qualified = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Pick(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey('game.id'))
    date = db.Column(db.Date, nullable=False)
    league = db.Column(db.String(10), nullable=False)
    matchup = db.Column(db.String(200), nullable=False)
    pick = db.Column(db.String(50), nullable=False)
    edge = db.Column(db.Float)
    result = db.Column(db.String(10))
    actual_total = db.Column(db.Float)
    is_lock = db.Column(db.Boolean, default=False)
    posted_to_discord = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

with app.app_context():
    db.create_all()

def calculate_projection(away_ppg, away_opp, home_ppg, home_opp):
    exp_away = (away_ppg + home_opp) / 2
    exp_home = (home_ppg + away_opp) / 2
    return exp_away + exp_home

def check_qualification(projected, line, league):
    threshold = THRESHOLDS.get(league, 8.0)
    diff = projected - line
    edge = abs(diff)
    
    if projected >= line + threshold:
        return True, "O", edge
    elif line >= projected + threshold:
        return True, "U", edge
    return False, None, edge

@app.route('/')
def dashboard():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    games = Game.query.filter_by(date=today).order_by(Game.edge.desc()).all()
    qualified = [g for g in games if g.is_qualified]
    lock = qualified[0] if qualified else None
    return render_template('dashboard.html', games=games, qualified=qualified, lock=lock, today=today, thresholds=THRESHOLDS)

@app.route('/add_game', methods=['POST'])
def add_game():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    
    game = Game(
        date=today,
        league=request.form['league'],
        away_team=request.form['away_team'],
        home_team=request.form['home_team'],
        game_time=request.form.get('game_time', ''),
        line=float(request.form['line']) if request.form.get('line') else None,
        away_ppg=float(request.form['away_ppg']) if request.form.get('away_ppg') else None,
        away_opp_ppg=float(request.form['away_opp_ppg']) if request.form.get('away_opp_ppg') else None,
        home_ppg=float(request.form['home_ppg']) if request.form.get('home_ppg') else None,
        home_opp_ppg=float(request.form['home_opp_ppg']) if request.form.get('home_opp_ppg') else None
    )
    
    if all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg, game.line]):
        game.projected_total = calculate_projection(game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg)
        qualified, direction, edge = check_qualification(game.projected_total, game.line, game.league)
        game.is_qualified = qualified
        game.direction = direction
        game.edge = edge
    
    db.session.add(game)
    db.session.commit()
    return redirect(url_for('dashboard'))

@app.route('/update_line/<int:game_id>', methods=['POST'])
def update_line(game_id):
    game = Game.query.get_or_404(game_id)
    data = request.get_json()
    
    if 'line' in data:
        game.line = float(data['line'])
    
    if all([game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg, game.line]):
        game.projected_total = calculate_projection(game.away_ppg, game.away_opp_ppg, game.home_ppg, game.home_opp_ppg)
        qualified, direction, edge = check_qualification(game.projected_total, game.line, game.league)
        game.is_qualified = qualified
        game.direction = direction
        game.edge = edge
    
    db.session.commit()
    return jsonify({
        'success': True,
        'projected': round(game.projected_total, 1) if game.projected_total else None,
        'edge': round(game.edge, 1) if game.edge else None,
        'qualified': game.is_qualified,
        'direction': game.direction
    })

@app.route('/delete_game/<int:game_id>', methods=['POST'])
def delete_game(game_id):
    game = Game.query.get_or_404(game_id)
    db.session.delete(game)
    db.session.commit()
    return redirect(url_for('dashboard'))

def get_team_stats():
    from nba_api.stats.endpoints import leaguedashteamstats
    import time
    
    stats = {"nba": {}, "nhl": {}, "cbb": {}}
    
    try:
        time.sleep(1)
        offense = leaguedashteamstats.LeagueDashTeamStats(
            season='2025-26', season_type_all_star='Regular Season',
            measure_type_detailed_defense='Base', per_mode_detailed='PerGame'
        )
        off_df = offense.get_data_frames()[0]
        
        defense = leaguedashteamstats.LeagueDashTeamStats(
            season='2025-26', season_type_all_star='Regular Season',
            measure_type_detailed_defense='Opponent', per_mode_detailed='PerGame'
        )
        def_df = defense.get_data_frames()[0]
        
        opp_dict = {row['TEAM_ID']: row['OPP_PTS'] for _, row in def_df.iterrows()}
        
        for _, row in off_df.iterrows():
            team_name = row['TEAM_NAME']
            ppg = row['PTS']
            opp_ppg = opp_dict.get(row['TEAM_ID'])
            if ppg and opp_ppg:
                nick = team_name.split()[-1].lower()
                stats["nba"][nick] = {"name": team_name, "ppg": ppg, "opp_ppg": opp_ppg}
                if "76ers" in team_name: stats["nba"]["76ers"] = stats["nba"][nick]
                if "Trail Blazers" in team_name: stats["nba"]["blazers"] = stats["nba"][nick]
    except Exception as e:
        print(f"NBA stats error: {e}")
    
    try:
        nhl_url = "https://api.nhle.com/stats/rest/en/team/summary?cayenneExp=seasonId=20252026"
        resp = requests.get(nhl_url, timeout=30)
        for team in resp.json().get("data", []):
            name = team.get("teamFullName", "")
            games_played = team.get("gamesPlayed", 1)
            if games_played > 0:
                ppg = team.get("goalsFor", 0) / games_played
                opp_ppg = team.get("goalsAgainst", 0) / games_played
                nick = name.split()[-1].lower()
                stats["nhl"][nick] = {"name": name, "ppg": ppg, "opp_ppg": opp_ppg}
    except Exception as e:
        print(f"NHL stats error: {e}")
    
    return stats

def find_team_stats(name, stats_dict):
    name_lower = name.lower()
    if name_lower in stats_dict:
        return stats_dict[name_lower]
    for key, val in stats_dict.items():
        if name_lower in key or key in name_lower:
            return val
        name_parts = name_lower.split()
        for part in name_parts:
            if len(part) > 3 and part in key:
                return val
    return None

@app.route('/fetch_games', methods=['POST'])
def fetch_games():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%Y%m%d")
    
    stats = get_team_stats()
    games_added = 0
    
    try:
        nba_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={today_str}"
        resp = requests.get(nba_url, timeout=30)
        events = resp.json().get("events", [])
        
        for event in events:
            comps = event.get("competitions", [{}])[0]
            teams = comps.get("competitors", [])
            if len(teams) == 2:
                away = next((t for t in teams if t.get("homeAway") == "away"), None)
                home = next((t for t in teams if t.get("homeAway") == "home"), None)
                if away and home:
                    away_name = away.get("team", {}).get("shortDisplayName", "")
                    home_name = home.get("team", {}).get("shortDisplayName", "")
                    game_time = event.get("status", {}).get("type", {}).get("shortDetail", "")
                    
                    existing = Game.query.filter_by(date=today, league="NBA", away_team=away_name, home_team=home_name).first()
                    if not existing:
                        away_stats = find_team_stats(away_name, stats["nba"])
                        home_stats = find_team_stats(home_name, stats["nba"])
                        
                        game = Game(
                            date=today, league="NBA", away_team=away_name, home_team=home_name,
                            game_time=game_time,
                            away_ppg=away_stats["ppg"] if away_stats else None,
                            away_opp_ppg=away_stats["opp_ppg"] if away_stats else None,
                            home_ppg=home_stats["ppg"] if home_stats else None,
                            home_opp_ppg=home_stats["opp_ppg"] if home_stats else None
                        )
                        db.session.add(game)
                        games_added += 1
    except Exception as e:
        print(f"NBA games error: {e}")
    
    try:
        nhl_url = f"https://api-web.nhle.com/v1/schedule/{today.strftime('%Y-%m-%d')}"
        resp = requests.get(nhl_url, timeout=30)
        game_weeks = resp.json().get("gameWeek", [])
        
        for gw in game_weeks:
            if gw.get("date") == today.strftime("%Y-%m-%d"):
                for game_data in gw.get("games", []):
                    away_name = game_data.get("awayTeam", {}).get("placeName", {}).get("default", "")
                    home_name = game_data.get("homeTeam", {}).get("placeName", {}).get("default", "")
                    start_time = game_data.get("startTimeUTC", "")
                    
                    if away_name and home_name:
                        existing = Game.query.filter_by(date=today, league="NHL", away_team=away_name, home_team=home_name).first()
                        if not existing:
                            away_stats = find_team_stats(away_name, stats["nhl"])
                            home_stats = find_team_stats(home_name, stats["nhl"])
                            
                            game = Game(
                                date=today, league="NHL", away_team=away_name, home_team=home_name,
                                game_time=start_time[:10] if start_time else "",
                                away_ppg=away_stats["ppg"] if away_stats else None,
                                away_opp_ppg=away_stats["opp_ppg"] if away_stats else None,
                                home_ppg=home_stats["ppg"] if home_stats else None,
                                home_opp_ppg=home_stats["opp_ppg"] if home_stats else None
                            )
                            db.session.add(game)
                            games_added += 1
    except Exception as e:
        print(f"NHL games error: {e}")
    
    db.session.commit()
    return jsonify({"success": True, "games_added": games_added})

@app.route('/fetch_stats', methods=['POST'])
def fetch_stats():
    stats = get_team_stats()
    return jsonify({"success": True, "stats": stats, "counts": {k: len(v) for k, v in stats.items()}})

@app.route('/post_discord', methods=['POST'])
def post_discord():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    today_str = today.strftime("%B %d, %Y")
    
    games = Game.query.filter_by(date=today, is_qualified=True).order_by(Game.edge.desc()).limit(5).all()
    
    if not games:
        return jsonify({"success": False, "message": "No qualified picks to post"})
    
    msg = f"🎯 PICKS OF THE DAY - {today_str}\n\n"
    
    for league in ["NBA", "CBB", "CFB", "NHL"]:
        league_games = [g for g in games if g.league == league]
        if league_games:
            emoji = {"NBA": "🏀", "CBB": "🏀", "CFB": "🏈", "NHL": "🏒"}.get(league, "🎯")
            msg += f"{emoji} {league}\n"
            for g in league_games:
                msg += f"{g.away_team}/{g.home_team} ({g.game_time})\n"
                msg += f"Game Total {g.direction}{g.line}\n\n"
    
    lock = games[0]
    msg += f"🔒 Lock Of The Day:\n"
    msg += f"{lock.away_team}/{lock.home_team} ({lock.game_time})\n"
    msg += f"Game Total {lock.direction}{lock.line}\n"
    
    webhook = os.environ.get("SPORTS_DISCORD_WEBHOOK")
    if webhook:
        resp = requests.post(webhook, json={"content": msg})
        
        for g in games:
            pick = Pick(
                game_id=g.id,
                date=today,
                league=g.league,
                matchup=f"{g.away_team} @ {g.home_team}",
                pick=f"{g.direction}{g.line}",
                edge=g.edge,
                is_lock=(g.id == lock.id),
                posted_to_discord=True
            )
            db.session.add(pick)
        db.session.commit()
        
        return jsonify({"success": True, "status": resp.status_code, "picks_count": len(games)})
    
    return jsonify({"success": False, "message": "Discord webhook not configured"})

@app.route('/history')
def history():
    picks = Pick.query.order_by(Pick.date.desc(), Pick.edge.desc()).all()
    wins = len([p for p in picks if p.result == 'W'])
    losses = len([p for p in picks if p.result == 'L'])
    pending = len([p for p in picks if not p.result])
    return render_template('history.html', picks=picks, wins=wins, losses=losses, pending=pending)

@app.route('/update_result/<int:pick_id>', methods=['POST'])
def update_result(pick_id):
    pick = Pick.query.get_or_404(pick_id)
    data = request.get_json()
    pick.result = data.get('result')
    if data.get('actual_total'):
        pick.actual_total = float(data['actual_total'])
    db.session.commit()
    return jsonify({"success": True})

@app.route('/clear_games', methods=['POST'])
def clear_games():
    et = pytz.timezone('America/New_York')
    today = datetime.now(et).date()
    Game.query.filter_by(date=today).delete()
    db.session.commit()
    return redirect(url_for('dashboard'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
