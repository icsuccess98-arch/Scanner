import os
import re
import json
import logging
import requests
from datetime import datetime, timezone
from functools import lru_cache

logger = logging.getLogger(__name__)

DISCORD_CHANNEL_ID = '1472481082468466791'
DISCORD_API_BASE = 'https://discord.com/api/v9'

def get_discord_headers():
    token = os.environ.get('DISCORD_AUTH_TOKEN', '')
    if not token:
        logger.error("DISCORD_AUTH_TOKEN not set")
        return None
    return {
        'Authorization': token,
        'User-Agent': 'Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'X-Discord-Locale': 'en-US',
    }

_cache = {}
CACHE_TTL = 300

def fetch_discord_messages(limit=50):
    cache_key = f'discord_msgs_{DISCORD_CHANNEL_ID}'
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            logger.info(f"Using cached Discord messages ({len(cached_data)} msgs)")
            return cached_data

    headers = get_discord_headers()
    if not headers:
        return []

    try:
        url = f'{DISCORD_API_BASE}/channels/{DISCORD_CHANNEL_ID}/messages?limit={limit}'
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            messages = resp.json()
            _cache[cache_key] = (now, messages)
            logger.info(f"Fetched {len(messages)} Discord messages")
            return messages
        else:
            logger.error(f"Discord API error: {resp.status_code} - {resp.text[:200]}")
            return []
    except Exception as e:
        logger.error(f"Discord fetch error: {e}")
        return []

def parse_spread_section(desc, section_name):
    pattern = rf'(?:📊\s*\*\*{section_name}\*\*|{section_name})(.*?)(?=(?:🔢|🔥|📊\s*\*\*(?:SET|TOTAL|TOP|GAME|MATCH))|$)'
    match = re.search(pattern, desc, re.DOTALL)
    if not match:
        return []

    picks = []
    lines = match.group(1).strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line:
            continue
        pick = parse_spread_line(line)
        if pick:
            pick['spread_type'] = 'game' if 'GAME' in section_name else 'set'
            picks.append(pick)
    return picks

def parse_all_spreads(messages):
    all_cards = []
    seen_msg_ids = set()

    for msg in messages:
        if not msg.get('embeds'):
            continue

        msg_id = msg.get('id', '')
        if msg_id in seen_msg_ids:
            continue
        seen_msg_ids.add(msg_id)

        timestamp = msg.get('timestamp', '')
        try:
            msg_time = datetime.fromisoformat(timestamp.replace('+00:00', '+00:00'))
        except:
            msg_time = datetime.now(timezone.utc)

        for embed in msg['embeds']:
            desc = embed.get('description', '')
            if not desc:
                continue

            footer_text = ''
            if embed.get('footer'):
                footer_text = embed['footer'].get('text', '')

            record_match = re.search(r'Record:\s*([\d]+-[\d]+)\s*\((\d+)%\)', desc)
            record = record_match.group(1) if record_match else ''
            win_pct = record_match.group(2) if record_match else ''

            pending_match = re.search(r'(\d+)\s*pending', desc)
            pending = pending_match.group(1) if pending_match else ''

            matches_match = re.search(r'\*\*(\d+)\s*matches\*\*', desc)
            total_matches = matches_match.group(1) if matches_match else ''

            game_picks = parse_spread_section(desc, 'GAME SPREAD')
            set_picks = parse_spread_section(desc, 'SET SPREAD')

            all_picks = game_picks + set_picks

            if not all_picks:
                continue

            top_plays = parse_top_plays(desc)

            all_cards.append({
                'timestamp': msg_time.isoformat(),
                'date': msg_time.strftime('%b %d, %Y'),
                'time': msg_time.strftime('%I:%M %p ET'),
                'record': record,
                'win_pct': win_pct,
                'pending': pending,
                'total_matches': total_matches,
                'game_picks': game_picks,
                'set_picks': set_picks,
                'picks': all_picks,
                'top_plays': top_plays,
                'footer': footer_text,
                'color': embed.get('color', 3066993),
            })

    return all_cards

def parse_spread_line(line):
    line = line.replace('**', '')

    result = None
    if '✅' in line:
        result = 'win'
        line = line.replace('✅', '').strip()
    elif '❌' in line:
        result = 'loss'
        line = line.replace('❌', '').strip()

    match = re.match(
        r'([A-Za-zÀ-ÿ\.\s\'-]+?)\s*([+-]?\d+\.?\d*)\s*\(([+-]\d+)\)\s*\|\s*(\d+)%',
        line.strip()
    )
    if match:
        player = match.group(1).strip()
        spread = match.group(2)
        odds = match.group(3)
        confidence = match.group(4)

        try:
            spread_val = float(spread)
        except:
            spread_val = 0

        try:
            odds_val = int(odds)
        except:
            odds_val = 0

        return {
            'player': player,
            'spread': spread,
            'spread_val': spread_val,
            'odds': odds,
            'odds_val': odds_val,
            'confidence': int(confidence),
            'result': result,
        }
    return None

def parse_top_plays(desc):
    top_section = re.search(r'🔥\s*\*\*TOP PLAYS\*\*(.*?)$', desc, re.DOTALL)
    if not top_section:
        return []

    top_plays = []
    lines = top_section.group(1).strip().split('\n')
    for line in lines:
        line = line.strip().replace('**', '')
        if not line:
            continue

        result = None
        if '✅' in line:
            result = 'win'
            line = line.replace('✅', '').strip()
        elif '❌' in line:
            result = 'loss'
            line = line.replace('❌', '').strip()

        rank_match = re.match(r'(?:🥇|🥈|🥉|#\d+)\s*(.+?)\s*\|\s*(\d+)%', line)
        if rank_match:
            top_plays.append({
                'player': rank_match.group(1).strip(),
                'confidence': int(rank_match.group(2)),
                'result': result,
            })

    return top_plays

def analyze_four_brains(pick, player_stats=None):
    confidence = pick.get('confidence', 50)
    spread_val = abs(pick.get('spread_val', 0))
    odds_val = pick.get('odds_val', -110)
    player_name = pick.get('player', '')

    pstats = None

    if pstats and not pstats.get('elo_only'):
        tour = pstats.get('tour', 'ATP')
        is_wta = tour == 'WTA'

        # Stats brain: Net Rating (Hold% - Break% Differential) + 1st Serve Won%
        # Basketball equivalent: Net Rating
        # Hold/Break diff is #1 predictor. 1st Serve Won% shows serve dominance quality.
        # ATP: net > 0.55 or 1st serve > 73%. WTA: net > 0.30 or 1st serve > 68%
        net_rating = pstats.get('net_rating', 0)
        first_serve_won = pstats.get('first_serve_won', 0)
        if is_wta:
            stats = net_rating > 0.30 or first_serve_won > 0.68
        else:
            stats = net_rating > 0.55 or first_serve_won > 0.73

        # Trends brain: Return Points Won% + 2nd Serve Won% (pressure tolerance)
        # Basketball equivalent: OREB%
        # RPW is more predictive than break% long-term. 2nd serve measures pressure.
        # ATP: RPW > 38% or 2nd serve > 52%. WTA: RPW > 42% or 2nd serve > 46%
        rpw = pstats.get('rpw', 0)
        second_serve = pstats.get('second_serve_won', 0)
        if is_wta:
            trends = rpw > 0.42 or second_serve > 0.46
        else:
            trends = rpw > 0.38 or second_serve > 0.52

        # Value brain: Double Fault Rate (TOV%) + Break Point Opps Created (FT Rate) + odds
        # Basketball equivalent: TOV% + FT Rate
        # Low DF = low risk. High BP creation = more scoring opportunities.
        # DF < 4% AND (odds >= -130 OR bp/match >= threshold)
        df_rate = pstats.get('df_rate', 0.05)
        bp_per_match = pstats.get('bp_per_match', 0)
        bp_threshold = 8.0 if is_wta else 7.0
        value = df_rate < 0.04 and (odds_val >= -130 or bp_per_match >= bp_threshold)

        # Matchup brain: Surface Elo (SOS) + Dominance Ratio
        # Basketball equivalent: Strength of Schedule
        # "Surface Elo > Overall Elo when betting" - use hard court Elo preferentially
        # Dominance ratio > 1.10 = strong favorite profile
        elo = pstats.get('hard_elo') or pstats.get('elo')
        dominance = pstats.get('dominance_ratio', 0)
        elo_threshold = 1800 if is_wta else 1900
        matchup = (elo is not None and elo > elo_threshold) or dominance > 1.10
    elif pstats:
        # Elo-only player (outside leadersource top 50, but has Elo ratings)
        # Use confidence for stats/trends, Elo for matchup, odds for value
        stats = confidence >= 60
        trends = spread_val <= 5.0

        value = odds_val >= -115

        elo = pstats.get('elo')
        # Can't determine tour from Elo-only, so use 1800 as safe middle ground
        matchup = elo is not None and elo > 1800
    else:
        # No data at all - pure confidence-based fallback
        stats = confidence >= 60
        trends = spread_val <= 5.0
        value = odds_val >= -115
        matchup = (confidence >= 58 and spread_val <= 6.5) or confidence >= 64

    brains = {
        'stats': stats,
        'trends': trends,
        'value': value,
        'matchup': matchup,
        'has_real_stats': pstats is not None and not pstats.get('elo_only'),
        'has_elo': pstats is not None and pstats.get('elo') is not None,
    }
    brains['count'] = sum([stats, trends, value, matchup])

    # Attach key metrics for display in the UI
    if pstats and not pstats.get('elo_only'):
        brains['metrics'] = {
            'hold_pct': pstats.get('hold_pct'),
            'break_pct': pstats.get('break_pct'),
            'net_rating': pstats.get('net_rating'),
            'first_serve_won': pstats.get('first_serve_won'),
            'second_serve_won': pstats.get('second_serve_won'),
            'rpw': pstats.get('rpw'),
            'df_rate': pstats.get('df_rate'),
            'bp_per_match': pstats.get('bp_per_match'),
            'dominance_ratio': pstats.get('dominance_ratio'),
            'elo': pstats.get('hard_elo') or pstats.get('elo'),
            'matches': pstats.get('matches'),
            'tour': pstats.get('tour', 'ATP'),
        }

    return brains

def clear_discord_cache():
    _cache.clear()
    logger.info("Discord cache cleared")
