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

def get_tennis_game_spreads():
    messages = fetch_discord_messages(limit=50)
    if not messages:
        return {'success': False, 'error': 'No messages fetched', 'cards': []}

    cards = parse_all_spreads(messages)

    total_wins = 0
    total_losses = 0
    total_pending = 0
    total_spreads = 0
    for card in cards:
        for pick in card['picks']:
            total_spreads += 1
            if pick['result'] == 'win':
                total_wins += 1
            elif pick['result'] == 'loss':
                total_losses += 1
            else:
                total_pending += 1

    decided = total_wins + total_losses
    overall_pct = round(total_wins / decided * 100) if decided > 0 else 0

    return {
        'success': True,
        'cards': cards,
        'overall_record': f"{total_wins}-{total_losses}",
        'overall_pct': overall_pct,
        'total_spreads': total_spreads,
        'total_pending': total_pending,
        'total_wins': total_wins,
        'total_losses': total_losses,
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'message_count': len(messages),
    }

def clear_discord_cache():
    _cache.clear()
    logger.info("Discord cache cleared")
