import re
import json
import logging
import unicodedata
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

ATP_LEADERSOURCE_URL = 'https://www.tennisabstract.com/jsmatches/leadersource.js'
WTA_LEADERSOURCE_URL = 'https://www.tennisabstract.com/jsmatches/leadersource_wta.js'
ATP_ELO_URL = 'https://www.tennisabstract.com/reports/atp_elo_ratings.html'
WTA_ELO_URL = 'https://www.tennisabstract.com/reports/wta_elo_ratings.html'
PLAYER_JS_URL = 'https://www.tennisabstract.com/jsmatches/{name}.js'
ATP_PLAYER_PAGE_URL = 'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={name}'
WTA_PLAYER_PAGE_URL = 'https://www.tennisabstract.com/cgi-bin/wplayer-classic.cgi?p={name}'

_cache = {}
CACHE_TTL = 3600  # 60 minutes

# Field indices from matchhead in leadersource.js
IDX_DATE = 0
IDX_TOURN = 1
IDX_SURF = 2
IDX_LEVEL = 3
IDX_WL = 4
IDX_PLAYER = 5
IDX_RANK = 6
IDX_DFS = 28
IDX_PTS = 29
IDX_FIRSTS = 30
IDX_FWON = 31
IDX_SWON = 32
IDX_SGAMES = 33
IDX_SAVED = 34
IDX_CHANCES = 35
IDX_ODFS = 37
IDX_OPTS = 38
IDX_OFIRSTS = 39
IDX_OFWON = 40
IDX_OSWON = 41
IDX_RGAMES = 42  # ogames
IDX_OSAVED = 43
IDX_OCHANCES = 44

# Per-player JS file field indices (different layout — no player field)
PP_SURF = 2
PP_WL = 4
PP_RANK = 5
PP_DFS = 22
PP_PTS = 23
PP_FIRSTS = 24
PP_FWON = 25
PP_SWON = 26
PP_SGAMES = 27  # 'games'
PP_SAVED = 28
PP_CHANCES = 29
PP_OPTS = 32
PP_OFIRSTS = 33
PP_OFWON = 34
PP_OSWON = 35
PP_RGAMES = 36  # 'ogames'
PP_OSAVED = 37
PP_OCHANCES = 38


def _normalize(name):
    """Strip accents and lowercase for fuzzy comparison."""
    nfkd = unicodedata.normalize('NFKD', name)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def _build_name_index(stats):
    """Build lookup indices for fuzzy player name matching.

    Handles formats like 'F. Lastname', 'Firstname Lastname',
    multi-word surnames ('Pablo Carreno Busta' -> 'P. Busta'),
    and accent variations ('Krejčíková' -> 'Krejcikova').
    """
    index = {}
    for full_name in stats:
        norm = _normalize(full_name)
        # Index by normalized full name
        index[norm] = full_name

        parts = full_name.split()
        if len(parts) >= 2:
            first_initial = parts[0][0].upper()
            last = parts[-1]

            # "F. Lastname" format (most Discord picks)
            index[_normalize(f"{first_initial}. {last}")] = full_name

            # Handle multi-word surnames: "Pablo Carreno Busta" -> index "P. Busta"
            # and "Giovanni Mpetshi Perricard" -> index "G. Perricard"
            if len(parts) >= 3:
                for i in range(1, len(parts)):
                    partial_last = parts[i]
                    index[_normalize(f"{first_initial}. {partial_last}")] = full_name

            # Also index by just last name (for unique last names)
            norm_last = _normalize(last)
            if norm_last not in index:
                index[norm_last] = full_name

    return index


def fuzzy_lookup(pick_name, stats, _index_cache={}):
    """Look up a player from Discord pick name in stats dict.

    Returns the stats dict entry or None. Auto-upgrades Elo-only players
    by fetching their individual JS file for full stats.
    """
    if not pick_name or not stats:
        return None

    result = None
    matched_name = None

    # Exact match first
    if pick_name in stats:
        result = stats[pick_name]
        matched_name = pick_name
    else:
        # Build/get index (cached per stats id to avoid rebuilding)
        stats_id = id(stats)
        if stats_id not in _index_cache:
            _index_cache.clear()  # only keep one index at a time
            _index_cache[stats_id] = _build_name_index(stats)
        index = _index_cache[stats_id]

        norm_pick = _normalize(pick_name)

        # Try normalized lookup
        if norm_pick in index:
            matched_name = index[norm_pick]
            result = stats.get(matched_name)

        # Try last name of pick against index
        if not result:
            pick_parts = pick_name.replace('.', '').split()
            if len(pick_parts) >= 2:
                pick_last = _normalize(pick_parts[-1])
                pick_initial = pick_parts[0][0].upper()

                for full_name, s in stats.items():
                    full_parts = full_name.split()
                    if len(full_parts) < 2:
                        continue
                    full_initial = full_parts[0][0].upper()
                    if full_initial != pick_initial:
                        continue
                    for part in full_parts[1:]:
                        if _normalize(part) == pick_last:
                            result = s
                            matched_name = full_name
                            break
                    if result:
                        break

    # Auto-upgrade Elo-only players by fetching individual player JS
    if result and result.get('elo_only') and matched_name:
        result = upgrade_elo_only_player(matched_name, stats)

    return result


def _fetch_leadersource(url, cache_key):
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            logger.info(f"Using cached {cache_key} data")
            return cached_data

    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            logger.error(f"{cache_key} fetch failed: {resp.status_code}")
            return None

        text = resp.text
        match = re.search(r'var\s+matchmx\s*=\s*(\[.*?\])\s*;', text, re.DOTALL)
        if not match:
            logger.error(f"Could not find matchmx array in {cache_key}")
            return None

        raw = match.group(1)
        matchmx = json.loads(raw)
        logger.info(f"Parsed {len(matchmx)} match records from {cache_key}")
        _cache[cache_key] = (now, matchmx)
        return matchmx

    except Exception as e:
        logger.error(f"{cache_key} fetch error: {e}")
        return None


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _parse_match_data(matchmx, surface=None):
    players = {}

    for row in matchmx:
        if not row or len(row) < 45:
            continue

        player = row[IDX_PLAYER]
        if not player:
            continue

        # Surface filter
        if surface:
            surf = str(row[IDX_SURF]).lower() if row[IDX_SURF] else ''
            if surface.lower() not in surf:
                continue

        # Need serving stats to be meaningful
        pts = _safe_float(row[IDX_PTS])
        sgames = _safe_float(row[IDX_SGAMES])
        if pts == 0 or sgames == 0:
            continue

        if player not in players:
            players[player] = {
                'matches': 0,
                'fwon': 0, 'swon': 0, 'pts': 0,
                'firsts': 0, 'dfs': 0, 'sgames': 0,
                'saved': 0, 'chances': 0,
                'ofwon': 0, 'oswon': 0, 'opts': 0,
                'ofirsts': 0,
                'rgames': 0, 'osaved': 0, 'ochances': 0,
                'rank': None,
            }

        p = players[player]
        p['matches'] += 1
        p['fwon'] += _safe_float(row[IDX_FWON])
        p['swon'] += _safe_float(row[IDX_SWON])
        p['pts'] += pts
        p['firsts'] += _safe_float(row[IDX_FIRSTS])
        p['dfs'] += _safe_float(row[IDX_DFS])
        p['sgames'] += sgames
        p['saved'] += _safe_float(row[IDX_SAVED])
        p['chances'] += _safe_float(row[IDX_CHANCES])
        p['ofwon'] += _safe_float(row[IDX_OFWON])
        p['oswon'] += _safe_float(row[IDX_OSWON])
        p['opts'] += _safe_float(row[IDX_OPTS])
        p['ofirsts'] += _safe_float(row[IDX_OFIRSTS])
        p['rgames'] += _safe_float(row[IDX_RGAMES])
        p['osaved'] += _safe_float(row[IDX_OSAVED])
        p['ochances'] += _safe_float(row[IDX_OCHANCES])

        # Capture rank from most recent match
        rank = row[IDX_RANK]
        if rank and p['rank'] is None:
            try:
                p['rank'] = int(rank)
            except (TypeError, ValueError):
                pass

    # Compute derived stats
    results = {}
    for name, p in players.items():
        if p['matches'] < 3:
            continue

        sgames = p['sgames']
        rgames = p['rgames']
        pts = p['pts']
        opts = p['opts']

        if sgames == 0 or rgames == 0 or pts == 0:
            continue

        holds = sgames - (p['chances'] - p['saved'])
        hold_pct = holds / sgames

        bpconv = p['ochances'] - p['osaved']
        break_pct = bpconv / rgames

        net_rating = hold_pct - break_pct

        rpw = 1 - (p['ofwon'] + p['oswon']) / opts if opts > 0 else 0

        df_rate = p['dfs'] / pts

        bp_per_match = p['ochances'] / p['matches']

        # 1st Serve Points Won % (serve dominance quality)
        firsts = p['firsts']
        first_serve_won_pct = p['fwon'] / firsts if firsts > 0 else 0

        # 2nd Serve Points Won % (pressure tolerance)
        second_serves = pts - firsts
        second_serve_won_pct = p['swon'] / second_serves if second_serves > 0 else 0

        # Dominance Ratio: return pts won / serve pts lost
        serve_pts_lost = pts - (p['fwon'] + p['swon'])
        return_pts_won = opts - (p['ofwon'] + p['oswon']) if opts > 0 else 0
        dominance_ratio = return_pts_won / serve_pts_lost if serve_pts_lost > 0 else 0

        results[name] = {
            'hold_pct': round(hold_pct, 4),
            'break_pct': round(break_pct, 4),
            'net_rating': round(net_rating, 4),
            'rpw': round(rpw, 4),
            'first_serve_won': round(first_serve_won_pct, 4),
            'second_serve_won': round(second_serve_won_pct, 4),
            'dominance_ratio': round(dominance_ratio, 4),
            'df_rate': round(df_rate, 4),
            'bp_per_match': round(bp_per_match, 2),
            'matches': p['matches'],
            'rank': p['rank'],
        }

    return results


def _fetch_elo_ratings(url, cache_key):
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            logger.info(f"Using cached {cache_key}")
            return cached_data

    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.error(f"Elo ratings fetch failed: {resp.status_code}")
            return {}

        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='reportable')
        if not table:
            table = soup.find('table')
        if not table:
            logger.error("No table found in Elo ratings page")
            return {}

        headers = []
        header_row = table.find('tr')
        if header_row:
            for th in header_row.find_all(['th', 'td']):
                headers.append(th.get_text(strip=True))

        # Map column names to indices (normalize \xa0 in headers)
        col_map = {}
        for i, h in enumerate(headers):
            h = h.replace('\xa0', ' ').strip().lower()
            if h in ('player', 'name'):
                col_map['player'] = i
            elif h == 'elo':
                col_map['elo'] = i
            elif h in ('helo', 'hard', 'helo rank'):
                # helo column, not helo rank
                if 'rank' not in h:
                    col_map['hard_elo'] = i
            elif h in ('celo', 'clay'):
                if 'rank' not in h:
                    col_map['clay_elo'] = i
            elif h in ('gelo', 'grass'):
                if 'rank' not in h:
                    col_map['grass_elo'] = i

        if 'player' not in col_map:
            logger.error(f"Could not find player column. Headers: {headers}")
            return {}

        elo_data = {}
        rows = table.find_all('tr')[1:]  # skip header
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) <= col_map.get('player', 0):
                continue

            player_cell = cells[col_map['player']]
            # Player name might be in a link; normalize non-breaking spaces
            link = player_cell.find('a')
            player_name = link.get_text(strip=True) if link else player_cell.get_text(strip=True)
            player_name = player_name.replace('\xa0', ' ').strip()
            if not player_name:
                continue

            entry = {}
            for key in ('elo', 'hard_elo', 'clay_elo', 'grass_elo'):
                if key in col_map and col_map[key] < len(cells):
                    val = cells[col_map[key]].get_text(strip=True)
                    try:
                        entry[key] = float(val)
                    except (ValueError, TypeError):
                        entry[key] = None
                else:
                    entry[key] = None

            elo_data[player_name] = entry

        logger.info(f"Parsed Elo ratings for {len(elo_data)} players")
        _cache[cache_key] = (now, elo_data)
        return elo_data

    except Exception as e:
        logger.error(f"Elo ratings fetch error: {e}")
        return {}


def _fetch_player_stats(full_name, surface=None, tour=None):
    """Fetch and compute stats from an individual player's JS file or classic page."""
    cache_key = f'player_{full_name}'
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return cached_data

    # Build URL: "Paula Badosa" -> "PaulaBadosa.js"
    js_name = full_name.replace(' ', '').replace('-', '')

    # Try JS file first, then fall back to classic player page
    text = None
    for url in [
        PLAYER_JS_URL.format(name=js_name),
        (WTA_PLAYER_PAGE_URL if tour == 'WTA' else ATP_PLAYER_PAGE_URL).format(name=js_name),
        (ATP_PLAYER_PAGE_URL if tour == 'WTA' else WTA_PLAYER_PAGE_URL).format(name=js_name),
    ]:
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200 and 'var matchmx' in resp.text:
                text = resp.text
                break
        except Exception:
            continue

    if not text:
        _cache[cache_key] = (now, None)
        return None

    try:
        match = re.search(r'var\s+matchmx\s*=\s*(\[.*?\])\s*;', text, re.DOTALL)
        if not match:
            _cache[cache_key] = (now, None)
            return None

        matchmx = json.loads(match.group(1))

        # Extract rank from the JS metadata
        rank = None
        rank_match = re.search(r'var\s+currentrank\s*=\s*(\d+)', text)
        if rank_match:
            rank = int(rank_match.group(1))

        # Aggregate stats from per-player matchmx (different indices)
        agg = {
            'matches': 0, 'fwon': 0, 'swon': 0, 'pts': 0,
            'firsts': 0, 'dfs': 0, 'sgames': 0,
            'saved': 0, 'chances': 0,
            'ofwon': 0, 'oswon': 0, 'opts': 0,
            'ofirsts': 0, 'rgames': 0, 'osaved': 0, 'ochances': 0,
        }

        for row in matchmx:
            if not row or len(row) < 39:
                continue
            if surface:
                surf = str(row[PP_SURF]).lower() if row[PP_SURF] else ''
                if surface.lower() not in surf:
                    continue
            pts = _safe_float(row[PP_PTS])
            sgames = _safe_float(row[PP_SGAMES])
            if pts == 0 or sgames == 0:
                continue

            agg['matches'] += 1
            agg['fwon'] += _safe_float(row[PP_FWON])
            agg['swon'] += _safe_float(row[PP_SWON])
            agg['pts'] += pts
            agg['firsts'] += _safe_float(row[PP_FIRSTS])
            agg['dfs'] += _safe_float(row[PP_DFS])
            agg['sgames'] += sgames
            agg['saved'] += _safe_float(row[PP_SAVED])
            agg['chances'] += _safe_float(row[PP_CHANCES])
            agg['ofwon'] += _safe_float(row[PP_OFWON])
            agg['oswon'] += _safe_float(row[PP_OSWON])
            agg['opts'] += _safe_float(row[PP_OPTS])
            agg['ofirsts'] += _safe_float(row[PP_OFIRSTS])
            agg['rgames'] += _safe_float(row[PP_RGAMES])
            agg['osaved'] += _safe_float(row[PP_OSAVED])
            agg['ochances'] += _safe_float(row[PP_OCHANCES])

        if agg['matches'] < 3 or agg['sgames'] == 0 or agg['rgames'] == 0 or agg['pts'] == 0:
            _cache[cache_key] = (now, None)
            return None

        p = agg
        holds = p['sgames'] - (p['chances'] - p['saved'])
        hold_pct = holds / p['sgames']
        bpconv = p['ochances'] - p['osaved']
        break_pct = bpconv / p['rgames']
        net_rating = hold_pct - break_pct
        rpw = 1 - (p['ofwon'] + p['oswon']) / p['opts'] if p['opts'] > 0 else 0
        df_rate = p['dfs'] / p['pts']
        bp_per_match = p['ochances'] / p['matches']
        firsts = p['firsts']
        first_serve_won = p['fwon'] / firsts if firsts > 0 else 0
        second_serves = p['pts'] - firsts
        second_serve_won = p['swon'] / second_serves if second_serves > 0 else 0
        serve_pts_lost = p['pts'] - (p['fwon'] + p['swon'])
        return_pts_won = p['opts'] - (p['ofwon'] + p['oswon']) if p['opts'] > 0 else 0
        dominance_ratio = return_pts_won / serve_pts_lost if serve_pts_lost > 0 else 0

        result = {
            'hold_pct': round(hold_pct, 4),
            'break_pct': round(break_pct, 4),
            'net_rating': round(net_rating, 4),
            'rpw': round(rpw, 4),
            'first_serve_won': round(first_serve_won, 4),
            'second_serve_won': round(second_serve_won, 4),
            'dominance_ratio': round(dominance_ratio, 4),
            'df_rate': round(df_rate, 4),
            'bp_per_match': round(bp_per_match, 2),
            'matches': p['matches'],
            'rank': rank,
        }
        logger.info(f"Fetched individual stats for {full_name}: {p['matches']} matches")
        _cache[cache_key] = (now, result)
        return result

    except Exception as e:
        logger.error(f"Player fetch error for {full_name}: {e}")
        _cache[cache_key] = (now, None)
        return None


def _merge_elo(stats, elo_data):
    for player_name, player_stats in stats.items():
        if player_name in elo_data:
            player_stats.update(elo_data[player_name])
        else:
            last_name = player_name.split()[-1] if ' ' in player_name else player_name
            for elo_name, elo_vals in elo_data.items():
                if elo_name.split()[-1] == last_name and elo_name[0] == player_name[0]:
                    player_stats.update(elo_vals)
                    break
            else:
                player_stats['elo'] = None
                player_stats['hard_elo'] = None
                player_stats['clay_elo'] = None
                player_stats['grass_elo'] = None


def _add_elo_only_players(all_stats, elo_data, tour):
    """Add Elo-only entries for players not in leadersource top 50."""
    added = 0
    for elo_name, elo_vals in elo_data.items():
        if elo_name in all_stats:
            continue
        # Create a minimal entry with Elo data only
        entry = {
            'hold_pct': None, 'break_pct': None, 'net_rating': None,
            'rpw': None, 'first_serve_won': None, 'second_serve_won': None,
            'dominance_ratio': None, 'df_rate': None, 'bp_per_match': None,
            'matches': 0, 'rank': None, 'tour': tour, 'elo_only': True,
        }
        entry.update(elo_vals)
        all_stats[elo_name] = entry
        added += 1
    return added


def get_tennis_abstract_stats(surface=None):
    all_stats = {}

    # ATP
    atp_matches = _fetch_leadersource(ATP_LEADERSOURCE_URL, 'atp_leadersource')
    atp_elo = _fetch_elo_ratings(ATP_ELO_URL, 'atp_elo')
    if atp_matches:
        atp_stats = _parse_match_data(atp_matches, surface=surface)
        _merge_elo(atp_stats, atp_elo)
        for name, s in atp_stats.items():
            s['tour'] = 'ATP'
        all_stats.update(atp_stats)
        logger.info(f"ATP full stats: {len(atp_stats)} players")
    elo_added = _add_elo_only_players(all_stats, atp_elo, 'ATP')
    logger.info(f"ATP Elo-only: {elo_added} additional players")

    # WTA
    wta_matches = _fetch_leadersource(WTA_LEADERSOURCE_URL, 'wta_leadersource')
    wta_elo = _fetch_elo_ratings(WTA_ELO_URL, 'wta_elo')
    if wta_matches:
        wta_stats = _parse_match_data(wta_matches, surface=surface)
        _merge_elo(wta_stats, wta_elo)
        for name, s in wta_stats.items():
            s['tour'] = 'WTA'
        all_stats.update(wta_stats)
        logger.info(f"WTA full stats: {len(wta_stats)} players")
    elo_added = _add_elo_only_players(all_stats, wta_elo, 'WTA')
    logger.info(f"WTA Elo-only: {elo_added} additional players")

    logger.info(f"Tennis Abstract total: {len(all_stats)} players (ATP+WTA)")
    return all_stats


def upgrade_elo_only_player(player_name, all_stats):
    """Fetch full stats for an Elo-only player via their individual JS file.

    Called on-demand when a pick matches an Elo-only player.
    Upgrades the entry in-place if successful.
    """
    entry = all_stats.get(player_name)
    if not entry or not entry.get('elo_only'):
        return entry

    individual = _fetch_player_stats(player_name, tour=entry.get('tour'))
    if individual:
        tour = entry.get('tour', 'ATP')
        elo_data = {k: entry[k] for k in ('elo', 'hard_elo', 'clay_elo', 'grass_elo') if k in entry}
        entry.update(individual)
        entry.update(elo_data)  # preserve Elo
        entry['tour'] = tour
        entry['elo_only'] = False
        logger.info(f"Upgraded {player_name} from Elo-only to full stats")
    return entry


ATP_PROFILE_URL = 'https://www.tennisabstract.com/cgi-bin/player.cgi?p={name}'
WTA_PROFILE_URL = 'https://www.tennisabstract.com/cgi-bin/wplayer.cgi?p={name}'


def _scrape_player_profile(player_name, tour=None):
    cache_key = f'profile_{player_name}'
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return cached_data

    slug = player_name.replace(' ', '').replace('-', '')
    nfkd = unicodedata.normalize('NFKD', slug)
    slug = ''.join(c for c in nfkd if not unicodedata.combining(c))

    urls = []
    if tour == 'WTA':
        urls = [WTA_PROFILE_URL.format(name=slug), ATP_PROFILE_URL.format(name=slug)]
    elif tour == 'ATP':
        urls = [ATP_PROFILE_URL.format(name=slug), WTA_PROFILE_URL.format(name=slug)]
    else:
        urls = [ATP_PROFILE_URL.format(name=slug), WTA_PROFILE_URL.format(name=slug)]

    html = None
    for url in urls:
        try:
            resp = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
            if resp.status_code == 200 and ('Recent Results' in resp.text or 'Tour-Level' in resp.text):
                html = resp.text
                break
        except Exception:
            continue

    if not html:
        _cache[cache_key] = (now, None)
        return None

    try:
        soup = BeautifulSoup(html, 'html.parser')
        profile = _parse_profile_tables(soup, player_name)
        _cache[cache_key] = (now, profile)
        return profile
    except Exception as e:
        logger.error(f"Profile scrape error for {player_name}: {e}")
        _cache[cache_key] = (now, None)
        return None


def _parse_profile_tables(soup, player_name):
    result = {}

    tables = soup.find_all('table')

    for table in tables:
        rows = table.find_all('tr')
        if not rows:
            continue

        headers = []
        header_row = rows[0]
        for cell in header_row.find_all(['th', 'td']):
            headers.append(cell.get_text(strip=True).replace('\xa0', ' '))

        h_lower = [h.lower() for h in headers]

        if 'year' in h_lower and 'w' in h_lower and 'l' in h_lower and 'win%' in h_lower:
            result['seasons'] = _parse_season_table(rows[1:], headers, h_lower)
        elif 'date' in h_lower and 'tournament' in h_lower and 'score' in h_lower:
            if 'recent_results' not in result:
                result['recent_results'] = _parse_results_table(rows[1:], headers, h_lower)

    if result.get('seasons'):
        current_year = str(datetime.now().year)
        for s in result['seasons']:
            if s.get('year') == current_year:
                result['current_season'] = s
                break

        if result.get('recent_results'):
            last_n = result['recent_results'][:10]
            wins = sum(1 for r in last_n if r.get('won'))
            losses = len(last_n) - wins
            result['recent_form'] = {'wins': wins, 'losses': losses, 'matches': last_n}

    return result if result else None


def _parse_season_table(rows, headers, h_lower):
    seasons = []
    col = {name: h_lower.index(name) for name in ['year', 'w', 'l', 'win%', 'm'] if name in h_lower}

    opt_cols = {}
    for name in ['hld%', 'brk%', 'a%', 'df%', '1stin', '1st%', '2nd%', 'spw', 'rpw', 'tpw', 'dr', 'best']:
        if name in h_lower:
            opt_cols[name] = h_lower.index(name)

    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < len(col):
            continue

        year_text = cells[col.get('year', 0)].get_text(strip=True)
        if year_text.lower() in ('career', 'total'):
            year_text = 'Career'
        else:
            year_text = re.sub(r'[^\d]', '', year_text)
            if not year_text:
                continue

        def get_val(idx):
            if idx < len(cells):
                return cells[idx].get_text(strip=True).replace('%', '')
            return ''

        season = {
            'year': year_text,
            'matches': int(get_val(col.get('m', 0)) or 0) if col.get('m') is not None else 0,
            'wins': int(get_val(col.get('w', 0)) or 0) if col.get('w') is not None else 0,
            'losses': int(get_val(col.get('l', 0)) or 0) if col.get('l') is not None else 0,
        }

        win_pct_str = get_val(col.get('win%', 0)) if col.get('win%') is not None else ''
        try:
            season['win_pct'] = float(win_pct_str)
        except (ValueError, TypeError):
            season['win_pct'] = 0

        for name, idx in opt_cols.items():
            val = get_val(idx)
            clean_name = name.replace('%', '_pct').replace(' ', '_')
            try:
                season[clean_name] = float(val) if val else None
            except (ValueError, TypeError):
                season[clean_name] = None

        if name == 'best' and idx in opt_cols.values():
            season['best'] = get_val(opt_cols.get('best', 0))

        seasons.append(season)

    return seasons


def _parse_results_table(rows, headers, h_lower):
    results = []
    col = {}
    for name in ['date', 'tournament', 'surface', 'rd', 'rk', 'vrk', 'score']:
        if name in h_lower:
            col[name] = h_lower.index(name)

    desc_col = None
    for i, h in enumerate(h_lower):
        if h == '' and i > 3:
            desc_col = i
            break

    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 5:
            continue

        def get_val(name):
            if name in col and col[name] < len(cells):
                return cells[col[name]].get_text(strip=True)
            return ''

        match_desc = ''
        won = False
        if desc_col and desc_col < len(cells):
            desc_text = cells[desc_col].get_text(strip=True)
            match_desc = desc_text
            bold_tags = cells[desc_col].find_all('b')
            if bold_tags:
                bold_text = bold_tags[0].get_text(strip=True)
                if desc_text.startswith(bold_text) or 'd.' in desc_text.split(bold_text)[0] if bold_text in desc_text else False:
                    pass
            if ' d. ' in desc_text:
                parts = desc_text.split(' d. ')
                won = any('**' in str(cells[desc_col]) or cells[desc_col].find('b') is not None for _ in [0])
                bold = cells[desc_col].find('b')
                if bold:
                    bold_name = bold.get_text(strip=True)
                    won = desc_text.index(bold_name) < desc_text.index(' d. ') if bold_name in desc_text and ' d. ' in desc_text else False

        score = get_val('score')
        vrk = get_val('vrk')
        try:
            vrk_int = int(vrk)
        except (ValueError, TypeError):
            vrk_int = None

        entry = {
            'date': get_val('date'),
            'tournament': get_val('tournament'),
            'surface': get_val('surface'),
            'round': get_val('rd'),
            'rank': get_val('rk'),
            'opp_rank': vrk_int,
            'score': score,
            'won': won,
            'description': match_desc,
        }
        results.append(entry)

    return results


def fetch_player_profile_stats(player_names, tour_map=None):
    profiles = {}
    for name in player_names:
        tour = tour_map.get(name) if tour_map else None
        profile = _scrape_player_profile(name, tour=tour)
        if profile:
            profiles[name] = profile
            logger.info(f"Profile scraped for {name}: season={profile.get('current_season', {}).get('year', 'N/A')}")
    return profiles


def clear_tennis_cache():
    _cache.clear()
    logger.info("Tennis Abstract cache cleared")
