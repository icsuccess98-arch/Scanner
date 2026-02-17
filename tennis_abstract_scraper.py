import re
import json
import logging
import time
import unicodedata
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Elo rating pages — used for player name directory + surface Elo
ATP_ELO_URL = 'https://www.tennisabstract.com/reports/atp_elo_ratings.html'
WTA_ELO_URL = 'https://www.tennisabstract.com/reports/wta_elo_ratings.html'

# Individual player data sources (tried in order)
PLAYER_JS_URL = 'https://www.tennisabstract.com/jsmatches/{slug}.js'
ATP_CLASSIC_URL = 'https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={slug}'
WTA_CLASSIC_URL = 'https://www.tennisabstract.com/cgi-bin/wplayer-classic.cgi?p={slug}'

_cache = {}
CACHE_TTL = 3600  # 60 minutes

# Per-player page matchmx field indices
# matchhead: date,tourn,surf,level,wl,rank,seed,entry,round,score,max,opp,
#            orank,oseed,oentry,ohand,obday,oht,ocountry,oactive,time,
#            aces,dfs,pts,firsts,fwon,swon,games,saved,chances,
#            oaces,odfs,opts,ofirsts,ofwon,oswon,ogames,osaved,ochances
PP_SURF = 2
PP_RANK = 5
PP_DFS = 22
PP_PTS = 23
PP_FIRSTS = 24
PP_FWON = 25
PP_SWON = 26
PP_SGAMES = 27
PP_SAVED = 28
PP_CHANCES = 29
PP_OPTS = 32
PP_OFWON = 34
PP_OSWON = 35
PP_RGAMES = 36
PP_OSAVED = 37
PP_OCHANCES = 38


def _normalize(name):
    nfkd = unicodedata.normalize('NFKD', name)
    return ''.join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _build_name_index(stats):
    index = {}
    for full_name in stats:
        norm = _normalize(full_name)
        index[norm] = full_name

        parts = full_name.split()
        if len(parts) >= 2:
            first_initial = parts[0][0].upper()
            last = parts[-1]
            index[_normalize(f"{first_initial}. {last}")] = full_name

            if len(parts) >= 3:
                for i in range(1, len(parts)):
                    index[_normalize(f"{first_initial}. {parts[i]}")] = full_name

            norm_last = _normalize(last)
            if norm_last not in index:
                index[norm_last] = full_name

    return index


def fuzzy_lookup(pick_name, stats, _index_cache={}):
    if not pick_name or not stats:
        return None

    result = None
    matched_name = None

    if pick_name in stats:
        result = stats[pick_name]
        matched_name = pick_name
    else:
        stats_id = id(stats)
        if stats_id not in _index_cache:
            _index_cache.clear()
            _index_cache[stats_id] = _build_name_index(stats)
        index = _index_cache[stats_id]

        norm_pick = _normalize(pick_name)
        if norm_pick in index:
            matched_name = index[norm_pick]
            result = stats.get(matched_name)

        if not result:
            pick_parts = pick_name.replace('.', '').split()
            if len(pick_parts) >= 2:
                pick_last = _normalize(pick_parts[-1])
                pick_initial = pick_parts[0][0].upper()
                for full_name, s in stats.items():
                    full_parts = full_name.split()
                    if len(full_parts) < 2:
                        continue
                    if full_parts[0][0].upper() != pick_initial:
                        continue
                    for part in full_parts[1:]:
                        if _normalize(part) == pick_last:
                            result = s
                            matched_name = full_name
                            break
                    if result:
                        break

    # Auto-fetch stats from player page if we only have Elo
    if result and result.get('elo_only') and matched_name:
        result = _upgrade_player(matched_name, stats)

    return result


# ---------------------------------------------------------------------------
# Elo ratings — fetched once to build player directory + surface Elo
# ---------------------------------------------------------------------------

def _fetch_elo_ratings(url, cache_key):
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return cached_data

    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.error(f"Elo ratings fetch failed: {resp.status_code}")
            return {}

        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='reportable') or soup.find('table')
        if not table:
            return {}

        headers = []
        header_row = table.find('tr')
        if header_row:
            for th in header_row.find_all(['th', 'td']):
                headers.append(th.get_text(strip=True))

        col_map = {}
        for i, h in enumerate(headers):
            h = h.replace('\xa0', ' ').strip().lower()
            if h in ('player', 'name'):
                col_map['player'] = i
            elif h == 'elo':
                col_map['elo'] = i
            elif h in ('helo',) and 'rank' not in h:
                col_map['hard_elo'] = i
            elif h in ('celo',) and 'rank' not in h:
                col_map['clay_elo'] = i
            elif h in ('gelo',) and 'rank' not in h:
                col_map['grass_elo'] = i

        if 'player' not in col_map:
            logger.error(f"Could not find player column. Headers: {headers}")
            return {}

        elo_data = {}
        for row in table.find_all('tr')[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) <= col_map['player']:
                continue

            player_cell = cells[col_map['player']]
            link = player_cell.find('a')
            name = link.get_text(strip=True) if link else player_cell.get_text(strip=True)
            name = name.replace('\xa0', ' ').strip()
            if not name:
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
            elo_data[name] = entry

        logger.info(f"Parsed Elo ratings for {len(elo_data)} players from {cache_key}")
        _cache[cache_key] = (now, elo_data)
        return elo_data

    except Exception as e:
        logger.error(f"Elo ratings fetch error: {e}")
        return {}


# ---------------------------------------------------------------------------
# Individual player page stats — fetched on demand per player
# ---------------------------------------------------------------------------

def _fetch_player_stats(full_name, tour=None):
    cache_key = f'player_{full_name}'
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return cached_data

    slug = full_name.replace(' ', '').replace('-', '')
    nfkd = unicodedata.normalize('NFKD', slug)
    slug = ''.join(c for c in nfkd if not unicodedata.combining(c))

    # Try sources in order: JS file, then classic player page (tour-aware)
    urls = [PLAYER_JS_URL.format(slug=slug)]
    if tour == 'WTA':
        urls += [WTA_CLASSIC_URL.format(slug=slug), ATP_CLASSIC_URL.format(slug=slug)]
    else:
        urls += [ATP_CLASSIC_URL.format(slug=slug), WTA_CLASSIC_URL.format(slug=slug)]

    for url in urls:
        try:
            if 'cgi-bin' in url:
                time.sleep(0.5)
            for attempt in range(4):
                resp = requests.get(url, timeout=15)
                if resp.status_code == 429:
                    wait = 2 * (attempt + 1)
                    logger.debug(f"Rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                    continue
                break
            if resp.status_code != 200 or 'var matchmx' not in resp.text:
                continue
            text = resp.text
        except Exception:
            continue

        try:
            match = re.search(r'var\s+matchmx\s*=\s*(\[.*?\])\s*;', text, re.DOTALL)
            if not match:
                continue

            matchmx = json.loads(match.group(1))

            rank = None
            rank_match = re.search(r'var\s+currentrank\s*=\s*(\d+)', text)
            if rank_match:
                rank = int(rank_match.group(1))

            # Aggregate counting stats across all matches
            agg = {k: 0 for k in ('matches', 'fwon', 'swon', 'pts', 'firsts', 'dfs',
                                   'sgames', 'saved', 'chances', 'ofwon', 'oswon',
                                   'opts', 'rgames', 'osaved', 'ochances')}

            for row in matchmx:
                if not row or len(row) < 39:
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
                agg['rgames'] += _safe_float(row[PP_RGAMES])
                agg['osaved'] += _safe_float(row[PP_OSAVED])
                agg['ochances'] += _safe_float(row[PP_OCHANCES])

            if agg['matches'] < 3 or agg['sgames'] == 0 or agg['rgames'] == 0 or agg['pts'] == 0:
                logger.debug(f"Skipping {url}: only {agg['matches']} valid matches")
                continue

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
            logger.info(f"Fetched stats for {full_name}: {p['matches']} matches")
            _cache[cache_key] = (now, result)
            return result

        except Exception as e:
            logger.error(f"Player fetch error for {full_name} from {url}: {e}")
            continue

    _cache[cache_key] = (now, None)
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _upgrade_player(player_name, all_stats):
    """Fetch full stats from player page for an Elo-only entry."""
    entry = all_stats.get(player_name)
    if not entry or not entry.get('elo_only'):
        return entry

    individual = _fetch_player_stats(player_name, tour=entry.get('tour'))
    if individual:
        tour = entry.get('tour', 'ATP')
        elo_data = {k: entry[k] for k in ('elo', 'hard_elo', 'clay_elo', 'grass_elo') if k in entry}
        entry.update(individual)
        entry.update(elo_data)
        entry['tour'] = tour
        entry['elo_only'] = False
        logger.info(f"Upgraded {player_name} to full stats from player page")
    return entry


def get_tennis_abstract_stats(surface=None):
    """Build player directory from Elo pages. Individual stats fetched on demand."""
    all_stats = {}

    # Fetch Elo pages for player names + surface Elo ratings
    for elo_url, cache_key, tour in [
        (ATP_ELO_URL, 'atp_elo', 'ATP'),
        (WTA_ELO_URL, 'wta_elo', 'WTA'),
    ]:
        elo_data = _fetch_elo_ratings(elo_url, cache_key)
        for name, elo_vals in elo_data.items():
            entry = {
                'hold_pct': None, 'break_pct': None, 'net_rating': None,
                'rpw': None, 'first_serve_won': None, 'second_serve_won': None,
                'dominance_ratio': None, 'df_rate': None, 'bp_per_match': None,
                'matches': 0, 'rank': None, 'tour': tour, 'elo_only': True,
            }
            entry.update(elo_vals)
            all_stats[name] = entry

    logger.info(f"Tennis Abstract directory: {len(all_stats)} players (ATP+WTA)")
    return all_stats


def _scrape_tournaments():
    """Scrape TA tournament pages. Returns {matchups: {}, draws: []}."""
    cache_key = 'tournament_data'
    now = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        cached_time, cached_data = _cache[cache_key]
        if now - cached_time < CACHE_TTL:
            return cached_data

    matchups = {}
    draws = []

    try:
        resp = requests.get('https://www.tennisabstract.com', timeout=15)
        if resp.status_code != 200:
            logger.error(f"TA homepage fetch failed: {resp.status_code}")
            result = {'matchups': matchups, 'draws': draws}
            _cache[cache_key] = (now, result)
            return result

        tournament_urls = set()
        for m in re.finditer(
            r'href="(https://www\.tennisabstract\.com/current/\d{4}(?:ATP|WTA)[^"]+\.html)"',
            resp.text
        ):
            tournament_urls.add(m.group(1))
        for m in re.finditer(r'href="(/current/\d{4}(?:ATP|WTA)[^"]+\.html)"', resp.text):
            tournament_urls.add(f'https://www.tennisabstract.com{m.group(1)}')

        logger.info(f"Found {len(tournament_urls)} current tournament pages")

        for url in tournament_urls:
            try:
                tresp = requests.get(url, timeout=15)
                if tresp.status_code != 200:
                    continue

                page = tresp.text

                # Extract tournament name from URL
                url_match = re.search(r'/current/\d{4}(ATP|WTA)(.+?)\.html', url)
                if url_match:
                    tour = url_match.group(1)
                    raw_name = url_match.group(2)
                    tourn_name = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', raw_name)
                else:
                    tour = 'ATP'
                    tourn_name = 'Unknown'

                tournament = {
                    'name': tourn_name,
                    'tour': tour,
                    'matchups': [],
                }

                for var_name, status in [('upcomingSingles', 'upcoming'), ('completedSingles', 'completed')]:
                    var_match = re.search(
                        rf"var\s+{var_name}\s*=\s*'(.*?)';",
                        page, re.DOTALL
                    )
                    if not var_match:
                        continue

                    html_content = var_match.group(1)
                    lines = html_content.split('<br/>')

                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue

                        # Extract round label
                        round_match = re.match(r'([A-Za-z0-9]+):', line)
                        round_label = round_match.group(1) if round_match else ''

                        soup = BeautifulSoup(line, 'html.parser')
                        links = soup.find_all('a')

                        players = []
                        for a in links:
                            text = a.get_text(strip=True)
                            if not text or text == 'd.' or re.match(r'^\[[\d-]+\]$', text):
                                continue
                            players.append(text)

                        if len(players) >= 2:
                            p1, p2 = players[0], players[1]
                            matchups[p1] = p2
                            matchups[p2] = p1

                            entry = {
                                'round': round_label,
                                'player1': p1,
                                'player2': p2,
                                'status': status,
                            }
                            if status == 'completed':
                                entry['winner'] = p1
                            tournament['matchups'].append(entry)

                if tournament['matchups']:
                    draws.append(tournament)

            except Exception as e:
                logger.debug(f"Error parsing tournament {url}: {e}")
                continue

    except Exception as e:
        logger.error(f"Matchup scraping error: {e}")

    logger.info(f"Scraped {len(matchups) // 2} matchups from {len(draws)} tournaments")
    result = {'matchups': matchups, 'draws': draws}
    _cache[cache_key] = (now, result)
    return result


def get_current_matchups():
    """Get flat {player_name: opponent_name} dict for opponent lookups."""
    return _scrape_tournaments()['matchups']


def get_tournament_draws():
    """Get structured tournament draws: [{name, tour, matchups: [{round, player1, player2, status}]}]."""
    return _scrape_tournaments()['draws']


def clear_tennis_cache():
    _cache.clear()
    logger.info("Tennis Abstract cache cleared")
