import requests
import logging
import re
import time
from datetime import datetime, date
from bs4 import BeautifulSoup
from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

_cache = TTLCache(maxsize=100, ttl=300)
_rankings_cache = TTLCache(maxsize=10, ttl=3600)

ESPN_ATP_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard'
ESPN_WTA_SCOREBOARD = 'https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard'
ESPN_PLAYER_SEARCH = 'https://site.web.api.espn.com/apis/common/v3/search'
ESPN_PLAYER_RESULTS = 'https://www.espn.com/tennis/player/results/_/id/{player_id}'
ATP_RANKINGS_URL = 'https://www.atptour.com/en/rankings/singles'
WTA_RANKINGS_URL = 'https://www.wtatennis.com/rankings/singles'


def get_todays_matches():
    cache_key = f'tennis_matches_{date.today()}'
    if cache_key in _cache:
        return _cache[cache_key]

    matches = []
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            atp_future = executor.submit(_fetch_espn_matches, 'atp', ESPN_ATP_SCOREBOARD)
            wta_future = executor.submit(_fetch_espn_matches, 'wta', ESPN_WTA_SCOREBOARD)
            atp_matches = atp_future.result(timeout=15)
            wta_matches = wta_future.result(timeout=15)
            matches = atp_matches + wta_matches
        logger.info(f"Tennis: Fetched {len(matches)} matches (ATP: {len(atp_matches)}, WTA: {len(wta_matches)})")
    except Exception as e:
        logger.error(f"Error fetching tennis matches: {e}")

    _cache[cache_key] = matches
    return matches


def _fetch_espn_matches(tour, url):
    matches = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        data = resp.json()
        events = data.get('events', [])
        for evt in events:
            tournament_name = evt.get('name', 'Unknown Tournament')
            for grp in evt.get('groupings', []):
                slug = grp.get('grouping', {}).get('slug', '')
                if 'singles' not in slug.lower():
                    continue
                gender = 'WTA' if tour == 'wta' else 'ATP'
                for comp in grp.get('competitions', []):
                    try:
                        status_info = comp.get('status', {}).get('type', {})
                        status = status_info.get('description', 'Unknown')
                        state = status_info.get('state', '')
                        competitors = comp.get('competitors', [])
                        if len(competitors) < 2:
                            continue

                        p1_data = competitors[0]
                        p2_data = competitors[1]
                        p1_athlete = p1_data.get('athlete', {})
                        p2_athlete = p2_data.get('athlete', {})

                        p1_name = p1_athlete.get('displayName', 'Unknown')
                        p2_name = p2_athlete.get('displayName', 'Unknown')
                        p1_id = p1_data.get('id') or p1_athlete.get('id', '')
                        p2_id = p2_data.get('id') or p2_athlete.get('id', '')
                        p1_country = p1_athlete.get('flag', {}).get('alt', '')
                        p2_country = p2_athlete.get('flag', {}).get('alt', '')
                        p1_seed = p1_data.get('seed', '')
                        p2_seed = p2_data.get('seed', '')

                        p1_sets = [ls.get('value', 0) for ls in p1_data.get('linescores', [])]
                        p2_sets = [ls.get('value', 0) for ls in p2_data.get('linescores', [])]

                        p1_winner = p1_data.get('winner', False)
                        p2_winner = p2_data.get('winner', False)

                        score_text = ''
                        notes = comp.get('notes', [])
                        if notes:
                            score_text = notes[0].get('text', '')

                        match_time = comp.get('date', '')
                        venue = comp.get('venue', {}).get('fullName', '')
                        court = comp.get('venue', {}).get('court', '')
                        round_name = ''
                        for note in notes:
                            if note.get('type') == 'round':
                                round_name = note.get('text', '')

                        set_totals = []
                        for i in range(max(len(p1_sets), len(p2_sets))):
                            s1 = int(p1_sets[i]) if i < len(p1_sets) else 0
                            s2 = int(p2_sets[i]) if i < len(p2_sets) else 0
                            set_totals.append(s1 + s2)

                        match = {
                            'tour': gender,
                            'tournament': tournament_name,
                            'venue': venue,
                            'court': court,
                            'round': round_name,
                            'status': status,
                            'state': state,
                            'completed': status_info.get('completed', False),
                            'match_time': match_time,
                            'player1': {
                                'name': p1_name,
                                'id': str(p1_id),
                                'country': p1_country,
                                'seed': str(p1_seed) if p1_seed else '',
                                'sets': p1_sets,
                                'winner': p1_winner,
                            },
                            'player2': {
                                'name': p2_name,
                                'id': str(p2_id),
                                'country': p2_country,
                                'seed': str(p2_seed) if p2_seed else '',
                                'sets': p2_sets,
                                'winner': p2_winner,
                            },
                            'set_totals': set_totals,
                            'score_text': score_text,
                        }
                        matches.append(match)
                    except Exception as e:
                        logger.debug(f"Error parsing tennis match: {e}")
                        continue
    except Exception as e:
        logger.error(f"Error fetching {tour} matches: {e}")
    return matches


def get_player_last10(player_name, player_id=None, tour='atp'):
    cache_key = f'tennis_l10_{player_name}'
    if cache_key in _cache:
        return _cache[cache_key]

    if not player_id:
        player_id = _search_espn_player(player_name, tour)

    if not player_id:
        logger.warning(f"Could not find ESPN player ID for {player_name}")
        return None

    results = _scrape_espn_player_results(player_id, player_name, tour)
    if results:
        _cache[cache_key] = results
    return results


def _search_espn_player(name, tour='atp'):
    try:
        resp = requests.get(ESPN_PLAYER_SEARCH, params={
            'query': name,
            'type': 'player',
            'sport': 'tennis',
            'limit': 5
        }, headers=HEADERS, timeout=10)
        data = resp.json()
        items = data.get('items', [])
        for item in items:
            if item.get('type') == 'player':
                return item.get('id')
    except Exception as e:
        logger.error(f"Error searching ESPN for {name}: {e}")
    return None


def _scrape_espn_player_results(player_id, player_name, tour='atp'):
    try:
        url = ESPN_PLAYER_RESULTS.format(player_id=player_id)
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, 'html.parser')
        tables = soup.find_all('table')

        singles_matches = []
        current_tournament = ''
        is_singles = False
        skip_table_indices = set()

        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue

            header_cells = rows[0].find_all(['th', 'td'])
            header_text = ' '.join(c.get_text(strip=True) for c in header_cells).upper()
            if 'ROUND' not in header_text or 'OPPONENT' not in header_text:
                continue

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                cell_texts = [c.get_text(strip=True) for c in cells]

                if len(cells) == 1:
                    section_text = cell_texts[0]
                    if "Men's Singles" in section_text or "Women's Singles" in section_text:
                        is_singles = True
                    elif "Doubles" in section_text:
                        is_singles = False
                    continue

                if not is_singles:
                    continue

                if len(cells) >= 4:
                    round_name = cell_texts[0]
                    opponent = cell_texts[1]
                    result = cell_texts[2]
                    score = cell_texts[3]

                    if result not in ('W', 'L') or not score or 'BYE' in score.upper():
                        continue

                    sets = _parse_set_scores(score)
                    if not sets:
                        continue

                    set_totals = [s['p1'] + s['p2'] for s in sets]
                    first_set_total = set_totals[0] if set_totals else 0
                    second_set_total = set_totals[1] if len(set_totals) > 1 else 0

                    match = {
                        'round': round_name,
                        'opponent': opponent.lstrip('(').split(')')[-1] if ')' in opponent else opponent,
                        'opponent_seed': '',
                        'result': result,
                        'score': score,
                        'sets': sets,
                        'set_totals': set_totals,
                        'first_set_total': first_set_total,
                        'second_set_total': second_set_total,
                        'total_games': sum(set_totals),
                        'won': result == 'W',
                    }
                    seed_match = re.match(r'\((\d+)\)', opponent)
                    if seed_match:
                        match['opponent_seed'] = seed_match.group(1)

                    singles_matches.append(match)

                    if len(singles_matches) >= 10:
                        break

            if len(singles_matches) >= 10:
                break

        if not singles_matches:
            return None

        last10 = singles_matches[:10]
        wins = sum(1 for m in last10 if m['won'])
        losses = len(last10) - wins
        avg_first_set = sum(m['first_set_total'] for m in last10) / len(last10) if last10 else 0
        avg_second_set = sum(m['second_set_total'] for m in last10 if m['second_set_total'] > 0)
        count_second = sum(1 for m in last10 if m['second_set_total'] > 0)
        avg_second_set = avg_second_set / count_second if count_second else 0

        over_105_first = sum(1 for m in last10 if m['first_set_total'] > 10.5)
        under_105_first = sum(1 for m in last10 if m['first_set_total'] <= 10.5)
        over_95_first = sum(1 for m in last10 if m['first_set_total'] > 9.5)
        under_95_first = sum(1 for m in last10 if m['first_set_total'] <= 9.5)

        over_105_second = sum(1 for m in last10 if m['second_set_total'] > 10.5)
        under_105_second = sum(1 for m in last10 if m['second_set_total'] <= 10.5)

        return {
            'player': player_name,
            'matches': last10,
            'record': f"{wins}-{losses}",
            'wins': wins,
            'losses': losses,
            'avg_first_set_total': round(avg_first_set, 1),
            'avg_second_set_total': round(avg_second_set, 1),
            'first_set_over_105': over_105_first,
            'first_set_under_105': under_105_first,
            'first_set_over_95': over_95_first,
            'first_set_under_95': under_95_first,
            'second_set_over_105': over_105_second,
            'second_set_under_105': under_105_second,
            'form': ''.join('W' if m['won'] else 'L' for m in last10),
        }
    except Exception as e:
        logger.error(f"Error scraping ESPN results for {player_name}: {e}")
        return None


def _parse_set_scores(score_str):
    sets = []
    score_str = score_str.replace('\xa0', ' ').strip()
    parts = re.split(r'[,\s]+', score_str)
    for part in parts:
        part = part.strip()
        match = re.match(r'(\d+)-(\d+)', part)
        if match:
            p1 = int(match.group(1))
            p2 = int(match.group(2))
            sets.append({'p1': p1, 'p2': p2})
    return sets


def get_rankings(tour='atp'):
    cache_key = f'tennis_rankings_{tour}'
    if cache_key in _rankings_cache:
        return _rankings_cache[cache_key]

    rankings = {}
    try:
        if tour == 'atp':
            rankings = _scrape_atp_rankings()
        else:
            rankings = _scrape_wta_rankings()
        if rankings:
            _rankings_cache[cache_key] = rankings
            logger.info(f"Tennis: Loaded {len(rankings)} {tour.upper()} rankings")
    except Exception as e:
        logger.error(f"Error fetching {tour} rankings: {e}")
    return rankings


def _scrape_atp_rankings():
    rankings = {}
    try:
        resp = requests.get(ATP_RANKINGS_URL, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, 'html.parser')
        tables = soup.find_all('table')
        target_table = tables[-1] if len(tables) > 1 else tables[0] if tables else None
        if not target_table:
            return rankings
        rows = target_table.find_all('tr')
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 4:
                rank_text = cells[0].get_text(strip=True)
                name_text = cells[1].get_text(strip=True)
                points_text = cells[3].get_text(strip=True).replace(',', '')
                try:
                    rank = int(rank_text)
                    name_clean = re.sub(r'^\d+', '', name_text).strip()
                    name_clean = re.sub(r'\s+', ' ', name_clean).strip()
                    rankings[name_clean.lower()] = {
                        'rank': rank,
                        'name': name_clean,
                        'points': points_text,
                    }
                except (ValueError, IndexError):
                    continue
    except Exception as e:
        logger.error(f"Error scraping ATP rankings: {e}")
    return rankings


def _scrape_wta_rankings():
    rankings = {}
    try:
        resp = requests.get(WTA_RANKINGS_URL, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, 'html.parser')
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 5:
                    rank_text = cells[0].get_text(strip=True).rstrip('-')
                    name_raw = cells[1].get_text(strip=True)
                    points_text = cells[4].get_text(strip=True).replace(',', '')
                    try:
                        rank = int(rank_text)
                        name_clean = re.sub(r'^\d+', '', name_raw).strip()
                        name_clean = re.sub(r'[A-Z]{2,3}$', '', name_clean).strip()
                        name_clean = re.sub(r'\s+', ' ', name_clean)
                        rankings[name_clean.lower()] = {
                            'rank': rank,
                            'name': name_clean,
                            'points': points_text,
                        }
                    except (ValueError, IndexError):
                        continue
    except Exception as e:
        logger.error(f"Error scraping WTA rankings: {e}")
    return rankings


def get_player_ranking(player_name, tour='atp'):
    rankings = get_rankings(tour)
    name_lower = player_name.lower().strip()

    if name_lower in rankings:
        return rankings[name_lower]

    for key, val in rankings.items():
        if name_lower in key or key in name_lower:
            return val
        name_parts = name_lower.split()
        if len(name_parts) >= 2:
            last_name = name_parts[-1]
            if last_name in key:
                return val

    return None


def analyze_matchup(match):
    tour = match.get('tour', 'ATP')
    p1 = match['player1']
    p2 = match['player2']
    p1_name = p1['name']
    p2_name = p2['name']

    tour_lower = tour.lower()

    with ThreadPoolExecutor(max_workers=4) as executor:
        p1_l10_future = executor.submit(get_player_last10, p1_name, p1.get('id'), tour_lower)
        p2_l10_future = executor.submit(get_player_last10, p2_name, p2.get('id'), tour_lower)
        p1_rank_future = executor.submit(get_player_ranking, p1_name, tour_lower)
        p2_rank_future = executor.submit(get_player_ranking, p2_name, tour_lower)

        p1_l10 = p1_l10_future.result(timeout=20)
        p2_l10 = p2_l10_future.result(timeout=20)
        p1_rank = p1_rank_future.result(timeout=20)
        p2_rank = p2_rank_future.result(timeout=20)

    analysis = {
        'player1_name': p1_name,
        'player2_name': p2_name,
        'player1_l10': p1_l10,
        'player2_l10': p2_l10,
        'player1_rank': p1_rank,
        'player2_rank': p2_rank,
        'tour': tour,
        'tournament': match.get('tournament', ''),
        'recommendations': [],
    }

    if p1_l10 and p2_l10:
        combined_avg_first = (p1_l10['avg_first_set_total'] + p2_l10['avg_first_set_total']) / 2
        combined_avg_second = (p1_l10['avg_second_set_total'] + p2_l10['avg_second_set_total']) / 2
        analysis['combined_avg_first_set'] = round(combined_avg_first, 1)
        analysis['combined_avg_second_set'] = round(combined_avg_second, 1)

        p1_over_pct_first = p1_l10['first_set_over_95'] / 10 * 100 if p1_l10.get('first_set_over_95') is not None else 50
        p2_over_pct_first = p2_l10['first_set_over_95'] / 10 * 100 if p2_l10.get('first_set_over_95') is not None else 50
        combined_over_pct = (p1_over_pct_first + p2_over_pct_first) / 2

        if combined_avg_first >= 10.0:
            confidence = min(95, int(combined_over_pct))
            analysis['recommendations'].append({
                'bet': 'OVER 9.5 1st Set',
                'confidence': confidence,
                'reasoning': f"Combined avg 1st set: {combined_avg_first:.1f} games. Both players trending high.",
                'type': 'over'
            })
        if combined_avg_first <= 10.0:
            under_pct = 100 - combined_over_pct
            confidence = min(95, int(under_pct))
            analysis['recommendations'].append({
                'bet': 'UNDER 10.5 1st Set',
                'confidence': confidence,
                'reasoning': f"Combined avg 1st set: {combined_avg_first:.1f} games. Both players trending low.",
                'type': 'under'
            })

        if tour == 'WTA' and combined_avg_first <= 10.0:
            p1_u105 = p1_l10.get('first_set_under_105', 0)
            p2_u105 = p2_l10.get('first_set_under_105', 0)
            combined_u = p1_u105 + p2_u105
            if combined_u >= 12:
                analysis['recommendations'].append({
                    'bet': 'UNDER 10.5 1st Set (WTA Special)',
                    'confidence': min(95, int(combined_u / 20 * 100)),
                    'reasoning': f"WTA optimizer: P1 under 10.5 in {p1_u105}/10, P2 in {p2_u105}/10 of last games.",
                    'type': 'wta_under'
                })

        rank_diff = 0
        p1_r = p1_rank.get('rank', 999) if p1_rank else 999
        p2_r = p2_rank.get('rank', 999) if p2_rank else 999
        rank_diff = abs(p1_r - p2_r)
        analysis['rank_diff'] = rank_diff
        analysis['rank_advantage'] = p1_name if p1_r < p2_r else p2_name
        analysis['rank_advantage_value'] = min(p1_r, p2_r)

        if rank_diff > 50 and min(p1_r, p2_r) <= 100:
            favored = p1_name if p1_r < p2_r else p2_name
            analysis['recommendations'].append({
                'bet': f'{favored} ML (Rank Advantage)',
                'confidence': min(90, 50 + rank_diff // 10),
                'reasoning': f"Significant rank gap: #{p1_r} vs #{p2_r} ({rank_diff} positions apart).",
                'type': 'ml'
            })

    return analysis


def run_batch_analysis(matches=None, women_only=False):
    if matches is None:
        matches = get_todays_matches()

    if women_only:
        matches = [m for m in matches if m.get('tour') == 'WTA']

    scheduled = [m for m in matches if m.get('state') == 'pre' or not m.get('completed', False)]

    if not scheduled:
        scheduled = matches[:20]

    analyses = []
    for match in scheduled[:30]:
        try:
            analysis = analyze_matchup(match)
            if analysis and analysis.get('player1_l10') and analysis.get('player2_l10'):
                analyses.append(analysis)
        except Exception as e:
            logger.error(f"Error analyzing {match.get('player1',{}).get('name','?')} vs {match.get('player2',{}).get('name','?')}: {e}")
            continue
        time.sleep(0.5)

    analyses.sort(key=lambda x: max((r.get('confidence', 0) for r in x.get('recommendations', [{'confidence': 0}])), default=0), reverse=True)

    return {
        'analyses': analyses,
        'total_matches': len(scheduled),
        'analyzed': len(analyses),
        'women_only': women_only,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
    }


def get_wta_under_optimizer(matches=None):
    if matches is None:
        matches = get_todays_matches()

    wta_matches = [m for m in matches if m.get('tour') == 'WTA']
    scheduled = [m for m in wta_matches if m.get('state') == 'pre' or not m.get('completed', False)]

    if not scheduled:
        scheduled = wta_matches[:15]

    picks = []
    for match in scheduled:
        try:
            analysis = analyze_matchup(match)
            if not analysis or not analysis.get('player1_l10') or not analysis.get('player2_l10'):
                continue

            p1_l10 = analysis['player1_l10']
            p2_l10 = analysis['player2_l10']

            p1_u105 = p1_l10.get('first_set_under_105', 0)
            p2_u105 = p2_l10.get('first_set_under_105', 0)
            combined_avg = analysis.get('combined_avg_first_set', 12)

            score = 0
            if p1_u105 >= 6:
                score += 2
            elif p1_u105 >= 5:
                score += 1

            if p2_u105 >= 6:
                score += 2
            elif p2_u105 >= 5:
                score += 1

            if combined_avg <= 10.0:
                score += 2
            elif combined_avg <= 10.5:
                score += 1

            if combined_avg <= 9.5:
                score += 1

            if score >= 3:
                picks.append({
                    'match': f"{analysis['player1_name']} vs {analysis['player2_name']}",
                    'tournament': analysis.get('tournament', ''),
                    'score': score,
                    'combined_avg': combined_avg,
                    'p1_under_rate': f"{p1_u105}/10",
                    'p2_under_rate': f"{p2_u105}/10",
                    'p1_avg_first': p1_l10['avg_first_set_total'],
                    'p2_avg_first': p2_l10['avg_first_set_total'],
                    'confidence': min(95, score * 15 + 20),
                    'analysis': analysis,
                })
        except Exception as e:
            logger.error(f"Error in WTA optimizer: {e}")
            continue
        time.sleep(0.5)

    picks.sort(key=lambda x: x['score'], reverse=True)
    return picks
