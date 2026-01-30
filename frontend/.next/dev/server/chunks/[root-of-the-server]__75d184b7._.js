module.exports = [
"[externals]/next/dist/compiled/next-server/app-route-turbo.runtime.dev.js [external] (next/dist/compiled/next-server/app-route-turbo.runtime.dev.js, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/compiled/next-server/app-route-turbo.runtime.dev.js", () => require("next/dist/compiled/next-server/app-route-turbo.runtime.dev.js"));

module.exports = mod;
}),
"[externals]/next/dist/compiled/@opentelemetry/api [external] (next/dist/compiled/@opentelemetry/api, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/compiled/@opentelemetry/api", () => require("next/dist/compiled/@opentelemetry/api"));

module.exports = mod;
}),
"[externals]/next/dist/compiled/next-server/app-page-turbo.runtime.dev.js [external] (next/dist/compiled/next-server/app-page-turbo.runtime.dev.js, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/compiled/next-server/app-page-turbo.runtime.dev.js", () => require("next/dist/compiled/next-server/app-page-turbo.runtime.dev.js"));

module.exports = mod;
}),
"[externals]/next/dist/server/app-render/work-unit-async-storage.external.js [external] (next/dist/server/app-render/work-unit-async-storage.external.js, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/server/app-render/work-unit-async-storage.external.js", () => require("next/dist/server/app-render/work-unit-async-storage.external.js"));

module.exports = mod;
}),
"[externals]/next/dist/server/app-render/work-async-storage.external.js [external] (next/dist/server/app-render/work-async-storage.external.js, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/server/app-render/work-async-storage.external.js", () => require("next/dist/server/app-render/work-async-storage.external.js"));

module.exports = mod;
}),
"[externals]/next/dist/shared/lib/no-fallback-error.external.js [external] (next/dist/shared/lib/no-fallback-error.external.js, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/shared/lib/no-fallback-error.external.js", () => require("next/dist/shared/lib/no-fallback-error.external.js"));

module.exports = mod;
}),
"[externals]/next/dist/server/app-render/after-task-async-storage.external.js [external] (next/dist/server/app-render/after-task-async-storage.external.js, cjs)", ((__turbopack_context__, module, exports) => {

const mod = __turbopack_context__.x("next/dist/server/app-render/after-task-async-storage.external.js", () => require("next/dist/server/app-render/after-task-async-storage.external.js"));

module.exports = mod;
}),
"[project]/src/lib/constants.ts [app-route] (ecmascript)", ((__turbopack_context__) => {
"use strict";

__turbopack_context__.s([
    "COLORS",
    ()=>COLORS,
    "CONFIDENCE_TIERS",
    ()=>CONFIDENCE_TIERS,
    "EDGE_THRESHOLDS",
    ()=>EDGE_THRESHOLDS,
    "HISTORY_THRESHOLDS",
    ()=>HISTORY_THRESHOLDS,
    "MIN_GAMES",
    ()=>MIN_GAMES,
    "NBA_TEAM_ABBR",
    ()=>NBA_TEAM_ABBR,
    "NBA_TEAM_LOGOS",
    ()=>NBA_TEAM_LOGOS,
    "NHL_TEAM_LOGOS",
    ()=>NHL_TEAM_LOGOS,
    "POLLING_INTERVALS",
    ()=>POLLING_INTERVALS,
    "getTeamLogo",
    ()=>getTeamLogo
]);
const COLORS = {
    dark: '#0a0a12',
    card: '#12101a',
    border: '#2d2640',
    gold: '#FFD700',
    goldLight: '#FFEC8B',
    purple: '#7B2CBF',
    purpleLight: '#9D4EDD',
    green: '#22c55e',
    red: '#ef4444',
    gray: '#6b7280'
};
const POLLING_INTERVALS = {
    liveScores: 2500,
    dashboard: 30000
};
const EDGE_THRESHOLDS = {
    NBA: 8.0,
    CBB: 8.0,
    NFL: 3.5,
    CFB: 3.5,
    NHL: 0.5
};
const MIN_GAMES = {
    NBA: 8,
    CBB: 8,
    NFL: 4,
    CFB: 4,
    NHL: 8
};
const CONFIDENCE_TIERS = {
    SUPERMAX_EDGE: 12.0,
    HIGH_EDGE: 10.0,
    MEDIUM_EDGE: 8.0
};
const HISTORY_THRESHOLDS = {
    QUALIFY_RATE: 0.60,
    SUPERMAX_RATE: 0.70,
    HIGH_RATE: 0.65
};
const NBA_TEAM_LOGOS = {
    Hawks: 'https://a.espncdn.com/i/teamlogos/nba/500/atl.png',
    Celtics: 'https://a.espncdn.com/i/teamlogos/nba/500/bos.png',
    Nets: 'https://a.espncdn.com/i/teamlogos/nba/500/bkn.png',
    Hornets: 'https://a.espncdn.com/i/teamlogos/nba/500/cha.png',
    Bulls: 'https://a.espncdn.com/i/teamlogos/nba/500/chi.png',
    Cavaliers: 'https://a.espncdn.com/i/teamlogos/nba/500/cle.png',
    Mavericks: 'https://a.espncdn.com/i/teamlogos/nba/500/dal.png',
    Nuggets: 'https://a.espncdn.com/i/teamlogos/nba/500/den.png',
    Pistons: 'https://a.espncdn.com/i/teamlogos/nba/500/det.png',
    Warriors: 'https://a.espncdn.com/i/teamlogos/nba/500/gs.png',
    Rockets: 'https://a.espncdn.com/i/teamlogos/nba/500/hou.png',
    Pacers: 'https://a.espncdn.com/i/teamlogos/nba/500/ind.png',
    Clippers: 'https://a.espncdn.com/i/teamlogos/nba/500/lac.png',
    Lakers: 'https://a.espncdn.com/i/teamlogos/nba/500/lal.png',
    Grizzlies: 'https://a.espncdn.com/i/teamlogos/nba/500/mem.png',
    Heat: 'https://a.espncdn.com/i/teamlogos/nba/500/mia.png',
    Bucks: 'https://a.espncdn.com/i/teamlogos/nba/500/mil.png',
    Timberwolves: 'https://a.espncdn.com/i/teamlogos/nba/500/min.png',
    Pelicans: 'https://a.espncdn.com/i/teamlogos/nba/500/no.png',
    Knicks: 'https://a.espncdn.com/i/teamlogos/nba/500/ny.png',
    Thunder: 'https://a.espncdn.com/i/teamlogos/nba/500/okc.png',
    Magic: 'https://a.espncdn.com/i/teamlogos/nba/500/orl.png',
    '76ers': 'https://a.espncdn.com/i/teamlogos/nba/500/phi.png',
    Suns: 'https://a.espncdn.com/i/teamlogos/nba/500/phx.png',
    'Trail Blazers': 'https://a.espncdn.com/i/teamlogos/nba/500/por.png',
    Blazers: 'https://a.espncdn.com/i/teamlogos/nba/500/por.png',
    Kings: 'https://a.espncdn.com/i/teamlogos/nba/500/sac.png',
    Spurs: 'https://a.espncdn.com/i/teamlogos/nba/500/sa.png',
    Raptors: 'https://a.espncdn.com/i/teamlogos/nba/500/tor.png',
    Jazz: 'https://a.espncdn.com/i/teamlogos/nba/500/utah.png',
    Wizards: 'https://a.espncdn.com/i/teamlogos/nba/500/wsh.png'
};
const NHL_TEAM_LOGOS = {
    Bruins: 'https://a.espncdn.com/i/teamlogos/nhl/500/bos.png',
    Sabres: 'https://a.espncdn.com/i/teamlogos/nhl/500/buf.png',
    'Red Wings': 'https://a.espncdn.com/i/teamlogos/nhl/500/det.png',
    Panthers: 'https://a.espncdn.com/i/teamlogos/nhl/500/fla.png',
    Canadiens: 'https://a.espncdn.com/i/teamlogos/nhl/500/mtl.png',
    Senators: 'https://a.espncdn.com/i/teamlogos/nhl/500/ott.png',
    Lightning: 'https://a.espncdn.com/i/teamlogos/nhl/500/tb.png',
    'Maple Leafs': 'https://a.espncdn.com/i/teamlogos/nhl/500/tor.png',
    Hurricanes: 'https://a.espncdn.com/i/teamlogos/nhl/500/car.png',
    'Blue Jackets': 'https://a.espncdn.com/i/teamlogos/nhl/500/cbj.png',
    Devils: 'https://a.espncdn.com/i/teamlogos/nhl/500/njd.png',
    Islanders: 'https://a.espncdn.com/i/teamlogos/nhl/500/nyi.png',
    Rangers: 'https://a.espncdn.com/i/teamlogos/nhl/500/nyr.png',
    Flyers: 'https://a.espncdn.com/i/teamlogos/nhl/500/phi.png',
    Penguins: 'https://a.espncdn.com/i/teamlogos/nhl/500/pit.png',
    Capitals: 'https://a.espncdn.com/i/teamlogos/nhl/500/wsh.png',
    Blackhawks: 'https://a.espncdn.com/i/teamlogos/nhl/500/chi.png',
    Avalanche: 'https://a.espncdn.com/i/teamlogos/nhl/500/col.png',
    Stars: 'https://a.espncdn.com/i/teamlogos/nhl/500/dal.png',
    Wild: 'https://a.espncdn.com/i/teamlogos/nhl/500/min.png',
    Predators: 'https://a.espncdn.com/i/teamlogos/nhl/500/nsh.png',
    Blues: 'https://a.espncdn.com/i/teamlogos/nhl/500/stl.png',
    Jets: 'https://a.espncdn.com/i/teamlogos/nhl/500/wpg.png',
    Ducks: 'https://a.espncdn.com/i/teamlogos/nhl/500/ana.png',
    Flames: 'https://a.espncdn.com/i/teamlogos/nhl/500/cgy.png',
    Oilers: 'https://a.espncdn.com/i/teamlogos/nhl/500/edm.png',
    'LA Kings': 'https://a.espncdn.com/i/teamlogos/nhl/500/la.png',
    Sharks: 'https://a.espncdn.com/i/teamlogos/nhl/500/sj.png',
    Kraken: 'https://a.espncdn.com/i/teamlogos/nhl/500/sea.png',
    Canucks: 'https://a.espncdn.com/i/teamlogos/nhl/500/van.png',
    'Golden Knights': 'https://a.espncdn.com/i/teamlogos/nhl/500/vgk.png',
    Utah: 'https://a.espncdn.com/i/teamlogos/nhl/500/uta.png'
};
const NBA_TEAM_ABBR = {
    hawks: 'atl',
    celtics: 'bos',
    nets: 'bkn',
    hornets: 'cha',
    bulls: 'chi',
    cavaliers: 'cle',
    mavericks: 'dal',
    nuggets: 'den',
    pistons: 'det',
    warriors: 'gs',
    rockets: 'hou',
    pacers: 'ind',
    clippers: 'lac',
    lakers: 'lal',
    grizzlies: 'mem',
    heat: 'mia',
    bucks: 'mil',
    timberwolves: 'min',
    pelicans: 'no',
    knicks: 'ny',
    thunder: 'okc',
    magic: 'orl',
    '76ers': 'phi',
    suns: 'phx',
    'trail blazers': 'por',
    blazers: 'por',
    kings: 'sac',
    spurs: 'sa',
    raptors: 'tor',
    jazz: 'utah',
    wizards: 'wsh'
};
function getTeamLogo(teamName, league) {
    const name = teamName.trim();
    if (league === 'NBA') {
        return NBA_TEAM_LOGOS[name] || `https://a.espncdn.com/i/teamlogos/nba/500/default-team-logo-500.png`;
    }
    if (league === 'NHL') {
        return NHL_TEAM_LOGOS[name] || `https://a.espncdn.com/i/teamlogos/nhl/500/default-team-logo-500.png`;
    }
    if (league === 'CBB') {
        return `https://a.espncdn.com/i/teamlogos/ncaa/500/${encodeURIComponent(name.toLowerCase().replace(/ /g, '-'))}.png`;
    }
    return '';
}
}),
"[project]/src/lib/espn-api.ts [app-route] (ecmascript)", ((__turbopack_context__) => {
"use strict";

__turbopack_context__.s([
    "fetchESPNScoreboard",
    ()=>fetchESPNScoreboard,
    "fetchLiveScores",
    ()=>fetchLiveScores,
    "fetchTeamStats",
    ()=>fetchTeamStats
]);
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/constants.ts [app-route] (ecmascript)");
;
const ESPN_BASE = 'https://site.api.espn.com/apis/site/v2/sports';
function parseESPNEvent(event, league) {
    const comp = event.competitions[0];
    const awayComp = comp.competitors.find((c)=>c.homeAway === 'away');
    const homeComp = comp.competitors.find((c)=>c.homeAway === 'home');
    const awayTeam = awayComp.team.shortDisplayName || awayComp.team.displayName;
    const homeTeam = homeComp.team.shortDisplayName || homeComp.team.displayName;
    const awayRecord = awayComp.records?.find((r)=>r.type === 'total')?.summary || '';
    const homeRecord = homeComp.records?.find((r)=>r.type === 'total')?.summary || '';
    const gameTime = new Date(event.date).toLocaleTimeString('en-US', {
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
        timeZone: 'America/New_York'
    });
    const line = comp.odds?.[0]?.overUnder || null;
    return {
        id: parseInt(event.id),
        league,
        date: event.date.split('T')[0],
        away_team: awayTeam,
        home_team: homeTeam,
        away_logo: awayComp.team.logo || (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getTeamLogo"])(awayTeam, league),
        home_logo: homeComp.team.logo || (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getTeamLogo"])(homeTeam, league),
        away_record: awayRecord,
        home_record: homeRecord,
        game_time: gameTime,
        line,
        projected_total: null,
        edge: null,
        direction: null,
        is_qualified: false
    };
}
async function fetchESPNScoreboard(league, dateStr) {
    const sportPath = {
        NBA: 'basketball/nba',
        CBB: 'basketball/mens-college-basketball',
        NFL: 'football/nfl',
        CFB: 'football/college-football',
        NHL: 'hockey/nhl'
    }[league];
    if (!sportPath) return [];
    const date = dateStr || new Date().toISOString().split('T')[0].replace(/-/g, '');
    const url = `${ESPN_BASE}/${sportPath}/scoreboard?dates=${date}`;
    try {
        const res = await fetch(url, {
            next: {
                revalidate: 60
            }
        });
        if (!res.ok) throw new Error(`ESPN API error: ${res.status}`);
        const data = await res.json();
        return data.events.map((e)=>parseESPNEvent(e, league));
    } catch (err) {
        console.error(`ESPN fetch error for ${league}:`, err);
        return [];
    }
}
async function fetchLiveScores(league) {
    const sportPath = {
        NBA: 'basketball/nba',
        CBB: 'basketball/mens-college-basketball',
        NFL: 'football/nfl',
        CFB: 'football/college-football',
        NHL: 'hockey/nhl'
    }[league];
    if (!sportPath) return {};
    const url = `${ESPN_BASE}/${sportPath}/scoreboard`;
    try {
        const res = await fetch(url, {
            cache: 'no-store'
        });
        if (!res.ok) return {};
        const data = await res.json();
        const scores = {};
        for (const event of data.events){
            const comp = event.competitions[0];
            const awayComp = comp.competitors.find((c)=>c.homeAway === 'away');
            const homeComp = comp.competitors.find((c)=>c.homeAway === 'home');
            const awayTeam = awayComp.team.shortDisplayName || awayComp.team.displayName;
            const homeTeam = homeComp.team.shortDisplayName || homeComp.team.displayName;
            const key = `${awayTeam}@${homeTeam}`;
            const status = event.status.type;
            if (status.state === 'in' || status.completed) {
                scores[key] = {
                    away_score: parseInt(awayComp.score || '0'),
                    home_score: parseInt(homeComp.score || '0'),
                    period: `Q${event.status.period}`,
                    clock: event.status.displayClock,
                    status: status.description,
                    is_final: status.completed
                };
            }
        }
        return scores;
    } catch (err) {
        console.error(`Live scores error for ${league}:`, err);
        return {};
    }
}
async function fetchTeamStats(teamName, league) {
    try {
        const sportPath = {
            NBA: 'basketball/nba',
            CBB: 'basketball/mens-college-basketball',
            NFL: 'football/nfl',
            CFB: 'football/college-football',
            NHL: 'hockey/nhl'
        }[league];
        if (!sportPath) return null;
        const searchUrl = `${ESPN_BASE}/${sportPath}/teams?limit=500`;
        const res = await fetch(searchUrl, {
            next: {
                revalidate: 86400
            }
        });
        if (!res.ok) return null;
        const data = await res.json();
        const teams = data.sports?.[0]?.leagues?.[0]?.teams || [];
        const teamEntry = teams.find((t)=>{
            const name = t.team.displayName.toLowerCase();
            const short = t.team.shortDisplayName?.toLowerCase() || '';
            return name.includes(teamName.toLowerCase()) || short.includes(teamName.toLowerCase());
        });
        if (!teamEntry) return null;
        const teamId = teamEntry.team.id;
        const statsUrl = `${ESPN_BASE}/${sportPath}/teams/${teamId}/statistics`;
        const statsRes = await fetch(statsUrl, {
            next: {
                revalidate: 3600
            }
        });
        if (!statsRes.ok) return null;
        const statsData = await statsRes.json();
        const splits = statsData.splits?.categories || [];
        let ppg = 0;
        let opp_ppg = 0;
        for (const cat of splits){
            for (const stat of cat.stats || []){
                if (stat.name === 'avgPoints' || stat.name === 'pointsPerGame') {
                    ppg = parseFloat(stat.value) || 0;
                }
                if (stat.name === 'avgPointsAgainst' || stat.name === 'opposingPointsPerGame') {
                    opp_ppg = parseFloat(stat.value) || 0;
                }
            }
        }
        return {
            ppg,
            opp_ppg,
            record: teamEntry.team.record?.items?.[0]?.summary
        };
    } catch (err) {
        console.error(`Team stats error for ${teamName}:`, err);
        return null;
    }
}
}),
"[project]/src/lib/odds-api.ts [app-route] (ecmascript)", ((__turbopack_context__) => {
"use strict";

__turbopack_context__.s([
    "fetchAltLines",
    ()=>fetchAltLines,
    "fetchOdds",
    ()=>fetchOdds,
    "matchOddsToGame",
    ()=>matchOddsToGame
]);
const SPORT_KEYS = {
    NBA: 'basketball_nba',
    CBB: 'basketball_ncaab',
    NFL: 'americanfootball_nfl',
    CFB: 'americanfootball_ncaaf',
    NHL: 'icehockey_nhl'
};
async function fetchOdds(league) {
    const apiKey = process.env.ODDS_API_KEY;
    if (!apiKey) {
        console.warn('ODDS_API_KEY not set');
        return [];
    }
    const sportKey = SPORT_KEYS[league];
    if (!sportKey) return [];
    const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/odds/?apiKey=${apiKey}&regions=us&markets=totals&bookmakers=bovada`;
    try {
        const res = await fetch(url, {
            next: {
                revalidate: 300
            }
        });
        if (!res.ok) {
            console.error(`Odds API error: ${res.status}`);
            return [];
        }
        const data = await res.json();
        const results = [];
        for (const game of data){
            const bovada = game.bookmakers.find((b)=>b.key === 'bovada');
            const totalsMarket = bovada?.markets.find((m)=>m.key === 'totals');
            const over = totalsMarket?.outcomes.find((o)=>o.name === 'Over');
            const under = totalsMarket?.outcomes.find((o)=>o.name === 'Under');
            results.push({
                away_team: game.away_team,
                home_team: game.home_team,
                total_line: over?.point || null,
                over_odds: over?.price || null,
                under_odds: under?.price || null,
                alt_lines: []
            });
        }
        return results;
    } catch (err) {
        console.error(`Odds API fetch error for ${league}:`, err);
        return [];
    }
}
async function fetchAltLines(league) {
    const apiKey = process.env.ODDS_API_KEY;
    if (!apiKey) return [];
    const sportKey = SPORT_KEYS[league];
    if (!sportKey) return [];
    const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/odds/?apiKey=${apiKey}&regions=us&markets=alternate_totals&bookmakers=bovada`;
    try {
        const res = await fetch(url, {
            next: {
                revalidate: 300
            }
        });
        if (!res.ok) return [];
        const data = await res.json();
        const results = [];
        for (const game of data){
            const bovada = game.bookmakers.find((b)=>b.key === 'bovada');
            const altMarket = bovada?.markets.find((m)=>m.key === 'alternate_totals');
            const altLines = [];
            if (altMarket?.outcomes) {
                const grouped = {};
                for (const outcome of altMarket.outcomes){
                    const line = outcome.point || 0;
                    if (!grouped[line]) grouped[line] = {};
                    if (outcome.name === 'Over') grouped[line].over = outcome.price;
                    if (outcome.name === 'Under') grouped[line].under = outcome.price;
                }
                for (const [line, odds] of Object.entries(grouped)){
                    if (odds.over && odds.under) {
                        const oddsValue = Math.max(odds.over, odds.under);
                        if (oddsValue >= -180) {
                            altLines.push({
                                line: parseFloat(line),
                                over_odds: odds.over,
                                under_odds: odds.under
                            });
                        }
                    }
                }
            }
            results.push({
                away_team: game.away_team,
                home_team: game.home_team,
                total_line: null,
                over_odds: null,
                under_odds: null,
                alt_lines: altLines.sort((a, b)=>a.line - b.line)
            });
        }
        return results;
    } catch (err) {
        console.error(`Alt lines fetch error for ${league}:`, err);
        return [];
    }
}
function matchOddsToGame(awayTeam, homeTeam, odds) {
    const normalize = (name)=>name.toLowerCase().replace(/[^a-z0-9]/g, '');
    const awayNorm = normalize(awayTeam);
    const homeNorm = normalize(homeTeam);
    return odds.find((o)=>{
        const oAway = normalize(o.away_team);
        const oHome = normalize(o.home_team);
        return (oAway.includes(awayNorm) || awayNorm.includes(oAway)) && (oHome.includes(homeNorm) || homeNorm.includes(oHome));
    }) || null;
}
}),
"[project]/src/lib/edge-calculator.ts [app-route] (ecmascript)", ((__turbopack_context__) => {
"use strict";

__turbopack_context__.s([
    "calculateEdge",
    ()=>calculateEdge,
    "calculateProjectedTotal",
    ()=>calculateProjectedTotal,
    "findSupermaxLock",
    ()=>findSupermaxLock,
    "getConfidenceTier",
    ()=>getConfidenceTier,
    "getDirection",
    ()=>getDirection,
    "qualifyPick",
    ()=>qualifyPick
]);
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/constants.ts [app-route] (ecmascript)");
;
function calculateProjectedTotal(awayPpg, homePpg, awayOppPpg, homeOppPpg, awayPace, homePace, league = 'NBA') {
    const awayOffense = awayPpg;
    const awayDefense = awayOppPpg;
    const homeOffense = homePpg;
    const homeDefense = homeOppPpg;
    const awayExpected = (awayOffense + homeDefense) / 2;
    const homeExpected = (homeOffense + awayDefense) / 2;
    let projectedTotal = awayExpected + homeExpected;
    if (awayPace && homePace && league === 'NBA') {
        const leagueAvgPace = 100.0;
        const combinedPace = (awayPace + homePace) / 2;
        const paceFactor = combinedPace / leagueAvgPace;
        projectedTotal *= paceFactor;
    }
    return Math.round(projectedTotal * 10) / 10;
}
function calculateEdge(projectedTotal, line) {
    return Math.round(Math.abs(projectedTotal - line) * 10) / 10;
}
function getDirection(projectedTotal, line) {
    return projectedTotal > line ? 'O' : 'U';
}
function getConfidenceTier(edge, league) {
    const threshold = __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["EDGE_THRESHOLDS"][league] || 8.0;
    if (edge >= __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["CONFIDENCE_TIERS"].SUPERMAX_EDGE) return 'SUPERMAX';
    if (edge >= __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["CONFIDENCE_TIERS"].HIGH_EDGE) return 'HIGH';
    if (edge >= threshold) return 'STANDARD';
    return 'LOW';
}
function qualifyPick(game) {
    const reasons = [];
    const league = game.league;
    const threshold = __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["EDGE_THRESHOLDS"][league] || 8.0;
    if (!game.line || !game.projected_total) {
        return {
            is_qualified: false,
            edge: 0,
            direction: null,
            confidence_tier: 'LOW',
            reasons: [
                'Missing line or projected total'
            ]
        };
    }
    const edge = calculateEdge(game.projected_total, game.line);
    const direction = getDirection(game.projected_total, game.line);
    if (edge < threshold) {
        reasons.push(`Edge ${edge} below ${threshold} threshold`);
        return {
            is_qualified: false,
            edge,
            direction,
            confidence_tier: 'LOW',
            reasons
        };
    }
    reasons.push(`Edge ${edge} meets ${threshold} threshold`);
    const historyCheck = checkHistoricalTrend(game, direction);
    if (!historyCheck.passes) {
        reasons.push(historyCheck.reason);
    }
    const confidenceTier = getConfidenceTier(edge, league);
    const is_qualified = edge >= threshold && historyCheck.passes;
    return {
        is_qualified,
        edge,
        direction,
        confidence_tier: confidenceTier,
        reasons
    };
}
function checkHistoricalTrend(game, direction) {
    if (direction === 'O') {
        const l5 = game.over_l5 ?? 0;
        const l20 = game.over_l20 ?? 0;
        if (l5 >= 100 && l20 >= 85) {
            return {
                passes: true,
                reason: `Over trending: L5=${l5}%, L20=${l20}%`
            };
        }
        if (l20 >= __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["HISTORY_THRESHOLDS"].QUALIFY_RATE * 100) {
            return {
                passes: true,
                reason: `Over history: L20=${l20}%`
            };
        }
        return {
            passes: false,
            reason: `Over history weak: L5=${l5}%, L20=${l20}%`
        };
    } else {
        const l5 = game.under_l5 ?? 0;
        const l20 = game.under_l20 ?? 0;
        if (l5 >= 100 && l20 >= 85) {
            return {
                passes: true,
                reason: `Under trending: L5=${l5}%, L20=${l20}%`
            };
        }
        if (l20 >= __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["HISTORY_THRESHOLDS"].QUALIFY_RATE * 100) {
            return {
                passes: true,
                reason: `Under history: L20=${l20}%`
            };
        }
        return {
            passes: false,
            reason: `Under history weak: L5=${l5}%, L20=${l20}%`
        };
    }
}
function findSupermaxLock(games) {
    const qualified = games.filter((g)=>g.is_qualified && g.edge);
    if (qualified.length === 0) return null;
    return qualified.reduce((best, current)=>{
        if (!best.edge) return current;
        if (!current.edge) return best;
        return current.edge > best.edge ? current : best;
    });
}
}),
"[project]/src/app/api/dashboard_data/route.ts [app-route] (ecmascript)", ((__turbopack_context__) => {
"use strict";

__turbopack_context__.s([
    "GET",
    ()=>GET
]);
var __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/node_modules/next/server.js [app-route] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$espn$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/espn-api.ts [app-route] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$odds$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/odds-api.ts [app-route] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/edge-calculator.ts [app-route] (ecmascript)");
var __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__ = __turbopack_context__.i("[project]/src/lib/constants.ts [app-route] (ecmascript)");
;
;
;
;
;
const LEAGUES = [
    'NBA',
    'CBB',
    'NHL'
];
let cachedData = null;
const CACHE_TTL = 30000;
async function GET() {
    try {
        if (cachedData && Date.now() - cachedData.timestamp < CACHE_TTL) {
            const qualifiedGames = cachedData.games.filter((g)=>g.is_qualified);
            const supermaxLock = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["findSupermaxLock"])(cachedData.games);
            if (supermaxLock) supermaxLock.is_supermax = true;
            return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
                games: cachedData.games,
                supermax_lock: supermaxLock,
                total_games: cachedData.games.length,
                qualified_games: qualifiedGames.length,
                today: new Date().toISOString().split('T')[0]
            });
        }
        const today = new Date().toISOString().split('T')[0];
        const dateStr = today.replace(/-/g, '');
        const allGames = [];
        const leaguePromises = LEAGUES.map(async (league)=>{
            try {
                const [games, odds] = await Promise.all([
                    (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$espn$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["fetchESPNScoreboard"])(league, dateStr),
                    (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$odds$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["fetchOdds"])(league)
                ]);
                const enrichedGames = await Promise.all(games.map(async (game)=>{
                    try {
                        const [awayStats, homeStats] = await Promise.all([
                            (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$espn$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["fetchTeamStats"])(game.away_team, league).catch(()=>null),
                            (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$espn$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["fetchTeamStats"])(game.home_team, league).catch(()=>null)
                        ]);
                        const gameOdds = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$odds$2d$api$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["matchOddsToGame"])(game.away_team, game.home_team, odds);
                        if (gameOdds?.total_line) {
                            game.line = gameOdds.total_line;
                        }
                        if (awayStats && homeStats && awayStats.ppg && homeStats.ppg) {
                            game.away_ppg = awayStats.ppg;
                            game.home_ppg = homeStats.ppg;
                            game.away_opp_ppg = awayStats.opp_ppg;
                            game.home_opp_ppg = homeStats.opp_ppg;
                            const projected = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["calculateProjectedTotal"])(awayStats.ppg, homeStats.ppg, awayStats.opp_ppg, homeStats.opp_ppg, undefined, undefined, league);
                            game.projected_total = projected;
                            if (game.line) {
                                game.edge = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["calculateEdge"])(projected, game.line);
                                game.direction = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getDirection"])(projected, game.line);
                                game.confidence_tier = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getConfidenceTier"])(game.edge, league);
                                const threshold = __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["EDGE_THRESHOLDS"][league] || 8.0;
                                game.is_qualified = game.edge >= threshold;
                            }
                        }
                        game.away_logo = game.away_logo || (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getTeamLogo"])(game.away_team, league);
                        game.home_logo = game.home_logo || (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getTeamLogo"])(game.home_team, league);
                        return game;
                    } catch (err) {
                        console.error(`Error enriching game ${game.away_team}@${game.home_team}:`, err);
                        game.away_logo = game.away_logo || (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getTeamLogo"])(game.away_team, league);
                        game.home_logo = game.home_logo || (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$constants$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["getTeamLogo"])(game.home_team, league);
                        return game;
                    }
                }));
                return enrichedGames;
            } catch (err) {
                console.error(`Error fetching ${league} games:`, err);
                return [];
            }
        });
        const leagueResults = await Promise.all(leaguePromises);
        leagueResults.forEach((games)=>allGames.push(...games));
        cachedData = {
            games: allGames,
            timestamp: Date.now()
        };
        const qualifiedGames = allGames.filter((g)=>g.is_qualified);
        const supermaxLock = (0, __TURBOPACK__imported__module__$5b$project$5d2f$src$2f$lib$2f$edge$2d$calculator$2e$ts__$5b$app$2d$route$5d$__$28$ecmascript$29$__["findSupermaxLock"])(allGames);
        if (supermaxLock) {
            supermaxLock.is_supermax = true;
        }
        return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
            games: allGames,
            supermax_lock: supermaxLock,
            total_games: allGames.length,
            qualified_games: qualifiedGames.length,
            today
        });
    } catch (error) {
        console.error('Dashboard data error:', error);
        return __TURBOPACK__imported__module__$5b$project$5d2f$node_modules$2f$next$2f$server$2e$js__$5b$app$2d$route$5d$__$28$ecmascript$29$__["NextResponse"].json({
            error: 'Failed to fetch dashboard data',
            games: [],
            supermax_lock: null,
            total_games: 0,
            qualified_games: 0
        }, {
            status: 500
        });
    }
}
}),
];

//# sourceMappingURL=%5Broot-of-the-server%5D__75d184b7._.js.map