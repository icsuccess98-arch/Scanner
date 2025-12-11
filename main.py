import os
import time
import requests
import logging
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval as TvInterval

# Suppress tvDatafeed error logging
logging.getLogger("tvDatafeed.main").setLevel(logging.CRITICAL)

# ---------------------------------------------------------
# TELEGRAM CONFIG
# ---------------------------------------------------------

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

TOPIC_DAILY = 1236
TOPIC_WEEKLY = 1236
TOPIC_MONTHLY = 1236

def send_telegram(msg, topic_id=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }
    if topic_id:
        payload["message_thread_id"] = topic_id
    requests.post(url, json=payload)

# ---------------------------------------------------------
# DISCORD CONFIG
# ---------------------------------------------------------

WEBHOOK_DAILY = os.environ.get("WEBHOOK_DAILY", "")
WEBHOOK_WEEKLY = os.environ.get("WEBHOOK_WEEKLY", "")
WEBHOOK_MONTHLY = os.environ.get("WEBHOOK_MONTHLY", "")

def send_discord(msg, webhook_url):
    if not webhook_url:
        return
    # Convert HTML to Discord markdown
    discord_msg = msg.replace("<b>", "**").replace("</b>", "**")
    discord_msg = discord_msg.replace("<u>", "__").replace("</u>", "__")
    payload = {"content": discord_msg}
    requests.post(webhook_url, json=payload)

def to_tv_symbol(sym):
    # Convert symbol to TradingView format
    tv_map = {
        "XAU_USD": "OANDA:XAUUSD",
        "XAG_USD": "OANDA:XAGUSD",
        "WTICO_USD": "TVC:USOIL",
        "NAS100_USD": "OANDA:NAS100USD",
        "US30_USD": "OANDA:US30USD",
        "SPX500_USD": "OANDA:SPX500USD",
        "BTC_USD": "COINBASE:BTCUSD",
        "ETH_USD": "COINBASE:ETHUSD",
        "SOL_USD": "COINBASE:SOLUSD",
        # Futures
        "MNQ": "CME:MNQ1!",
        "MES": "CME:MES1!",
        "MCL": "NYMEX:MCL1!",
        "MGC": "COMEX:MGC1!",
    }
    if sym in tv_map:
        return tv_map[sym]
    # Standard forex pair
    return f"FX:{sym.replace('_', '')}"

def send_discord_csv(symbols, title, webhook_url):
    if not webhook_url or not symbols:
        return
    # Create TradingView watchlist CSV
    tv_symbols = [to_tv_symbol(s) for s in symbols]
    csv_content = ",".join(tv_symbols)
    
    # Send as file attachment
    files = {
        "file": (f"{title.lower()}_watchlist.txt", csv_content, "text/plain")
    }
    data = {"content": f"📋 **{title} TradingView Watchlist**"}
    requests.post(webhook_url, data=data, files=files)

# ---------------------------------------------------------
# OANDA CONFIG
# ---------------------------------------------------------

OANDA_KEY = os.environ["OANDA_KEY"]
OANDA_ACCOUNT = "practice"
RUN_MODE = os.environ.get("RUN_MODE", "ALL")

OANDA_URL = f"https://api-fx{OANDA_ACCOUNT}.oanda.com/v3/instruments"
HEADERS = {"Authorization": f"Bearer {OANDA_KEY}"}

# ---------------------------------------------------------
# ASSET GROUPS
# ---------------------------------------------------------

METALS = ["XAU_USD", "XAG_USD"]
OIL = ["WTICO_USD"]
INDICES = ["NAS100_USD", "US30_USD", "SPX500_USD"]
CRYPTOS = ["BTC_USD", "SOL_USD", "ETH_USD"]

OANDA_SYMBOLS = METALS + OIL + INDICES + CRYPTOS

# TradingView Futures (micro contracts)
FUTURES = [
    {"symbol": "MNQ1!", "exchange": "CME", "name": "MNQ"},
    {"symbol": "MES1!", "exchange": "CME", "name": "MES"},
    {"symbol": "MCL1!", "exchange": "NYMEX", "name": "MCL"},
    {"symbol": "MGC1!", "exchange": "COMEX", "name": "MGC"},
]

GROUP_ORDER = {
    "METALS": METALS,
    "OIL": OIL,
    "INDICES": INDICES,
    "CRYPTOS": CRYPTOS,
    "FUTURES": [f["name"] for f in FUTURES]
}

# Initialize TradingView datafeed
tv = TvDatafeed()

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def pretty(symbol):
    # Futures symbols are already clean
    if symbol in ["MNQ", "MES", "MCL", "MGC"]:
        return symbol
    out = symbol.replace("_", "/")
    if out == "WTICO/USD":
        return "USOIL"
    return out

def candle(c):
    return {
        "open": float(c["mid"]["o"]),
        "high": float(c["mid"]["h"]),
        "low": float(c["mid"]["l"]),
        "close": float(c["mid"]["c"]),
    }

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

# ---------------------------------------------------------
# FETCH CANDLES (PREVIOUS CLOSED ONLY)
# ---------------------------------------------------------

def get_closed_candles(symbol, granularity, count=5):
    url = f"{OANDA_URL}/{symbol}/candles"
    params = {"granularity": granularity, "count": count, "price": "M"}
    r = requests.get(url, headers=HEADERS, params=params).json()

    if "candles" not in r:
        return None

    closed = [candle(x) for x in r["candles"] if x["complete"]]

    if len(closed) < 3:
        return None

    return closed

def get_futures_candles(symbol, exchange, granularity, count=5):
    """Get candles from TradingView for futures"""
    try:
        interval_map = {
            "D": TvInterval.in_daily,
            "W": TvInterval.in_weekly,
            "M": TvInterval.in_monthly,
        }
        interval = interval_map.get(granularity, TvInterval.in_daily)
        
        df = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            n_bars=count,
            fut_contract=1
        )
        
        if df is None or len(df) < 4:
            return None
        
        # Convert DataFrame to list of candle dicts
        candles = []
        for _, row in df.iterrows():
            candles.append({
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
        
        return candles
    except Exception:
        return None

# ---------------------------------------------------------
# STRAT LOGIC
# ---------------------------------------------------------

def strat_type(curr, prev):
    if curr["high"] > prev["high"] and curr["low"] < prev["low"]:
        return "3"
    if curr["high"] > prev["high"] and curr["low"] >= prev["low"]:
        return "2U"
    if curr["low"] < prev["low"] and curr["high"] <= prev["high"]:
        return "2D"
    return "1"

def failed_2(curr, prev):
    t = strat_type(curr, prev)
    if t == "2U" and curr["close"] < curr["open"]:
        return "Failed 2U"
    if t == "2D" and curr["close"] > curr["open"]:
        return "Failed 2D"
    return None

def ftfc_pass(d, w, m):
    d_dir = direction(d)
    if d_dir == direction(w) == direction(m):
        return d_dir
    return None

# ---------------------------------------------------------
# SORT BY ASSET CLASS
# ---------------------------------------------------------

def group_sort(symbol_list):
    ordered = []
    for group in GROUP_ORDER.values():
        ordered.extend([s for s in symbol_list if s in group])
    return ordered

# ---------------------------------------------------------
# A++ FORMAT BUILDER
# ---------------------------------------------------------

def format_aplus(a_list):
    msg = "🔥 *A++ Setups*\n"

    upside = []
    downside = []

    for sym, info in a_list:
        if "↑" in info["arrows"]:
            upside.append((sym, info))
        else:
            downside.append((sym, info))

    if upside:
        msg += "\n🔺 *Upside*\n"
        for sym, info in upside:
            msg += f"• {pretty(sym)} — M/W/D {info['arrows']} — D: {info['label']}\n"

    if downside:
        msg += "\n🔻 *Downside*\n"
        for sym, info in downside:
            msg += f"• {pretty(sym)} — M/W/D {info['arrows']} — D: {info['label']}\n"

    return msg + "\n"

# ---------------------------------------------------------
# MAIN SCANNER
# ---------------------------------------------------------

def build_message(title, aplus, double_inside, inside, outside, f2u, f2d, is_futures=False):
    """Build message body from scan results"""
    msg = ""
    
    if aplus:
        msg += "🔥 <b>A++ Setups</b>\n\n"
        ups = []
        dns = []

        for sym, lbl in aplus.items():
            lbl_short = lbl.replace("Failed 2U", "F2U").replace("Failed 2D", "F2D")
            if "F2D" in lbl_short:
                ups.append((sym, lbl_short))
            elif "F2U" in lbl_short:
                dns.append((sym, lbl_short))
            elif ("Double Inside" in lbl or "3-1" in lbl or "1-3" in lbl) and "↑" in lbl:
                ups.append((sym, lbl_short))
            else:
                dns.append((sym, lbl_short))

        if ups:
            msg += "<u><b>🟢 Upside</b></u>\n"
            for sym, lbl in ups:
                msg += f"• <b>{pretty(sym)}</b> — {lbl}\n"
            msg += "\n"

        if dns:
            msg += "<u><b>🔴 Downside</b></u>\n"
            for sym, lbl in dns:
                msg += f"• <b>{pretty(sym)}</b> — {lbl}\n"
            msg += "\n"

    if double_inside:
        msg += "<b>🟪 Double Inside (II)</b>\n"
        for x in (double_inside if is_futures else group_sort(double_inside)):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if inside:
        msg += "<b>📘 Inside (1)</b>\n"
        for x in (inside if is_futures else group_sort(inside)):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if outside:
        msg += "<b>📕 Outside (3)</b>\n"
        for x in (outside if is_futures else group_sort(outside)):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if f2u:
        msg += "<b>🔴 F2U</b>\n"
        for x in (f2u if is_futures else group_sort(f2u)):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if f2d:
        msg += "<b>🟢 F2D</b>\n"
        for x in (f2d if is_futures else group_sort(f2d)):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    return msg.strip()

def scan(title, granularity, topic_id=None, discord_webhook=None):

    # OANDA results
    inside = []
    outside = []
    double_inside = []
    f2u = []
    f2d = []
    aplus = {}
    
    # Futures results (separate)
    fut_inside = []
    fut_outside = []
    fut_double_inside = []
    fut_f2u = []
    fut_f2d = []
    fut_aplus = {}

    for symbol in OANDA_SYMBOLS:

        candles = get_closed_candles(symbol, granularity)
        if not candles:
            continue

        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]
        prev3 = candles[-4] if len(candles) >= 4 else None

        st = strat_type(curr, prev)

        if st == "1" and strat_type(prev, prev2) == "1":
            double_inside.append(symbol)

            arrows = (
                arrow(direction(get_closed_candles(symbol, "M")[-1])) +
                arrow(direction(get_closed_candles(symbol, "W")[-1])) +
                arrow(direction(curr))
            )

            aplus[symbol] = f"M/W/D {arrows} — Double Inside"
            continue

        if st == "1":
            inside.append(symbol)
            # Check for 3-1 pattern (Outside followed by Inside)
            if strat_type(prev, prev2) == "3":
                weekly = get_closed_candles(symbol, "W")
                monthly = get_closed_candles(symbol, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    aplus[symbol] = f"M/W/D {arrows} — 3-1"

        if st == "3":
            outside.append(symbol)
            # Check for 1-3 pattern (Inside followed by Outside)
            if strat_type(prev, prev2) == "1":
                weekly = get_closed_candles(symbol, "W")
                monthly = get_closed_candles(symbol, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    aplus[symbol] = f"M/W/D {arrows} — 1-3"

        f2 = failed_2(curr, prev)
        if f2:
            # Add to regular actionables list regardless of FTFC
            if f2 == "Failed 2U":
                f2u.append(symbol)
            if f2 == "Failed 2D":
                f2d.append(symbol)

            # Check if previous bar was 1 (Inside) or 3 (Outside) for A++ 
            prev_type = strat_type(prev, prev2)
            weekly = get_closed_candles(symbol, "W")
            monthly = get_closed_candles(symbol, "M")
            
            # Check for Double Inside before F2 (II-F2)
            is_double_inside = prev3 and prev_type == "1" and strat_type(prev2, prev3) == "1"
            
            if weekly and monthly:
                arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                
                # II-F2: Double Inside followed by Failed 2
                if is_double_inside:
                    if f2 == "Failed 2U":
                        aplus[symbol] = f"M/W/D {arrows} — II-F2U"
                    else:
                        aplus[symbol] = f"M/W/D {arrows} — II-F2D"
                # 1-F2 and 3-F2 are A++ without FTFC requirement
                elif prev_type == "1":
                    if f2 == "Failed 2U":
                        aplus[symbol] = f"M/W/D {arrows} — 1-F2U"
                    else:
                        aplus[symbol] = f"M/W/D {arrows} — 1-F2D"
                elif prev_type == "3":
                    if f2 == "Failed 2U":
                        aplus[symbol] = f"M/W/D {arrows} — 3-F2U"
                    else:
                        aplus[symbol] = f"M/W/D {arrows} — 3-F2D"
                else:
                    # Regular F2 requires FTFC alignment
                    ftfc = ftfc_pass(curr, weekly[-1], monthly[-1])
                    if ftfc:
                        if f2 == "Failed 2U" and ftfc == "DOWN":
                            aplus[symbol] = f"M/W/D {arrows} — F2U"
                        if f2 == "Failed 2D" and ftfc == "UP":
                            aplus[symbol] = f"M/W/D {arrows} — F2D"

        time.sleep(0.2)

    # Scan TradingView Futures
    for fut in FUTURES:
        symbol = fut["name"]  # Use clean name for display
        tv_symbol = fut["symbol"]
        exchange = fut["exchange"]
        
        candles = get_futures_candles(tv_symbol, exchange, granularity)
        if not candles:
            continue

        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]
        prev3 = candles[-4] if len(candles) >= 4 else None

        st = strat_type(curr, prev)

        if st == "1" and strat_type(prev, prev2) == "1":
            fut_double_inside.append(symbol)
            arrows = (
                arrow(direction(get_futures_candles(tv_symbol, exchange, "M")[-1])) +
                arrow(direction(get_futures_candles(tv_symbol, exchange, "W")[-1])) +
                arrow(direction(curr))
            )
            fut_aplus[symbol] = f"M/W/D {arrows} — Double Inside"
            continue

        if st == "1":
            fut_inside.append(symbol)
            if strat_type(prev, prev2) == "3":
                weekly = get_futures_candles(tv_symbol, exchange, "W")
                monthly = get_futures_candles(tv_symbol, exchange, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    fut_aplus[symbol] = f"M/W/D {arrows} — 3-1"

        if st == "3":
            fut_outside.append(symbol)
            if strat_type(prev, prev2) == "1":
                weekly = get_futures_candles(tv_symbol, exchange, "W")
                monthly = get_futures_candles(tv_symbol, exchange, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    fut_aplus[symbol] = f"M/W/D {arrows} — 1-3"

        f2 = failed_2(curr, prev)
        if f2:
            if f2 == "Failed 2U":
                fut_f2u.append(symbol)
            if f2 == "Failed 2D":
                fut_f2d.append(symbol)

            prev_type = strat_type(prev, prev2)
            weekly = get_futures_candles(tv_symbol, exchange, "W")
            monthly = get_futures_candles(tv_symbol, exchange, "M")
            
            is_double_inside = prev3 and prev_type == "1" and strat_type(prev2, prev3) == "1"
            
            if weekly and monthly:
                arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                
                if is_double_inside:
                    if f2 == "Failed 2U":
                        fut_aplus[symbol] = f"M/W/D {arrows} — II-F2U"
                    else:
                        fut_aplus[symbol] = f"M/W/D {arrows} — II-F2D"
                elif prev_type == "1":
                    if f2 == "Failed 2U":
                        fut_aplus[symbol] = f"M/W/D {arrows} — 1-F2U"
                    else:
                        fut_aplus[symbol] = f"M/W/D {arrows} — 1-F2D"
                elif prev_type == "3":
                    if f2 == "Failed 2U":
                        fut_aplus[symbol] = f"M/W/D {arrows} — 3-F2U"
                    else:
                        fut_aplus[symbol] = f"M/W/D {arrows} — 3-F2D"
                else:
                    ftfc = ftfc_pass(curr, weekly[-1], monthly[-1])
                    if ftfc:
                        if f2 == "Failed 2U" and ftfc == "DOWN":
                            fut_aplus[symbol] = f"M/W/D {arrows} — F2U"
                        if f2 == "Failed 2D" and ftfc == "UP":
                            fut_aplus[symbol] = f"M/W/D {arrows} — F2D"

        time.sleep(0.5)  # Rate limit for TradingView

    # Build headers
    tg_header = f"📊 <b>{title} Actionables</b>\n\n"
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_header = today.strftime("%b %d, %Y")
    from_day = yesterday.strftime("%a %b %d")
    dc_header = f"🗓 <b>{title} Actionable Strat — {date_header}</b>\n"
    dc_header += f"(From {from_day} close)\n\n"
    
    discord_only = os.environ.get("DISCORD_ONLY", "").upper() == "TRUE"
    
    # Build and send OANDA message
    msg = build_message(title, aplus, double_inside, inside, outside, f2u, f2d)
    if msg:
        tg_msg = tg_header + msg
        dc_msg = dc_header + msg
        if not discord_only:
            send_telegram(tg_msg, topic_id)
        send_discord(dc_msg, discord_webhook)
        
        # Send TradingView watchlist CSV for OANDA symbols only
        all_symbols = list(set(double_inside + inside + outside + f2u + f2d + list(aplus.keys())))
        send_discord_csv(all_symbols, title, discord_webhook)
    
    # Build and send Futures message (separate, no CSV)
    fut_msg = build_message(title, fut_aplus, fut_double_inside, fut_inside, fut_outside, fut_f2u, fut_f2d, is_futures=True)
    if fut_msg:
        fut_tg_header = f"📊 <b>{title} Futures</b>\n\n"
        fut_dc_header = f"🗓 <b>{title} Futures — {date_header}</b>\n"
        fut_dc_header += f"(From {from_day} close)\n\n"
        
        if not discord_only:
            send_telegram(fut_tg_header + fut_msg, topic_id)
        send_discord(fut_dc_header + fut_msg, discord_webhook)

# ---------------------------------------------------------
# RUNTIME
# ---------------------------------------------------------

if RUN_MODE in ("DAILY", "ALL"):
    scan("Daily", "D", TOPIC_DAILY, WEBHOOK_DAILY)

if RUN_MODE in ("WEEKLY", "ALL"):
    scan("Weekly", "W", TOPIC_WEEKLY, WEBHOOK_WEEKLY)

if RUN_MODE in ("MONTHLY", "ALL"):
    scan("Monthly", "M", TOPIC_MONTHLY, WEBHOOK_MONTHLY)

print("DONE (Telegram + Discord)")
