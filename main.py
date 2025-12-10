import os
import time
import requests
from datetime import datetime, timedelta

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

def to_tv_symbol(oanda_sym):
    # Convert OANDA symbol to TradingView format
    tv_map = {
        "XAU_USD": "OANDA:XAUUSD",
        "XAG_USD": "OANDA:XAGUSD",
        "WTICO_USD": "TVC:USOIL",
        "NAS100_USD": "OANDA:NAS100USD",
        "US30_USD": "OANDA:US30USD",
        "BTC_USD": "COINBASE:BTCUSD",
        "ETH_USD": "COINBASE:ETHUSD",
        "LTC_USD": "COINBASE:LTCUSD",
    }
    if oanda_sym in tv_map:
        return tv_map[oanda_sym]
    # Standard forex pair
    return f"FX:{oanda_sym.replace('_', '')}"

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

MAJORS = [
    "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF",
    "USD_CAD", "AUD_USD", "NZD_USD"
]

METALS = ["XAU_USD", "XAG_USD"]
OIL = ["WTICO_USD"]
INDICES = ["NAS100_USD", "US30_USD"]
CRYPTOS = ["BTC_USD", "ETH_USD", "LTC_USD"]

CROSSES = [
    "EUR_GBP", "EUR_JPY", "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_NZD",
    "GBP_JPY", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_NZD",
    "AUD_JPY", "AUD_CHF", "AUD_CAD", "AUD_NZD",
    "NZD_JPY", "NZD_CHF", "NZD_CAD",
    "CHF_JPY", "CAD_JPY"
]

OANDA_SYMBOLS = MAJORS + METALS + OIL + INDICES + CRYPTOS + CROSSES
FOREX_ONLY = MAJORS + CROSSES

GROUP_ORDER = {
    "MAJORS": MAJORS,
    "METALS": METALS,
    "OIL": OIL,
    "INDICES": INDICES,
    "CRYPTOS": CRYPTOS,
    "CROSSES": CROSSES
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def pretty(symbol):
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

def scan(title, granularity, topic_id=None, discord_webhook=None):

    inside = []
    outside = []
    double_inside = []
    f2u = []
    f2d = []
    aplus = {}

    for symbol in OANDA_SYMBOLS:

        candles = get_closed_candles(symbol, granularity)
        if not candles:
            continue

        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]

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

        if st == "3":
            outside.append(symbol)

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
            
            if weekly and monthly:
                arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                
                # 1-F2 and 3-F2 are A++ without FTFC requirement
                if prev_type == "1":
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

    # Telegram header (simple)
    tg_header = f"📊 <b>{title} Actionables</b>\n\n"
    
    # Discord header (with dates)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_header = today.strftime("%b %d, %Y")
    from_day = yesterday.strftime("%a %b %d")
    dc_header = f"🗓 <b>{title} Actionable Strat — {date_header}</b>\n"
    dc_header += f"(From {from_day} close)\n\n"
    
    msg = ""

    if aplus:
        msg += "🔥 <b>A++ Setups</b>\n\n"
        ups = []
        dns = []

        for sym, lbl in aplus.items():
            lbl_short = lbl.replace("Failed 2U", "F2U").replace("Failed 2D", "F2D")
            # Failed 2D = bullish (upside), Failed 2U = bearish (downside)
            # Double Inside: check arrow direction
            if "F2D" in lbl_short or ("Double Inside" in lbl and "↑" in lbl):
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
        for x in group_sort(double_inside):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if inside:
        msg += "<b>📘 Inside (1)</b>\n"
        for x in group_sort(inside):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if outside:
        msg += "<b>📕 Outside (3)</b>\n"
        for x in group_sort(outside):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if f2u:
        msg += "<b>🔴 F2U</b>\n"
        for x in group_sort(f2u):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    if f2d:
        msg += "<b>🟢 F2D</b>\n"
        for x in group_sort(f2d):
            msg += f"• {pretty(x)}\n"
        msg += "\n"

    msg = msg.strip()
    tg_msg = tg_header + msg
    dc_msg = dc_header + msg
    send_telegram(tg_msg, topic_id)
    send_discord(dc_msg, discord_webhook)
    
    # Send TradingView watchlist CSV to Discord immediately after
    all_symbols = list(set(double_inside + inside + outside + f2u + f2d + list(aplus.keys())))
    send_discord_csv(all_symbols, title, discord_webhook)

# ---------------------------------------------------------
# RUNTIME
# ---------------------------------------------------------

WEBHOOK_SPECIALTY = "https://discord.com/api/webhooks/1448256346913767477/LQmnxHMyFyXcYdrkGJGbZjeWLEOn0AEGWpvanLR9R_GdFPKtfzH1wyilRHpo-Len9VOJ"

def scan_ftfc_favorites():
    """Scan forex pairs for Rob's 4 FTFC Continuation patterns"""
    
    from datetime import datetime
    
    ftfc_up = []
    ftfc_down = []
    inside_2_cont = []  # 1) Inside → 2 continuation
    two_two_cont = []   # 2) 2-2 continuation
    three_2_cont = []   # 3) 3 → 2 continuation
    two_1_2_rev = []    # 4) 2-1-2 reversal
    
    for symbol in FOREX_ONLY:
        daily = get_closed_candles(symbol, "D", 5)
        weekly = get_closed_candles(symbol, "W", 5)
        monthly = get_closed_candles(symbol, "M", 5)
        
        if not daily or not weekly or not monthly:
            continue
        
        curr = daily[-1]
        prev = daily[-2]
        prev2 = daily[-3]
        
        # Check FTFC
        ftfc = ftfc_pass(curr, weekly[-1], monthly[-1])
        if not ftfc:
            time.sleep(0.15)
            continue
        
        arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
        
        if ftfc == "UP":
            ftfc_up.append(symbol)
        else:
            ftfc_down.append(symbol)
        
        st_curr = strat_type(curr, prev)
        st_prev = strat_type(prev, prev2)
        
        # 1) FTFC + Inside → 2 continuation
        if st_prev == "1" and st_curr == "2":
            if (ftfc == "UP" and curr["high"] > prev["high"]) or (ftfc == "DOWN" and curr["low"] < prev["low"]):
                label = f"{pretty(symbol)} — FTFC {ftfc} (M/W/D {arrows}) — Inside→2"
                inside_2_cont.append((symbol, label))
        
        # 2) FTFC + 2-2 continuation
        if st_prev == "2" and st_curr == "2":
            prev_dir = "UP" if prev["high"] > prev2["high"] else "DOWN"
            curr_dir = "UP" if curr["high"] > prev["high"] else "DOWN"
            if prev_dir == curr_dir == ftfc:
                label = f"{pretty(symbol)} — FTFC {ftfc} (M/W/D {arrows}) — 2→2 continuation"
                two_two_cont.append((symbol, label))
        
        # 3) FTFC + 3 → 2 continuation
        if st_prev == "3" and st_curr == "2":
            if (ftfc == "UP" and curr["high"] > prev["high"]) or (ftfc == "DOWN" and curr["low"] < prev["low"]):
                label = f"{pretty(symbol)} — FTFC {ftfc} (M/W/D {arrows}) — 3→2 continuation"
                three_2_cont.append((symbol, label))
        
        # 4) FTFC + 2-1-2 reversal (prev was 1 inside, before that was 2 against FTFC, now 2 with FTFC)
        if st_prev == "1" and st_curr == "2":
            prev3 = daily[-4] if len(daily) >= 4 else None
            if prev3:
                st_prev2 = strat_type(prev2, prev3)
                if st_prev2 == "2":
                    # Check if prev2 was against FTFC and curr is with FTFC
                    prev2_dir = "UP" if prev2["high"] > prev3["high"] else "DOWN"
                    curr_dir = "UP" if curr["high"] > prev["high"] else "DOWN"
                    if prev2_dir != ftfc and curr_dir == ftfc:
                        label = f"{pretty(symbol)} — FTFC {ftfc} (M/W/D {arrows}) — 2-1-2 reversal"
                        two_1_2_rev.append((symbol, label))
        
        time.sleep(0.15)
    
    # Build message
    now = datetime.now().strftime("%b %d, %Y %H:%M")
    msg = f"<b>FTFC Favorites — Forex Intraday Setups</b>\n"
    msg += f"As of {now}\n\n"
    
    msg += "<b>0) FTFC UNIVERSE (Month/Week/Day all same direction)</b>\n"
    if ftfc_up:
        msg += f"• FTFC ↑ : {', '.join([pretty(s) for s in ftfc_up])}\n"
    if ftfc_down:
        msg += f"• FTFC ↓ : {', '.join([pretty(s) for s in ftfc_down])}\n"
    msg += "\n"
    
    msg += "<b>1) FTFC + Inside → 2 continuation</b>\n"
    if inside_2_cont:
        for sym, lbl in inside_2_cont:
            msg += f"• {lbl}\n"
    else:
        msg += "<i>None</i>\n"
    msg += "\n"
    
    msg += "<b>2) FTFC + 2-2 continuation</b>\n"
    if two_two_cont:
        for sym, lbl in two_two_cont:
            msg += f"• {lbl}\n"
    else:
        msg += "<i>None</i>\n"
    msg += "\n"
    
    msg += "<b>3) FTFC + 3 → 2 continuation</b>\n"
    if three_2_cont:
        for sym, lbl in three_2_cont:
            msg += f"• {lbl}\n"
    else:
        msg += "<i>None</i>\n"
    msg += "\n"
    
    msg += "<b>4) FTFC + 2-1-2 reversal</b>\n"
    if two_1_2_rev:
        for sym, lbl in two_1_2_rev:
            msg += f"• {lbl}\n"
    else:
        msg += "<i>None</i>\n"
    
    send_discord(msg.strip(), WEBHOOK_SPECIALTY)

if RUN_MODE in ("DAILY", "ALL"):
    scan("Daily", "D", TOPIC_DAILY, WEBHOOK_DAILY)

if RUN_MODE in ("WEEKLY", "ALL"):
    scan("Weekly", "W", TOPIC_WEEKLY, WEBHOOK_WEEKLY)

if RUN_MODE in ("MONTHLY", "ALL"):
    scan("Monthly", "M", TOPIC_MONTHLY, WEBHOOK_MONTHLY)

if RUN_MODE in ("SPECIALTY", "ALL"):
    scan_ftfc_favorites()

print("DONE (Telegram + Discord)")
