import os
import time
import requests
from datetime import datetime, timedelta
from tvDatafeed import TvDatafeed, Interval

# ---------------------------------------------------------
# DISCORD CONFIG
# ---------------------------------------------------------

WEBHOOK_CRYPTO_DAILY = os.environ.get("Cryptodiscord", "")
WEBHOOK_CRYPTO_WEEKLY = os.environ.get("Cryptodiscord", "")

def send_discord(msg, webhook_url):
    if not webhook_url:
        return
    discord_msg = msg.replace("<b>", "**").replace("</b>", "**")
    discord_msg = discord_msg.replace("<u>", "__").replace("</u>", "__")
    payload = {"content": discord_msg}
    requests.post(webhook_url, json=payload)

def send_discord_csv(symbols, title, webhook_url):
    if not webhook_url or not symbols:
        return
    tv_symbols = [f"COINBASE:{s.replace('-', '')}" for s in symbols]
    csv_content = ",".join(tv_symbols)
    files = {
        "file": (f"{title.lower()}_crypto_watchlist.txt", csv_content, "text/plain")
    }
    data = {"content": f"📋 **{title} Crypto TradingView Watchlist**"}
    requests.post(webhook_url, data=data, files=files)

# ---------------------------------------------------------
# TRADINGVIEW CONFIG
# ---------------------------------------------------------

RUN_MODE = os.environ.get("RUN_MODE", "ALL")

tv = TvDatafeed()

# ---------------------------------------------------------
# CRYPTO SYMBOLS (TradingView format: symbol, exchange)
# ---------------------------------------------------------

CRYPTO_SYMBOLS = [
    ("BTCUSD", "COINBASE"), ("ETHUSD", "COINBASE"), ("SOLUSD", "COINBASE"),
    ("XRPUSD", "COINBASE"), ("ADAUSD", "COINBASE"), ("DOGEUSD", "COINBASE"),
    ("LTCUSD", "COINBASE"), ("BCHUSD", "COINBASE"), ("LINKUSD", "COINBASE"),
    ("UNIUSD", "COINBASE"), ("AAVEUSD", "COINBASE"), ("DOTUSD", "COINBASE"),
    ("XLMUSD", "COINBASE"), ("ATOMUSD", "COINBASE"), ("ENSUSD", "COINBASE"),
    ("ETCUSD", "COINBASE"), ("ARBUSD", "COINBASE"), ("ICPUSD", "COINBASE"),
    ("FILUSD", "COINBASE"), ("NEARUSD", "COINBASE"), ("APTUSD", "COINBASE"),
    ("AVAXUSD", "COINBASE"), ("SEIUSD", "COINBASE"), ("OPUSD", "COINBASE"),
    ("IMXUSD", "COINBASE"), ("COMPUSD", "COINBASE"), ("CRVUSD", "COINBASE"),
    ("SUSHIUSD", "COINBASE"), ("LDOUSD", "COINBASE"), ("SHIBUSD", "COINBASE"),
    ("PEPEUSD", "COINBASE"), ("FLOKIUSD", "COINBASE"), ("BONKUSD", "COINBASE"),
    ("BLURUSD", "COINBASE"), ("AXSUSD", "COINBASE"), ("SANDUSD", "COINBASE"),
    ("APEUSD", "COINBASE"), ("RENDERUSD", "COINBASE"), ("FETUSD", "COINBASE"),
    ("GRTUSD", "COINBASE"), ("ONDOUSD", "COINBASE"), ("INJUSD", "COINBASE"),
    ("ALGOUSD", "COINBASE"), ("HBARUSD", "COINBASE"), ("FLOWUSD", "COINBASE"),
    ("VETUSD", "COINBASE"), ("QNTUSD", "COINBASE"), ("SNXUSD", "COINBASE"),
    ("MINAUSD", "COINBASE"), ("POLUSD", "COINBASE"), ("ZROUSD", "COINBASE"),
    ("JTOUSD", "COINBASE"), ("PAXGUSD", "COINBASE"), ("WLDUSD", "COINBASE"),
    ("EIGENUSD", "COINBASE"), ("TIAUSD", "COINBASE"), ("PYTHIUSD", "COINBASE"),
    ("WUSD", "COINBASE"), ("STRKUSD", "COINBASE"), ("SUPERUSD", "COINBASE"),
    ("GMTUSD", "COINBASE"), ("ALTUSD", "COINBASE"), ("SKYUSD", "COINBASE")
]

GROUP_ORDER = {
    "MAJOR": ["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "ADAUSD", "DOGEUSD", "LTCUSD", "BCHUSD"],
    "DEFI": ["LINKUSD", "UNIUSD", "AAVEUSD", "COMPUSD", "CRVUSD", "SUSHIUSD", "LDOUSD", "SNXUSD"],
    "L1_L2": ["DOTUSD", "ATOMUSD", "AVAXUSD", "NEARUSD", "APTUSD", "SEIUSD", "OPUSD", "ARBUSD", "IMXUSD", "POLUSD", "INJUSD", "TIAUSD", "STRKUSD"],
    "MEME": ["SHIBUSD", "PEPEUSD", "FLOKIUSD", "BONKUSD", "WLDUSD"],
    "AI_COMPUTE": ["RENDERUSD", "FETUSD", "GRTUSD", "ONDOUSD"],
    "GAMING": ["BLURUSD", "AXSUSD", "SANDUSD", "APEUSD", "IMXUSD"],
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def pretty(symbol):
    return symbol.replace("USD", "")

def to_product_id(symbol):
    return symbol[:-3] + "-USD"

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

# ---------------------------------------------------------
# FETCH CANDLES FROM TRADINGVIEW
# ---------------------------------------------------------

def get_tv_candles(symbol, exchange, granularity, count=5):
    if granularity == "D":
        interval = Interval.in_daily
    elif granularity == "W":
        interval = Interval.in_weekly
    elif granularity == "M":
        interval = Interval.in_monthly
    else:
        interval = Interval.in_daily
    
    try:
        df = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=count + 2)
        
        if df is None or df.empty:
            return None
        
        now = datetime.utcnow()
        today = now.date()
        df = df[df.index.date < today]
        
        if len(df) < 3:
            return None
        
        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "time": idx,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"])
            })
        
        return candles[-count:] if len(candles) >= count else candles
        
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
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
# SORT BY GROUP
# ---------------------------------------------------------

def group_sort(symbol_list):
    ordered = []
    seen = set()
    for group in GROUP_ORDER.values():
        for s in symbol_list:
            if s in group and s not in seen:
                ordered.append(s)
                seen.add(s)
    for s in symbol_list:
        if s not in seen:
            ordered.append(s)
    return ordered

# ---------------------------------------------------------
# MAIN SCANNER
# ---------------------------------------------------------

def scan(title, granularity, discord_webhook=None):

    inside = []
    outside = []
    double_inside = []
    f2u = []
    f2d = []
    aplus = {}

    for symbol, exchange in CRYPTO_SYMBOLS:

        candles = get_tv_candles(symbol, exchange, granularity)
        if not candles:
            continue

        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]
        prev3 = candles[-4] if len(candles) >= 4 else None

        st = strat_type(curr, prev)

        if st == "1" and strat_type(prev, prev2) == "1":
            double_inside.append(symbol)
            weekly = get_tv_candles(symbol, exchange, "W")
            monthly = get_tv_candles(symbol, exchange, "M")
            if weekly and monthly:
                arrows = (
                    arrow(direction(monthly[-1])) +
                    arrow(direction(weekly[-1])) +
                    arrow(direction(curr))
                )
                aplus[symbol] = f"M/W/D {arrows} — Double Inside"
            continue

        if st == "1":
            inside.append(symbol)
            if strat_type(prev, prev2) == "3":
                weekly = get_tv_candles(symbol, exchange, "W")
                monthly = get_tv_candles(symbol, exchange, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    aplus[symbol] = f"M/W/D {arrows} — 3-1"

        if st == "3":
            outside.append(symbol)
            if strat_type(prev, prev2) == "1":
                weekly = get_tv_candles(symbol, exchange, "W")
                monthly = get_tv_candles(symbol, exchange, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    aplus[symbol] = f"M/W/D {arrows} — 1-3"

        f2 = failed_2(curr, prev)
        if f2:
            if f2 == "Failed 2U":
                f2u.append(symbol)
            if f2 == "Failed 2D":
                f2d.append(symbol)

            prev_type = strat_type(prev, prev2)
            weekly = get_tv_candles(symbol, exchange, "W")
            monthly = get_tv_candles(symbol, exchange, "M")
            
            is_double_inside = prev3 and prev_type == "1" and strat_type(prev2, prev3) == "1"
            
            if prev_type == "1" or prev_type == "3" or is_double_inside:
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    if is_double_inside:
                        pattern = "II-" + f2.replace("Failed ", "F")
                    elif prev_type == "1":
                        pattern = "1-" + f2.replace("Failed ", "F")
                    else:
                        pattern = "3-" + f2.replace("Failed ", "F")
                    aplus[symbol] = f"M/W/D {arrows} — {pattern}"

    msg_parts = []

    msg_parts.append(f"📊 **CRYPTO {title} SCAN** (TradingView Data)\n")
    msg_parts.append(f"_Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_\n")

    if aplus:
        upside = []
        downside = []
        for sym, info in aplus.items():
            arrows = info.split(" — ")[0].replace("M/W/D ", "")
            if "↑" in arrows[-1:]:
                upside.append((sym, info))
            else:
                downside.append((sym, info))
        
        msg_parts.append("\n🔥 **A++ SETUPS (No FTFC Required)**\n")
        if upside:
            msg_parts.append("🔺 Upside:\n")
            for sym, info in upside:
                msg_parts.append(f"• {pretty(sym)} — {info}\n")
        if downside:
            msg_parts.append("🔻 Downside:\n")
            for sym, info in downside:
                msg_parts.append(f"• {pretty(sym)} — {info}\n")

    if f2u or f2d:
        msg_parts.append("\n⚡ **Failed 2 (FTFC Required)**\n")
        if f2d:
            msg_parts.append("🔺 F2D (Bullish):\n")
            for sym in group_sort(f2d):
                if sym not in aplus:
                    msg_parts.append(f"• {pretty(sym)}\n")
        if f2u:
            msg_parts.append("🔻 F2U (Bearish):\n")
            for sym in group_sort(f2u):
                if sym not in aplus:
                    msg_parts.append(f"• {pretty(sym)}\n")

    if inside:
        msg_parts.append("\n📦 **Inside (1)**\n")
        for sym in group_sort(inside):
            msg_parts.append(f"• {pretty(sym)}\n")

    if outside:
        msg_parts.append("\n💥 **Outside (3)**\n")
        for sym in group_sort(outside):
            msg_parts.append(f"• {pretty(sym)}\n")

    if double_inside:
        msg_parts.append("\n📦📦 **Double Inside (II)**\n")
        for sym in group_sort(double_inside):
            msg_parts.append(f"• {pretty(sym)}\n")

    msg = "".join(msg_parts)

    print(msg)

    if discord_webhook:
        send_discord(msg, discord_webhook)
        
        all_setups = list(aplus.keys()) + f2u + f2d
        if all_setups:
            send_discord_csv([to_product_id(s) for s in all_setups], title, discord_webhook)

    print("DONE (Crypto Discord)")

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    
    if RUN_MODE == "ALL":
        print("=== DAILY SCAN ===")
        scan("Daily", "D", WEBHOOK_CRYPTO_DAILY)
        print("\n=== WEEKLY SCAN ===")
        scan("Weekly", "W", WEBHOOK_CRYPTO_WEEKLY)
    elif RUN_MODE == "DAILY":
        scan("Daily", "D", WEBHOOK_CRYPTO_DAILY)
    elif RUN_MODE == "WEEKLY":
        scan("Weekly", "W", WEBHOOK_CRYPTO_WEEKLY)
    else:
        print(f"Unknown RUN_MODE: {RUN_MODE}")
