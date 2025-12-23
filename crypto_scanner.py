import os
import time
import requests
import json
import hmac
import hashlib
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt

# ---------------------------------------------------------
# DISCORD CONFIG
# ---------------------------------------------------------

WEBHOOK_CRYPTO_DAILY = os.environ.get("WEBHOOK_CRYPTO_DAILY", "")
WEBHOOK_CRYPTO_WEEKLY = os.environ.get("WEBHOOK_CRYPTO_WEEKLY", "")

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
# COINBASE CONFIG
# ---------------------------------------------------------

COINBASE_API_KEY = os.environ.get("COINBASE_API_KEY", "")
COINBASE_PRIVATE_KEY = os.environ.get("COINBASE_PRIVATE_KEY", "")
RUN_MODE = os.environ.get("RUN_MODE", "ALL")

def get_coinbase_jwt():
    request_method = "GET"
    request_path = "/api/v3/brokerage/products"
    
    private_key_bytes = COINBASE_PRIVATE_KEY.replace("\\n", "\n").encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None, backend=default_backend())
    
    uri = f"{request_method} api.coinbase.com{request_path}"
    
    payload = {
        "sub": COINBASE_API_KEY,
        "iss": "coinbase-cloud",
        "nbf": int(time.time()),
        "exp": int(time.time()) + 120,
        "uri": uri,
    }
    
    token = jwt.encode(payload, private_key, algorithm="ES256")
    return token

def get_candles_jwt(product_id, granularity):
    request_method = "GET"
    request_path = f"/api/v3/brokerage/products/{product_id}/candles"
    
    private_key_bytes = COINBASE_PRIVATE_KEY.replace("\\n", "\n").encode('utf-8')
    private_key = serialization.load_pem_private_key(private_key_bytes, password=None, backend=default_backend())
    
    uri = f"{request_method} api.coinbase.com{request_path}"
    
    payload = {
        "sub": COINBASE_API_KEY,
        "iss": "coinbase-cloud",
        "nbf": int(time.time()),
        "exp": int(time.time()) + 120,
        "uri": uri,
    }
    
    token = jwt.encode(payload, private_key, algorithm="ES256")
    return token

# ---------------------------------------------------------
# CRYPTO SYMBOLS (Coinbase product IDs)
# ---------------------------------------------------------

CRYPTO_SYMBOLS = [
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "DOGE-USD", "LTC-USD",
    "BCH-USD", "LINK-USD", "UNI-USD", "AAVE-USD", "DOT-USD", "XLM-USD", "ATOM-USD",
    "ENS-USD", "ETC-USD", "ARB-USD", "ICP-USD", "FIL-USD", "NEAR-USD", "APT-USD",
    "TRX-USD", "AVAX-USD", "SEI-USD", "OP-USD", "IMX-USD", "MATIC-USD", "COMP-USD",
    "CRV-USD", "SUSHI-USD", "RUNE-USD", "DYDX-USD", "LDO-USD", "SHIB-USD", "PEPE-USD",
    "FLOKI-USD", "BONK-USD", "BLUR-USD", "AXS-USD", "SAND-USD", "GALA-USD", "APE-USD",
    "ENJ-USD", "RENDER-USD", "FET-USD", "GRT-USD", "ONDO-USD", "INJ-USD", "ALGO-USD",
    "HBAR-USD", "FTM-USD", "FLOW-USD", "VET-USD", "QNT-USD", "THETA-USD", "SNX-USD",
    "MINA-USD", "POL-USD", "ZRO-USD", "JTO-USD", "PAXG-USD", "MNT-USD", "WLD-USD",
    "EIGEN-USD", "JUP-USD", "TIA-USD", "PYTH-USD", "W-USD", "STRK-USD", "DYM-USD",
    "SUPER-USD", "GMT-USD", "PEOPLE-USD", "ALT-USD", "ACE-USD", "ORDI-USD", "SKY-USD"
]

GROUP_ORDER = {
    "MAJOR": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "DOGE-USD", "LTC-USD", "BCH-USD"],
    "DEFI": ["LINK-USD", "UNI-USD", "AAVE-USD", "COMP-USD", "CRV-USD", "SUSHI-USD", "RUNE-USD", "DYDX-USD", "LDO-USD", "JUP-USD"],
    "L1_L2": ["DOT-USD", "ATOM-USD", "AVAX-USD", "NEAR-USD", "APT-USD", "SEI-USD", "OP-USD", "ARB-USD", "IMX-USD", "MATIC-USD", "INJ-USD", "TIA-USD", "STRK-USD"],
    "MEME": ["SHIB-USD", "PEPE-USD", "FLOKI-USD", "BONK-USD", "WLD-USD"],
    "AI_COMPUTE": ["RENDER-USD", "FET-USD", "GRT-USD", "ONDO-USD"],
    "GAMING": ["BLUR-USD", "AXS-USD", "SAND-USD", "GALA-USD", "APE-USD", "ENJ-USD", "IMX-USD"],
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def pretty(symbol):
    return symbol.replace("-USD", "")

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

# ---------------------------------------------------------
# FETCH CANDLES FROM COINBASE
# ---------------------------------------------------------

def get_coinbase_candles(product_id, granularity, count=5):
    granularity_map = {
        "D": "ONE_DAY",
        "W": "ONE_DAY",
        "M": "ONE_DAY"
    }
    
    cb_granularity = granularity_map.get(granularity, "ONE_DAY")
    
    if granularity == "W":
        days_back = count * 7 + 7
    elif granularity == "M":
        days_back = count * 31 + 31
    else:
        days_back = count + 2
    
    end_time = int(time.time())
    start_time = end_time - (days_back * 86400)
    
    url = f"https://api.coinbase.com/api/v3/brokerage/products/{product_id}/candles"
    params = {
        "start": str(start_time),
        "end": str(end_time),
        "granularity": cb_granularity
    }
    
    jwt_token = get_candles_jwt(product_id, granularity)
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    
    try:
        r = requests.get(url, headers=headers, params=params)
        data = r.json()
        
        if "candles" not in data:
            return None
        
        candles_raw = data["candles"]
        
        candles = []
        for c in candles_raw:
            candles.append({
                "time": int(c["start"]),
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"])
            })
        
        candles.sort(key=lambda x: x["time"])
        
        if granularity == "W":
            candles = aggregate_weekly(candles)
        elif granularity == "M":
            candles = aggregate_monthly(candles)
        
        if len(candles) < 3:
            return None
        
        return candles[-count:] if len(candles) >= count else candles
        
    except Exception as e:
        print(f"Error fetching {product_id}: {e}")
        return None

def aggregate_weekly(daily_candles):
    weekly = {}
    for c in daily_candles:
        dt = datetime.fromtimestamp(c["time"])
        week_start = dt - timedelta(days=dt.weekday())
        week_key = week_start.strftime("%Y-%W")
        
        if week_key not in weekly:
            weekly[week_key] = {
                "time": c["time"],
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"]
            }
        else:
            weekly[week_key]["high"] = max(weekly[week_key]["high"], c["high"])
            weekly[week_key]["low"] = min(weekly[week_key]["low"], c["low"])
            weekly[week_key]["close"] = c["close"]
    
    result = list(weekly.values())
    result.sort(key=lambda x: x["time"])
    return result

def aggregate_monthly(daily_candles):
    monthly = {}
    for c in daily_candles:
        dt = datetime.fromtimestamp(c["time"])
        month_key = dt.strftime("%Y-%m")
        
        if month_key not in monthly:
            monthly[month_key] = {
                "time": c["time"],
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"]
            }
        else:
            monthly[month_key]["high"] = max(monthly[month_key]["high"], c["high"])
            monthly[month_key]["low"] = min(monthly[month_key]["low"], c["low"])
            monthly[month_key]["close"] = c["close"]
    
    result = list(monthly.values())
    result.sort(key=lambda x: x["time"])
    return result

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

    for symbol in CRYPTO_SYMBOLS:

        candles = get_coinbase_candles(symbol, granularity)
        if not candles:
            continue

        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]
        prev3 = candles[-4] if len(candles) >= 4 else None

        st = strat_type(curr, prev)

        if st == "1" and strat_type(prev, prev2) == "1":
            double_inside.append(symbol)
            weekly = get_coinbase_candles(symbol, "W")
            monthly = get_coinbase_candles(symbol, "M")
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
                weekly = get_coinbase_candles(symbol, "W")
                monthly = get_coinbase_candles(symbol, "M")
                if weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                    aplus[symbol] = f"M/W/D {arrows} — 3-1"

        if st == "3":
            outside.append(symbol)
            if strat_type(prev, prev2) == "1":
                weekly = get_coinbase_candles(symbol, "W")
                monthly = get_coinbase_candles(symbol, "M")
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
            weekly = get_coinbase_candles(symbol, "W")
            monthly = get_coinbase_candles(symbol, "M")
            
            is_double_inside = prev3 and prev_type == "1" and strat_type(prev2, prev3) == "1"
            
            if weekly and monthly:
                arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))
                
                if is_double_inside:
                    if f2 == "Failed 2U":
                        aplus[symbol] = f"M/W/D {arrows} — II-F2U"
                    else:
                        aplus[symbol] = f"M/W/D {arrows} — II-F2D"
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
                    ftfc = ftfc_pass(curr, weekly[-1], monthly[-1])
                    if ftfc:
                        if f2 == "Failed 2U" and ftfc == "DOWN":
                            aplus[symbol] = f"M/W/D {arrows} — F2U"
                        if f2 == "Failed 2D" and ftfc == "UP":
                            aplus[symbol] = f"M/W/D {arrows} — F2D"

        time.sleep(0.3)

    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_header = today.strftime("%b %d, %Y")
    from_day = yesterday.strftime("%a %b %d")
    dc_header = f"🗓 <b>{title} Crypto Strat — {date_header}</b>\n"
    dc_header += f"(From {from_day} close)\n\n"
    
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
    dc_msg = dc_header + msg
    
    send_discord(dc_msg, discord_webhook)
    
    all_symbols = list(set(double_inside + inside + outside + f2u + f2d + list(aplus.keys())))
    send_discord_csv(all_symbols, title, discord_webhook)

# ---------------------------------------------------------
# RUNTIME
# ---------------------------------------------------------

if RUN_MODE in ("DAILY", "ALL"):
    scan("Daily", "D", WEBHOOK_CRYPTO_DAILY)

if RUN_MODE in ("WEEKLY", "ALL"):
    scan("Weekly", "W", WEBHOOK_CRYPTO_WEEKLY)

print("DONE (Crypto Discord)")
