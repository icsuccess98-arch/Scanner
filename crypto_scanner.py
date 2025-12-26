import os
import time
import requests
from datetime import datetime, timedelta
from coinbase.rest import RESTClient

COINBASE_API_KEY = os.environ.get("COINBASE_API_KEY", "")
COINBASE_PRIVATE_KEY = os.environ.get("COINBASE_PRIVATE_KEY", "")
CRYPTO_WEBHOOK = os.environ.get("CRYPTO_DISCORD_WEBHOOK", "")

PERP_SYMBOLS = {
    "BIP-20DEC30-CDE": "BTC",
    "ETP-20DEC30-CDE": "ETH",
    "SLP-20DEC30-CDE": "SOL",
    "BCP-20DEC30-CDE": "BCH",
    "LCP-20DEC30-CDE": "LTC",
    "XPP-20DEC30-CDE": "XRP",
    "AVP-20DEC30-CDE": "AVAX",
}

def send_discord(msg):
    if not CRYPTO_WEBHOOK:
        print("No crypto webhook configured")
        return
    payload = {"content": msg}
    try:
        requests.post(CRYPTO_WEBHOOK, json=payload)
    except Exception as e:
        print(f"Discord error: {e}")

def get_coinbase_client():
    if not COINBASE_API_KEY or not COINBASE_PRIVATE_KEY:
        print("Coinbase API keys not configured")
        return None
    try:
        client = RESTClient(api_key=COINBASE_API_KEY, api_secret=COINBASE_PRIVATE_KEY)
        return client
    except Exception as e:
        print(f"Coinbase client error: {e}")
        return None

def get_candles(client, product_id, granularity="ONE_DAY", limit=5):
    try:
        end = int(datetime.now().timestamp())
        granularity_seconds = {
            "ONE_MINUTE": 60,
            "FIVE_MINUTE": 300,
            "FIFTEEN_MINUTE": 900,
            "ONE_HOUR": 3600,
            "SIX_HOUR": 21600,
            "ONE_DAY": 86400
        }
        seconds = granularity_seconds.get(granularity, 86400)
        start = end - (seconds * (limit + 2))
        
        response = client.get_candles(
            product_id=product_id,
            start=str(start),
            end=str(end),
            granularity=granularity
        )
        
        if not response or not hasattr(response, 'candles'):
            return None
            
        candles = []
        for c in response.candles:
            candles.append({
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "time": int(c.start)
            })
        
        candles.sort(key=lambda x: x["time"])
        return candles[-limit:] if len(candles) >= limit else None
        
    except Exception as e:
        print(f"Error fetching {product_id}: {e}")
        return None

def direction(candle):
    return "UP" if candle["close"] > candle["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

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

def ftfc_pass(daily, weekly, monthly):
    d_dir = direction(daily)
    w_dir = direction(weekly)
    m_dir = direction(monthly)
    if d_dir == w_dir == m_dir:
        return d_dir
    return None

def pretty(product_id):
    return PERP_SYMBOLS.get(product_id, product_id)

def scan_perpetuals():
    print("Starting Coinbase Perpetuals Scan...")
    
    client = get_coinbase_client()
    if not client:
        return
    
    inside = []
    outside = []
    double_inside = []
    f2u = []
    f2d = []
    aplus = {}
    
    for product_id, name in PERP_SYMBOLS.items():
        print(f"Scanning {name} ({product_id})...")
        
        daily = get_candles(client, product_id, "ONE_DAY", 5)
        if not daily or len(daily) < 4:
            continue
        
        curr = daily[-1]
        prev = daily[-2]
        prev2 = daily[-3]
        prev3 = daily[-4] if len(daily) >= 4 else None
        
        st = strat_type(curr, prev)
        
        weekly = get_candles(client, product_id, "ONE_DAY", 7)
        monthly = get_candles(client, product_id, "ONE_DAY", 30)
        
        weekly_candle = None
        monthly_candle = None
        if weekly and len(weekly) >= 7:
            weekly_candle = {
                "open": weekly[0]["open"],
                "high": max(c["high"] for c in weekly),
                "low": min(c["low"] for c in weekly),
                "close": weekly[-1]["close"]
            }
        if monthly and len(monthly) >= 20:
            monthly_candle = {
                "open": monthly[0]["open"],
                "high": max(c["high"] for c in monthly),
                "low": min(c["low"] for c in monthly),
                "close": monthly[-1]["close"]
            }
        
        if st == "1" and strat_type(prev, prev2) == "1":
            double_inside.append(product_id)
            if weekly_candle and monthly_candle:
                arrows = arrow(direction(monthly_candle)) + arrow(direction(weekly_candle)) + arrow(direction(curr))
                aplus[product_id] = f"M/W/D {arrows} — Double Inside"
            continue
        
        if st == "1":
            inside.append(product_id)
            if strat_type(prev, prev2) == "3" and weekly_candle and monthly_candle:
                arrows = arrow(direction(monthly_candle)) + arrow(direction(weekly_candle)) + arrow(direction(curr))
                aplus[product_id] = f"M/W/D {arrows} — 3-1"
        
        if st == "3":
            outside.append(product_id)
            if strat_type(prev, prev2) == "1" and weekly_candle and monthly_candle:
                arrows = arrow(direction(monthly_candle)) + arrow(direction(weekly_candle)) + arrow(direction(curr))
                aplus[product_id] = f"M/W/D {arrows} — 1-3"
        
        f2 = failed_2(curr, prev)
        if f2:
            if f2 == "Failed 2U":
                f2u.append(product_id)
            if f2 == "Failed 2D":
                f2d.append(product_id)
            
            prev_type = strat_type(prev, prev2)
            is_double_inside = prev3 and prev_type == "1" and strat_type(prev2, prev3) == "1"
            
            if weekly_candle and monthly_candle:
                arrows = arrow(direction(monthly_candle)) + arrow(direction(weekly_candle)) + arrow(direction(curr))
                if is_double_inside:
                    aplus[product_id] = f"M/W/D {arrows} — II-F2U" if f2 == "Failed 2U" else f"M/W/D {arrows} — II-F2D"
                elif prev_type == "1":
                    aplus[product_id] = f"M/W/D {arrows} — 1-F2U" if f2 == "Failed 2U" else f"M/W/D {arrows} — 1-F2D"
                elif prev_type == "3":
                    aplus[product_id] = f"M/W/D {arrows} — 3-F2U" if f2 == "Failed 2U" else f"M/W/D {arrows} — 3-F2D"
                else:
                    ftfc = ftfc_pass(curr, weekly_candle, monthly_candle)
                    if ftfc:
                        if f2 == "Failed 2U" and ftfc == "DOWN":
                            aplus[product_id] = f"M/W/D {arrows} — F2U"
                        if f2 == "Failed 2D" and ftfc == "UP":
                            aplus[product_id] = f"M/W/D {arrows} — F2D"
        
        time.sleep(0.3)
    
    today = datetime.now()
    date_header = today.strftime("%b %d, %Y")
    header = f"🪙 **Crypto Perpetuals STRAT — {date_header}**\n\n"
    msg = ""
    
    if aplus:
        msg += "🔥 **A++ Setups**\n\n"
        ups = []
        dns = []
        for sym, lbl in aplus.items():
            if "F2D" in lbl or "↑↑↑" in lbl:
                ups.append((sym, lbl))
            elif "F2U" in lbl or "↓↓↓" in lbl:
                dns.append((sym, lbl))
            elif "↑" in lbl:
                ups.append((sym, lbl))
            else:
                dns.append((sym, lbl))
        if ups:
            msg += "__**🟢 Upside (Long)**__\n"
            for sym, lbl in ups:
                msg += f"• **{pretty(sym)}** — {lbl}\n"
            msg += "\n"
        if dns:
            msg += "__**🔴 Downside (Short)**__\n"
            for sym, lbl in dns:
                msg += f"• **{pretty(sym)}** — {lbl}\n"
            msg += "\n"
    
    if double_inside:
        msg += "**🟪 Double Inside (II)**\n"
        for x in double_inside:
            msg += f"• {pretty(x)}\n"
        msg += "\n"
    
    if inside:
        msg += "**📘 Inside (1)**\n"
        for x in inside:
            msg += f"• {pretty(x)}\n"
        msg += "\n"
    
    if outside:
        msg += "**📕 Outside (3)**\n"
        for x in outside:
            msg += f"• {pretty(x)}\n"
        msg += "\n"
    
    if f2u:
        msg += "**🔴 F2U (Short Bias)**\n"
        for x in f2u:
            msg += f"• {pretty(x)}\n"
        msg += "\n"
    
    if f2d:
        msg += "**🟢 F2D (Long Bias)**\n"
        for x in f2d:
            msg += f"• {pretty(x)}\n"
        msg += "\n"
    
    if msg:
        full_msg = header + msg.strip()
        print(full_msg)
        send_discord(full_msg)
        print("\nSent to Discord!")
    else:
        print("No perpetuals signals found")

if __name__ == "__main__":
    scan_perpetuals()
