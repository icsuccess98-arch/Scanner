#!/usr/bin/env python3
"""
Crypto Trading Scanner - Coinbase Perpetuals ONLY
Completely separate from Forex/OANDA workflows
Sends ONLY to crypto Discord webhook
"""

import os
import time
import requests
from datetime import datetime, timedelta
from coinbase.rest import RESTClient

CRYPTO_WEBHOOK = "https://discord.com/api/webhooks/1454128407293071393/kxpMSHKwzCqgcCdt-P3S3pqqJ_oCYaJ8wCujtzYI42c4-4hxpmxI59pPNM5ppoqueXC4"

COINBASE_API_KEY = os.environ.get("COINBASE_API_KEY", "")
COINBASE_PRIVATE_KEY = os.environ.get("COINBASE_PRIVATE_KEY", "")

PERP_PRODUCTS = {
    "BTC": "BTC-PERP-INTX",
    "ETH": "ETH-PERP-INTX",
    "SOL": "SOL-PERP-INTX",
    "XRP": "XRP-PERP-INTX",
    "DOGE": "DOGE-PERP-INTX",
    "AVAX": "AVAX-PERP-INTX",
    "LINK": "LINK-PERP-INTX",
    "SUI": "SUI-PERP-INTX",
    "DOT": "DOT-PERP-INTX",
    "ATOM": "ATOM-PERP-INTX",
    "LTC": "LTC-PERP-INTX",
    "BCH": "BCH-PERP-INTX",
    "NEAR": "NEAR-PERP-INTX",
    "UNI": "UNI-PERP-INTX",
    "APT": "APT-PERP-INTX",
    "INJ": "INJ-PERP-INTX",
    "OP": "OP-PERP-INTX",
    "ARB": "ARB-PERP-INTX",
    "FIL": "FIL-PERP-INTX",
    "RENDER": "RENDER-PERP-INTX",
    "HBAR": "HBAR-PERP-INTX",
    "FET": "FET-PERP-INTX",
    "TIA": "TIA-PERP-INTX",
    "SEI": "SEI-PERP-INTX",
    "AAVE": "AAVE-PERP-INTX",
    "STX": "STX-PERP-INTX",
    "IMX": "IMX-PERP-INTX",
    "WIF": "WIF-PERP-INTX",
    "SUSHI": "SUSHI-PERP-INTX",
    "RUNE": "RUNE-PERP-INTX",
    "CRV": "CRV-PERP-INTX",
    "LDO": "LDO-PERP-INTX",
    "SNX": "SNX-PERP-INTX",
    "ONDO": "ONDO-PERP-INTX",
    "WLD": "WLD-PERP-INTX",
}

def get_coinbase_client():
    if not COINBASE_API_KEY or not COINBASE_PRIVATE_KEY:
        print("Missing Coinbase API credentials")
        return None
    try:
        return RESTClient(api_key=COINBASE_API_KEY, api_secret=COINBASE_PRIVATE_KEY)
    except Exception as e:
        print(f"Coinbase client error: {e}")
        return None

def get_closed_candles(client, product_id, granularity="ONE_DAY", limit=5):
    """Get only CLOSED/COMPLETED candles - excludes the current incomplete candle"""
    try:
        now = int(datetime.now().timestamp())
        granularity_seconds = {
            "ONE_MINUTE": 60,
            "FIVE_MINUTE": 300,
            "FIFTEEN_MINUTE": 900,
            "ONE_HOUR": 3600,
            "SIX_HOUR": 21600,
            "ONE_DAY": 86400,
            "ONE_WEEK": 604800,
            "ONE_MONTH": 2592000
        }
        seconds = granularity_seconds.get(granularity, 86400)
        start = now - (seconds * (limit + 10))
        
        response = client.get_candles(
            product_id=product_id,
            start=str(start),
            end=str(now),
            granularity=granularity
        )
        
        if not response or not hasattr(response, 'candles'):
            return None
            
        candles = []
        for c in response.candles:
            candle_start = int(c.start)
            candle_end = candle_start + seconds
            if candle_end <= now:
                candles.append({
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "time": candle_start
                })
        
        candles.sort(key=lambda x: x["time"])
        
        if len(candles) < 3:
            return None
        
        return candles[-limit:] if len(candles) >= limit else candles
        
    except Exception as e:
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

def send_discord(msg, webhook):
    msg = msg.replace("<b>", "**").replace("</b>", "**")
    msg = msg.replace("<u>", "__").replace("</u>", "__")
    try:
        resp = requests.post(webhook, json={"content": msg}, timeout=10)
        return resp.status_code in [200, 204]
    except Exception as e:
        print(f"Discord error: {e}")
        return False

def send_discord_csv(symbols, title, webhook):
    if not symbols:
        return
    today = datetime.now().strftime("%Y%m%d")
    csv_content = ",".join(symbols)
    filename = f"crypto_{title.lower().replace(' ', '_')}_{today}.csv"
    
    try:
        files = {"file": (filename, csv_content, "text/csv")}
        resp = requests.post(webhook, files=files, timeout=15)
        return resp.status_code in [200, 204]
    except Exception as e:
        print(f"CSV upload error: {e}")
        return False

def scan_crypto(timeframe, title):
    print(f"\n{'='*50}")
    print(f"Starting Crypto {title} Scan - Coinbase Perpetuals")
    print(f"{'='*50}")
    
    client = get_coinbase_client()
    if not client:
        print("Failed to initialize Coinbase client")
        return
    
    inside_list = []
    outside_list = []
    double_inside_list = []
    f2u_list = []
    f2d_list = []
    aplus = {}
    
    for ticker, product_id in PERP_PRODUCTS.items():
        print(f"Scanning {ticker} ({product_id})...")
        
        candles = get_closed_candles(client, product_id, timeframe, limit=5)
        if not candles or len(candles) < 3:
            continue
        
        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]
        prev3 = candles[-4] if len(candles) >= 4 else None
        
        st = strat_type(curr, prev)
        
        if st == "1" and strat_type(prev, prev2) == "1":
            double_inside_list.append(ticker)
            daily = get_closed_candles(client, product_id, "ONE_DAY", limit=3)
            weekly = get_closed_candles(client, product_id, "ONE_WEEK", limit=3) if timeframe != "ONE_WEEK" else candles
            monthly = get_closed_candles(client, product_id, "ONE_MONTH", limit=3) if timeframe != "ONE_MONTH" else candles
            if daily and weekly and monthly:
                arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(daily[-1]))
                aplus[ticker] = f"M/W/D {arrows} — Double Inside"
            continue
        
        if st == "1":
            inside_list.append(ticker)
            if strat_type(prev, prev2) == "3":
                daily = get_closed_candles(client, product_id, "ONE_DAY", limit=3)
                weekly = get_closed_candles(client, product_id, "ONE_WEEK", limit=3) if timeframe != "ONE_WEEK" else candles
                monthly = get_closed_candles(client, product_id, "ONE_MONTH", limit=3) if timeframe != "ONE_MONTH" else candles
                if daily and weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(daily[-1]))
                    aplus[ticker] = f"M/W/D {arrows} — 3-1"
        
        if st == "3":
            outside_list.append(ticker)
            if strat_type(prev, prev2) == "1":
                daily = get_closed_candles(client, product_id, "ONE_DAY", limit=3)
                weekly = get_closed_candles(client, product_id, "ONE_WEEK", limit=3) if timeframe != "ONE_WEEK" else candles
                monthly = get_closed_candles(client, product_id, "ONE_MONTH", limit=3) if timeframe != "ONE_MONTH" else candles
                if daily and weekly and monthly:
                    arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(daily[-1]))
                    aplus[ticker] = f"M/W/D {arrows} — 1-3"
        
        f2 = failed_2(curr, prev)
        if f2:
            if f2 == "Failed 2U":
                f2u_list.append(ticker)
            if f2 == "Failed 2D":
                f2d_list.append(ticker)
            
            prev_type = strat_type(prev, prev2)
            daily = get_closed_candles(client, product_id, "ONE_DAY", limit=3)
            weekly = get_closed_candles(client, product_id, "ONE_WEEK", limit=3) if timeframe != "ONE_WEEK" else candles
            monthly = get_closed_candles(client, product_id, "ONE_MONTH", limit=3) if timeframe != "ONE_MONTH" else candles
            
            is_double_inside = prev3 and prev_type == "1" and strat_type(prev2, prev3) == "1"
            
            if daily and weekly and monthly:
                arrows = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(daily[-1]))
                
                if is_double_inside:
                    if f2 == "Failed 2U":
                        aplus[ticker] = f"M/W/D {arrows} — II-F2U"
                    else:
                        aplus[ticker] = f"M/W/D {arrows} — II-F2D"
                elif prev_type == "1":
                    if f2 == "Failed 2U":
                        aplus[ticker] = f"M/W/D {arrows} — 1-F2U"
                    else:
                        aplus[ticker] = f"M/W/D {arrows} — 1-F2D"
                elif prev_type == "3":
                    if f2 == "Failed 2U":
                        aplus[ticker] = f"M/W/D {arrows} — 3-F2U"
                    else:
                        aplus[ticker] = f"M/W/D {arrows} — 3-F2D"
                else:
                    ftfc = ftfc_pass(daily[-1], weekly[-1], monthly[-1])
                    if ftfc:
                        if f2 == "Failed 2U" and ftfc == "DOWN":
                            aplus[ticker] = f"M/W/D {arrows} — F2U"
                        if f2 == "Failed 2D" and ftfc == "UP":
                            aplus[ticker] = f"M/W/D {arrows} — F2D"
        
        time.sleep(0.3)
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_header = today.strftime("%b %d, %Y")
    from_day = yesterday.strftime("%a %b %d")
    
    header = f"🗓 <b>Crypto {title} Actionable Strat — {date_header}</b>\n"
    header += f"(From {from_day} close)\n\n"
    msg = ""
    
    if aplus:
        msg += "🔥 <b>A++ Setups</b>\n\n"
        ups = []
        dns = []
        for sym, lbl in aplus.items():
            lbl_short = lbl.replace("Failed 2U", "F2U").replace("Failed 2D", "F2D")
            if "F2D" in lbl or "↑↑↑" in lbl:
                ups.append((sym, lbl_short))
            elif "F2U" in lbl or "↓↓↓" in lbl:
                dns.append((sym, lbl_short))
            elif "↑" in lbl:
                ups.append((sym, lbl_short))
            else:
                dns.append((sym, lbl_short))
        if ups:
            msg += "<u><b>🟢 Upside</b></u>\n"
            for sym, lbl in ups:
                msg += f"• <b>{sym}</b> — {lbl}\n"
            msg += "\n"
        if dns:
            msg += "<u><b>🔴 Downside</b></u>\n"
            for sym, lbl in dns:
                msg += f"• <b>{sym}</b> — {lbl}\n"
            msg += "\n"
    
    if double_inside_list:
        msg += "<b>🟪 Double Inside (II)</b>\n"
        for x in double_inside_list:
            msg += f"• {x}\n"
        msg += "\n"
    
    if inside_list:
        msg += "<b>📘 Inside (1)</b>\n"
        for x in inside_list:
            msg += f"• {x}\n"
        msg += "\n"
    
    if outside_list:
        msg += "<b>📕 Outside (3)</b>\n"
        for x in outside_list:
            msg += f"• {x}\n"
        msg += "\n"
    
    if f2u_list:
        msg += "<b>🔴 F2U</b>\n"
        for x in f2u_list:
            msg += f"• {x}\n"
        msg += "\n"
    
    if f2d_list:
        msg += "<b>🟢 F2D</b>\n"
        for x in f2d_list:
            msg += f"• {x}\n"
        msg += "\n"
    
    if msg:
        full_msg = header + msg.strip()
        if send_discord(full_msg, CRYPTO_WEBHOOK):
            print(f"Crypto {title} scan sent to Discord!")
        all_symbols = list(set(double_inside_list + inside_list + outside_list + f2u_list + f2d_list + list(aplus.keys())))
        send_discord_csv(all_symbols, title, CRYPTO_WEBHOOK)
    else:
        no_signals_msg = f"🗓 <b>Crypto {title} Actionable Strat — {date_header}</b>\n\nNo qualifying patterns found today."
        send_discord(no_signals_msg, CRYPTO_WEBHOOK)
        print(f"No crypto {title} signals found")

if __name__ == "__main__":
    run_mode = os.environ.get("CRYPTO_RUN_MODE", "DAILY").upper()
    
    if run_mode == "DAILY":
        scan_crypto("ONE_DAY", "Daily")
    elif run_mode == "WEEKLY":
        scan_crypto("ONE_WEEK", "Weekly")
    elif run_mode == "MONTHLY":
        scan_crypto("ONE_MONTH", "Monthly")
    else:
        print(f"Unknown run mode: {run_mode}")
    
    print("\nCrypto scan complete!")
