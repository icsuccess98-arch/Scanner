import os
import requests
import yfinance as yf
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

WEBHOOK_DAILY = os.environ.get("WEBHOOK_DAILY", "")

def send_discord(msg, webhook_url):
    if not webhook_url:
        return
    if len(msg) <= 2000:
        requests.post(webhook_url, json={"content": msg})
    else:
        chunks = []
        lines = msg.split('\n')
        current = ""
        for line in lines:
            if len(current) + len(line) + 1 > 1900:
                chunks.append(current)
                current = line
            else:
                current = current + "\n" + line if current else line
        if current:
            chunks.append(current)
        for chunk in chunks:
            requests.post(webhook_url, json={"content": chunk})

def to_tv_symbol(ticker):
    return f"NASDAQ:{ticker}"

def send_discord_csv(symbols, title, webhook_url):
    if not webhook_url or not symbols:
        return
    tv_symbols = [to_tv_symbol(s) for s in symbols]
    csv_content = ",".join(tv_symbols)
    files = {"file": (f"{title.lower()}_watchlist.txt", csv_content, "text/plain")}
    data = {"content": f"**{title} TradingView Watchlist**"}
    requests.post(webhook_url, data=data, files=files)

ALL_STOCKS = [
    "SPY", "QQQ", "IWM", "DIA", "VTI", "GLD", "SLV", "USO", "TLT", "HYG",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB",
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "JNJ",
    "WMT", "PG", "UNH", "HD", "DIS", "MA", "BAC", "XOM", "PFE"
]

def get_candles(ticker, period="3mo", interval="1d"):
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        if data.empty or len(data) < 4:
            return None
        candles = []
        for idx, row in data.iterrows():
            candles.append({
                "open": float(row["Open"].iloc[0]) if hasattr(row["Open"], 'iloc') else float(row["Open"]),
                "high": float(row["High"].iloc[0]) if hasattr(row["High"], 'iloc') else float(row["High"]),
                "low": float(row["Low"].iloc[0]) if hasattr(row["Low"], 'iloc') else float(row["Low"]),
                "close": float(row["Close"].iloc[0]) if hasattr(row["Close"], 'iloc') else float(row["Close"]),
            })
        return candles
    except:
        return None

def get_weekly_candles(ticker):
    return get_candles(ticker, period="1y", interval="1wk")

def get_monthly_candles(ticker):
    return get_candles(ticker, period="5y", interval="1mo")

def get_hourly_candles(ticker):
    return get_candles(ticker, period="5d", interval="1h")

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

def strat_type(curr, prev):
    inside = curr["high"] <= prev["high"] and curr["low"] >= prev["low"]
    outside = curr["high"] > prev["high"] and curr["low"] < prev["low"]
    if inside:
        return "1"
    if outside:
        return "3"
    two_up = curr["high"] > prev["high"] and curr["low"] >= prev["low"]
    two_down = curr["low"] < prev["low"] and curr["high"] <= prev["high"]
    if two_up:
        return "2U"
    if two_down:
        return "2D"
    return "2"

def failed_2(curr, prev):
    broke_high = curr["high"] > prev["high"]
    broke_low = curr["low"] < prev["low"]
    closed_below_mid = curr["close"] < (prev["high"] + prev["low"]) / 2
    closed_above_mid = curr["close"] > (prev["high"] + prev["low"]) / 2
    if broke_high and not broke_low and closed_below_mid:
        return "F2U"
    if broke_low and not broke_high and closed_above_mid:
        return "F2D"
    return None

def scan():
    ftfc_up = []
    ftfc_down = []
    ftfc_data = {}
    
    pure_rob_inside = []
    pure_rob_22_cont = []
    pure_rob_212 = []
    hybrid_daily_f2 = []
    hybrid_hourly_f2 = []
    
    all_setups = []

    for ticker in ALL_STOCKS:
        try:
            daily = get_candles(ticker)
            weekly = get_weekly_candles(ticker)
            monthly = get_monthly_candles(ticker)
            
            if not daily or not weekly or not monthly:
                continue
            if len(daily) < 4 or len(weekly) < 2 or len(monthly) < 2:
                continue
            
            d_dir = direction(daily[-1])
            w_dir = direction(weekly[-1])
            m_dir = direction(monthly[-1])
            
            if d_dir != w_dir or w_dir != m_dir:
                continue
                
            ftfc_dir = d_dir
            arrows_str = arrow(m_dir) + arrow(w_dir) + arrow(d_dir)
            
            if ftfc_dir == "UP":
                ftfc_up.append(ticker)
            else:
                ftfc_down.append(ticker)
            
            ftfc_data[ticker] = {"dir": ftfc_dir, "arrows": arrows_str}
            
            curr = daily[-1]
            prev = daily[-2]
            prev2 = daily[-3]
            prev3 = daily[-4] if len(daily) >= 4 else None
            
            st = strat_type(curr, prev)
            prev_st = strat_type(prev, prev2)
            
            if st == "1":
                label = "Inside"
                if prev_st == "1":
                    label = "Double Inside"
                pure_rob_inside.append((ticker, ftfc_dir, arrows_str, label))
                all_setups.append(ticker)
            
            if ftfc_dir == "UP" and st == "2U" and prev_st == "2U":
                pure_rob_22_cont.append((ticker, ftfc_dir, arrows_str, "2U→2U"))
                all_setups.append(ticker)
            elif ftfc_dir == "DOWN" and st == "2D" and prev_st == "2D":
                pure_rob_22_cont.append((ticker, ftfc_dir, arrows_str, "2D→2D"))
                all_setups.append(ticker)
            
            if prev3:
                prev2_st = strat_type(prev2, prev3)
                if prev_st == "1":
                    if ftfc_dir == "UP" and prev2_st == "2D" and st == "2U":
                        pure_rob_212.append((ticker, ftfc_dir, arrows_str, "2D-1-2U"))
                        all_setups.append(ticker)
                    elif ftfc_dir == "DOWN" and prev2_st == "2U" and st == "2D":
                        pure_rob_212.append((ticker, ftfc_dir, arrows_str, "2U-1-2D"))
                        all_setups.append(ticker)
                    elif prev2_st == "3":
                        if ftfc_dir == "UP" and st == "2U":
                            pure_rob_212.append((ticker, ftfc_dir, arrows_str, "3-1-2U"))
                            all_setups.append(ticker)
                        elif ftfc_dir == "DOWN" and st == "2D":
                            pure_rob_212.append((ticker, ftfc_dir, arrows_str, "3-1-2D"))
                            all_setups.append(ticker)
            
            f2 = failed_2(curr, prev)
            if f2:
                if f2 == "F2D" and ftfc_dir == "UP":
                    hybrid_daily_f2.append((ticker, ftfc_dir, arrows_str, "Failed 2 Down"))
                    all_setups.append(ticker)
                elif f2 == "F2U" and ftfc_dir == "DOWN":
                    hybrid_daily_f2.append((ticker, ftfc_dir, arrows_str, "Failed 2 Up"))
                    all_setups.append(ticker)
            
            hourly = get_hourly_candles(ticker)
            if hourly and len(hourly) >= 3:
                h_curr = hourly[-1]
                h_prev = hourly[-2]
                h_f2 = failed_2(h_curr, h_prev)
                if h_f2:
                    if h_f2 == "F2D" and ftfc_dir == "UP":
                        hybrid_hourly_f2.append((ticker, ftfc_dir, arrows_str, "1h F2D"))
                        all_setups.append(ticker)
                    elif h_f2 == "F2U" and ftfc_dir == "DOWN":
                        hybrid_hourly_f2.append((ticker, ftfc_dir, arrows_str, "1h F2U"))
                        all_setups.append(ticker)
        except:
            continue

    today = datetime.now()
    date_header = today.strftime("%b %d, %Y %H:%M")
    
    msg1 = f"🌍 **FTFC WORLD** — As of {date_header}\n\n"
    msg1 += "**0) FTFC UNIVERSE** — Month/Week/Day all same direction\n\n"
    
    if ftfc_up:
        msg1 += "**FTFC ↑**\n"
        for t in sorted(ftfc_up):
            msg1 += f"• {t} (M/W/D ↑↑↑)\n"
        msg1 += "\n"
    
    if ftfc_down:
        msg1 += "**FTFC ↓**\n"
        for t in sorted(ftfc_down):
            msg1 += f"• {t} (M/W/D ↓↓↓)\n"
        msg1 += "\n"
    
    send_discord(msg1.strip(), WEBHOOK_DAILY)
    
    msg2 = f"📋 **FTFC RECIPE** — Pure Rob + Hybrid Setups\n\n"
    
    if pure_rob_inside:
        msg2 += "**1) PURE ROB — Inside / Double Inside + FTFC**\n"
        for ticker, ftfc_dir, arrows, label in pure_rob_inside:
            emoji = "🟢" if ftfc_dir == "UP" else "🔴"
            msg2 += f"{emoji} **{ticker}** — M/W/D {arrows} — D: {label}\n"
        msg2 += "\n"
    
    if pure_rob_22_cont:
        msg2 += "**2) PURE ROB — 2-2 Continuation + FTFC**\n"
        for ticker, ftfc_dir, arrows, label in pure_rob_22_cont:
            emoji = "🟢" if ftfc_dir == "UP" else "🔴"
            msg2 += f"{emoji} **{ticker}** — M/W/D {arrows} — D: {label}\n"
        msg2 += "\n"
    
    if pure_rob_212:
        msg2 += "**3) PURE ROB — 2-1-2 or 3-1-2 + FTFC**\n"
        for ticker, ftfc_dir, arrows, label in pure_rob_212:
            emoji = "🟢" if ftfc_dir == "UP" else "🔴"
            msg2 += f"{emoji} **{ticker}** — M/W/D {arrows} — D: {label}\n"
        msg2 += "\n"
    else:
        msg2 += "**3) PURE ROB — 2-1-2 or 3-1-2 + FTFC**\n_None_\n\n"
    
    if hybrid_daily_f2:
        msg2 += "**4) HYBRID — Daily Failed 2 with FTFC**\n"
        for ticker, ftfc_dir, arrows, label in hybrid_daily_f2:
            emoji = "🟢" if ftfc_dir == "UP" else "🔴"
            msg2 += f"{emoji} **{ticker}** — M/W/D {arrows} — D: {label}\n"
        msg2 += "\n"
    else:
        msg2 += "**4) HYBRID — Daily Failed 2 with FTFC**\n_None_\n\n"
    
    if hybrid_hourly_f2:
        msg2 += "**5) HYBRID — 1h Failed 2 back into FTFC**\n"
        for ticker, ftfc_dir, arrows, label in hybrid_hourly_f2:
            emoji = "🟢" if ftfc_dir == "UP" else "🔴"
            msg2 += f"{emoji} **{ticker}** — M/W/D {arrows} — {label}\n"
        msg2 += "\n"
    else:
        msg2 += "**5) HYBRID — 1h Failed 2 back into FTFC**\n_None_\n\n"
    
    send_discord(msg2.strip(), WEBHOOK_DAILY)
    
    all_symbols = list(set(all_setups))
    send_discord_csv(all_symbols, "FTFC Setups", WEBHOOK_DAILY)

scan()
print("DONE (FTFC World + Recipe)")
