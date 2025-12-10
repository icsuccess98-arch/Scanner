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
    
    inside_bars = []
    outside_list = []
    f2_setups = []
    aplus_setups = []
    
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
                inside_bars.append((ticker, ftfc_dir, arrows_str, label))
                all_setups.append(ticker)
            
            if st == "3":
                outside_list.append(ticker)
                all_setups.append(ticker)
            
            f2 = failed_2(curr, prev)
            if f2:
                if prev_st == "1":
                    if f2 == "F2U":
                        aplus_setups.append((ticker, ftfc_dir, arrows_str, "1-F2U", "DOWN"))
                        all_setups.append(ticker)
                    elif f2 == "F2D":
                        aplus_setups.append((ticker, ftfc_dir, arrows_str, "1-F2D", "UP"))
                        all_setups.append(ticker)
                elif prev_st == "3":
                    if f2 == "F2U":
                        aplus_setups.append((ticker, ftfc_dir, arrows_str, "3-F2U", "DOWN"))
                        all_setups.append(ticker)
                    elif f2 == "F2D":
                        aplus_setups.append((ticker, ftfc_dir, arrows_str, "3-F2D", "UP"))
                        all_setups.append(ticker)
                else:
                    if f2 == "F2U":
                        f2_setups.append((ticker, ftfc_dir, arrows_str, "F2U", "DOWN"))
                        all_setups.append(ticker)
                    elif f2 == "F2D":
                        f2_setups.append((ticker, ftfc_dir, arrows_str, "F2D", "UP"))
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
    
    msg2 = f"📋 **FTFC SETUPS**\n\n"
    
    if aplus_setups:
        msg2 += "🔥 **A++ SETUPS** — 1-F2 / 3-F2\n\n"
        ups = [s for s in aplus_setups if s[4] == "UP"]
        dns = [s for s in aplus_setups if s[4] == "DOWN"]
        if ups:
            msg2 += "__🟢 Upside__\n"
            for ticker, ftfc_dir, arrows, label, trade_dir in ups:
                msg2 += f"• **{ticker}** — M/W/D {arrows} — {label}\n"
            msg2 += "\n"
        if dns:
            msg2 += "__🔴 Downside__\n"
            for ticker, ftfc_dir, arrows, label, trade_dir in dns:
                msg2 += f"• **{ticker}** — M/W/D {arrows} — {label}\n"
            msg2 += "\n"
    
    double_inside_list = [s for s in inside_bars if s[3] == "Double Inside"]
    inside_list = [s for s in inside_bars if s[3] == "Inside"]
    
    if double_inside_list:
        msg2 += "**🟪 Double Inside (II)**\n"
        for ticker, ftfc_dir, arrows, label in double_inside_list:
            msg2 += f"• {ticker}\n"
        msg2 += "\n"
    
    if inside_list:
        msg2 += "**📘 Inside (1)**\n"
        for ticker, ftfc_dir, arrows, label in inside_list:
            msg2 += f"• {ticker}\n"
        msg2 += "\n"
    
    if outside_list:
        msg2 += "**📕 Outside (3)**\n"
        for ticker in outside_list:
            msg2 += f"• {ticker}\n"
        msg2 += "\n"
    
    f2u_list = [s[0] for s in f2_setups if s[3] == "F2U"] + [s[0] for s in aplus_setups if "F2U" in s[3]]
    f2d_list = [s[0] for s in f2_setups if s[3] == "F2D"] + [s[0] for s in aplus_setups if "F2D" in s[3]]
    
    if f2u_list:
        msg2 += "**🔴 F2U (Downside)**\n"
        for t in list(set(f2u_list)):
            msg2 += f"• {t}\n"
        msg2 += "\n"
    
    if f2d_list:
        msg2 += "**🟢 F2D (Upside)**\n"
        for t in list(set(f2d_list)):
            msg2 += f"• {t}\n"
        msg2 += "\n"
    
    send_discord(msg2.strip(), WEBHOOK_DAILY)
    
    all_symbols = list(set(all_setups))
    send_discord_csv(all_symbols, "FTFC Setups", WEBHOOK_DAILY)

scan()
print("DONE (FTFC World + Recipe)")
