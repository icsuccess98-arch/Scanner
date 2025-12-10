import os
import time
import requests
import yfinance as yf
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

def html_to_markdown(msg):
    out = msg
    out = out.replace("<b>", "**").replace("</b>", "**")
    out = out.replace("<u>", "__").replace("</u>", "__")
    return out

def send_discord(msg, webhook_url):
    if not webhook_url:
        return
    md = html_to_markdown(msg)
    requests.post(webhook_url, json={"content": md})

def to_tv_symbol(ticker):
    return f"NASDAQ:{ticker}"

def send_discord_csv(symbols, title, webhook_url):
    if not webhook_url or not symbols:
        return
    tv_symbols = [to_tv_symbol(s) for s in symbols]
    csv_content = ",".join(tv_symbols)
    files = {
        "file": (f"{title.lower()}_stocks_watchlist.txt", csv_content, "text/plain")
    }
    data = {"content": f"📋 **{title} Stocks TradingView Watchlist**"}
    requests.post(webhook_url, data=data, files=files)

# ---------------------------------------------------------
# RUN MODE
# ---------------------------------------------------------

RUN_MODE = os.environ.get("RUN_MODE", "ALL")

# ---------------------------------------------------------
# TOP 200 STOCK TICKERS
# ---------------------------------------------------------

# ETFs
ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "VXX", "UVXY", "SQQQ", "TQQQ",
    "GLD", "SLV", "USO", "TLT", "HYG", "LQD", "EEM", "EFA", "XLF", "XLE",
    "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC", "VNQ",
    "ARKK", "ARKG", "ARKF", "IYR", "SMH", "SOXX", "IBB", "XBI", "KRE", "XRT"
]

# Mega Cap Tech
MEGA_TECH = [
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "NVDA", "AVGO", "ORCL"
]

# Tech & Software
TECH = [
    "AMD", "INTC", "QCOM", "MU", "CSCO", "IBM", "TXN", "AMAT", "LRCX", "KLAC",
    "ADI", "MRVL", "NXPI", "ON", "MCHP", "SNPS", "CDNS", "FTNT", "PANW", "CRWD",
    "ZS", "NET", "DDOG", "SNOW", "MDB", "PLTR", "DOCN", "PATH", "U", "RBLX"
]

# Software & Cloud
SOFTWARE = [
    "CRM", "ADBE", "NOW", "INTU", "WDAY", "TEAM", "DOCU", "ZM", "OKTA", "HUBS",
    "VEEV", "SPLK", "TWLO", "SQ", "PYPL", "SHOP", "COIN", "HOOD", "AFRM", "UPST"
]

# Consumer & Retail
CONSUMER = [
    "WMT", "COST", "TGT", "HD", "LOW", "AMZN", "BABA", "JD", "PDD", "EBAY",
    "ETSY", "W", "CHWY", "DG", "DLTR", "ROST", "TJX", "BBY", "GME", "AMC"
]

# Financials
FINANCIALS = [
    "JPM", "BAC", "WFC", "C", "GS", "MS", "USB", "PNC", "TFC", "SCHW",
    "BLK", "SPGI", "ICE", "CME", "COIN", "V", "MA", "AXP", "COF", "DFS"
]

# Healthcare & Pharma
HEALTHCARE = [
    "UNH", "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "AMGN", "GILD", "REGN",
    "VRTX", "BIIB", "MRNA", "BNTX", "CVS", "CI", "HUM", "ELV", "MCK", "ABC"
]

# Energy
ENERGY = [
    "XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "PXD",
    "DVN", "HAL", "BKR", "FANG", "HES", "APA", "MRO", "OKE", "WMB", "KMI"
]

# Industrials & Defense
INDUSTRIALS = [
    "BA", "LMT", "RTX", "NOC", "GD", "CAT", "DE", "UNP", "UPS", "FDX",
    "DAL", "UAL", "AAL", "LUV", "GE", "HON", "MMM", "EMR", "ITW", "PH"
]

# Communication & Media
MEDIA = [
    "DIS", "NFLX", "CMCSA", "T", "VZ", "TMUS", "CHTR", "WBD", "PARA", "FOX",
    "ROKU", "SPOT", "SNAP", "PINS", "MTCH", "LYFT", "UBER", "ABNB", "BKNG", "EXPE"
]

# Cannabis & Misc
MISC = [
    "TLRY", "CGC", "ACB", "SNDL", "MARA", "RIOT", "CLSK", "BITF", "HIVE", "HUT",
    "SOFI", "LCID", "RIVN", "NIO", "XPEV", "LI", "FSR", "F", "GM", "STLA"
]

# Utilities & REITS
UTILITIES = [
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "XEL", "ED", "PEG"
]

ALL_STOCKS = list(set(
    ETFS + MEGA_TECH + TECH + SOFTWARE + CONSUMER + 
    FINANCIALS + HEALTHCARE + ENERGY + INDUSTRIALS + MEDIA + MISC + UTILITIES
))

GROUP_ORDER = {
    "ETFS": ETFS,
    "MEGA_TECH": MEGA_TECH,
    "TECH": TECH,
    "SOFTWARE": SOFTWARE,
    "CONSUMER": CONSUMER,
    "FINANCIALS": FINANCIALS,
    "HEALTHCARE": HEALTHCARE,
    "ENERGY": ENERGY,
    "INDUSTRIALS": INDUSTRIALS,
    "MEDIA": MEDIA,
    "MISC": MISC,
    "UTILITIES": UTILITIES
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

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

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

# ---------------------------------------------------------
# STRAT LOGIC
# ---------------------------------------------------------

def strat_type(curr, prev):
    inside = curr["high"] <= prev["high"] and curr["low"] >= prev["low"]
    outside = curr["high"] > prev["high"] and curr["low"] < prev["low"]
    if inside:
        return "1"
    if outside:
        return "3"
    return "2"

def failed_2(curr, prev):
    broke_high = curr["high"] > prev["high"]
    broke_low = curr["low"] < prev["low"]
    closed_below_mid = curr["close"] < (prev["high"] + prev["low"]) / 2
    closed_above_mid = curr["close"] > (prev["high"] + prev["low"]) / 2

    if broke_high and not broke_low and closed_below_mid:
        return "Failed 2U"
    if broke_low and not broke_high and closed_above_mid:
        return "Failed 2D"
    return None

def ftfc_pass(daily, weekly, monthly):
    dd = direction(daily)
    wd = direction(weekly)
    md = direction(monthly)
    if dd == wd == md == "UP":
        return "UP"
    if dd == wd == md == "DOWN":
        return "DOWN"
    return None

def group_sort(symbols):
    order = []
    for grp, members in GROUP_ORDER.items():
        for s in members:
            if s in symbols and s not in order:
                order.append(s)
    for s in symbols:
        if s not in order:
            order.append(s)
    return order

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
    ftfc_up = []
    ftfc_down = []

    # First pass: identify FTFC stocks only
    ftfc_stocks = []
    for ticker in ALL_STOCKS:
        try:
            daily = get_candles(ticker)
            weekly = get_weekly_candles(ticker)
            monthly = get_monthly_candles(ticker)
            
            if not daily or not weekly or not monthly:
                continue
            if len(daily) < 2 or len(weekly) < 2 or len(monthly) < 2:
                continue
            
            d_dir = direction(daily[-1])
            w_dir = direction(weekly[-1])
            m_dir = direction(monthly[-1])
            
            if d_dir == w_dir == m_dir:
                ftfc_stocks.append(ticker)
                if d_dir == "UP":
                    ftfc_up.append(ticker)
                else:
                    ftfc_down.append(ticker)
        except:
            continue

    # Second pass: scan only FTFC stocks for setups
    for ticker in ftfc_stocks:
        try:
            if granularity == "D":
                candles = get_candles(ticker)
            elif granularity == "W":
                candles = get_weekly_candles(ticker)
            else:
                candles = get_monthly_candles(ticker)

            if not candles or len(candles) < 4:
                continue

            curr = candles[-1]
            prev = candles[-2]
            prev2 = candles[-3]

            st = strat_type(curr, prev)

            if st == "1" and strat_type(prev, prev2) == "1":
                double_inside.append(ticker)
                weekly = get_weekly_candles(ticker)
                monthly = get_monthly_candles(ticker)
                if weekly and monthly and len(weekly) >= 2 and len(monthly) >= 2:
                    arrows_str = (
                        arrow(direction(monthly[-1])) +
                        arrow(direction(weekly[-1])) +
                        arrow(direction(curr))
                    )
                    aplus[ticker] = f"M/W/D {arrows_str} — Double Inside"
                continue

            if st == "1":
                inside.append(ticker)

            if st == "3":
                outside.append(ticker)

            f2 = failed_2(curr, prev)
            if f2:
                if f2 == "Failed 2U":
                    f2u.append(ticker)
                if f2 == "Failed 2D":
                    f2d.append(ticker)

                prev_type = strat_type(prev, prev2)
                weekly = get_weekly_candles(ticker)
                monthly = get_monthly_candles(ticker)

                if weekly and monthly and len(weekly) >= 2 and len(monthly) >= 2:
                    arrows_str = arrow(direction(monthly[-1])) + arrow(direction(weekly[-1])) + arrow(direction(curr))

                    if prev_type == "1":
                        if f2 == "Failed 2U":
                            aplus[ticker] = f"M/W/D {arrows_str} — 1-F2U"
                        else:
                            aplus[ticker] = f"M/W/D {arrows_str} — 1-F2D"
                    elif prev_type == "3":
                        if f2 == "Failed 2U":
                            aplus[ticker] = f"M/W/D {arrows_str} — 3-F2U"
                        else:
                            aplus[ticker] = f"M/W/D {arrows_str} — 3-F2D"
                    else:
                        ftfc = ftfc_pass(curr, weekly[-1], monthly[-1])
                        if ftfc:
                            if f2 == "Failed 2U" and ftfc == "DOWN":
                                aplus[ticker] = f"M/W/D {arrows_str} — F2U"
                            if f2 == "Failed 2D" and ftfc == "UP":
                                aplus[ticker] = f"M/W/D {arrows_str} — F2D"

        except Exception as e:
            continue

    # Telegram header (simple)
    tg_header = f"📊 <b>{title} Stock Actionables</b>\n\n"

    # Discord header (with dates)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date_header = today.strftime("%b %d, %Y")
    from_day = yesterday.strftime("%a %b %d")
    dc_header = f"🗓 <b>{title} Stock Actionable Strat — {date_header}</b>\n"
    dc_header += f"(From {from_day} close)\n\n"

    msg = ""
    
    # FTFC Universe section
    msg += "<b>0) FTFC UNIVERSE</b> — M/W/D all same direction\n"
    if ftfc_up:
        msg += f"• FTFC ↑ : {', '.join(ftfc_up)}\n"
    if ftfc_down:
        msg += f"• FTFC ↓ : {', '.join(ftfc_down)}\n"
    msg += "\n"

    if aplus:
        msg += "🔥 <b>A++ Setups</b>\n\n"
        ups = []
        dns = []

        for sym, lbl in aplus.items():
            lbl_short = lbl.replace("Failed 2U", "F2U").replace("Failed 2D", "F2D")
            if "F2D" in lbl_short or ("Double Inside" in lbl and "↑" in lbl):
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

    if double_inside:
        msg += "<b>🟪 Double Inside (II)</b>\n"
        for x in group_sort(double_inside):
            msg += f"• {x}\n"
        msg += "\n"

    if inside:
        msg += "<b>📘 Inside (1)</b>\n"
        for x in group_sort(inside):
            msg += f"• {x}\n"
        msg += "\n"

    if outside:
        msg += "<b>📕 Outside (3)</b>\n"
        for x in group_sort(outside):
            msg += f"• {x}\n"
        msg += "\n"

    if f2u:
        msg += "<b>🔴 F2U</b>\n"
        for x in group_sort(f2u):
            msg += f"• {x}\n"
        msg += "\n"

    if f2d:
        msg += "<b>🟢 F2D</b>\n"
        for x in group_sort(f2d):
            msg += f"• {x}\n"
        msg += "\n"

    msg = msg.strip()
    tg_msg = tg_header + msg
    dc_msg = dc_header + msg
    
    discord_only = os.environ.get("DISCORD_ONLY", "").upper() == "TRUE"
    if not discord_only:
        send_telegram(tg_msg, topic_id)
    send_discord(dc_msg, discord_webhook)

    all_symbols = list(set(double_inside + inside + outside + f2u + f2d + list(aplus.keys())))
    send_discord_csv(all_symbols, title, discord_webhook)

# ---------------------------------------------------------
# RUNTIME
# ---------------------------------------------------------

if RUN_MODE in ("DAILY", "ALL"):
    scan("Daily", "D", TOPIC_DAILY, WEBHOOK_DAILY)

if RUN_MODE in ("WEEKLY", "ALL"):
    scan("Weekly", "W", TOPIC_WEEKLY, WEBHOOK_WEEKLY)

if RUN_MODE in ("MONTHLY", "ALL"):
    scan("Monthly", "M", TOPIC_MONTHLY, WEBHOOK_MONTHLY)

print("DONE (Stocks - Telegram + Discord)")
