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

def send_discord(msg, webhook_url):
    if not webhook_url:
        return
    # Convert HTML to Discord markdown
    discord_msg = msg.replace("<b>", "**").replace("</b>", "**")
    discord_msg = discord_msg.replace("<u>", "__").replace("</u>", "__")
    payload = {"content": discord_msg}
    requests.post(webhook_url, json=payload)

def send_discord_embed(title, description, webhook_url):
    """Send Discord embed for box-style formatting"""
    if not webhook_url:
        return
    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": 15105570  # Orange color
        }]
    }
    requests.post(webhook_url, json=payload)

def to_tv_symbol(sym):
    # Stock symbols go to NYSE/NASDAQ
    return f"NASDAQ:{sym}"

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

def send_discord_categorized_csv(categories, title, webhook_url):
    """Send categorized TradingView watchlist with section headers"""
    if not webhook_url:
        return
    
    # Build categorized content for TradingView
    # TradingView format: ###SECTION_NAME### followed by symbols
    lines = []
    
    for cat_name, symbols in categories.items():
        if symbols:
            lines.append(f"####{cat_name}####")
            for sym in symbols:
                lines.append(to_tv_symbol(sym))
    
    if not lines:
        return
    
    csv_content = "\n".join(lines)
    
    files = {
        "file": (f"{title.lower()}_categorized_watchlist.txt", csv_content, "text/plain")
    }
    data = {"content": f"📋 **{title} Categorized TradingView Watchlist**"}
    requests.post(webhook_url, data=data, files=files)

# ---------------------------------------------------------
# RUN MODE CONFIG
# ---------------------------------------------------------

RUN_MODE = os.environ.get("RUN_MODE", "ALL")

# ---------------------------------------------------------
# ASSET GROUPS BY TIER
# ---------------------------------------------------------

# TIER 1: Anchor Funds (Foundational Income Funds)
TIER_1 = ["CLM", "CRF", "ECC", "EIC", "GUT", "GOF", "ASGI", "YYY", "HIPS", 
          "PSEC", "BCAT", "ECAT", "BIZD", "USA", "ASG", "REM", "CCIF"]

# TIER 2: Leveraged Index Options Funds  
TIER_2 = ["QQQY", "XDTE", "RDTE", "IWMY", "USOY", "GLDY", "YMAX", "YMAG",
          "AIPI", "CEPI", "GIAX", "KLIP", "GDXY", "SPYI", "BITO"]

# TIER 3: Single-Stock or Select Portfolio ETFs
TIER_3 = ["MST", "TSLY", "NVDY", "OARK", "XYZY", "TSMY", "MSTY", "SNOY", 
          "CRSH", "BABO", "AMDY", "GPTY"]

# Additional holdings not in tier system
OTHER = ["TLT", "CVX", "AMGN", "UBER"]

ALL_STOCKS = TIER_1 + TIER_2 + TIER_3 + OTHER

def get_tier(symbol):
    if symbol in TIER_1:
        return 1
    elif symbol in TIER_2:
        return 2
    elif symbol in TIER_3:
        return 3
    else:
        return 4  # Other

GROUP_ORDER = {
    "TIER_1": TIER_1,
    "TIER_2": TIER_2,
    "TIER_3": TIER_3,
    "OTHER": OTHER
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def pretty(symbol):
    return symbol

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

# Cache for 20MA status
ma20_cache = {}

def is_under_20ma(symbol):
    """Check if symbol is trading below its 20-day moving average"""
    if symbol in ma20_cache:
        return ma20_cache[symbol]
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="2mo", interval="1d")
        if len(df) >= 20:
            df["SMA20"] = df["Close"].rolling(window=20).mean()
            last_close = df["Close"].iloc[-1]
            last_sma20 = df["SMA20"].iloc[-1]
            result = last_close < last_sma20
            ma20_cache[symbol] = result
            return result
    except:
        pass
    ma20_cache[symbol] = False
    return False

# ---------------------------------------------------------
# FETCH CANDLES (YFINANCE)
# ---------------------------------------------------------

def get_closed_candles(symbol, granularity, count=5):
    """Fetch candles using yfinance. Granularity: D=daily, W=weekly, M=monthly"""
    try:
        ticker = yf.Ticker(symbol)
        
        # Map granularity to yfinance interval and period
        if granularity == "D":
            df = ticker.history(period="1mo", interval="1d")
        elif granularity == "W":
            df = ticker.history(period="6mo", interval="1wk")
        elif granularity == "M":
            df = ticker.history(period="2y", interval="1mo")
        else:
            df = ticker.history(period="1mo", interval="1d")
        
        if df.empty or len(df) < 3:
            return None
        
        # Convert to list of candle dicts (most recent last)
        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"])
            })
        
        # Exclude the current incomplete candle (last one in the series)
        # We want only fully closed candles for accurate STRAT analysis
        closed_candles = candles[:-1] if len(candles) > 1 else candles
        
        # Return last 'count' closed candles
        return closed_candles[-count:] if len(closed_candles) >= count else closed_candles
        
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

    for symbol in ALL_STOCKS:

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

    # Discord header (with dates) - matching box format
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    date_header = today.strftime("%b %d, %Y")
    dc_header = f"🗓 **{title} Actionable Strat — {date_header}**\n\n"
    dc_header += f"**{title} Actionable Strat — for {tomorrow.strftime('%a %b %d')}**\n"
    dc_header += f"(From {today.strftime('%a %b %d')} close)\n\n"
    
    # Get M/W/D arrows for a symbol
    def get_mwd_arrows(sym):
        try:
            monthly = get_closed_candles(sym, "M")
            weekly = get_closed_candles(sym, "W")
            daily = get_closed_candles(sym, "D")
            if monthly and weekly and daily:
                m_arr = "↑" if direction(monthly[-1]) == "UP" else "↓"
                w_arr = "↑" if direction(weekly[-1]) == "UP" else "↓"
                d_arr = "↑" if direction(daily[-1]) == "UP" else "↓"
                return f"{m_arr}{w_arr}{d_arr}"
        except:
            pass
        return "---"
    
    # Collect patterns by tier and type (U20 is now its own category)
    tier_data = {1: {"u20": [], "f2u": [], "f2d": [], "ii": [], "inside": [], "outside": []},
                 2: {"u20": [], "f2u": [], "f2d": [], "ii": [], "inside": [], "outside": []},
                 3: {"u20": [], "f2u": [], "f2d": [], "ii": [], "inside": [], "outside": []},
                 4: {"u20": [], "f2u": [], "f2d": [], "ii": [], "inside": [], "outside": []}}
    
    # First pass: identify ALL U20 tickers (show all, not just those with patterns)
    u20_tickers = set()
    for sym in ALL_STOCKS:
        if is_under_20ma(sym):
            u20_tickers.add(sym)
            tier = get_tier(sym)
            tier_data[tier]["u20"].append(f"• {sym}")
    
    # Process F2U patterns (skip U20 tickers - they're already listed)
    for sym in f2u:
        if sym not in u20_tickers:
            tier = get_tier(sym)
            arrows = get_mwd_arrows(sym)
            pattern_label = aplus.get(sym, "F2U")
            tier_data[tier]["f2u"].append(f"• **{sym}** — M/W/D {arrows} — {pattern_label}")
    
    # Process F2D patterns (skip U20 tickers)
    for sym in f2d:
        if sym not in u20_tickers:
            tier = get_tier(sym)
            arrows = get_mwd_arrows(sym)
            pattern_label = aplus.get(sym, "F2D")
            tier_data[tier]["f2d"].append(f"• **{sym}** — M/W/D {arrows} — {pattern_label}")
    
    # Process Double Inside (skip U20 tickers)
    for sym in double_inside:
        if sym not in f2u and sym not in f2d and sym not in u20_tickers:
            tier = get_tier(sym)
            tier_data[tier]["ii"].append(f"• {sym}")
    
    # Process Inside (1) (skip U20 tickers)
    for sym in inside:
        if sym not in f2u and sym not in f2d and sym not in double_inside and sym not in u20_tickers:
            tier = get_tier(sym)
            tier_data[tier]["inside"].append(f"• {sym}")
    
    # Process Outside (3) (skip U20 tickers)
    for sym in outside:
        if sym not in f2u and sym not in f2d and sym not in u20_tickers:
            tier = get_tier(sym)
            tier_data[tier]["outside"].append(f"• {sym}")
    
    # Combine all tickers by category (across all tiers)
    all_u20 = []
    all_f2d = []
    all_ii = []
    all_inside = []
    all_outside = []
    
    for tier in [1, 2, 3, 4]:
        data = tier_data[tier]
        all_u20.extend([item.replace("• ", "") for item in data["u20"]])
        all_f2d.extend([item.replace("• ", "") for item in data["f2d"]])
        all_ii.extend([item.replace("• ", "") for item in data["ii"]])
        all_inside.extend([item.replace("• ", "") for item in data["inside"]])
        all_outside.extend([item.replace("• ", "") for item in data["outside"]])
    
    # Build embed description
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    
    embed_title = f"🗓 {title} Actionable Strat — {today.strftime('%b %d, %Y')}"
    
    desc = f"**{title} Actionable Strat — for {tomorrow.strftime('%a %b %d')}**\n"
    desc += f"(From {today.strftime('%a %b %d')} close)\n\n"
    
    if all_u20:
        desc += f"**U20**\n{', '.join(all_u20)}\n\n"
    
    if all_inside:
        desc += f"**Inside Day (1)**\n{', '.join(all_inside)}\n\n"
    
    if all_f2d:
        desc += f"**Failed 2s**\n{', '.join(all_f2d)}\n\n"
    
    if all_outside:
        desc += f"**3-Bar (3)**\n{', '.join(all_outside)}\n\n"
    
    if all_ii:
        desc += f"**Double Inside (II)**\n{', '.join(all_ii)}\n\n"
    
    desc = desc.strip()
    
    if desc:
        send_discord_embed(embed_title, desc, discord_webhook)
    
    # Send categorized TradingView watchlist CSV to Discord
    categories = {
        "U20": all_u20,
        "INSIDE_DAY": all_inside,
        "FAILED_2s": all_f2d,
        "3_BAR": all_outside,
        "DOUBLE_INSIDE": all_ii
    }
    send_discord_categorized_csv(categories, title, discord_webhook)

# ---------------------------------------------------------
# RUNTIME
# ---------------------------------------------------------

if RUN_MODE in ("DAILY", "ALL"):
    scan("Daily", "D", TOPIC_DAILY, WEBHOOK_DAILY)

if RUN_MODE in ("WEEKLY", "ALL"):
    scan("Weekly", "W", TOPIC_WEEKLY, WEBHOOK_WEEKLY)

if RUN_MODE in ("MONTHLY", "ALL"):
    scan("Monthly", "M", TOPIC_MONTHLY, WEBHOOK_MONTHLY)

print("DONE (Discord)")
