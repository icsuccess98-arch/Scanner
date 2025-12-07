import os
import time
import requests

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

OANDA_KEY = os.environ["OANDA_KEY"]
OANDA_ACCOUNT = "practice"
RUN_MODE = os.environ.get("RUN_MODE", "ALL")

WEBHOOK_DAILY = os.environ["WEBHOOK_DAILY"]
WEBHOOK_WEEKLY = os.environ["WEBHOOK_WEEKLY"]
WEBHOOK_MONTHLY = os.environ["WEBHOOK_MONTHLY"]

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
CRYPTOS = ["BTC_USD", "ETH_USD", "BCH_USD", "LTC_USD"]

CROSSES = [
    "EUR_GBP", "EUR_JPY", "EUR_AUD", "EUR_CAD", "EUR_CHF", "EUR_NZD",
    "GBP_JPY", "GBP_AUD", "GBP_CAD", "GBP_CHF", "GBP_NZD",
    "AUD_JPY", "AUD_CHF", "AUD_CAD", "AUD_NZD",
    "NZD_JPY", "NZD_CHF", "NZD_CAD",
    "CHF_JPY", "CAD_JPY"
]

OANDA_SYMBOLS = MAJORS + METALS + OIL + INDICES + CRYPTOS + CROSSES

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
        return "WTICO/USD"
    return out

def candle(c):
    return {
        "open": float(c["mid"]["o"]),
        "high": float(c["mid"]["h"]),
        "low": float(c["mid"]["l"]),
        "close": float(c["mid"]["c"])
    }

def direction(c):
    return "UP" if c["close"] > c["open"] else "DOWN"

def arrow(d):
    return "↑" if d == "UP" else "↓"

# ---------------------------------------------------------
# FETCH CANDLES
# ---------------------------------------------------------

def get_oanda_candles(symbol, granularity, count=20):
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
    s = strat_type(curr, prev)
    if s == "2U" and curr["close"] < curr["open"]:
        return "Failed 2U"
    if s == "2D" and curr["close"] > curr["open"]:
        return "Failed 2D"
    return None

# ---------------------------------------------------------
# FTFC CHECK (only used for A++)
# ---------------------------------------------------------

def ftfc_pass(d, w, m):
    d_dir, w_dir, m_dir = direction(d), direction(w), direction(m)
    if d_dir == w_dir == m_dir:
        return d_dir
    return None

# ---------------------------------------------------------
# ORDER RESULTS BY ASSET CLASS
# ---------------------------------------------------------

def group_sort(symbols):
    ordered = []
    for class_list in GROUP_ORDER.values():
        subset = [s for s in symbols if s in class_list]
        ordered.extend(sorted(subset))
    return ordered

# ---------------------------------------------------------
# A++ FORMATTING
# ---------------------------------------------------------

def build_aplus_section(aplus_dict):
    if not aplus_dict:
        return ""

    up = []
    dn = []

    for sym, info in aplus_dict.items():
        if info["dir"] == "UP":
            up.append((sym, info))
        else:
            dn.append((sym, info))

    msg = "🔥 **A++ Setups**\n"

    if up:
        msg += "\n🔺 **Upside**\n"
        for sym, info in up:
            msg += f"• {pretty(sym)} — M/W/D {info['arrows']} — D: {info['label']}\n"

    if dn:
        msg += "\n🔻 **Downside**\n"
        for sym, info in dn:
            msg += f"• {pretty(sym)} — M/W/D {info['arrows']} — D: {info['label']}\n"

    return msg + "\n"

# ---------------------------------------------------------
# MAIN SCANNER
# ---------------------------------------------------------

def scan(title, granularity, webhook):

    inside = []
    outside = []
    double_inside = []
    f2u = []
    f2d = []
    aplus = {}

    for symbol in OANDA_SYMBOLS:

        candles = get_oanda_candles(symbol, granularity, 15)
        if not candles:
            continue

        curr = candles[-1]
        prev = candles[-2]
        prev2 = candles[-3]

        st = strat_type(curr, prev)

        # DOUBLE INSIDE = AUTOMATIC A++
        if st == "1" and strat_type(prev, prev2) == "1":
            double_inside.append(symbol)

            weekly = get_oanda_candles(symbol, "W")
            monthly = get_oanda_candles(symbol, "M")
            if weekly and monthly:
                m_dir = direction(monthly[-1])
                w_dir = direction(weekly[-1])
                d_dir = direction(curr)
                arrows = arrow(m_dir) + arrow(w_dir) + arrow(d_dir)
                aplus[symbol] = {
                    "arrows": arrows,
                    "label": "Double Inside",
                    "dir": d_dir
                }
            continue

        # INSIDE / OUTSIDE
        if st == "1":
            inside.append(symbol)
        elif st == "3":
            outside.append(symbol)

        # FAILED 2 — always add to list, A++ only if FTFC aligns
        f2 = failed_2(curr, prev)
        if f2:
            # Always add to Failed 2 lists
            if f2 == "Failed 2U":
                f2u.append(symbol)
            else:
                f2d.append(symbol)

            # Check FTFC for A++ status
            weekly = get_oanda_candles(symbol, "W")
            monthly = get_oanda_candles(symbol, "M")

            if weekly and monthly:
                m_dir = direction(monthly[-1])
                w_dir = direction(weekly[-1])
                d_dir = direction(curr)
                ftfc = ftfc_pass(curr, weekly[-1], monthly[-1])

                if ftfc:
                    arrows = arrow(m_dir) + arrow(w_dir) + arrow(d_dir)

                    if f2 == "Failed 2U" and ftfc == "DOWN":
                        aplus[symbol] = {
                            "arrows": arrows,
                            "label": "Failed 2 Up",
                            "dir": "DOWN"
                        }

                    if f2 == "Failed 2D" and ftfc == "UP":
                        aplus[symbol] = {
                            "arrows": arrows,
                            "label": "Failed 2 Down",
                            "dir": "UP"
                        }

        time.sleep(0.25)

    # Build message
    msg = f"📊 **{title} Actionables**\n\n"
    msg += build_aplus_section(aplus)

    if double_inside:
        msg += f"**{title} Double Inside (II)**\n" + "\n".join(f"• {pretty(x)}" for x in group_sort(double_inside)) + "\n\n"

    if inside:
        msg += f"**{title} Inside (1)**\n" + "\n".join(f"• {pretty(x)}" for x in group_sort(inside)) + "\n\n"

    if outside:
        msg += f"**{title} Outside (3)**\n" + "\n".join(f"• {pretty(x)}" for x in group_sort(outside)) + "\n\n"

    if f2u:
        msg += f"**{title} Failed 2U**\n" + "\n".join(f"• {pretty(x)}" for x in group_sort(f2u)) + "\n\n"

    if f2d:
        msg += f"**{title} Failed 2D**\n" + "\n".join(f"• {pretty(x)}" for x in group_sort(f2d)) + "\n\n"

    requests.post(webhook, json={"content": msg})

# ---------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------

if RUN_MODE in ("DAILY", "ALL"):
    scan("Daily", "D", WEBHOOK_DAILY)

if RUN_MODE in ("WEEKLY", "ALL"):
    scan("Weekly", "W", WEBHOOK_WEEKLY)

if RUN_MODE in ("MONTHLY", "ALL"):
    scan("Monthly", "M", WEBHOOK_MONTHLY)

print("DONE")
