"""
Stock Setups Scanner - The Strat Methodology
Standalone app for scanning stock trading setups
"""
from flask import Flask, render_template, jsonify, request
from flask_compress import Compress
from datetime import datetime
import logging
import os
import yfinance as yf

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', os.urandom(24).hex())
Compress(app)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STOCK_WATCHLIST = [
    # ETFs
    "SPY", "QQQ", "IWM", "XLF", "XLE", "XLK", "XLV", "XLI", "XLU", "XLY", "XLP",
    "ARKK", "SMH", "TLT", "HYG", "IYR", "MSOS", "GLD",
    # Mega Caps
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "JPM", "GS",
    # Tech/Growth
    "AMD", "CRM", "NFLX", "PYPL", "SHOP", "UBER", "COIN", "PLTR", "SOFI", "IONQ",
    "PANW", "CRWD", "NET", "SNOW", "DDOG", "SMCI", "ARM", "MRVL", "AVGO", "INTC",
    "ORCL", "CSCO", "QCOM", "AMAT", "RBLX", "ROKU", "PINS", "WDAY", "DOCU", "ADBE",
    "MU", "TSM", "OKLO", "TLRY", "EBAY",
    # Crypto/Blockchain
    "MARA", "RIOT",
    # Consumer/Retail
    "COST", "WMT", "TGT", "MCD", "SBUX", "NKE", "DIS", "ABNB", "ETSY", "DLTR", "ANF",
    # Healthcare/Pharma
    "UNH", "LLY", "PFE", "JNJ", "ABBV", "NVO", "CVS", "PEP", "KHC",
    # Financials
    "BAC", "WFC", "MS", "GE", "UBS",
    # Energy/Commodities
    "CVX", "XOM", "OXY", "URA",
    # Airlines/Travel
    "UAL", "DAL", "AAL", "LUV", "CCL", "BA", "FDX", "CAT",
    # Other Popular
    "KO", "CMG", "KR", "UPS", "DKNG", "STZ", "ZM", "AAP", "NEE"
]

def get_stock_candles(symbol: str, period: str = "3mo", interval: str = "1d") -> list:
    """Fetch stock candles using yfinance"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        if df.empty:
            return []
        
        candles = []
        for idx, row in df.iterrows():
            candles.append({
                "date": idx.strftime("%Y-%m-%d"),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"])
            })
        return candles
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")
        return []

def stock_direction(candle: dict) -> str:
    """Determine candle direction"""
    return "UP" if candle["close"] > candle["open"] else "DOWN"

def stock_strat_type(curr: dict, prev: dict) -> str:
    """Determine Strat candle type: 1 (inside), 2U/2D (directional), 3 (outside)"""
    if curr["high"] > prev["high"] and curr["low"] < prev["low"]:
        return "3"
    if curr["high"] > prev["high"] and curr["low"] >= prev["low"]:
        return "2U"
    if curr["low"] < prev["low"] and curr["high"] <= prev["high"]:
        return "2D"
    return "1"

def stock_failed_2(curr: dict, prev: dict) -> str:
    """Check for Failed 2 pattern"""
    t = stock_strat_type(curr, prev)
    if t == "2U" and curr["close"] < curr["open"]:
        return "Failed 2U"
    if t == "2D" and curr["close"] > curr["open"]:
        return "Failed 2D"
    return None

def stock_ftfc_check(daily: dict, weekly: dict, monthly: dict) -> dict:
    """Check Full Time Frame Continuity"""
    d_dir = stock_direction(daily)
    w_dir = stock_direction(weekly)
    m_dir = stock_direction(monthly)
    
    all_up = d_dir == "UP" and w_dir == "UP" and m_dir == "UP"
    all_down = d_dir == "DOWN" and w_dir == "DOWN" and m_dir == "DOWN"
    
    return {
        "daily": d_dir,
        "weekly": w_dir,
        "monthly": m_dir,
        "ftfc": all_up or all_down,
        "direction": "BULLISH" if all_up else ("BEARISH" if all_down else "MIXED")
    }

def scan_stock_setups(timeframe: str = "daily") -> dict:
    """Scan stocks for Strat setups"""
    results = {
        "timeframe": timeframe,
        "timestamp": datetime.now().isoformat(),
        "inside_bars": [],
        "outside_bars": [],
        "two_up": [],
        "two_down": [],
        "failed_2u": [],
        "failed_2d": [],
        "double_inside": [],
        "aplus_setups": [],
        "ftfc_bullish": [],
        "ftfc_bearish": []
    }
    
    if timeframe == "weekly":
        period = "1y"
        interval = "1wk"
    elif timeframe == "monthly":
        period = "2y"
        interval = "1mo"
    else:
        period = "3mo"
        interval = "1d"
    
    for symbol in STOCK_WATCHLIST:
        try:
            candles = get_stock_candles(symbol, period, interval)
            if not candles or len(candles) < 4:
                continue
            
            curr = candles[-1]
            prev = candles[-2]
            prev2 = candles[-3]
            
            strat = stock_strat_type(curr, prev)
            direction = stock_direction(curr)
            arrow = "↑" if direction == "UP" else "↓"
            failed = stock_failed_2(curr, prev)
            
            setup_info = {
                "symbol": symbol,
                "strat": strat,
                "direction": direction,
                "arrow": arrow,
                "close": round(curr["close"], 2),
                "change_pct": round((curr["close"] - prev["close"]) / prev["close"] * 100, 2),
                "high": round(curr["high"], 2),
                "low": round(curr["low"], 2)
            }
            
            if strat == "1":
                results["inside_bars"].append(setup_info)
                if stock_strat_type(prev, prev2) == "1":
                    setup_info["pattern"] = "Double Inside"
                    results["double_inside"].append(setup_info)
            elif strat == "3":
                results["outside_bars"].append(setup_info)
            elif strat == "2U":
                results["two_up"].append(setup_info)
            elif strat == "2D":
                results["two_down"].append(setup_info)
            
            if failed == "Failed 2U":
                setup_info["pattern"] = "Failed 2U"
                results["failed_2u"].append(setup_info)
            elif failed == "Failed 2D":
                setup_info["pattern"] = "Failed 2D"
                results["failed_2d"].append(setup_info)
            
            if timeframe == "daily":
                try:
                    weekly_candles = get_stock_candles(symbol, "1y", "1wk")
                    monthly_candles = get_stock_candles(symbol, "2y", "1mo")
                    
                    if weekly_candles and monthly_candles and len(weekly_candles) >= 1 and len(monthly_candles) >= 1:
                        ftfc = stock_ftfc_check(curr, weekly_candles[-1], monthly_candles[-1])
                        if ftfc["ftfc"]:
                            setup_info["ftfc"] = ftfc
                            if ftfc["direction"] == "BULLISH":
                                results["ftfc_bullish"].append(setup_info)
                            else:
                                results["ftfc_bearish"].append(setup_info)
                            
                            if strat == "1" and ftfc["ftfc"]:
                                setup_info["aplus"] = True
                                setup_info["bias"] = ftfc["direction"]
                                results["aplus_setups"].append(setup_info)
                except Exception as e:
                    logger.debug(f"FTFC check failed for {symbol}: {e}")
        
        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")
            continue
    
    return results

@app.route('/')
def index():
    return render_template('stocks_standalone.html')

@app.route('/api/stock_setups')
def api_stock_setups():
    timeframe = request.args.get('timeframe', 'daily')
    results = scan_stock_setups(timeframe)
    return jsonify(results)

@app.route('/api/status')
def api_status():
    return jsonify({"status": "ok", "app": "Stock Setups Scanner"})

@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response

if __name__ == '__main__':
    logger.info("Stock Setups Scanner Starting")
    app.run(host='0.0.0.0', port=5000, debug=True)
