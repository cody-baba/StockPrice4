# app.py
from fastapi import FastAPI, Request, Response, HTTPException
import yfinance as yf
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import threading
import json

app = FastAPI()

# Simple in-memory cache: {(symbol, interval): {"ts": datetime, "data": hist_df}}
CACHE = {}
CACHE_LOCK = threading.Lock()
CACHE_TTL = timedelta(seconds=90)  # cache results for 90 seconds

# Limit concurrent downloads to avoid being rate-limited
DOWNLOAD_SEMAPHORE = threading.Semaphore(3)

VALID_INTERVALS = {
    "1m", "2m", "5m", "15m", "30m", "60m", "90m",
    "1d", "5d", "1wk", "1mo", "3mo"
}

def df_to_records(hist):
    """Convert DataFrame to list of dict records with date, open, close."""
    records = []
    # Ensure index is datetime
    hist = hist.copy()
    if hasattr(hist.index, "to_pydatetime"):
        hist = hist.reset_index()
        date_col = hist.columns[0]
    else:
        hist = hist.reset_index()
        date_col = hist.columns[0]

    for row in hist.itertuples(index=False):
        # row[0] is date, then Open, High, Low, Close, ...
        date_val = getattr(row, date_col) if hasattr(row, date_col) else row[0]
        try:
            date_str = date_val.strftime("%Y-%m-%d %H:%M:%S") if hasattr(date_val, "strftime") and (date_val.hour or date_val.minute or date_val.second) else date_val.strftime("%Y-%m-%d")
        except Exception:
            date_str = str(date_val)
        # Attempt to find Open and Close by name
        open_val = None
        close_val = None
        # Use attribute access if available
        if hasattr(row, "Open"):
            open_val = row.Open
        elif "Open" in hist.columns:
            open_val = row[hist.columns.index("Open")]
        if hasattr(row, "Close"):
            close_val = row.Close
        elif "Close" in hist.columns:
            close_val = row[hist.columns.index("Close")]

        records.append({
            "date": date_str,
            "open": None if open_val is None else float(open_val),
            "close": None if close_val is None else float(close_val)
        })
    return records

def build_xml(symbol: str, current_price, interval: str, records):
    root = ET.Element("stock")
    ET.SubElement(root, "symbol").text = symbol.upper()
    ET.SubElement(root, "current_price").text = str(current_price) if current_price is not None else "N/A"
    ET.SubElement(root, "source").text = "YahooFinance (yfinance)"
    ET.SubElement(root, "interval").text = interval
    ET.SubElement(root, "records_returned").text = str(len(records))

    history_elem = ET.SubElement(root, "history")
    for r in records:
        rec = ET.SubElement(history_elem, "record")
        ET.SubElement(rec, "date").text = r["date"]
        ET.SubElement(rec, "open").text = "N/A" if r["open"] is None else str(r["open"])
        ET.SubElement(rec, "close").text = "N/A" if r["close"] is None else str(r["close"])
    return ET.tostring(root, encoding="utf-8")

def get_from_cache(symbol: str, interval: str):
    key = (symbol.upper(), interval)
    with CACHE_LOCK:
        entry = CACHE.get(key)
        if entry:
            if datetime.utcnow() - entry["ts"] < CACHE_TTL:
                return entry["data"]
            else:
                del CACHE[key]
    return None

def set_cache(symbol: str, interval: str, data):
    key = (symbol.upper(), interval)
    with CACHE_LOCK:
        CACHE[key] = {"ts": datetime.utcnow(), "data": data}

def fetch_with_retries(symbol: str, interval: str, period: str = "6mo", retries: int = 3, backoff: float = 1.0):
    """Attempt to download data with retries and exponential backoff."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            # Acquire semaphore to limit concurrency
            with DOWNLOAD_SEMAPHORE:
                # threads=False can help avoid some internal threading issues
                hist = yf.download(symbol, period=period, interval=interval, auto_adjust=True, threads=False, progress=False)
            # yfinance returns a DataFrame; check if empty
            if hist is None:
                raise RuntimeError("yfinance returned None")
            if not hist.empty:
                return hist
            # empty DataFrame -> treat as failure and retry
            last_exc = RuntimeError("Empty DataFrame returned")
        except Exception as e:
            last_exc = e
        # backoff before next attempt
        time.sleep(backoff * (2 ** (attempt - 1)))
    # after retries, return None and last exception
    raise last_exc if last_exc is not None else RuntimeError("Unknown error fetching data")

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/stock/{symbol}")
def stock_info(request: Request, symbol: str, interval: str = "1d", period: str = "6mo", max_records: int = 180):
    interval = interval.lower()
    if interval not in VALID_INTERVALS:
        # normalize some common aliases
        alias_map = {"daily": "1d", "weekly": "1wk", "monthly": "1mo"}
        if interval in alias_map:
            interval = alias_map[interval]
        else:
            error_root = ET.Element("error")
            ET.SubElement(error_root, "message").text = f"Unsupported interval '{interval}'"
            return Response(content=ET.tostring(error_root, encoding="utf-8"), media_type="application/xml", status_code=400)

    # Check cache first
    cached = get_from_cache(symbol, interval)
    if cached is not None:
        hist = cached
    else:
        try:
            # Try to fetch; if empty, fallback to daily
            hist = fetch_with_retries(symbol, interval, period=period, retries=3, backoff=1.0)
        except Exception:
            # fallback to daily
            try:
                hist = fetch_with_retries(symbol, "1d", period=period, retries=2, backoff=1.0)
            except Exception as e:
                error_root = ET.Element("error")
                ET.SubElement(error_root, "message").text = f"No data returned for symbol '{symbol}' with interval '{interval}' or fallback: {str(e)}"
                return Response(content=ET.tostring(error_root, encoding="utf-8"), media_type="application/xml", status_code=502)
        # store in cache
        set_cache(symbol, interval, hist)

    # Limit records and build response
    try:
        # Ensure we have at least one row
        if hist.empty:
            raise ValueError("Empty data after fetch")

        # Keep most recent max_records
        hist = hist.tail(max_records)
        records = df_to_records(hist)
        current_price = None
        # Try to get latest close
        try:
            current_price = float(hist["Close"].iloc[-1])
        except Exception:
            current_price = None

        # Content negotiation: if client wants JSON, return JSON
        accept = request.headers.get("accept", "")
        if "application/json" in accept.lower():
            payload = {
                "symbol": symbol.upper(),
                "current_price": current_price,
                "source": "YahooFinance (yfinance)",
                "interval": interval,
                "records_returned": len(records),
                "history": records
            }
            return Response(content=json.dumps(payload), media_type="application/json")

        # Default: XML
        xml_bytes = build_xml(symbol, current_price, interval, records)
        return Response(content=xml_bytes, media_type="application/xml")

    except Exception as e:
        error_root = ET.Element("error")
        ET.SubElement(error_root, "message").text = str(e)
        return Response(content=ET.tostring(error_root, encoding="utf-8"), media_type="application/xml", status_code=500)
