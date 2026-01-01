from fastapi import FastAPI, Response
import yfinance as yf
import xml.etree.ElementTree as ET

app = FastAPI()

VALID_INTERVALS = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1d", "5d", "1wk", "1mo", "3mo"}

@app.get("/stock/{symbol}")
def stock_info(symbol: str, interval: str = "1d"):
    try:
        if interval not in VALID_INTERVALS:
            raise ValueError(f"Unsupported interval '{interval}'")

        hist = yf.download(symbol, period="3mo", interval=interval)
        if hist.empty:
            # fallback to daily if original interval fails
            hist = yf.download(symbol, period="3mo", interval="1d")
            if hist.empty:
                raise ValueError(f"No data returned for symbol '{symbol}' with interval '{interval}' or fallback")

        current_price = hist["Close"].iloc[-1]

        root = ET.Element("stock")
        ET.SubElement(root, "symbol").text = symbol
        ET.SubElement(root, "current_price").text = str(current_price)
        ET.SubElement(root, "period").text = "3mo"
        ET.SubElement(root, "interval").text = interval

        history_elem = ET.SubElement(root, "history")
        for date, row in hist.iterrows():
            record = ET.SubElement(history_elem, "record")
            ET.SubElement(record, "date").text = date.strftime("%Y-%m-%d")
            ET.SubElement(record, "open").text = str(row["Open"])
            ET.SubElement(record, "close").text = str(row["Close"])

        xml_str = ET.tostring(root, encoding="utf-8")
        return Response(content=xml_str, media_type="application/xml")

    except Exception as e:
        error_root = ET.Element("error")
        ET.SubElement(error_root, "message").text = str(e)
        xml_str = ET.tostring(error_root, encoding="utf-8")
        return Response(content=xml_str, media_type="application/xml")
