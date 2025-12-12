from fastapi import FastAPI, Response
import yfinance as yf
import xml.etree.ElementTree as ET

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/stock/{symbol}")
def stock_info(symbol: str, interval: str = "1d"):
    ticker = yf.Ticker(symbol)
    info = ticker.info or {}
    current_price = info.get("currentPrice")

    hist = ticker.history(period="3mo", interval=interval)

    # Build XML root
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

    # Convert to string
    xml_str = ET.tostring(root, encoding="utf-8")

    return Response(content=xml_str, media_type="application/xml")

