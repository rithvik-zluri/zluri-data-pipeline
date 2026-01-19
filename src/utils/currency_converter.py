import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")
BASE_URL = "https://api.exchangerate.host/historical"

# In-memory cache: { "2022-01-01": { "USDINR": 74.5, ... } }
_fx_cache = {}


def fetch_historical_fx(date_str: str):
    """
    Calls FX API.
    Returns: quotes dict { 'USDINR': 74.5, ... }
    """
    if date_str in _fx_cache:
        return _fx_cache[date_str]

    if not API_KEY:
        raise RuntimeError("EXCHANGE_RATE_API_KEY not found in environment (.env)")

    url = f"{BASE_URL}?access_key={API_KEY}&date={date_str}"

    try:
        resp = requests.get(url, timeout=20)
        data = resp.json()

        quotes = data.get("quotes")
        if not quotes:
            raise RuntimeError(f"No quotes in API response: {data}")

        _fx_cache[date_str] = quotes
        return quotes

    except Exception as e:
        print(f"FX API failed for {date_str}: {e}")
        _fx_cache[date_str] = None
        return None


def get_rate_to_usd(date_val, currency: str):
    """
    API gives: 1 USD = X currency  (USDINR = 74.5)
    We return: currency -> USD = 1 / X
    """
    if currency == "USD":
        return 1.0

    if date_val is None or currency is None:
        return None

    date_str = date_val.strftime("%Y-%m-%d")

    quotes = fetch_historical_fx(date_str)
    if not quotes:
        return None

    key = f"USD{currency}"
    rate = quotes.get(key)

    if rate is None:
        return None

    try:
        return 1.0 / float(rate)
    except Exception:
        return None
