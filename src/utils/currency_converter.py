# src/utils/currency_converter.py

import requests
from datetime import datetime

EXCHANGE_RATE_API_KEY = "0b43ce96244c7c5d99a8bc88df6a3cf6"
BASE_URL = "https://api.exchangerate.host"


def get_exchange_rate(from_currency: str, to_currency: str, date: str) -> float:
    """
    date format: YYYY-MM-DD
    """
    url = f"{BASE_URL}/{date}"
    params = {
        "base": from_currency,
        "symbols": to_currency,
        "access_key": EXCHANGE_RATE_API_KEY
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    if "rates" not in data or to_currency not in data["rates"]:
        raise ValueError(f"Rate not found for {from_currency} -> {to_currency} on {date}")

    return data["rates"][to_currency]
