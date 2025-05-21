import requests
from datetime import datetime, timezone
from db import insert_btc_dominance

def fetch_btc_dominance(conn, logger):
    url = "https://api.coingecko.com/api/v3/global"
    try:
        resp = requests.get(url, timeout=10)
        dominance = resp.json()['data']['market_cap_percentage']['btc']
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        # Нормализуем timestamp до начала минуты
        now = now.replace(second=0, microsecond=0)
        insert_btc_dominance(conn, now, dominance)
        logger.info(f"BTC dominance {dominance:.2f}% saved ({now})")
    except Exception as e:
        logger.error(f"Error fetching BTC dominance: {e}")
