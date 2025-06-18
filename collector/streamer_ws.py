import asyncio
import json
import logging
import websockets
from db import get_conn, insert_trade
from config import DB, SYMBOLS, INTERVALS 
from utils import normalize_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ws_streamer")

async def listen_symbol(symbol, interval):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    conn = get_conn(DB)

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info(f"WS connected: {symbol} [{interval}]")
                async for msg in ws:
                    data = json.loads(msg).get('k', {})
                    if not data.get('x'):  # бар ещё не закрыт
                        continue

                    row = {
                        'timestamp': normalize_timestamp(data['t'], interval),
                        'unix': int(data['t']) // 1000,
                        'symbol': symbol,
                        'open': float(data['o']),
                        'high': float(data['h']),
                        'low': float(data['l']),
                        'close': float(data['c']),
                        'volume_base': float(data['v']),
                        'volume_quote': float(data['q'])
                    }

                    try:
                        insert_trade(conn, row, interval)
                        logger.info(f"Inserted {symbol} [{interval}] @ {row['timestamp']}")
                    except Exception as e:
                        logger.error(f"Insert error {symbol} [{interval}]: {e} Row: {row}")

        except Exception as e:
            logger.error(f"WS error {symbol} [{interval}]: {e}")
            await asyncio.sleep(10)

async def main():
    tasks = [
        listen_symbol(symbol, interval)
        for symbol in SYMBOLS
        for interval in INTERVALS
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
