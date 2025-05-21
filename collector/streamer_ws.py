import asyncio
import json
import logging
import websockets
import pytz
from datetime import datetime
from db import get_conn, insert_trade
from config import DB, MONETS, INTERVAL

logging.basicConfig(level="INFO")
logger = logging.getLogger("ws_streamer")

def truncate_to_minute(ts):
    """Обрезает datetime/timestamp до начала минуты."""
    return ts.replace(second=0, microsecond=0)

async def listen_symbol(symbol, interval="1m"):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    conn = get_conn(DB)
    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info(f"WS connected for {symbol}")
                async for msg in ws:
                    data = json.loads(msg)['k']
                    if not data['x']:  # bar not closed
                        continue
                    ts_aware = datetime.utcfromtimestamp(data['t']/1000).replace(tzinfo=pytz.UTC)
                    ts_aware = truncate_to_minute(ts_aware)
                    row = {
                        'timestamp': ts_aware,
                        'unix': int(data['t'] // 1000),
                        'symbol': symbol,
                        'open': float(data['o']),
                        'high': float(data['h']),
                        'low': float(data['l']),
                        'close': float(data['c']),
                        'volume_base': float(data['v']),
                        'volume_quote': float(data['q'])
                    }
                    try:
                        insert_trade(conn, row)
                        logger.info(f"Inserted bar for {symbol} {row['timestamp']}")
                    except Exception as e:
                        logger.error(f"WS insert error for {symbol}: {e} Row: {row}")
        except Exception as e:
            logger.error(f"WS error for {symbol}: {e}")
            await asyncio.sleep(10)

async def main():
    await asyncio.gather(*[listen_symbol(sym, INTERVAL) for sym in MONETS])

if __name__ == "__main__":
    asyncio.run(main())
