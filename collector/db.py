import psycopg2
import logging

logger = logging.getLogger("db")

def get_conn(config):
    return psycopg2.connect(**config)

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            timestamp TIMESTAMPTZ NOT NULL,
            unix BIGINT,
            symbol TEXT NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume_base DOUBLE PRECISION,
            volume_quote DOUBLE PRECISION,
            PRIMARY KEY (symbol, timestamp)
        );
        """)
        cur.execute("SELECT create_hypertable('trades', 'timestamp', if_not_exists => TRUE);")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS btc_dominance (
            timestamp TIMESTAMPTZ NOT NULL PRIMARY KEY,
            dominance DOUBLE PRECISION
        );
        """)
        cur.execute("SELECT create_hypertable('btc_dominance', 'timestamp', if_not_exists => TRUE);")
        conn.commit()

def _normalize_timestamp(ts):
    """Нормализует таймштамп до начала минуты."""
    if ts is None:
        return None
    # Поддержка как pd.Timestamp, так и datetime
    if hasattr(ts, 'replace'):
        return ts.replace(second=0, microsecond=0)
    return ts

def insert_trade(conn, row):
    try:
        norm_ts = _normalize_timestamp(row['timestamp'])
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (timestamp, unix, symbol, open, high, low, close, volume_base, volume_quote)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING;
            """, (
                norm_ts,
                int(row['unix']),
                row['symbol'],
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                float(row['volume_base']),
                float(row['volume_quote']),
            ))
        conn.commit()
    except Exception as e:
        logger.error(f"insert_trade ERROR for {row.get('symbol', '???')} at {row.get('timestamp', '???')}: {e}")

def insert_btc_dominance(conn, timestamp, dominance):
    try:
        norm_ts = _normalize_timestamp(timestamp)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO btc_dominance (timestamp, dominance)
                VALUES (%s, %s)
                ON CONFLICT (timestamp) DO NOTHING;
            """, (norm_ts, dominance))
        conn.commit()
    except Exception as e:
        logger.error(f"insert_btc_dominance ERROR at {timestamp}: {e}")

def purge_old_rows(conn, symbol, max_rows):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM trades
                WHERE symbol = %s AND timestamp < (
                    SELECT timestamp FROM trades
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    OFFSET %s LIMIT 1
                )
            """, (symbol, symbol, max_rows))
        conn.commit()
    except Exception as e:
        logger.error(f"purge_old_rows ERROR for {symbol}: {e}")

def get_latest_bar_ts(conn, symbol):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(timestamp) FROM trades WHERE symbol = %s
        """, (symbol,))
        result = cur.fetchone()
        return result[0]

def get_earliest_bar_ts(conn, symbol):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(timestamp) FROM trades WHERE symbol = %s
        """, (symbol,))
        result = cur.fetchone()
        return result[0]

def get_timestamps_in_range(conn, symbol, since, until):
    # since/until могут быть тоже с секундами — нормализуем:
    since = _normalize_timestamp(since)
    until = _normalize_timestamp(until)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp FROM trades
            WHERE symbol = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """, (symbol, since, until))
        return [r[0] for r in cur.fetchall()]

def count_rows(conn, symbol):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM trades WHERE symbol = %s", (symbol,))
        return cur.fetchone()[0]
