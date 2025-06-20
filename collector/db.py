import psycopg2
import logging
import pandas as pd
from psycopg2 import sql
from datetime import timedelta 

logger = logging.getLogger("db")


def get_conn(config):
    return psycopg2.connect(**config)


def _normalize_timestamp(ts, interval):
    if ts is None or not hasattr(ts, 'replace'):
        return ts

    if interval.endswith("h"):
        hours = int(interval[:-1])
        floored_hour = ts.hour - (ts.hour % hours)
        return ts.replace(hour=floored_hour, minute=0, second=0, microsecond=0)

    elif interval.endswith("m"):
        minutes = int(interval[:-1])
        floored_minute = (ts.minute // minutes) * minutes
        return ts.replace(minute=floored_minute, second=0, microsecond=0)

    else:
        raise ValueError(f"Unsupported interval: {interval}")


def init_db(conn, intervals):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        for interval in intervals:
            table = f"trades_{interval}"
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    bucket TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume_base DOUBLE PRECISION,
                    volume_quote DOUBLE PRECISION,
                    PRIMARY KEY (symbol, bucket)
                );
            """)
            cur.execute(f"""
                SELECT create_hypertable('{table}', 'bucket', if_not_exists => TRUE);
            """)
        conn.commit()


def insert_trade(conn, row, interval):
    try:
        norm_ts = _normalize_timestamp(row['timestamp'], interval)
        table = f"trades_{interval}"
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table} (bucket, symbol, open, high, low, close, volume_base, volume_quote)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, bucket) DO NOTHING;
            """, (
                norm_ts,
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


def insert_bulk_trades(conn, rows, interval, overwrite=False):
    try:
        table = f"trades_{interval}"
        with conn.cursor() as cur:
            prepared = []
            for row in rows:
                try:
                    norm_ts = _normalize_timestamp(row['timestamp'], interval)
                    prepared.append((
                        norm_ts,
                        row['symbol'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        float(row['volume_base']),
                        float(row['volume_quote']),
                    ))
                except Exception as e:
                    logger.warning(f"Skipping row in {table}: {e}")

            if not prepared:
                return

            args_str = ",".join(
                cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", r).decode("utf-8") for r in prepared
            )

            if overwrite:
                conflict_action = """
                    ON CONFLICT (symbol, bucket) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume_base = EXCLUDED.volume_base,
                        volume_quote = EXCLUDED.volume_quote
                """
            else:
                conflict_action = "ON CONFLICT (symbol, bucket) DO NOTHING"

            cur.execute(f"""
                INSERT INTO {table} (bucket, symbol, open, high, low, close, volume_base, volume_quote)
                VALUES {args_str}
                {conflict_action};
            """)
        conn.commit()
    except Exception as e:
        logger.error(f"insert_bulk_trades ERROR for {table}: {e}")


def purge_old_rows(conn, symbol, max_rows, interval):
    try:
        table = f"trades_{interval}"
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM {table}
                WHERE symbol = %s AND bucket < (
                    SELECT bucket FROM {table}
                    WHERE symbol = %s
                    ORDER BY bucket DESC
                    OFFSET %s LIMIT 1
                )
            """, (symbol, symbol, max_rows))
        conn.commit()
    except Exception as e:
        logger.error(f"purge_old_rows ERROR for {symbol} in {table}: {e}")


def get_latest_bar_ts(conn, symbol, interval):
    table = f"trades_{interval}"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT MAX(bucket) FROM {table} WHERE symbol = %s
        """, (symbol,))
        result = cur.fetchone()
        return result[0]


def get_earliest_bar_ts(conn, symbol, interval):
    table = f"trades_{interval}"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT MIN(bucket) FROM {table} WHERE symbol = %s
        """, (symbol,))
        result = cur.fetchone()
        return result[0]


def get_timestamps_in_range(conn, symbol, since, until, interval):
    since = _normalize_timestamp(since, interval)
    until = _normalize_timestamp(until, interval)
    table = f"trades_{interval}"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT bucket FROM {table}
            WHERE symbol = %s AND bucket BETWEEN %s AND %s
            ORDER BY bucket
        """, (symbol, since, until))
        return [r[0] for r in cur.fetchall()]


def count_rows(conn, symbol, interval):
    table = f"trades_{interval}"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE symbol = %s
        """, (symbol,))
        return cur.fetchone()[0]


def find_gaps(conn, symbol, interval, since, until):
    delta = timedelta(minutes=int(interval[:-1]) if interval.endswith("m") else int(interval[:-1]) * 60)
    since = _normalize_timestamp(since, interval)
    until = _normalize_timestamp(until, interval)
    expected = pd.date_range(start=since, end=until, freq=delta, tz="UTC")
    actual = set(_normalize_timestamp(ts, interval) for ts in get_timestamps_in_range(conn, symbol, since, until, interval))

    missing = sorted(ts for ts in expected if ts not in actual)

    gaps = []
    if not missing:
        return gaps

    start = missing[0]
    prev = missing[0]
    for ts in missing[1:]:
        if (ts - prev) != delta:
            gaps.append((start, prev))
            start = ts
        prev = ts
    gaps.append((start, prev))
    return gaps


# new methods for dummy bar reporting
def find_dummy_bars(conn, symbol, interval, since, until):
    since = _normalize_timestamp(since, interval)
    until = _normalize_timestamp(until, interval)
    table = f"trades_{interval}"
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            SELECT bucket FROM {table}
            WHERE symbol = %s
              AND bucket BETWEEN %s AND %s
              AND open = 0 AND high = 0 AND low = 0 AND close = 0
              AND volume_base = 0 AND volume_quote = 0
            ORDER BY bucket
        """
        ).format(table=sql.Identifier(table)), (symbol, since, until))
        return [r[0] for r in cur.fetchall()]


def find_dummy_ranges(conn, symbol, interval, since, until):
    timestamps = find_dummy_bars(conn, symbol, interval, since, until)
    if not timestamps:
        return []
    delta = timedelta(
        minutes=int(interval[:-1]) if interval.endswith("m") else 60 * int(interval[:-1])
    )
    ranges = []
    start = timestamps[0]
    prev = timestamps[0]
    for ts in timestamps[1:]:
        if ts - prev != delta:
            ranges.append((start, prev))
            start = ts
        prev = ts
    ranges.append((start, prev))
    return ranges
