import os
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from db import (
    insert_trade, purge_old_rows, get_latest_bar_ts, get_earliest_bar_ts, get_timestamps_in_range
)
from utils import ensure_utc_aware

def truncate_to_minute(ts):
    if isinstance(ts, pd.Timestamp):
        return ts.replace(second=0, microsecond=0)
    if isinstance(ts, datetime):
        return ts.replace(second=0, microsecond=0)
    return ts

def import_csv_files(conn, folder, logger):
    files = [f for f in os.listdir(folder) if f.endswith('.csv')]
    for fname in files:
        logger.info(f"Importing {fname}")
        df = pd.read_csv(os.path.join(folder, fname))
        df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
        if 'date' in df.columns:
            df['timestamp'] = pd.to_datetime(df['date'], utc=True)
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        else:
            logger.warning(f"{fname} is missing 'date' or 'timestamp'. Skipping.")
            continue
        df['timestamp'] = df['timestamp'].apply(truncate_to_minute)
        df.rename(columns={
            "volume": "volume_base",
            "volume_btc": "volume_base",
            "volume_usd": "volume_quote",
            "vol_base": "volume_base",
            "vol_quote": "volume_quote"
        }, inplace=True)
        for _, row in df.iterrows():
            try:
                insert_trade(conn, row)
            except Exception as e:
                logger.error(f"CSV insert error: {e} Row: {row}")
        logger.info(f"Imported {fname} ({len(df)})")

def find_gap_ranges(missing, max_bulk=1000):
    """
    Находит непрерывные диапазоны пропусков (bulk-гэпы) в списке недостающих баров.
    Возвращает список (start, end) диапазонов, каждый не длиннее max_bulk.
    """
    if not missing:
        return []
    missing = sorted(missing)
    gap_ranges = []
    start = prev = missing[0]
    for ts in missing[1:]:
        # если не сосед по минуте или переполнили лимит
        if (ts - prev) != pd.Timedelta(minutes=1) or (ts - start).total_seconds() / 60 >= max_bulk:
            gap_ranges.append((start, prev))
            start = ts
        prev = ts
    gap_ranges.append((start, prev))
    return gap_ranges

def bulk_gap_fill(conn, symbol, since, until, logger, bulk_limit=1000):
    def norm(ts):
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return truncate_to_minute(ts)

    pd_since = norm(since)
    pd_until = norm(until)
    ms_since = int(pd_since.timestamp() * 1000)
    ms_until = int(pd_until.timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    n_loaded = 0
    iteration = 0

    logger.info(f"{symbol}: bulk_gap_fill START: {pd_since} → {pd_until}")

    while ms_since < ms_until:
        iteration += 1
        step_ms = min(bulk_limit * 60_000, ms_until - ms_since)
        step_end_ms = ms_since + step_ms

        params = {
            "symbol": symbol,
            "interval": "1m",
            "limit": bulk_limit,
            "startTime": ms_since,
            "endTime": step_end_ms
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            try:
                data = resp.json()
            except Exception as e:
                logger.error(f"{symbol}: JSON decode error at ms {ms_since}: {e}, status={resp.status_code}, text={resp.text[:200]}")
                break

            if not data:
                logger.warning(f"{symbol}: No data received at ms {ms_since} (possibly at the end of history?)")
                break
            if "code" in data:
                logger.error(f"{symbol}: API ERROR: {data}")
                break

            if data:
                dt_first = pd.to_datetime(data[0][0], unit='ms', utc=True)
                dt_last = pd.to_datetime(data[-1][0], unit='ms', utc=True)
                logger.info(
                    f"{symbol}: iter {iteration}: loaded {len(data)} bars (total {n_loaded + len(data)}) "
                    f"[{dt_first} — {dt_last}]"
                )

            for bar in data:
                try:
                    ts_bar = truncate_to_minute(pd.to_datetime(bar[0], unit="ms", utc=True))
                    row = {
                        'timestamp': ts_bar,
                        'unix': int(bar[0])//1000,
                        'symbol': symbol,
                        'open': float(bar[1]), 'high': float(bar[2]), 'low': float(bar[3]), 'close': float(bar[4]),
                        'volume_base': float(bar[5]), 'volume_quote': float(bar[7])
                    }
                    insert_trade(conn, row)
                except Exception as e:
                    logger.error(f"{symbol}: DB insert error: {e} @ {row.get('timestamp', '?')}")

            n_loaded += len(data)

            if len(data) < bulk_limit:
                logger.info(f"{symbol}: bulk_gap_fill early exit — received only {len(data)} bars, probably at end of available data.")
                break

            ms_since = data[-1][0] + 60_000
        except Exception as e:
            logger.error(f"{symbol}: Bulk fill error: {e} (ms_since={ms_since})")
            break

        time.sleep(0.2)

    logger.info(f"{symbol}: bulk_gap_fill FINISH: total loaded {n_loaded} bars ({pd_since} → {pd_until})")

def fetch_and_fill_gaps(conn, symbol, since, until, logger, bulk_limit=1000, single_gap_limit=10):
    now = ensure_utc_aware(pd.Timestamp.utcnow())
    since = truncate_to_minute(ensure_utc_aware(since))
    until = truncate_to_minute(ensure_utc_aware(until))
    until = min(until, truncate_to_minute(now - timedelta(minutes=1)))
    rng = pd.date_range(since, until, freq='1min', tz="UTC")

    got = set([truncate_to_minute(ensure_utc_aware(ts)) for ts in get_timestamps_in_range(conn, symbol, since, until)])
    missing = [ts for ts in rng if truncate_to_minute(ts) not in got]

    logger.info(f"{symbol}: fetch_and_fill_gaps range {since} → {until}")
    logger.info(f"{symbol}: {len(missing)} missing bars")

    if not missing:
        logger.info(f"{symbol}: gap fill complete (no missing bars left)")
        return

    # Сначала закрываем крупные гэпы bulk-запросами
    if len(missing) > single_gap_limit:
        gap_ranges = find_gap_ranges(missing, max_bulk=bulk_limit)
        logger.info(f"{symbol}: Found {len(gap_ranges)} bulk gap ranges to fill.")
        for gap_start, gap_end in gap_ranges:
            logger.info(f"{symbol}: Bulk filling {gap_start} — {gap_end}")
            bulk_gap_fill(conn, symbol, gap_start, gap_end + pd.Timedelta(minutes=1), logger, bulk_limit=bulk_limit)
        # После bulk-загрузок повторно смотрим что осталось
        fetch_and_fill_gaps(conn, symbol, since, until, logger, bulk_limit, single_gap_limit)
        return

    # Если осталось мало — одиночные запросы
    if 0 < len(missing) <= single_gap_limit:
        logger.info(f"{symbol}: Few gaps left, fetching individually: {[str(x) for x in missing]}")
        url = "https://api.binance.com/api/v3/klines"
        for ts in missing:
            ms = int(ts.timestamp() * 1000)
            params = {
                "symbol": symbol,
                "interval": "1m",
                "limit": 1,
                "startTime": ms,
                "endTime": ms + 60_000
            }
            try:
                resp = requests.get(url, params=params, timeout=10)
                bars = resp.json()
                if not bars:
                    logger.warning(f"{symbol}: No data from API for {ts}")
                    continue
                bar = bars[0]
                row = {
                    'timestamp': truncate_to_minute(pd.to_datetime(bar[0], unit="ms", utc=True)),
                    'unix': int(bar[0])//1000,
                    'symbol': symbol,
                    'open': float(bar[1]), 'high': float(bar[2]), 'low': float(bar[3]), 'close': float(bar[4]),
                    'volume_base': float(bar[5]), 'volume_quote': float(bar[7])
                }
                try:
                    insert_trade(conn, row)
                except Exception as e:
                    logger.error(f"{symbol}: Gap fill insert error: {e} Row: {row}")
            except Exception as e:
                logger.error(f"{symbol}: Gap fill error: {e} for ts {ts}")
            time.sleep(0.1)
        logger.info(f"{symbol}: gap fill complete (all remaining gaps filled)")
        return

def repair_gaps(conn, symbol, since, until, logger):
    fetch_and_fill_gaps(conn, symbol, since, until, logger)

def fetch_history(conn, symbol, bars, logger):
    now = truncate_to_minute(ensure_utc_aware(pd.Timestamp.utcnow()))
    earliest = now - pd.Timedelta(minutes=bars)

    min_ts_db = get_earliest_bar_ts(conn, symbol)
    max_ts_db = get_latest_bar_ts(conn, symbol)

    def norm(ts):
        if ts is None:
            return None
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return truncate_to_minute(ts)

    min_ts = norm(min_ts_db)
    max_ts = norm(max_ts_db)

    logger.info(f"{symbol}: [fetch_history] now={now}, earliest={earliest}, min_ts={min_ts}, max_ts={max_ts}")

    if min_ts is None or max_ts is None:
        logger.info(f"{symbol}: No data in DB — bulk loading whole history ({bars} min, {earliest} → {now})")
        bulk_gap_fill(conn, symbol, earliest, now, logger)
    else:
        if min_ts > earliest:
            logger.info(f"{symbol}: Bulk old data {earliest} → {min_ts}")
            bulk_gap_fill(conn, symbol, earliest, min_ts, logger)
        if max_ts < now - pd.Timedelta(minutes=1):
            logger.info(f"{symbol}: Bulk new data {max_ts} → {now}")
            bulk_gap_fill(conn, symbol, max_ts, now, logger)
        logger.info(f"{symbol}: Checking/filling local gaps {min_ts} → {max_ts}")
        fetch_and_fill_gaps(conn, symbol, min_ts, max_ts, logger)

    logger.info(f"{symbol}: History fully synchronized")

def scan_data_integrity(conn, symbol, since, until, logger):
    def norm(ts):
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return truncate_to_minute(ts)

    since = norm(since)
    until = norm(until)
    rng = pd.date_range(since, until, freq='1min', tz="UTC")

    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp FROM trades
            WHERE symbol = %s AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """, (symbol, since, until))
        got = set([truncate_to_minute(pd.Timestamp(r[0]).tz_convert("UTC")) for r in cur.fetchall()])

    missing = [ts for ts in rng if ts not in got]
    if missing:
        logger.error(f"{symbol}: INCONSISTENT DATA! {len(missing)} missing bars between {since} and {until}")
        for ts in missing[:10]:
            logger.error(f"{symbol}:   Missing: {ts}")
        if len(missing) > 10:
            logger.error(f"{symbol}:   ...and {len(missing)-10} more missing bars")
        return False
    logger.info(f"{symbol}: DATA INTEGRITY OK for {since} to {until}")
    return True