import os
import re
import pandas as pd
import requests
import time
from db import (
    insert_trade,
    insert_bulk_trades,
    purge_old_rows,
    get_latest_bar_ts,
    get_earliest_bar_ts,
    get_timestamps_in_range
)
from utils import normalize_timestamp


def import_csv_files(conn, folder, interval, logger):
    files = [f for f in os.listdir(folder) if f.endswith('.csv')]
    pattern = re.compile(r'^(?P<symbol>[A-Z0-9]+)_(?P<interval>[0-9]+[mh])\.csv$', re.IGNORECASE)

    for fname in files:
        match = pattern.match(fname)
        if not match:
            logger.warning(f"{fname}: invalid filename format, skipping.")
            continue

        symbol = match.group('symbol').upper()
        file_interval = match.group('interval')

        if file_interval != interval:
            continue

        path = os.path.join(folder, fname)
        logger.info(f"Importing {fname}")

        df = pd.read_csv(path)
        df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
        if 'date' in df.columns:
            df['timestamp'] = pd.to_datetime(df['date'], utc=True)
        elif 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        else:
            logger.warning(f"{fname} is missing 'date' or 'timestamp'. Skipping.")
            continue

        df['timestamp'] = df['timestamp'].apply(lambda x: normalize_timestamp(x, interval))
        df.rename(columns={
            "volume": "volume_base",
            "volume_base": "volume_base",
            "volume_quote": "volume_quote",
            "vol_base": "volume_base",
            "vol_quote": "volume_quote"
        }, inplace=True)

        for _, row in df.iterrows():
            try:
                insert_trade(conn, row.to_dict(), interval)
            except Exception as e:
                logger.error(f"CSV insert error: {e} Row: {row}")
        logger.info(f"Imported {fname} ({len(df)})")


def find_gap_ranges(missing, interval, max_bulk=1000):
    if not missing:
        return []
    missing = sorted(missing)
    delta = pd.Timedelta(minutes=int(interval[:-1]) if interval.endswith('m') else int(interval[:-1]) * 60)
    gap_ranges = []
    start = prev = missing[0]
    for ts in missing[1:]:
        if (ts - prev) != delta or (ts - start) // delta >= max_bulk:
            gap_ranges.append((start, prev))
            start = ts
        prev = ts
    gap_ranges.append((start, prev))
    return gap_ranges


def bulk_gap_fill(conn, symbol, since, until, interval, logger, bulk_limit=1000):
    pd_since = normalize_timestamp(since, interval)
    pd_until = normalize_timestamp(until, interval)
    ms_since = int(pd_since.timestamp() * 1000)
    ms_until = int(pd_until.timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    n_loaded = 0
    logger.info(f"{symbol} [{interval}]: bulk_gap_fill {pd_since} → {pd_until}")

    while ms_since < ms_until:
        step_ms = min(bulk_limit * 60_000, ms_until - ms_since)
        step_end_ms = ms_since + step_ms

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": bulk_limit,
            "startTime": ms_since,
            "endTime": step_end_ms
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
        except Exception as e:
            logger.error(f"{symbol} [{interval}]: API error {e}")
            break

        if not data or isinstance(data, dict) and "code" in data:
            logger.warning(f"{symbol} [{interval}]: No or error data: {data}")
            break

        rows = []
        for bar in data:
            ts_bar = normalize_timestamp(pd.to_datetime(bar[0], unit="ms", utc=True), interval)
            rows.append({
                'timestamp': ts_bar,
                'unix': int(bar[0]) // 1000,
                'symbol': symbol,
                'open': float(bar[1]),
                'high': float(bar[2]),
                'low': float(bar[3]),
                'close': float(bar[4]),
                'volume_base': float(bar[5]),
                'volume_quote': float(bar[7])
            })

        try:
            insert_bulk_trades(conn, rows, interval)
        except Exception as e:
            logger.error(f"{symbol} [{interval}]: bulk insert error: {e}")

        n_loaded += len(rows)
        if len(rows) < bulk_limit:
            break

        ms_since = data[-1][0] + 60_000
        time.sleep(0.1)

    logger.info(f"{symbol} [{interval}]: bulk_gap_fill FINISHED: {n_loaded} bars")


def fetch_and_fill_gaps(conn, symbol, since, until, interval, logger):
    delta = pd.Timedelta(minutes=int(interval[:-1]) if interval.endswith('m') else int(interval[:-1]) * 60)
    since = normalize_timestamp(since, interval)
    until = normalize_timestamp(until, interval)
    rng = pd.date_range(since, until, freq=delta, tz="UTC")

    got = set(get_timestamps_in_range(conn, symbol, since, until, interval))
    missing = [ts for ts in rng if ts not in got]

    if not missing:
        logger.info(f"{symbol} [{interval}]: no gaps")
        return

    logger.info(f"{symbol} [{interval}]: {len(missing)} missing bars")

    ranges = find_gap_ranges(missing, interval)
    url = "https://api.binance.com/api/v3/klines"

    for start, end in ranges:
        total_bars = int((end - start) / delta) + 1
        if total_bars > 5:
            bulk_gap_fill(conn, symbol, start, end + delta, interval, logger)
        else:
            for ts in pd.date_range(start, end, freq=delta, tz="UTC"):
                ms = int(ts.timestamp() * 1000)
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "limit": 1,
                    "startTime": ms,
                    "endTime": ms + 60_000
                }
                try:
                    resp = requests.get(url, params=params, timeout=10)
                    bars = resp.json()
                    if not bars:
                        logger.warning(f"{symbol} [{interval}]: no data for {ts}")
                        continue
                    bar = bars[0]
                    row = {
                        'timestamp': normalize_timestamp(pd.to_datetime(bar[0], unit="ms", utc=True), interval),
                        'unix': int(bar[0]) // 1000,
                        'symbol': symbol,
                        'open': float(bar[1]),
                        'high': float(bar[2]),
                        'low': float(bar[3]),
                        'close': float(bar[4]),
                        'volume_base': float(bar[5]),
                        'volume_quote': float(bar[7])
                    }
                    insert_trade(conn, row, interval)
                except Exception as e:
                    logger.error(f"{symbol} [{interval}]: insert error: {e}")
                time.sleep(0.1)


def repair_gaps(conn, symbol, since, until, interval, logger):
    fetch_and_fill_gaps(conn, symbol, since, until, interval, logger)


def fetch_history(conn, symbol, bars, interval, logger):
    delta = pd.Timedelta(minutes=int(interval[:-1]) if interval.endswith('m') else int(interval[:-1]) * 60)
    now = normalize_timestamp(pd.Timestamp.utcnow(), interval)
    earliest = now - delta * bars

    min_ts = get_earliest_bar_ts(conn, symbol, interval)
    max_ts = get_latest_bar_ts(conn, symbol, interval)

    if min_ts is None or max_ts is None:
        logger.info(f"{symbol} [{interval}]: DB empty, full load {earliest} → {now}")
        bulk_gap_fill(conn, symbol, earliest, now, interval, logger)
    else:
        if min_ts > earliest:
            bulk_gap_fill(conn, symbol, earliest, min_ts, interval, logger)
        if max_ts < now - delta:
            bulk_gap_fill(conn, symbol, max_ts, now, interval, logger)
        fetch_and_fill_gaps(conn, symbol, min_ts, max_ts, interval, logger)


def scan_data_integrity(conn, symbol, since, until, interval, logger):
    delta = pd.Timedelta(minutes=int(interval[:-1]) if interval.endswith('m') else int(interval[:-1]) * 60)
    since = normalize_timestamp(since, interval)
    until = normalize_timestamp(until, interval)
    rng = pd.date_range(since, until, freq=delta, tz="UTC")

    got = set(get_timestamps_in_range(conn, symbol, since, until, interval))
    missing = [ts for ts in rng if ts not in got]

    if missing:
        logger.error(f"{symbol} [{interval}]: {len(missing)} missing bars from {since} to {until}")
        for ts in missing[:10]:
            logger.error(f"  Missing: {ts}")
        if len(missing) > 10:
            logger.error(f"  ...and {len(missing) - 10} more")
        return False

    logger.info(f"{symbol} [{interval}]: data integrity OK")
    return True
