import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from db import insert_btc_dominance, get_earliest_btc_dom_ts, get_latest_btc_dom_ts, get_btc_dom_timestamps_in_range

def truncate_to_minute(ts):
    if isinstance(ts, pd.Timestamp):
        return ts.replace(second=0, microsecond=0)
    if isinstance(ts, datetime):
        return ts.replace(second=0, microsecond=0)
    return ts

def count_btc_dom_missing_bars(conn, since, until):
    rng = pd.date_range(since, until, freq='1min')
    got = set([truncate_to_minute(ts) for ts in get_btc_dom_timestamps_in_range(conn, since, until)])
    missing = [ts for ts in rng if truncate_to_minute(ts) not in got]
    return missing

def find_gap_ranges(missing, max_bulk=90):  # максимум 90 минут за раз!
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

def repair_btc_dom_gaps(conn, since, until, logger, range_minutes=90):
    # Получаем список всех минутных гэпов
    missing = count_btc_dom_missing_bars(conn, since, until)
    if not missing:
        logger.info(f"BTC_DOMINANCE: gap fill complete (no missing bars left)")
        return

    # Разбиваем на диапазоны <= 90 минут (или любой range_minutes)
    gap_ranges = find_gap_ranges(missing, max_bulk=range_minutes)
    for gap_start, gap_end in gap_ranges:
        logger.info(f"BTC_DOMINANCE: Filling gaps {gap_start} — {gap_end}")
        from_ts = int(gap_start.timestamp())
        to_ts = int((gap_end + pd.Timedelta(minutes=1)).timestamp())  # inclusive

        btc_url = f"https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range?vs_currency=usd&from={from_ts}&to={to_ts}"
        global_url = f"https://api.coingecko.com/api/v3/global/market_cap_chart/range?vs_currency=usd&from={from_ts}&to={to_ts}"

        try:
            btc_data = requests.get(btc_url, timeout=10).json()
            global_data = requests.get(global_url, timeout=10).json()
        except Exception as e:
            logger.error(f"BTC_DOMINANCE: API error: {e} @ {gap_start} — {gap_end}")
            continue

        btc_caps = btc_data.get('market_caps', [])
        total_caps = global_data.get('market_cap', [])

        btc_df = pd.DataFrame(btc_caps, columns=["timestamp_ms", "btc_cap"])
        btc_df["btc_cap"] = pd.to_numeric(btc_df["btc_cap"], errors="coerce")
        total_df = pd.DataFrame(total_caps, columns=["timestamp_ms", "total_cap"])
        total_df["total_cap"] = pd.to_numeric(total_df["total_cap"], errors="coerce")
        btc_df["timestamp_ms"] = pd.to_numeric(btc_df["timestamp_ms"])
        total_df["timestamp_ms"] = pd.to_numeric(total_df["timestamp_ms"])

        df = pd.merge_asof(
            btc_df.sort_values("timestamp_ms"),
            total_df.sort_values("timestamp_ms"),
            on="timestamp_ms",
            direction="nearest",
            tolerance=60_000  # 1 минута в миллисекундах
        ).dropna(subset=["btc_cap", "total_cap"])

        df["dominance"] = df["btc_cap"] / df["total_cap"] * 100
        df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df["timestamp"] = df["timestamp"].apply(truncate_to_minute)

        for _, row in df.iterrows():
            try:
                insert_btc_dominance(conn, row["timestamp"], float(row["dominance"]))
            except Exception as e:
                logger.error(f"BTC_DOMINANCE: DB insert error: {e} @ {row.get('timestamp', '?')}")
        time.sleep(1)  # аккуратно с лимитами CoinGecko!

def fetch_btc_dom_history(conn, bars, logger, max_attempts=5):
    now = truncate_to_minute(datetime.utcnow().replace(tzinfo=timezone.utc))
    earliest = now - pd.Timedelta(minutes=bars)
    min_ts = get_earliest_btc_dom_ts(conn)
    max_ts = get_latest_btc_dom_ts(conn)
    fail_reason = None

    def norm(ts):
        if ts is None:
            return None
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return truncate_to_minute(ts)

    min_ts = norm(min_ts)
    max_ts = norm(max_ts)

    if min_ts is None or max_ts is None:
        logger.info(f"BTC_DOMINANCE: No data in DB — bulk loading whole history ({bars} min, {earliest} → {now})")
        repair_btc_dom_gaps(conn, earliest, now, logger)
    else:
        if min_ts > earliest:
            repair_btc_dom_gaps(conn, earliest, min_ts, logger)
        if max_ts < now - pd.Timedelta(minutes=1):
            repair_btc_dom_gaps(conn, max_ts, now, logger)
        repair_btc_dom_gaps(conn, min_ts, max_ts, logger)

    # До max_attempts повторяем попытку добить все пропуски
    for _ in range(max_attempts):
        missing = count_btc_dom_missing_bars(conn, earliest, now)
        if not missing:
            break
        logger.info(f"BTC_DOMINANCE: {len(missing)} missing bars left, attempting repair.")
        repair_btc_dom_gaps(conn, earliest, now, logger)

    total_rows = len(get_btc_dom_timestamps_in_range(conn, earliest, now))
    missing = count_btc_dom_missing_bars(conn, earliest, now)
    return total_rows, len(missing), max_attempts, fail_reason
