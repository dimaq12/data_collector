import logging
import time
from datetime import datetime, timedelta
import pandas as pd

from config import DB, SYMBOLS, HIST_BARS, DAYS_TO_CHECK, MAX_ROWS_PER_SYMBOL, CSV_ARCHIVE_PATH, LOG_LEVEL, INTERVALS
from db import (
    get_conn, init_db, purge_old_rows,
    get_earliest_bar_ts, get_latest_bar_ts,
    get_timestamps_in_range, count_rows
)
from data_worker import (
    import_csv_files, fetch_history,
    scan_data_integrity, repair_gaps
)
from utils import normalize_timestamp

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("orchestrator")

SYNC_SLEEP_SEC = 60


def count_missing_bars(conn, symbol, interval, since, until):
    delta = pd.Timedelta(minutes=int(interval[:-1]) if interval.endswith('m') else int(interval[:-1]) * 60)
    since = normalize_timestamp(since, interval)
    until = normalize_timestamp(until, interval)
    expected = pd.date_range(since, until, freq=delta, tz="UTC")
    got = set(normalize_timestamp(ts, interval) for ts in get_timestamps_in_range(conn, symbol, since, until, interval))
    missing = [ts for ts in expected if ts not in got]
    return len(missing)


def generate_report(reports):
    lines = ["\n====== SYNC REPORT ======"]
    for key, info in reports.items():
        lines.append(f"--- {key} ---")
        lines.append(f"  Total rows:   {info['total_rows']}")
        lines.append(f"  Missing bars: {info['missing_bars']}")
        if info['missing_bars'] > 0:
            lines.append(f"  Repair attempts: {info['repair_attempts']}")
            lines.append(f"  Still missing:   {info['final_missing']}")
            if info.get('fail_reason'):
                lines.append(f"  Fail reason: {info['fail_reason']}")
        else:
            lines.append("  Status: OK")
        lines.append("")
    lines.append("===== END OF REPORT =====\n")
    logger.info("\n".join(lines))


def orchestrate_eternal_sync():
    logger.info("Starting eternal sync loop...")
    while True:
        try:
            conn = get_conn(DB)
            init_db(conn, INTERVALS)

            for interval in INTERVALS:
                import_csv_files(conn, CSV_ARCHIVE_PATH, interval, logger)

                reports = {}

                for symbol in SYMBOLS:
                    fetch_history(conn, symbol, HIST_BARS, interval, logger)

                    now = normalize_timestamp(pd.Timestamp.utcnow(), interval)
                    since = now - timedelta(days=DAYS_TO_CHECK)
                    until = now

                    repair_attempts = 0
                    fail_reason = None
                    max_attempts = 5

                    missing_bars = count_missing_bars(conn, symbol, interval, since, until)

                    while missing_bars > 0 and repair_attempts < max_attempts:
                        repair_gaps(conn, symbol, since, until, interval, logger)
                        repair_attempts += 1
                        missing_bars = count_missing_bars(conn, symbol, interval, since, until)

                    try:
                        purge_old_rows(conn, symbol, MAX_ROWS_PER_SYMBOL, interval)
                    except Exception as e:
                        fail_reason = f"Purge failed: {e}"

                    min_ts = get_earliest_bar_ts(conn, symbol, interval)
                    max_ts = get_latest_bar_ts(conn, symbol, interval)

                    if min_ts and max_ts:
                        since = normalize_timestamp(min_ts, interval)
                        until = normalize_timestamp(max_ts, interval)
                        missing_bars = count_missing_bars(conn, symbol, interval, since, until)
                        if missing_bars > 0:
                            repair_gaps(conn, symbol, since, until, interval, logger)
                            missing_bars = count_missing_bars(conn, symbol, interval, since, until)
                    else:
                        fail_reason = "No data after purge"

                    total_rows = count_rows(conn, symbol, interval)
                    reports[f"{symbol}_{interval}"] = {
                        'total_rows': total_rows,
                        'missing_bars': missing_bars,
                        'repair_attempts': repair_attempts,
                        'final_missing': missing_bars,
                        'fail_reason': fail_reason
                    }

                generate_report(reports)
            conn.close()

        except Exception as e:
            logger.critical(f"CRITICAL: {e}")
        time.sleep(SYNC_SLEEP_SEC)


if __name__ == "__main__":
    orchestrate_eternal_sync()
