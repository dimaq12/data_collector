import logging
import time
from datetime import datetime, timedelta, timezone
from config import DB, MONETS, HIST_BARS, DAYS_TO_CHECK, MAX_ROWS_PER_SYMBOL, CSV_ARCHIVE_PATH, LOG_LEVEL
from db import get_conn, init_db, purge_old_rows, get_earliest_bar_ts, get_latest_bar_ts, get_timestamps_in_range
from data_worker import import_csv_files, fetch_history, scan_data_integrity, repair_gaps
from db import count_rows
import pandas as pd

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("orchestrator")

SYNC_SLEEP_SEC = 60  # Sleep time between full sync cycles

def truncate_to_minute(ts):
    if isinstance(ts, pd.Timestamp):
        return ts.replace(second=0, microsecond=0)
    if isinstance(ts, datetime):
        return ts.replace(second=0, microsecond=0)
    return ts

def generate_report(reports, btc_dom_report=None):
    lines = []
    lines.append("\n====== SYNC REPORT ======")
    for symbol, info in reports.items():
        lines.append(f"--- {symbol} ---")
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
    if btc_dom_report:
        lines.append("--- BTC Dominance ---")
        lines.append(btc_dom_report)
    lines.append("===== END OF REPORT =====\n")
    logger.info("\n".join(lines))


def count_missing_bars(conn, symbol, since, until):
    rng = pd.date_range(since, until, freq='1min')
    got = set([truncate_to_minute(ts) for ts in get_timestamps_in_range(conn, symbol, since, until)])
    missing = [ts for ts in rng if truncate_to_minute(ts) not in got]
    return len(missing)

def orchestrate_eternal_sync():
    logger.info("Starting eternal sync loop...")
    while True:
        try:
            conn = get_conn(DB)
            init_db(conn)
            import_csv_files(conn, CSV_ARCHIVE_PATH, logger)

            reports = {}

            for symbol in MONETS:
                fetch_history(conn, symbol, HIST_BARS, logger)

                until = truncate_to_minute(datetime.utcnow().replace(tzinfo=timezone.utc))
                since = truncate_to_minute(until - timedelta(days=DAYS_TO_CHECK))

                repair_attempts = 0
                fail_reason = None
                max_attempts = 5

                missing_bars = count_missing_bars(conn, symbol, since, until)

                while missing_bars > 0 and repair_attempts < max_attempts:
                    repair_gaps(conn, symbol, since, until, logger)
                    repair_attempts += 1
                    missing_bars = count_missing_bars(conn, symbol, since, until)

                try:
                    purge_old_rows(conn, symbol, MAX_ROWS_PER_SYMBOL)
                except Exception as e:
                    fail_reason = f"Purge failed: {e}"

                # Final check after purge
                min_ts = get_earliest_bar_ts(conn, symbol)
                max_ts = get_latest_bar_ts(conn, symbol)
                if min_ts and max_ts:
                    since = truncate_to_minute(pd.Timestamp(min_ts).tz_convert("UTC"))
                    until = truncate_to_minute(pd.Timestamp(max_ts).tz_convert("UTC"))
                    missing_bars = count_missing_bars(conn, symbol, since, until)
                    if missing_bars > 0:
                        repair_gaps(conn, symbol, since, until, logger)
                        missing_bars = count_missing_bars(conn, symbol, since, until)
                else:
                    fail_reason = "No data after purge"

                total_rows = count_rows(conn, symbol)
                reports[symbol] = {
                    'total_rows': total_rows,
                    'missing_bars': missing_bars,
                    'repair_attempts': repair_attempts,
                    'final_missing': missing_bars,
                    'fail_reason': fail_reason
                }
            conn.close()
        except Exception as e:
            logger.critical(f"CRITICAL: {e}")

        time.sleep(SYNC_SLEEP_SEC)

if __name__ == "__main__":
    orchestrate_eternal_sync()
