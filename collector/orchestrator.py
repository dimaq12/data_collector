import logging
import time
import signal
import sys
from config import DB, SYMBOLS, INTERVALS, CSV_ARCHIVE_PATH, LOG_LEVEL, HIST_BARS
from db import get_conn, init_db
from data_integrity import DataIntegrity

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("orchestrator")

SYNC_SLEEP_SEC = 60
stop_signal_received = False
conn = None  # глобальное соединение

def handle_exit(signum, frame):
    global stop_signal_received
    logger.info(f"\n🛑 Received signal {signum}, shutting down gracefully...")
    stop_signal_received = True

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def orchestrate_clean_sync():
    global conn

    logger.info("🚀 Starting clean sync loop...")

    # ленивый кеш экземпляров DataIntegrity
    di_map: dict[tuple[str,str], DataIntegrity] = {}

    try:
        conn = get_conn(DB)
        init_db(conn, INTERVALS)

        while not stop_signal_received:
            all_dummy_segments = []

            for symbol in SYMBOLS:
                for interval in INTERVALS:
                    key = (symbol, interval)
                    if key not in di_map:
                        di_map[key] = DataIntegrity(
                            symbol=symbol,
                            interval=interval,
                            hist_bars=HIST_BARS,
                            conn=conn,
                            logger=logger,
                            verbose=False
                        )
                    di = di_map[key]

                    logger.info(f"\n⏳ Syncing {symbol} [{interval}]")

                    # сброс временных полей
                    di.scan_log.clear()
                    di.gap_chunks.clear()
                    di.filled.clear()
                    di.inserted = 0
                    di.state = "pending"

                    di.sync(CSV_ARCHIVE_PATH)
                    di.validate()

                    report = di.to_report()
                    if report["state"] == "integrated":
                        logger.info(f"✅ {symbol} [{interval}] fully synced")
                    else:
                        logger.warning(f"⚠️ {symbol} [{interval}] issues: {report}")

                    dummy_segs = di.report_dummy_segments()
                    if dummy_segs:
                        all_dummy_segments.append({
                            "symbol": symbol,
                            "interval": interval,
                            "segments": dummy_segs
                        })

            if all_dummy_segments:
                logger.info("\n=== Final Dummy-Segments Report ===")
                for item in all_dummy_segments:
                    sym, inter, segs = item["symbol"], item["interval"], item["segments"]
                    logger.info(f"{sym} [{inter}]:")
                    for seg in segs:
                        logger.info(
                            f"  • {seg['start']} → {seg['end']} ({seg['count']} bars)"
                        )
            else:
                logger.info("✅ No zero-volume segments found in this run.")

            time.sleep(SYNC_SLEEP_SEC)

    except Exception as e:
        logger.critical(f"🔥 CRITICAL ERROR in orchestrator: {e}")

    finally:
        if conn:
            try:
                conn.close()
                logger.info("🔌 Database connection closed.")
            except Exception as e:
                logger.warning(f"⚠️ Error during DB close: {e}")

if __name__ == "__main__":
    orchestrate_clean_sync()
