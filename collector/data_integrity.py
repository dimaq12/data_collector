import os
import re
import time
import logging
from datetime import datetime
import concurrent.futures

import pandas as pd
import requests

from db import (
    insert_bulk_trades,
    get_earliest_bar_ts,
    get_latest_bar_ts,
    find_gaps,
    find_dummy_ranges
)
from utils import normalize_timestamp


class DataIntegrity:
    def __init__(
        self,
        symbol: str,
        interval: str,
        hist_bars: int,
        conn,
        logger: logging.Logger,
        verbose: bool = False,
        fill_empty: bool = True 
    ):
        self.symbol = symbol
        self.interval = interval
        self.hist_bars = hist_bars
        self.conn = conn
        self.verbose = verbose
        self.fill_empty = fill_empty
        self.logger = logger.getChild(f"DataIntegrity[{symbol}:{interval}]")

        self.delta = self._parse_interval(interval)
        self.start_time = datetime.utcnow()
        self.earliest_binance_ts = None

        # align "now" to the interval grid
        self.now = normalize_timestamp(datetime.utcnow(), self.interval)
        if self.interval in ("1m", "3m"):
            self.since = self.now - self.delta * self.hist_bars
        else:
            earliest = self._find_binance_earliest_ts()
            self.earliest_binance_ts = earliest
            self.since = max(earliest, self.now - self.delta * self.hist_bars)

        self.until = self.now

        # scan/fill state
        self.initial_gaps = []
        self.gaps = []
        self.filled = []
        self.inserted = 0
        self.validate_gaps = []
        self.state = "pending"

        # verbose logs
        self.scan_log = []
        self.gap_chunks = []

    def sync(self, csv_folder: str):
        self._import_local_csv(csv_folder)
        self.scan()
        self.fill_gaps(overwrite=True)
        self.validate()

    def _import_local_csv(self, folder: str):
        pattern = re.compile(rf"^{self.symbol}_{self.interval}\.csv$", re.IGNORECASE)
        for fname in os.listdir(folder):
            if not pattern.match(fname):
                continue
            path = os.path.join(folder, fname)
            try:
                df = pd.read_csv(path)
            except Exception as e:
                self.logger.warning(f"CSV read error {fname}: {e}")
                continue

            # normalize columns
            df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]
            if "date" in df.columns:
                df["timestamp"] = pd.to_datetime(df["date"], utc=True)
            elif "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            else:
                self.logger.warning(f"{fname} missing timestamp column, skipping")
                continue

            # snap to interval grid
            df["timestamp"] = df["timestamp"].apply(
                lambda ts: normalize_timestamp(ts, self.interval)
            )

            rows = []
            for bar in df.to_dict("records"):
                rows.append({
                    "timestamp": bar["timestamp"],
                    "symbol": self.symbol,
                    "open": float(bar.get("open", 0)),
                    "high": float(bar.get("high", 0)),
                    "low": float(bar.get("low", 0)),
                    "close": float(bar.get("close", 0)),
                    "volume_base": float(bar.get("volume_base", bar.get("volume", 0))),
                    "volume_quote": float(bar.get("volume_quote", 0)),
                })
            if rows:
                insert_bulk_trades(self.conn, rows, self.interval, overwrite=False)
                self.logger.info(f"Imported {len(rows)} rows from {fname}")

    def scan(self):
        self.min_ts = get_earliest_bar_ts(self.conn, self.symbol, self.interval)
        self.max_ts = get_latest_bar_ts(self.conn, self.symbol, self.interval)

        if not self.min_ts or not self.max_ts:
            self.initial_gaps = [(self.since, self.until)]
        else:
            self.initial_gaps = find_gaps(
                self.conn, self.symbol, self.interval, self.since, self.until
            )

        self.gaps = list(self.initial_gaps)
        self.state = "gaps_detected" if self.gaps else "no_gaps"
        self.logger.info(f"Scan complete: {len(self.gaps)} gaps detected")

        if self.verbose:
            for start, end in self.gaps:
                expected = self._expected_bars(start, end)
                self.scan_log.append({
                    "start": start,
                    "end": end,
                    "expected_bars": expected
                })

    def fill_gaps(self, overwrite: bool = True):
        if not self.gaps:
            self.logger.info("No gaps to fill")
            return

        def fill_chunk(start: datetime, end: datetime):
            start = normalize_timestamp(start, self.interval)
            end = normalize_timestamp(end, self.interval) + self.delta  # make end exclusive

            query_start = start - self.delta * 2
            query_end = end + self.delta * 2

            try:
                bars = self._fetch_with_retry(query_start, query_end)
                self.logger.debug(
                    f"⚙️ fetch window {query_start}→{query_end}: received {len(bars)} bars"
                )

                # only those bars exactly aligned
                bars_in_gap = [
                    b for b in bars
                    if start <= pd.to_datetime(b[0], unit="ms", utc=True) < end
                ]
                timestamps_returned = {
                    normalize_timestamp(pd.to_datetime(b[0], unit="ms", utc=True), self.interval)
                    for b in bars_in_gap
                }

                expected_timestamps = [
                    start + i * self.delta
                    for i in range(self._expected_bars(start, end - self.delta))
                ]

                # если включён режим пустышек — добавить «отсутствующие» интервалы
                dummy_rows = []
                if self.fill_empty:
                    missing = [ts for ts in expected_timestamps if ts not in timestamps_returned]
                    for ts in missing:
                        dummy_rows.append({
                            "timestamp": ts,
                            "symbol": self.symbol,
                            "open": 0.0,
                            "high": 0.0,
                            "low": 0.0,
                            "close": 0.0,
                            "volume_base": 0.0,
                            "volume_quote": 0.0,
                        })
                    if missing:
                        self.logger.debug(
                            f"🆕 Inserting {len(missing)} empty candles for {start}→{end - self.delta}"
                        )

                # реальные бары
                real_rows = self._bars_to_rows(bars_in_gap)
                total_rows = real_rows + dummy_rows

                if total_rows:
                    insert_bulk_trades(self.conn, total_rows, self.interval, overwrite=overwrite)
                    self.logger.info(
                        f"💾 Inserted {len(real_rows)} real + {len(dummy_rows)} dummy bars "
                        f"{start} → {end - self.delta}"
                    )
                else:
                    self.logger.warning(f"❌ No bars (даже пустышек) для {start} → {end - self.delta}")

                # после вставки проверяем остаток
                remaining = find_gaps(
                    self.conn, self.symbol, self.interval, start, end
                )
                gap_cleared = len(remaining) == 0

                return {
                    "start": start,
                    "end": end - self.delta,
                    "expected": len(expected_timestamps),
                    "received": len(bars_in_gap),
                    "inserted_real": len(real_rows),
                    "inserted_dummy": len(dummy_rows),
                    "gap_cleared": gap_cleared
                }

            except Exception as e:
                self.logger.error(f"Error filling {start}→{end - self.delta}: {e}")
                return {
                    "start": start,
                    "end": end - self.delta,
                    "expected": self._expected_bars(start, end - self.delta),
                    "received": 0,
                    "inserted_real": 0,
                    "inserted_dummy": 0,
                    "gap_cleared": False,
                    "error": str(e)
                }

        chunks = [
            (normalize_timestamp(s, self.interval), normalize_timestamp(e, self.interval))
            for s, e in self.gaps
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fill_chunk, s, e) for s, e in chunks]
            for fut in concurrent.futures.as_completed(futures):
                r = fut.result()
                self.gap_chunks.append(r)
                self.inserted += r.get("inserted_real", 0) + r.get("inserted_dummy", 0)
                if r.get("gap_cleared"):
                    self.filled.append((r["start"], r["end"], r["inserted_real"] + r["inserted_dummy"]))
                    self.logger.info(
                        f"✅ Filled chunk {r['start']} → {r['end']}"
                    )
                else:
                    self.logger.warning(
                        f"⚠️ Partial/failed fill {r['start']} → {r['end']} "
                        f"(got {r['received']} of {r['expected']})"
                    )

        self.state = "filled"

    def validate(self):
        if self.state not in ("filled", "no_gaps"):
            self.logger.warning("Validate called in invalid state")

        self.validate_gaps = find_gaps(
            self.conn, self.symbol, self.interval, self.since, self.until
        )
        if not self.validate_gaps:
            self.state = "integrated"
            self.logger.info("Data integrated successfully, no remaining gaps")
        else:
            self.state = "partial"
            self.logger.warning(
                f"Validation found {len(self.validate_gaps)} remaining gaps"
            )

    def to_report(self) -> dict:
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        report = {
            "symbol": self.symbol,
            "interval": self.interval,
            "state": self.state,
            "initial_gaps": len(self.initial_gaps),
            "filled_chunks": len(self.filled),
            "inserted_bars": self.inserted,
            "remaining_gaps": len(self.validate_gaps),
            "duration_sec": duration
        }
        if self.verbose:
            report["scan_log"] = self.scan_log
            report["gap_chunks"] = self.gap_chunks
            if self.initial_gaps:
                progress = (
                    100
                    * (len(self.initial_gaps) - len(self.validate_gaps))
                    / len(self.initial_gaps)
                )
                report["progress_percent"] = round(progress, 2)
        return report

    # =============== PRIVATE HELPERS ===============

    def _parse_interval(self, interval: str) -> pd.Timedelta:
        unit = interval[-1]
        value = int(interval[:-1])
        if unit == "m":
            return pd.Timedelta(minutes=value)
        elif unit == "h":
            return pd.Timedelta(hours=value)
        else:
            raise ValueError(f"Unsupported interval '{interval}'")

    def _expected_bars(self, start: datetime, end: datetime) -> int:
        # count of steps inclusive
        return int((end - start) / self.delta) + 1

    def _fetch_with_retry(self, start: datetime, end: datetime, limit: int = 1000, retries: int = 3):
        for attempt in range(1, retries + 1):
            try:
                return self._fetch_binance_bars(start, end, limit)
            except Exception as e:
                if attempt == retries:
                    raise
                wait = 2 ** attempt
                self.logger.warning(f"Fetch attempt {attempt} failed, retrying in {wait}s...")
                time.sleep(wait)

    def _fetch_binance_bars(self, start: datetime, end: datetime, limit: int = 1000):
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "startTime": int(start.timestamp() * 1000),
            "endTime":   int(end.timestamp() * 1000),
            "limit":     limit
        }
        prepared = requests.Request("GET", url, params=params).prepare()
        self.logger.debug(f"▶️ Request URL: {prepared.url}")
        resp = requests.get(url, params=params, timeout=15)
        self.logger.debug(f"🔹 Status: {resp.status_code}, length: {len(resp.text)}")
        data = resp.json()
        if isinstance(data, list) and data:
            first_ts = pd.to_datetime(data[0][0], unit="ms", utc=True)
            last_ts  = pd.to_datetime(data[-1][0], unit="ms", utc=True)
            self.logger.debug(f"🔹 Binance returned {len(data)} bars, first={first_ts}, last={last_ts}")
        elif isinstance(data, dict) and data.get("code"):
            self.logger.error(f"⚠️ Binance error: {data}")
            raise ValueError(f"Binance error: {data}")
        return data

    def _bars_to_rows(self, bars) -> list:
        rows = []
        for bar in bars:
            ts = normalize_timestamp(
                pd.to_datetime(bar[0], unit="ms", utc=True),
                self.interval
            )
            rows.append({
                "timestamp": ts,
                "symbol": self.symbol,
                "open": float(bar[1]),
                "high": float(bar[2]),
                "low": float(bar[3]),
                "close": float(bar[4]),
                "volume_base": float(bar[5]),
                "volume_quote": float(bar[7]),
            })
        return rows

    def _find_binance_earliest_ts(self):
        self.logger.info(f"Locating earliest Binance data for {self.symbol}[{self.interval}]")
        limit = self.now - self.hist_bars * self.delta
        lower, upper = limit, self.now
        earliest = None

        for _ in range(30):
            if (upper - lower) <= self.delta:
                break
            mid = lower + (upper - lower) / 2
            chunk_end = mid + self.delta * 1000
            try:
                bars = self._fetch_binance_bars(mid, chunk_end)
                if bars:
                    ts = pd.to_datetime(bars[0][0], unit="ms", utc=True)
                    earliest = ts
                    upper = mid
                else:
                    lower = mid
            except Exception as e:
                self.logger.warning(f"Binance scan error: {e}")
                break
            time.sleep(0.3)

        if not earliest:
            raise RuntimeError(f"Could not determine earliest ts for {self.symbol}[{self.interval}]")
        result = limit if earliest <= limit else earliest
        self.logger.info(f"Earliest data at {result}")
        return result

    def report_dummy_segments(self) -> list[dict]:
        """
        Собирает из БД все сегменты подряд идущих баров,
        у которых open=high=low=close=volume_base=volume_quote=0.

        Возвращает список словарей вида:
            [
                {
                    'start': Timestamp('2023-03-24 12:40:00+00:00'),
                    'end':   Timestamp('2023-03-24 13:55:00+00:00'),
                    'count': 16
                },
                ...
            ]
        """
        # вызываем функцию из db.py, которая группирует «пустышки» в диапазоны
        dummy_ranges = find_dummy_ranges(
            self.conn,
            self.symbol,
            self.interval,
            self.since,
            self.until
        )

        report = []
        for start, end in dummy_ranges:
            # считаем, сколько баров в диапазоне [start, end] с шагом self.delta
            count = int((end - start) / self.delta) + 1
            report.append({
                'start': start,
                'end':   end,
                'count': count
            })

        return report