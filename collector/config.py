import os

DB = {
    'host': os.environ.get('DB_HOST', 'timescaledb'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'dbname': os.environ.get('DB_NAME', 'tsdb'),
    'user': os.environ.get('DB_USER', 'tsadmin'),
    'password': os.environ.get('DB_PASS', 'tspassword')
}

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "DOGEUSDT", "BNBUSDT", "SOLUSDT"
]

INTERVALS = ["1m", "5m", "15m", "30m", "1h"]
HIST_BARS = int(os.environ.get('HIST_BARS', 300_000))
DAYS_TO_CHECK = int(os.environ.get('DAYS_TO_CHECK', 2))
MAX_ROWS_PER_SYMBOL = int(os.environ.get('MAX_ROWS_PER_SYMBOL', 30_000))
CSV_ARCHIVE_PATH = os.environ.get('CSV_ARCHIVE_PATH', 'csv_archives')

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
