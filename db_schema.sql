CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trades (
  timestamp TIMESTAMPTZ NOT NULL,
  unix BIGINT,
  symbol TEXT NOT NULL,
  open DOUBLE PRECISION,
  high DOUBLE PRECISION,
  low DOUBLE PRECISION,
  close DOUBLE PRECISION,
  volume_btc DOUBLE PRECISION,
  volume_usd DOUBLE PRECISION,
  PRIMARY KEY (symbol, timestamp)
);

SELECT create_hypertable('trades', 'timestamp');

CREATE TABLE IF NOT EXISTS btc_dominance (
  timestamp TIMESTAMPTZ NOT NULL PRIMARY KEY,
  dominance DOUBLE PRECISION
);

SELECT create_hypertable('btc_dominance', 'timestamp');
