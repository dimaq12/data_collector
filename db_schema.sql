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

-- 1-Minute Bars
CREATE TABLE IF NOT EXISTS trades_1m (
  bucket      TIMESTAMPTZ NOT NULL,
  symbol      TEXT            NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume_btc  DOUBLE PRECISION,
  volume_usd  DOUBLE PRECISION,
  PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('trades_1m', 'bucket');

-- 5-Minute Bars
CREATE TABLE IF NOT EXISTS trades_5m (
  bucket      TIMESTAMPTZ NOT NULL,
  symbol      TEXT            NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume_btc  DOUBLE PRECISION,
  volume_usd  DOUBLE PRECISION,
  PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('trades_5m', 'bucket');

-- 15-Minute Bars
CREATE TABLE IF NOT EXISTS trades_15m (
  bucket      TIMESTAMPTZ NOT NULL,
  symbol      TEXT            NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume_btc  DOUBLE PRECISION,
  volume_usd  DOUBLE PRECISION,
  PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('trades_15m', 'bucket');

-- 30-Minute Bars
CREATE TABLE IF NOT EXISTS trades_30m (
  bucket      TIMESTAMPTZ NOT NULL,
  symbol      TEXT            NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume_btc  DOUBLE PRECISION,
  volume_usd  DOUBLE PRECISION,
  PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('trades_30m', 'bucket');

-- 1-Hour Bars
CREATE TABLE IF NOT EXISTS trades_1h (
  bucket      TIMESTAMPTZ NOT NULL,
  symbol      TEXT            NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  volume_btc  DOUBLE PRECISION,
  volume_usd  DOUBLE PRECISION,
  PRIMARY KEY (symbol, bucket)
);
SELECT create_hypertable('trades_1h', 'bucket');