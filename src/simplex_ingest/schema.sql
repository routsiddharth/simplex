-- Simplex ingest schema (DuckDB). Idempotent: safe to run on every startup.
-- raw_events is the append-only source of truth; snapshots is a regenerable
-- derived grid; book_state is a replay accelerator; markets is the catalog;
-- audit_results records book-vs-REST reconciliations.

-- Catalog of known markets. subscribed=true => in the active WS set.
CREATE TABLE IF NOT EXISTS markets (
    market_id           VARCHAR PRIMARY KEY,
    platform            VARCHAR  NOT NULL,
    title               VARCHAR,
    description         VARCHAR,
    resolution_criteria VARCHAR,
    series_ticker       VARCHAR,
    event_ticker        VARCHAR,
    created_at          TIMESTAMP,
    closes_at           TIMESTAMP,
    resolved_at         TIMESTAMP,
    status              VARCHAR,
    subscribed          BOOLEAN  NOT NULL DEFAULT FALSE,
    raw_metadata        JSON,
    last_seen_ts        TIMESTAMP
);

-- Append-only normalized event log. Never deleted. Ordering for replay uses
-- received_ts (our monotonic ingest clock) with DuckDB rowid as tiebreak;
-- `sequence` is the exchange per-subscription seq used for gap detection.
CREATE TABLE IF NOT EXISTS raw_events (
    ts          TIMESTAMP,            -- exchange event time (UTC), may be null
    received_ts TIMESTAMP NOT NULL,   -- our receive time (UTC, microsecond)
    platform    VARCHAR  NOT NULL,
    market_id   VARCHAR  NOT NULL,
    event_type  VARCHAR  NOT NULL,
    sequence    BIGINT,               -- exchange per-subscription seq
    payload     JSON     NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_events_market_seq ON raw_events (market_id, sequence);
CREATE INDEX IF NOT EXISTS idx_raw_events_market_ts  ON raw_events (market_id, ts);
CREATE INDEX IF NOT EXISTS idx_raw_events_received    ON raw_events (received_ts);

-- Serialized in-memory order books, for fast restart (replay from here forward).
CREATE TABLE IF NOT EXISTS book_state (
    market_id       VARCHAR PRIMARY KEY,
    last_sequence   BIGINT,
    last_ts         TIMESTAMP,        -- received_ts of last applied event
    serialized_book JSON NOT NULL
);

-- 10s materialized grid. PK gives idempotency against window re-runs.
CREATE TABLE IF NOT EXISTS snapshots (
    ts                TIMESTAMP NOT NULL,
    platform          VARCHAR   NOT NULL,
    market_id         VARCHAR   NOT NULL,
    yes_bid           DOUBLE,
    yes_ask           DOUBLE,
    yes_mid           DOUBLE,
    bid_depth_3c_usd  DOUBLE,
    ask_depth_3c_usd  DOUBLE,
    bid_levels_in_3c  INTEGER,
    ask_levels_in_3c  INTEGER,
    volume_10s        DOUBLE,
    last_trade_price  DOUBLE,
    status            VARCHAR,
    built_ts          TIMESTAMP,
    PRIMARY KEY (ts, platform, market_id)
);

-- One row per market per audit pass.
CREATE TABLE IF NOT EXISTS audit_results (
    ts                 TIMESTAMP NOT NULL,
    market_id          VARCHAR   NOT NULL,
    status             VARCHAR,          -- no_diff | small_diff | large_diff | error
    levels_diff_count  INTEGER,
    max_size_delta_pct DOUBLE,
    action_taken       VARCHAR,          -- none | book_reset
    details_json       JSON
);
CREATE INDEX IF NOT EXISTS idx_audit_market_ts ON audit_results (market_id, ts);
