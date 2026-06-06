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

-- Self-managed tracked set: which series the catalog poller expands into the
-- active subscription set. Rewritten atomically by the discovery loop each cycle
-- (admit/rank by predicates). Replaces the human-curated simplex_allowlist.yaml.
CREATE TABLE IF NOT EXISTS tracked_series (
    series_ticker      VARCHAR PRIMARY KEY,
    admitted_at        TIMESTAMP,        -- first admission; preserved across re-admits
    last_check_at      TIMESTAMP,        -- most recent discovery cycle that admitted it
    passes_p1          BOOLEAN,          -- partition predicate
    passes_p2          BOOLEAN,          -- hierarchy predicate
    passes_p3          BOOLEAN,          -- tradeability predicate
    n_partition_events INTEGER,
    n_hierarchy_events INTEGER,
    volume_24h         DOUBLE,
    rank_position      INTEGER           -- 1..MAX_TRACKED_SERIES, best first
);

-- LLM extraction layer (Stage 3). Both tables below hold *non-regenerable*
-- LLM-derived artifacts: unlike snapshots/book_state they cannot be rebuilt from
-- raw_events (the output is costly + non-deterministic + partly human-curated),
-- so they get raw_events-grade durability — never casually wiped, included in
-- backup. See ARCHITECTURE.md §5, §9.

-- Per-market semantic representation (Stage A). One row per market, keyed on the
-- market_id. Market descriptions don't change after listing, so a row is cached
-- until extraction_version bumps (a prompt/schema change), not re-extracted each
-- cycle. entities/dependencies are JSON arrays of normalized strings.
CREATE TABLE IF NOT EXISTS market_semantics (
    market_id          VARCHAR PRIMARY KEY,
    platform           VARCHAR NOT NULL,
    underlying_event   VARCHAR,          -- what real-world event this market is about
    resolves_yes_when  VARCHAR,          -- condition that settles YES
    resolves_no_when   VARCHAR,          -- condition that settles NO
    resolution_timing  VARCHAR,          -- when / by what date it resolves
    entities           JSON,             -- normalized real-world entities it depends on
    dependencies       JSON,             -- real-world things its outcome hinges on
    model              VARCHAR,          -- model id that produced this
    extraction_version INTEGER,          -- bump invalidates the cache (re-extract)
    extracted_at       TIMESTAMP,
    raw_response       JSON              -- full model response, for audit
);

-- Pairwise typed logical edges between related markets (Stage B). One row per
-- unordered pair, canonicalized so market_id_a < market_id_b. trust_tier drives
-- how the (future) solver consumes the edge: `trusted` => hard constraint,
-- `soft` => soft constraint, `review` => manual-review queue (the queue is the
-- query `trust_tier='review' AND review_status='pending'` — no separate table).
-- A `trusted` edge requires agreement_status='agreed' (two independent
-- extractions concurred); see ARCHITECTURE.md §3 (Extraction loop) and §12.
CREATE TABLE IF NOT EXISTS market_edges (
    platform           VARCHAR NOT NULL,
    market_id_a        VARCHAR NOT NULL,   -- canonical: a < b
    market_id_b        VARCHAR NOT NULL,
    relationship_type  VARCHAR,            -- same_event|implies|mutually_exclusive|partition_member|conditional|correlated|unrelated
    direction          VARCHAR,            -- a_implies_b|b_implies_a|symmetric|none (for implies/conditional)
    confidence         DOUBLE,             -- model self-reported, [0,1]
    trust_tier         VARCHAR,            -- trusted|soft|review
    agreement_status   VARCHAR,            -- single|agreed|disagreed
    rationale          VARCHAR,            -- model's reasoning at classification time
    model              VARCHAR,            -- primary classification model
    verify_model       VARCHAR,            -- second model used for the trusted-promotion check (or null)
    extraction_version INTEGER,
    classified_at      TIMESTAMP,
    raw_response       JSON,
    review_status      VARCHAR DEFAULT 'pending',  -- pending|approved|rejected (only meaningful for trust_tier='review')
    reviewed_at        TIMESTAMP,
    reviewed_by        VARCHAR,
    PRIMARY KEY (platform, market_id_a, market_id_b)
);
CREATE INDEX IF NOT EXISTS idx_edges_review ON market_edges (trust_tier, review_status);
CREATE INDEX IF NOT EXISTS idx_edges_market_a ON market_edges (market_id_a);
CREATE INDEX IF NOT EXISTS idx_edges_market_b ON market_edges (market_id_b);

-- In-flight Anthropic Message Batches (Stage 3, async spend path). A batch is
-- submit-now / retrieve-later: cycle N submits, cycle N+k polls and reconciles.
-- The batch_id + the work it covers MUST be durable (not in-process memory) or a
-- restart orphans a submitted batch — Anthropic keeps results 29 days, so a
-- persisted id is recoverable. `payload` maps each request's custom_id to the
-- context needed to write its result: a market_id (purpose='semantics'), a
-- canonical [a, b] pair (purpose='pair_primary'), or {pair, primary:{...}} carrying
-- the primary classification forward to its verify (purpose='pair_verify'). A row
-- is deleted once reconciled (its results have been written), so this table holds
-- only genuinely-open batches. NOT regenerable from raw_events — durable like the
-- rest of the graph. See ARCHITECTURE.md §3 and docs/LLM-COST-MIGRATION.md.
CREATE TABLE IF NOT EXISTS llm_batches (
    batch_id           VARCHAR PRIMARY KEY,
    provider           VARCHAR NOT NULL,   -- 'anthropic'
    purpose            VARCHAR NOT NULL,   -- semantics | pair_primary | pair_verify
    model              VARCHAR,
    extraction_version INTEGER,
    request_count      INTEGER,
    submitted_at       TIMESTAMP,
    payload            JSON NOT NULL       -- custom_id -> result-handling context
);
