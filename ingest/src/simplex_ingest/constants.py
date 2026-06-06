"""Tuning knobs for the Simplex ingest subsystem.

Every operational parameter lives here as a documented constant. The ``.env``
file holds only secrets and deployment-specific values (credentials, Kalshi
environment, data directory). Nothing in this file reads an environment
variable: change behavior by editing a constant and redeploying.

Units are stated per constant. Prices are in *dollars* (Kalshi's fixed-point
"dollars" surface, e.g. 0.56 = 56 cents). Sizes are contract counts.
"""

from __future__ import annotations

# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------

PLATFORM = "kalshi"
"""Platform tag written to every row. Stage 1 is Kalshi-only; later exchanges
get their own tag so tables stay multi-venue from day one."""


# --------------------------------------------------------------------------
# Snapshot builder
# --------------------------------------------------------------------------

SNAPSHOT_INTERVAL_SECONDS = 10
"""Cadence of the 10s materialized grid. One row per active market per tick.
Lower = finer time resolution but more rows and more CPU per tick; the
downstream coherence solver was specced against a 10s grid, so changing this
ripples downstream."""

CHECKPOINT_INTERVAL_SECONDS = 60
"""How often the in-memory order books are serialized to ``book_state``.
Smaller = less replay work after a restart but more write churn. The builder
also checkpoints on clean shutdown regardless of this value, so this only
bounds the *crash* replay window."""

DEPTH_BAND_PRICE_UNITS = 0.03
"""Half-width of the depth band, in dollars. ``bid_depth_3c_usd`` /
``ask_depth_3c_usd`` sum (price x size) for resting orders within this many
dollars of the best price on each side. Default 0.03 = 3 cents. This is the
core anti-stale-quote signal: a thick book inside 3c looks very different from
a lone top-of-book order. The snapshot column names hard-code "3c" for
readability; if you change this value, the names stay but their meaning
shifts — note it in your analysis."""


# --------------------------------------------------------------------------
# Catalog poller
# --------------------------------------------------------------------------

CATALOG_REFRESH_SECONDS = 300
"""How often the catalog poller re-reads the `tracked_series` table (maintained
by the discovery loop), re-expands it against Kalshi REST, and reconciles the
active subscription set. 5 min balances freshness of newly-opened markets and
of discovery's latest tracked set against REST budget."""

CATALOG_MIN_MARKET_VOLUME = 0.0
"""Liquidity floor (contracts) for a market to enter the active set. 0.0 keeps
every open market in a tracked series. Raise to prune dead markets and shrink
the WS firehose. Applied in the catalog poller."""


# --------------------------------------------------------------------------
# WebSocket connection / reconnect
# --------------------------------------------------------------------------

WS_RECONNECT_MIN_SECONDS = 1.0
"""Initial backoff before the first WS reconnect attempt."""

WS_RECONNECT_MAX_SECONDS = 60.0
"""Cap on WS reconnect backoff. Retries are unbounded — we never give up — the
delay just stops growing here."""

WS_RECONNECT_BACKOFF_FACTOR = 2.0
"""Multiplier applied each failed WS reconnect, before jitter."""

WS_PING_INTERVAL_SECONDS = 10.0
"""websockets keepalive ping interval. Also bounds how quickly a silently dead
connection is detected."""

WS_PING_TIMEOUT_SECONDS = 10.0
"""How long to wait for a pong before considering the connection dead."""

WS_OPEN_TIMEOUT_SECONDS = 15.0
"""Timeout for establishing the WS handshake."""

WS_CHANNEL_ORDERBOOK = "orderbook_delta"
WS_CHANNEL_TRADE = "trade"
WS_CHANNEL_LIFECYCLE = "market_lifecycle_v2"
"""Kalshi WS channel names. orderbook_delta is subscribed one-market-per-sid so
each market gets an isolated, monotonic ``seq`` stream (clean per-market gap
detection + re-anchor). trade and lifecycle are bulk subscriptions."""


# --------------------------------------------------------------------------
# DuckDB write batching
# --------------------------------------------------------------------------

RAW_EVENT_BATCH_SIZE = 200
"""Flush the raw_events write buffer once this many events accumulate. Batching
keeps DuckDB single-row insert overhead down on the WS firehose."""

RAW_EVENT_FLUSH_SECONDS = 1.0
"""Flush the raw_events buffer at least this often even if it hasn't filled, so
the snapshot builder sees fresh events promptly and SIGTERM loses nothing."""

RESET_REQUEST_QUEUE_MAXSIZE = 10000
"""Bound on the book-reset request queue (snapshot/audit -> WS loop). Reset
requests are per-market and drained every RAW_EVENT_FLUSH_SECONDS, so this is
far above any real backlog; it exists only so a pathological producer can't grow
the queue without bound (the put_nowait sites log and drop on QueueFull)."""


# --------------------------------------------------------------------------
# Order book / canaries
# --------------------------------------------------------------------------

CANARY_PRICE_MIN_USD = 0.01
CANARY_PRICE_MAX_USD = 0.99
"""Valid resting price range (1c..99c). A level outside this range trips the
out-of-range-price canary and forces a book reset."""

STALE_MARKET_SECONDS = 3600
"""A subscribed, still-open market with no events for this long trips the
stale-market canary (informational only — no reset)."""


# --------------------------------------------------------------------------
# REST rate limiting
# --------------------------------------------------------------------------

REST_CALLS_PER_SECOND = 8.0
"""Token-bucket refill rate for general catalog/discovery REST calls. Kept under
the ~10 req/s Basic-tier read budget to leave headroom for the audit loop."""

REST_BURST = 8
"""Token-bucket capacity for general REST calls (max burst)."""

REST_MAX_RETRIES = 5
"""Retries on 429 / transient REST errors before surfacing the error to the
caller. Backoff between retries reuses the WS backoff bounds."""


# --------------------------------------------------------------------------
# Hourly book audit
# --------------------------------------------------------------------------

AUDIT_TICK_SECONDS = 3600
"""How often the audit loop wakes. A pass runs only if the current UTC hour is
inside the audit window below. Default hourly."""

AUDIT_WINDOW_START_UTC_HOUR = 0
AUDIT_WINDOW_END_UTC_HOUR = 24
"""Audit runs only when AUDIT_WINDOW_START <= UTC hour < AUDIT_WINDOW_END.
Default 0..24 = always. Narrow this to a quiet band (e.g. 8..9) to confine the
REST-heavy reconciliation to low-traffic hours."""

AUDIT_REST_CALLS_PER_SECOND = 4.0
"""Token-bucket refill rate for the audit loop's orderbook fetches, separate
from the general REST budget so an audit pass can't starve the catalog poller."""

AUDIT_REST_BURST = 4
"""Token-bucket capacity for audit REST calls."""

AUDIT_ORDERBOOK_DEPTH = 100
"""``depth`` param on GET /markets/{ticker}/orderbook during audits. Deep enough
to compare full near-touch structure, not just top-of-book."""

AUDIT_STRUCTURAL_DIFF_MAX_LEVELS = 4
"""Audit classification is primarily *structural*: a price level present in one
book but absent in the other (a level appeared or disappeared). The in-memory
book is frozen ~100ms before the REST fetch, so small per-level *size* drift on
levels present in both books is expected market movement, not corruption. A pass
with at most this many structural mismatches across both sides AND no gross size
drift (see ``AUDIT_SMALL_DIFF_MAX_SIZE_PCT``) is 'small' (info log, no action);
otherwise the books genuinely disagree -> 'large': warn + forced book reset.
Normal near-touch churn moves only a level or two in the freeze->REST window; a
desynced book mismatches many."""

AUDIT_SMALL_DIFF_MAX_SIZE_PCT = 50.0
"""Backstop size-drift ceiling. Even with few/no structural mismatches, a shared
level whose size differs by more than this percentage (of the larger size)
escalates the pass to 'large' -> book reset. This is the one independent check
against a *magnitude* bug (e.g. a fixed-point/parse error that decodes the right
levels at the wrong sizes), which the structural test alone cannot see. Set high
deliberately: legitimate ~100ms freeze->REST drift on a level is small, while a
decode/scale bug is off by integer factors (often >100%), so this catches gross
corruption without re-triggering resets on ordinary market movement."""


# --------------------------------------------------------------------------
# Discovery loop (predicate-based, self-managing tracked set)
# --------------------------------------------------------------------------

DISCOVERY_INTERVAL_SECONDS = 3600
"""How often the discovery loop sweeps all open Kalshi series, re-evaluates the
predicates, and rewrites the `tracked_series` table. The catalog poller picks up
the new set within one CATALOG_REFRESH_SECONDS afterward. 1 h balances catalog
churn against the full-events REST sweep cost."""

MAX_TRACKED_SERIES = 30
"""Hard cap on the tracked set. Admitted series beyond this are evicted worst-
first by `rank_key`. Bounds the WS subscription firehose."""

PREDICATE_PARTITION_MIN_MARKETS = 3
"""P1: a mutually-exclusive event needs at least this many tradeable markets to
count as a partition. Below this it's too thin to be a useful coherence
constraint."""

PREDICATE_HIERARCHY_MIN_MARKETS = 2
"""P2: an event needs at least this many *distinct* markets (distinct subtitle /
strike) to count as a hierarchy."""

PREDICATE_MIN_VOLUME_24H = 1000.0
"""P3: a series' summed tradeable-market volume must reach this floor (contracts)
to be admitted — internal structure is worthless if nobody trades it."""

DISCOVERY_STARTUP_GRACE_SECONDS = 90.0
"""On a cold boot, how long to wait before warning that `tracked_series` is still
empty. Discovery populates it eagerly within seconds of start, so an empty table
past this grace means discovery is failing (REST down, bad creds) and the WS set
will stay idle. Informational — the process stays up either way."""

DATA_RETENTION_CYCLES = 2
"""How many *completed* discovery cycles of time-series data to retain beyond the
one a row belongs to. Discovery owns the hourly market set; a row written while a
given set was active is kept until DATA_RETENTION_CYCLES whole cycles have
completed after that set's hour ends, then pruned. The prune runs at the end of
every discovery cycle, so retention is inherently in sync with the hourly
recompute. Applies ONLY to the regenerable/append time-series tables
(`raw_events`, `snapshots`, `audit_results`, `book_state`) — NOT to the durable
LLM-derived `market_semantics`/`market_edges` graph (see ARCHITECTURE §5/§9)."""

DATA_RETENTION_SECONDS = (DATA_RETENTION_CYCLES + 1) * DISCOVERY_INTERVAL_SECONDS
"""Concrete cutoff age derived from the cycle count. The ``+1`` covers the row's
own (up to one-cycle-long) active hour, so a row is pruned once its hour has
completed *and* DATA_RETENTION_CYCLES further cycles have elapsed. With the
defaults (2 cycles, 1 h interval) the oldest retained data is ≈ 3 h; pruning each
cycle deletes everything with a timestamp older than ``now - this``."""

GRAPH_PRUNE_AFTER_RESOLVED_SECONDS = 3600
"""How long after a market *resolves* its LLM graph (`market_semantics` +
`market_edges`) is deleted. A resolved market is terminal — it never reopens, so
its semantics/edges are dead weight for the live coherence engine, and (unlike a
market temporarily dropped from the tracked set) there is no re-admission and so
no re-spend risk in deleting them. Resolution time is `markets.resolved_at`,
sourced authoritatively from Kalshi `settlement_ts` (see the discovery loop's
resolution reconciliation). 1 h = the data lingers one discovery cycle past
resolution, then goes."""

GRAPH_RESOLUTION_CHECK_BATCH = 200
"""Max markets to reconcile against Kalshi REST per discovery cycle. The
candidates are graph markets that left the active set with no known resolution
time; each costs one `GET /markets/{ticker}`, so this caps the REST spend while a
backlog drains across cycles."""


# --------------------------------------------------------------------------
# LLM extraction layer (Stage 3) — semantics + relationship edges
# --------------------------------------------------------------------------
# Behavior is tuned here; the ONLY env value the layer adds is the secret
# OPENROUTER_API_KEY (see config.py). Model ids are constants so they swap
# without touching the env surface. The layer soft-fails (idles) when the key is
# absent, so plain ingest still runs without it.

EXTRACTION_INTERVAL_SECONDS = 300
"""How often the extraction loop wakes to (a) extract semantics for newly-active
markets lacking them and (b) classify any new candidate pairs. 5 min keeps the
graph close behind the catalog poller (also 5 min) without burning model spend on
an empty work queue — both phases are no-ops when there's nothing new."""

EXTRACTION_BATCH_SIZE = 50
"""Max markets extracted AND max pairs classified per cycle. Bounds per-cycle
model spend and keeps a cycle short enough to heartbeat comfortably; the backlog
drains over subsequent cycles. Work is idempotent, so a partial cycle just
resumes next tick."""

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
"""OpenRouter's OpenAI-compatible API root. Not a secret (the API *key* is, and
lives in the env per config.py)."""

EXTRACTION_MODEL = "anthropic/claude-sonnet-4.6"
"""Stage A (per-market semantics): high-volume, runs once per market then caches,
so a fast/cheap reasoning tier fits. Confirm the exact slug against OpenRouter's
model catalog at deploy time."""

PAIR_MODEL = "anthropic/claude-opus-4.8"
"""Stage B primary (pair relationship): harder reasoning, lower volume (only
candidate pairs), so the strongest tier earns its cost here."""

PAIR_VERIFY_MODEL = "anthropic/claude-sonnet-4.6"
"""Independent second opinion used ONLY to promote a high-confidence edge to the
`trusted` (hard-constraint) tier. Deliberately a *different* model from
PAIR_MODEL so agreement is genuine independence, not one model agreeing with
itself. Disagreement demotes the edge to the manual-review queue."""

EXTRACTION_PROMPT_VERSION = 1
"""Cache key for both tables. Bump when the prompts/output schema change so the
loop re-extracts semantics and re-classifies pairs (rows at an older version are
treated as missing). Otherwise market_semantics is cached forever (descriptions
don't change post-listing)."""

LLM_TEMPERATURE = 0.0
"""Sampling temperature for all extraction calls. 0.0 for stable, repeatable
structured output; verification independence comes from a different *model*, not
from temperature."""

LLM_CALLS_PER_SECOND = 2.0
"""Token-bucket refill rate for OpenRouter calls. Conservative — extraction is a
background backlog drain, not latency-critical, and model spend is the real
budget. Raise to drain a large first-boot backlog faster."""

LLM_BURST = 4
"""Token-bucket capacity (max burst) for OpenRouter calls."""

LLM_MAX_RETRIES = 4
"""Retries on 429 / transient 5xx / transport errors before the client raises
LLMError (which the loop logs-and-skips for that one item)."""

LLM_REQUEST_TIMEOUT_SECONDS = 60.0
"""Per-request timeout for a chat completion. Reasoning models on hard pairs can
be slow; generous so a slow-but-valid response isn't dropped."""

PAIR_ENTITY_OVERLAP_MIN = 2
"""Minimum shared normalized entities for two markets in different events/series
to become a candidate pair. 2 avoids pairing on a single common entity (e.g. a
year or a country) while still catching genuinely-related cross-series markets."""

EDGE_TRUSTED_CONFIDENCE = 0.85
"""At/above this primary-model confidence, an edge is a *candidate* for the
`trusted` tier — promoted only if PAIR_VERIFY_MODEL independently agrees on the
relationship type. `trusted` edges become the solver's hard constraints, so a
wrong one is the highest-blast-radius failure; the agreement gate guards exactly
this band."""

EDGE_SOFT_CONFIDENCE = 0.6
"""At/above this (but below trusted) an edge enters the graph as `soft` (a soft
solver constraint). Below it, the model is too unsure to trust automatically →
the `review` tier (manual-review queue). Disagreement on a trusted candidate also
lands in `review`."""


# --------------------------------------------------------------------------
# Supervisor / health
# --------------------------------------------------------------------------

SUPERVISOR_RESTART_MIN_SECONDS = 1.0
SUPERVISOR_RESTART_MAX_SECONDS = 30.0
SUPERVISOR_RESTART_BACKOFF_FACTOR = 2.0
"""Backoff bounds for restarting a crashed loop. A loop that crashes repeatedly
backs off up to the max; a loop that runs cleanly for a while resets its
backoff."""

HEALTH_PORT = 8080
"""Default port for the /health endpoint. 200 if all six loops are alive, 503
otherwise. Overridden at runtime by the ``$PORT`` env var if the host platform
injects one (Railway, Heroku, etc) — see :func:`health.start_health_server`."""

HEALTH_HEARTBEAT_TIMEOUT_SECONDS = 90.0
"""A loop is 'alive' for /health if it has emitted a heartbeat within this
window. Must comfortably exceed the slowest loop's idle period (the catalog
poller at CATALOG_REFRESH_SECONDS heartbeats around its sleep, so this is keyed
off the faster loops)."""


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------

LOG_LEVEL = "INFO"
"""Root log level. DEBUG is very chatty (per-event, per-delta). Structured
JSON lines go to stdout; the platform collects them."""
