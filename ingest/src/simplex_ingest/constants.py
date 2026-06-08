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

SNAPSHOT_INTERVAL_SECONDS = 60
"""Cadence of the materialized grid. One row per active market per tick.
Lower = finer time resolution but more rows and more CPU per tick; the
downstream coherence solver was specced against a 10s grid, so changing this
ripples downstream.

**Scoped to 60s (2026-06-07):** widened from 10s to 1 minute for the initial
World-Cup-only smoke test (see DISCOVERY_SERIES_ALLOWLIST_PREFIXES). At a small,
coherent market set the goal is to validate the pipeline end-to-end with far less
per-tick write/firehose pressure — a 6× slower grid cuts snapshot-write and
reset-churn load while the product is being shaken out. Restore 10s (and re-tune
the solver's grid assumption) when scaling the market set back up."""

CHECKPOINT_INTERVAL_SECONDS = 60
"""How often the in-memory order books are serialized to ``book_state``.
Smaller = less replay work after a restart but more write churn. The builder
also checkpoints on clean shutdown regardless of this value, so this only
bounds the *crash* replay window."""

SNAPSHOT_APPLIER_POLL_SECONDS = 0.5
"""How long the snapshot *applier* idles when it has drained all pending
``raw_events`` and is caught up. The applier runs continuously (separate from
the 10s grid *sampler*), draining new events in small yielding chunks; when
there's nothing new it polls at this cadence. Kept well under
``RAW_EVENT_FLUSH_SECONDS`` (1s) so freshly-flushed events are applied promptly,
keeping the books near-current for the sampler."""

SNAPSHOT_APPLY_YIELD_EVERY = 500
"""Cooperative-yield granularity inside the applier's event-apply loop: after
this many events applied, ``await asyncio.sleep(0)`` to let the event loop
service the WebSocket keepalive (ping/pong) and other loops. This is what makes
a missed-ping disconnect structurally impossible on one core regardless of the
event backlog — the apply burst can never monopolize the loop past the ping
deadline."""

SNAPSHOT_ROW_YIELD_EVERY = 500
"""Cooperative-yield granularity inside the sampler's row-build loop: yield
after this many markets, **between markets, never mid-market**, so every emitted
snapshot row stays internally consistent while the build still can't monopolize
the event loop at a large active-market count."""

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

CATALOG_MIN_MARKET_VOLUME = 100.0
"""Liquidity floor (contracts) for a market to enter the active set. Applied in
the catalog poller; markets below it are not subscribed.

Set from the live volume distribution (`catalog volume distribution`, logged each
refresh): at floor=0 the catalog carried ~5.5k markets, of which ~2.6k had <100
lifetime contracts and ~1.8k had <1 — near-dead markets that bloat the WS
firehose and the per-tick snapshot grid while carrying no coherence signal. A
floor of 100 drops that tail (~47%), keeping anything with real activity. Raise
further to shed more load at the cost of thin partition members; lower it to keep
more of the long tail. Pair with ``MAX_ACTIVE_MARKETS`` (the hard ceiling)."""

MAX_ACTIVE_MARKETS = 6000
"""Hard ceiling on the active-market count *after* series→market fan-out, applied
in the catalog poller. ``MAX_TRACKED_SERIES`` bounds the *series* count, but a
high-cardinality series (an election primary fans out to hundreds of candidate
sub-markets) breaks the implicit "a series is a handful of markets" assumption
and drives the WS firehose load. This bounds *markets* directly: when fan-out
exceeds the ceiling, markets are kept greedily by volume (highest first) and the
remainder dropped — capping the firehose by the value the coherence engine cares
about rather than by arbitrary truncation.

Set as a backstop above the current live count (~5.4k) so it does not bite today;
tighten it (with ``CATALOG_MIN_MARKET_VOLUME``) once the logged volume
distribution shows where the signal-bearing markets end. Cap *markets*, not
series."""


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

WS_RECONNECT_RESET_SECONDS = 30.0
"""A connection that stayed up at least this long counts as a sustained success,
so its shard's reconnect backoff resets to the floor. Without this a shard that
flapped hard once (backoff climbed to the 60 s cap) would keep waiting ~60 s
between attempts forever, even after the underlying cause cleared."""

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
# WebSocket sharding
# --------------------------------------------------------------------------
# A single connection carrying ~2.9 k one-market-per-sid orderbook subscriptions
# proved unstable in production: every connection died ~5–8 s after the initial
# subscribe burst with a codeless ``ConnectionClosedError`` (not keepalive — the
# loop was no longer saturated after the snapshot-write fix), then reconnected and
# re-anchored *every* book. The cure is to (a) spread the subscriptions across a
# few connections so each one's connect burst + blast-radius is smaller, and
# (b) pace each connection's subscribe burst instead of firing it all at once.
# See docs/INGEST-STABILIZATION-LOG.md §5 and docs/ARCHITECTURE.md.

WS_SHARD_COUNT = 4
"""Number of concurrent WS connections (shards). Markets are partitioned across
shards **by series** (a series's markets stay co-located). Kalshi caps a user at
**5 concurrent WS connections**, so this must stay ≤ 4 to leave headroom for the
brief old+new overlap while one shard reconnects (4 steady + 1 transient = 5).
All shards feed the single in-process ``raw_events`` writer — the DuckDB
single-writer invariant is preserved (one process, one writer)."""

WS_SUBSCRIBE_CHUNK = 50
"""Orderbook subscribes are sent in chunks of this size with a pause between
chunks (see WS_SUBSCRIBE_PACING_SECONDS) rather than all at once, so neither the
outbound subscribe burst nor the inbound snapshot flood it triggers overwhelms
the connection at anchor time — the empirical cause of the connect-time death."""

WS_SUBSCRIBE_PACING_SECONDS = 0.05
"""Pause between subscribe chunks. At ~725 markets/shard this adds <1 s to a
(re)connect — negligible — while smoothing the snapshot burst over ~0.7 s."""

WS_COORD_TICK_SECONDS = 0.5
"""How often the shard coordinator wakes to dispatch book-reset requests to the
owning shard, react to the catalog resubscribe signal (re-partition), beat the
health heartbeat, and roll up metrics. Early-wakes on the resubscribe signal."""

WS_IDLE_POLL_SECONDS = 5.0
"""How often a shard with no assigned markets re-checks for an assignment (it
holds no connection while empty, so it can't be event-driven by inbound frames).
Also bounds how fast it picks up its first markets after a cold start."""


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
first by `rank_key`. Bounds the WS subscription firehose. Only applies in the
default predicate+rank path — when DISCOVERY_SERIES_ALLOWLIST_PREFIXES is set the
allowlist replaces predicates/rank/cap entirely."""

DISCOVERY_SERIES_ALLOWLIST_PREFIXES: tuple[str, ...] = ("KXWC",)
"""**Initial scope-down (2026-06-07): World Cup 2026 only.** When non-empty,
discovery admits *exactly* the open series whose ticker starts with one of these
prefixes and **bypasses the predicates, the `rank_key` ranking, and the
MAX_TRACKED_SERIES cap** — the whole "rank all series and keep the top N" path is
skipped. `KXWC` covers the FIFA World Cup 2026 series (`KXWCGAME`,
`KXWCGROUPWIN`, `KXWCGROUPORDER`, `KXWC1H`, …).

Rationale: at full catalog scale (~2.9k markets across 30 ranked series) the WS
firehose produced a steady reset/seq-gap churn that obscured whether the rest of
the pipeline (snapshot grid → extraction graph) is correct. Pinning to one small,
internally-rich domain (a World Cup has partitions, group orders, conditionals —
exactly the structure the solver wants) lets the product be validated end-to-end
before the firehose/sharding issues are solved.

Set to an empty tuple `()` to restore the predicate-driven admit/rank/cap path.
The market-level bounds (`CATALOG_MIN_MARKET_VOLUME`, `MAX_ACTIVE_MARKETS`) still
apply on top, as a backstop against a combinatorial series (e.g. group-order
permutations)."""

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

EXTRACTION_MAX_PAIRS_PER_DAY = 2000
"""Hard ceiling on **new** Stage-B pair classifications dispatched per UTC day
(sync classified + batch submitted), counting work already done or in flight that
day. The time-to-resolution gate (`route_pair`) cannot bound volume for a
long-lived event set — a multi-week tournament puts every pair >48h out, so it all
routes to batch and nothing is skipped (see docs/LLM-CALL-REDUCTION.md §1.2/§5.1).
This budget is the bound the gate can't provide: it converts an unbounded backfill
burst (≈121k pairs at full World-Cup scale) into a predictable daily spend. Pairs
are dispatched **value-ordered** (longest remaining life first — best amortization)
so the budget buys the highest-signal edges first; the remainder is deferred to
later days and logged (never silently dropped). Set to 0 to pause all new pair
spend; raise once the candidate set is shrunk (tighter predicates / embeddings)."""

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
"""OpenRouter's OpenAI-compatible API root. Not a secret (the API *key* is, and
lives in the env per config.py)."""

EXTRACTION_MODEL = "deepseek/deepseek-v4-flash-20260423"
"""Stage A (per-market semantics): high-volume, runs once per market then caches.
**Temporary cost measure (2026-06-07):** pointed at the cheapest reliable
structured-output model on OpenRouter (~$0.10/M in) instead of Sonnet, to hold
spend near zero while the ingest is being stabilized and the discounted Anthropic
batch path is unavailable (no account credits). Restore a stronger tier
(`anthropic/claude-sonnet-4.6`) once extraction quality matters again. Confirm the
exact slug against OpenRouter's model catalog at deploy time. (OpenRouter dotted id.)"""

PAIR_MODEL = "deepseek/deepseek-v4-flash-20260423"
"""Stage B primary (pair relationship). **Temporary cost measure (2026-06-07):**
same cheapest-reliable model as EXTRACTION_MODEL (was `anthropic/claude-sonnet-4.6`).
This is the cost *rate* lever — it lands before any batch routing. Restore Sonnet
when extraction quality matters again. (OpenRouter dotted id.)"""

PAIR_VERIFY_MODEL = "google/gemini-3.1-flash-lite-20260507"
"""Independent second opinion used ONLY to promote a high-confidence edge to the
`trusted` (hard-constraint) tier. Must be a *different* model from PAIR_MODEL so
agreement is genuine independence, not one model agreeing with itself (same-model
verification would flood the trusted tier). **Temporary cost measure
(2026-06-07):** a cheap *different-family* model (Gemini Flash-Lite) rather than
Opus — it still cross-checks the low-volume ≥EDGE_TRUSTED_CONFIDENCE band from an
independent model, just far cheaper. Restore `anthropic/claude-opus-4.8` when the
trusted tier needs the strongest guard. (OpenRouter dotted id.)"""

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

PAIR_ENTITY_OVERLAP_MIN = 3
"""Minimum shared normalized entities for two markets to become a candidate pair
on the entity-overlap basis. **Raised 2 → 3 (2026-06-08)** per
docs/LLM-CALL-REDUCTION.md §4.4: at the World-Cup scale the ≥2 rule created
near-cliques among every market naming the same two teams (average candidate degree
≈186 — mostly noise), so the bulk of pairs came back `unrelated`. Requiring 3 shared
entities keeps genuinely-related markets (same match + same prop family) while
cutting the noisy tail. This is a stopgap; the principled replacement is semantic-
similarity candidate generation (embeddings, §4.2), at which point this predicate is
retired. Same-event pairing is unaffected (always on); same-series-only pairing is
gated by ``PAIR_ON_SAME_SERIES``."""

PAIR_ON_SAME_SERIES = False
"""Whether two markets in the same series (but different event) are a candidate
**purely** on that basis. **Disabled (2026-06-08)** per docs/LLM-CALL-REDUCTION.md
§4.4: same-series-only pairing was a large, low-value bucket (a series fans out
wide), and same-series pairs that are genuinely related still surface via shared
entities (``PAIR_ENTITY_OVERLAP_MIN``) or a shared event. Same-*event* pairing
(Kalshi already grouped those) is always on, independent of this flag. Set True to
restore the old behavior."""

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
# Time-to-resolution spend gate (Stage B) — see docs/LLM-COST-MIGRATION.md §4
# --------------------------------------------------------------------------
# An edge's useful life is min(life of A, life of B): a relationship between two
# live prices dies when either endpoint settles, and the graph is pruned 1h after
# resolution (GRAPH_PRUNE_AFTER_RESOLVED_SECONDS). So LLM spend on a pair is an
# investment amortized over the pair's *remaining* live life. The gate applies
# that amortization at spend time:
#   remaining_life < FLOOR            → don't spend at all (negative-ROI edge)
#   FLOOR ≤ remaining_life < BATCH    → sync (pay full price to get it in time)
#   remaining_life ≥ BATCH            → batch (latency irrelevant; take the 50% off)
# remaining_life is computed from markets.closes_at (carried into the candidate
# path). When neither endpoint's close time is known the pair is routed sync
# (conservative: never silently skip a possibly-useful edge on missing data).

EDGE_REMAINING_LIFE_FLOOR_SECONDS = 86400
"""24 h. Below this remaining life (nearer of the two endpoints) a candidate pair
is not classified at all — the edge can't outlive its payback window, so it's
negative-ROI regardless of how cheaply the call is bought. This floor is the part
of the gate that bites even with no batch provider configured."""

EDGE_BATCH_THRESHOLD_SECONDS = 172800
"""48 h. At/above this remaining life a candidate pair is routed to the Anthropic
batch path (flat 50% discount, ~1h–24h turnaround). Set comfortably above the
worst-case batch turnaround so a batched edge still lands with ample life left.
With no ANTHROPIC_API_KEY configured the batch route degrades to sync (the edge is
still produced, just at full price)."""


# --------------------------------------------------------------------------
# Anthropic Message Batches — the asynchronous, discounted spend path (Stage 3)
# --------------------------------------------------------------------------
# Non-latency-sensitive extraction work (Stage A bulk backfill, long-horizon
# Stage B pairs) is routed to the Anthropic Message Batches API: a flat 50% off
# list price, results within ~1h–24h, retrievable for 29 days. This is a SECOND
# provider seam distinct from OpenRouter — different host, different auth, and
# crucially a DIFFERENT model-id spelling (Anthropic uses dashed ids; OpenRouter
# uses dotted ids with an `anthropic/` prefix). Never cross the two. The seam is
# optional: built only when ANTHROPIC_API_KEY is set (config.py); absent it, every
# batch-routed item degrades to the synchronous OpenRouter path.

ANTHROPIC_BASE_URL = "https://api.anthropic.com/v1"
"""Anthropic-direct API root for the Message Batches endpoints. Not a secret (the
API *key* is, and lives in the env per config.py)."""

ANTHROPIC_VERSION = "2023-06-01"
"""Value of the mandatory ``anthropic-version`` header on every Anthropic-direct
request."""

BATCH_MAX_TOKENS = 1500
"""``max_tokens`` for batched Messages requests. This is a *mandatory* field on
the Anthropic Messages/Batches API (the request is rejected without it) — not the
cost-saving output cap discussed in the migration doc (which was deliberately not
applied to the synchronous OpenRouter path). 1500 is generous headroom for the
few-hundred-token semantics / classification JSON."""

# Anthropic-direct (dashed) model ids for the batch path — its OWN cost tiering,
# NOT a mirror of the OpenRouter (dotted) sync ids above. The batch path spends a
# cheap Haiku workhorse on the high-volume bulk (Stage A semantics + Stage B
# primary) and reserves a stronger Sonnet for the low-volume Stage B verify (trust)
# gate. (Cost re-tier 2026-06-07 — see docs/LLM-COST-MIGRATION.md §1.)
BATCH_EXTRACTION_MODEL = "claude-haiku-4-5"
"""Stage A (semantics) on the batch path. **Cost choice (2026-06-07):** Haiku
rather than Sonnet — semantics extraction is structured, low-difficulty work well
within Haiku's class, and at $1/$5 per M Haiku is the cheapest Anthropic tier. This
is the highest-volume Anthropic call (one per market, plus a full re-extract on any
EXTRACTION_PROMPT_VERSION bump), so it's the biggest single spend lever. Restore
`claude-sonnet-4-6` if semantics quality regresses."""

BATCH_PAIR_MODEL = "claude-haiku-4-5"
"""Stage B primary (pair relationship) on the batch path. **Cost choice
(2026-06-07):** Haiku rather than Sonnet — pairwise classification is structured
work, and this is the other high-volume call (≈one per candidate pair, which grows
~quadratically with markets). Sub-trusted edges rest on this call alone; trusted
candidates (≥ EDGE_TRUSTED_CONFIDENCE) are re-checked by the stronger
BATCH_PAIR_VERIFY_MODEL below. Restore `claude-sonnet-4-6` if primary quality
regresses."""

BATCH_PAIR_VERIFY_MODEL = "claude-sonnet-4-6"
"""Stage B verify (trust gate) on the batch path. **Cost choice (2026-06-07):**
Sonnet — the stronger guard, spent deliberately on the band that matters most.
Verify fires only on high-confidence candidates (≥ EDGE_TRUSTED_CONFIDENCE), which
promote to the solver's *hard constraints*, so a wrong one is the highest-blast-
radius failure; that band is low-volume, so a pricier model here barely moves total
spend. It also satisfies the trust-gate invariant: Sonnet is a *different* model
from BATCH_PAIR_MODEL (Haiku), so a promoted `trusted` edge reflects two independent
models agreeing, not one model agreeing with itself. Drop to `claude-haiku-4-5` only
if verify volume itself becomes a cost problem."""

BATCH_BULK_SEMANTICS_THRESHOLD = 100
"""Stage A backlog size (markets missing current-version semantics) at/above which
a cycle routes the whole backlog to the batch path instead of draining it
synchronously. Steady-state trickle (a few newly-listed markets) stays sync —
it's cheap, low-volume, and gates that market's Stage B; only a bulk event (first
boot, an EXTRACTION_PROMPT_VERSION bump) is worth the submit/retrieve latency."""

BATCH_SUBMIT_MAX_REQUESTS = 1000
"""Upper bound on requests packed into a single batch submission per cycle. Far
below Anthropic's per-batch ceiling (100k requests / 256MB); it just keeps any one
submission bounded so a huge backlog drains over a few cycles rather than one
giant batch."""

BATCH_MAX_AGE_SECONDS = 172800
"""48 h. Orphan backstop: an open `llm_batches` row older than this is dropped at
reconcile and its items re-spend next cycle. Anthropic batches finish (or expire)
within ~24 h, so a row still un-reconciled past this is wedged — a purged/404'd
id, a permanently-stuck batch, or unfetchable results. Without this, a terminal
poll/results error would orphan the row forever (its covered markets/pairs are
excluded from re-selection while it's open), silently dropping them from the
graph. At-least-once re-spend is the right trade vs. permanent loss."""


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

METRICS_LOG_INTERVAL_SECONDS = 60.0
"""How often the hot-path loops emit a rolled-up metrics line (``snapshot
metrics`` / ``ws metrics``): applied-events/sec, parsed-frames/sec, mean/last
sample-build duration, reconnect + resync counts. Tick/sample duration is the
single most useful signal — it shows the worker getting slow *before* a
connection dies. Rolled up at this cadence rather than per-tick to keep the log
greppable."""
