# Simplex — Architecture

> **Maintenance:** this file is the source of truth for *how the system is
> designed*. Any change that alters the process model, the loops, the data model,
> the data flow, or a cross-cutting invariant **must** update this file in the
> same change. Deployment/runtime-ops details live in [`DEPLOY.md`](./DEPLOY.md);
> the rule is restated in the repo-root [`CLAUDE.md`](../CLAUDE.md).

---

## 1. What Simplex is

Simplex is a **real-time probabilistic coherence engine for prediction markets**
(Kalshi today). Prediction markets price many overlapping questions
independently; their prices imply constraints on a shared world — a partition's
markets should sum to 1, a conditional should respect its parent, mutually
exclusive outcomes can't both be likely — but nothing forces the order books to
honor them. Simplex ingests the live books, will compute the **maximum-entropy
joint distribution** consistent with the quoted marginals, and rank where the
quotes diverge most from that maximally-noncommittal joint. Those divergences are
the incoherences: mispriced relationships, stale legs, latent arbitrage.

It is built in stages. **Stages 1–3 (ingest, deploy, the LLM extraction layer)
exist today.**

| Stage | Scope | State |
|------|-------|-------|
| 1. Ingest | Live Kalshi books → normalized event log → 10s snapshot grid, with self-managing catalog discovery, book reconstruction, checkpointing, and an hourly REST reconciliation audit. | **Built** |
| 2. Deploy | Containerized, 24/7 on a mounted volume (Railway). | **Built** — see [`DEPLOY.md`](./DEPLOY.md) |
| 3. Extraction | LLM layer over the catalog: per-market semantic representations + pairwise **typed relationship edges** (the market graph), trust-tiered (hard / soft / manual-review) for the solver. | **Built** — see §3 (Extraction), §5 |
| 4. Solver | Max-entropy joint over related markets given snapshot marginals + the Stage-3 **edge constraints**; coherence/deviation scoring. | Planned |
| 5. Graph viz | Surface the largest incoherences over the edge graph and over time. | Planned |
| 6. Export | Continuous R2 export of `snapshots` / `raw_events` (and the durable extraction tables). | Planned |
| — | LLM-in-the-loop autonomous trading + trade datastore. | Planned — see §11 |
| — | Other venues (Polymarket, Manifold) via the `BaseSubscriber` seam. | Planned |

**Why extraction precedes the solver.** The solver computes the joint *given the
relationships between markets* — those typed edges are its structural input, so
the edge graph is a prerequisite, not a follow-on. Stage 3 builds it.

What remains **unbuilt** is the solver itself and everything downstream of it.
Stage 1 produces the regular, per-market gap-checked marginal time series the
solver will consume — a 10 s grid, contiguous while the process runs but with
possible **gaps across restarts** (the grid is forward-only; see §3), all fully
regenerable from `raw_events`. Stage 3 produces the *structural* half — the
relationship edges the solver combines with those marginals. Unlike the grid, the
extraction outputs are **not** regenerable from `raw_events` (see §5, §9).

---

## 2. Process model

One long-running Python process. Inside it, **six supervised async loops** share
a single embedded **DuckDB** file on a mounted volume. Everything runs on one
asyncio event loop; the only work dispatched to threads is DuckDB I/O (via
`asyncio.to_thread`), and every DB statement is serialized behind a single write
lock — because **DuckDB is single-writer** (see §9). The sixth loop (extraction)
is in-process for exactly this reason: a separate process **cannot open the live
DuckDB at all** while ingest holds the lock — the same constraint that keeps the
future solver in-process (see §11).

```
      Kalshi REST              Kalshi REST                Kalshi WebSocket
          │                        │                             │
  ┌───────▼────────────┐  ┌────────▼──────────────┐  ┌───────────▼────────────┐
  │ discovery (hourly) │  │ catalog poller (5 min)│  │ websocket subscriber   │
  │ predicates → admit │  │ tracked_series → open │  │ orderbook (1 sub/mkt)  │
  │ / rank / cap →     │─▶│ markets → active set  │─▶│ + trade + lifecycle    │
  │ tracked_series     │  │ → markets table       │  │ → raw_events (append)  │
  └────────────────────┘  └───────────────────────┘  └───────────┬────────────┘
                                                                  │
        ┌──────────────────────────┐       ┌───────────────▼────────────┐
        │  book audit (hourly)      │       │  snapshot builder (10 s)   │
        │  in-memory book vs REST   │◀─────▶│  replays raw_events,        │
        │  → audit_results          │ books │  maintains order books,    │
        │  large diff → book reset  │       │  emits snapshots (LOCF),   │
        └──────────────────────────┘       │  checkpoints book_state     │
                                            └────────────────────────────┘

   OpenRouter (sync) / Anthropic Batches (async)   markets catalog
        │                                                │
  ┌─────▼────────────────────────────────────────────── ▼──────────┐
  │ extraction (5 min)                                              │
  │ reconcile: poll open llm_batches → write results               │
  │ A: market → market_semantics (cached; bulk → batch)            │
  │ B: candidate pairs → time gate (skip/sync/batch) → classify    │
  │    → trust tier → market_edges (trusted/soft/review)           │
  └────────────────────────────────────────────────────────────────┘
```

A `supervisor` runs each loop's `run()` forever; a crash is logged and the loop
restarted with jittered backoff (reset after a sustained healthy run) without
taking the others down. SIGTERM/SIGINT triggers a clean shutdown: cancel loops →
flush buffered `raw_events` → checkpoint books → close DB + REST/LLM clients →
exit 0.

`/health` (port `$PORT`, default 8080) returns 200 **only** when all six loops
have emitted a heartbeat within `HEALTH_HEARTBEAT_TIMEOUT_SECONDS` (90 s). The
extraction loop heartbeats even when idle (no `OPENROUTER_API_KEY`), so a
key-less deploy stays healthy.

Code map: entry `app.py` (`run()` wires runtime, loops, signals, health,
shutdown) / `__main__.py`. Shared state in `runtime.py` (`BookStore` of
per-market `BookReconstructor`s, `Heartbeats`, the `request_reset` seam, the
`Loop` Protocol). Per-market reconstruction in `reconstruct.py`. Loop supervisor
in `supervisor.py`. Loops in `loops/`; their shared idle-with-heartbeat sleep is
`util.idle_sleep`.

---

## 3. The six loops

### Discovery (`loops/discovery.py`) — the self-managing tracked set
- **Cadence:** eager run on boot, then every `DISCOVERY_INTERVAL_SECONDS` (1 h).
- Sweeps **all** open Kalshi events once (`rest.get_events(status="open",
  with_nested_markets=True)`), aggregates by series, applies the **predicates**
  (§4), ranks the admitted set, caps it at `MAX_TRACKED_SERIES` (30), and
  rewrites the `tracked_series` table **atomically**.
- **Never wipes the working set on a blip:** a transient empty sweep or a REST
  error leaves the prior `tracked_series` intact. `admitted_at` is preserved
  across re-admits.
- Replaces the old hand-curated `simplex_allowlist.yaml` + weighted-score
  discovery script (both deleted). Dry-run inspector:
  `python -m simplex_ingest.loops.discovery`.
- **Also owns retention.** At the end of every cycle `prune()` does three things,
  each independently guarded:
  1. **Time-series rolling window** — prunes `raw_events`/`snapshots`/
     `audit_results`/`book_state` to the last `DATA_RETENTION_CYCLES` cycles
     (`db.prune_time_series`). It prunes regardless of whether the sweep admitted
     anything (a failed/empty sweep must not let the volume grow).
  2. **Resolution reconciliation** — for graph markets that have left the active
     set with no known resolution time, it looks them up via Kalshi REST
     (`reconcile_resolutions` → `rest.get_market`) and persists
     `markets.resolved_at` from the authoritative `settlement_ts` (falling back
     to `close_time`). This is needed because a settled market's event is no
     longer *open*, so it never reappears in the catalog's open-events sweep.
  3. **Resolved-graph prune** — deletes `market_semantics`/`market_edges` for
     markets resolved more than `GRAPH_PRUNE_AFTER_RESOLVED_SECONDS` (1 h) ago
     (`db.prune_resolved_graph`; an edge goes if *either* endpoint resolved).
  Anchoring all of this to the discovery cycle keeps retention in sync with the
  hourly market-set recompute by construction.

### Catalog poller (`loops/catalog.py`)
- **Cadence:** every `CATALOG_REFRESH_SECONDS` (5 min).
- Reads the tracked series from `tracked_series`, expands each to its open
  markets via REST, keeps tradeable markets above `CATALOG_MIN_MARKET_VOLUME`,
  upserts the `markets` catalog, sets the **active subscription set**
  (`subscribed = TRUE`), and signals the WS loop (`resubscribe_event`).
- **Soft-fails** on an empty tracked set (logs, WS stays idle) — there is no
  longer a fatal allowlist startup gate.
- Closed/removed markets keep their rows and history; only `subscribed` flips.

### WebSocket subscriber (`loops/websocket.py`)
- Holds **one** persistent connection. Subscribes `orderbook_delta`
  **one-market-per-subscription** (so each market's `seq` is an isolated,
  monotonic stream → clean per-market gap detection), plus bulk `trade` and
  `market_lifecycle_v2`.
- Reconciles **incrementally** on the catalog signal (subscribe/unsubscribe per
  market for orderbook; `update_subscription` add/delete for bulk channels) —
  never a full reconnect. Dropping a market also drops its `BookStore` entry.
- Parses each message into `NormalizedEvent`s and buffers them to `raw_events`
  (batched). Reconnects with exponential backoff + jitter, unbounded.
- Drains **book-reset requests** (from the snapshot builder on a gap/canary, or
  the audit loop on a large diff): re-subscribes that one market's orderbook to
  force a fresh snapshot.

### Snapshot builder (`loops/snapshots.py`)
- **Cadence:** emits on the `SNAPSHOT_INTERVAL_SECONDS` (10 s) grid boundary.
- Replays `raw_events` in ingest order, forwarding each to that market's
  **`BookReconstructor`** (`reconstruct.py`) — a deep per-market state machine
  that owns the sequence discipline (§6). Every tick emits one row per active
  market to `snapshots` (top-of-book, within-band depth, window trade volume,
  last trade, status), carrying the book forward (LOCF) for quiet markets.
  Idempotent on `(ts, platform, market_id)`.
- **Anchoring & gaps:** deltas apply only after a snapshot anchors the book; a
  `seq` gap discards that market's book and requests a fresh snapshot.
- **Checkpoints** every `CHECKPOINT_INTERVAL_SECONDS` (60 s) to `book_state`
  (and on clean shutdown) so a restart resumes from the checkpoint + replay
  forward instead of from scratch. `replay_floor_ts` skips events already folded
  into a checkpoint. The replay cursor starts at the **earliest checkpoint
  floor**; with no checkpoints (first boot, or only freshly-discovered markets)
  it starts at the **current tail** and books anchor from the next live WS
  snapshot — it never rewinds to the start of the append-only `raw_events` log
  (which would rescan unbounded history and stall the loop as the DB grows).
- **Grid is forward-only.** The builder emits the current boundary each tick; it
  does **not** backfill boundaries missed during a restart/downtime, even where
  the underlying events survive in `raw_events`. So the `snapshots` grid can have
  gaps across restarts, and the solver must tolerate missing `ts` rows. This is a
  deliberate Stage-1 simplification — the grid is fully regenerable from
  `raw_events`, so dense re-materialization can be added later (alongside the
  planned R2/Parquet export, §11) without changing the ingest.
- **Canaries** (per market, per tick): `crossed_book`, `negative_size`,
  `out_of_range_price` force a book reset; a stale-market check is informational.

### Book audit (`loops/audit.py`)
- **Cadence:** wakes every `AUDIT_TICK_SECONDS` (1 h); runs a pass only when the
  UTC hour is in `[AUDIT_WINDOW_START_UTC_HOUR, AUDIT_WINDOW_END_UTC_HOUR)`
  (default 0–24 = always).
- For each subscribed market: **freezes** the in-memory book *before* the REST
  call, fetches the REST orderbook (dedicated token bucket so it can't starve
  catalog/discovery), diffs them, classifies `no_diff` / `small_diff` /
  `large_diff`, writes a row to `audit_results`, and forces a **book reset** on a
  large diff. One market's error never aborts the pass.
- **Classification is primarily structural, with a size backstop.** The freeze
  happens ~100 ms before the REST fetch, so *small* per-level size drift on
  levels present in both books is expected market movement, not desync. The main
  signal is **structural** divergence — a price level present in one book but
  absent in the other. `large_diff` (→ reset) fires when structural mismatches
  across both sides exceed `AUDIT_STRUCTURAL_DIFF_MAX_LEVELS` (4) **or** when a
  shared level's size differs by more than `AUDIT_SMALL_DIFF_MAX_SIZE_PCT` (50 %);
  otherwise `small_diff` (near-touch churn, no action). The size ceiling is a
  deliberately-high backstop: it stays quiet through ordinary liquid-market churn
  but catches a *magnitude* bug (e.g. a fixed-point/decode error that yields the
  right levels at wrong sizes) that the structural test alone cannot see.

### Extraction (`loops/extraction.py`) — the LLM semantic + edge layer (Stage 3)
- **Cadence:** every `EXTRACTION_INTERVAL_SECONDS` (5 min). Each cycle runs three
  phases — **reconcile** (drain finished async batches), **Phase A** (semantics),
  **Phase B** (edges) — all idempotent no-ops when there's no new work, so the
  cadence costs nothing on a quiet catalog. The synchronous work is bounded per
  cycle by `EXTRACTION_BATCH_SIZE` (50); a backlog drains across cycles.
- **Reasoning belongs at pair time, not extraction time.** Phase A makes each
  market *rich* (a comparison-ready semantic record); Phase B does the logical
  comparison **by model**, not by code — comparison-by-code is brittle across a
  domain this varied. The relationship taxonomy and prompts live in `llm/`.
- **Phase A — per-market semantics.** Active markets with no current-version
  `market_semantics` row are sent to `EXTRACTION_MODEL` (a Sonnet tier — cheap,
  high-volume) one at a time. The model returns the underlying event, the YES/NO
  resolution conditions, resolution timing, and the real-world entities /
  dependencies the market hinges on. Cached on `market_id` and **never
  re-extracted** unless `EXTRACTION_PROMPT_VERSION` bumps (descriptions don't
  change after listing).
- **Phase B — pairwise typed edges.** Candidate pairs are picked **cheaply** by
  `pair_candidates.py` (a pure module: same `event_ticker` OR same
  `series_ticker` OR ≥ `PAIR_ENTITY_OVERLAP_MIN` shared entities) so Stage B never
  classifies all O(n²) pairs. Each surviving candidate (after the spend gate
  below) goes to `PAIR_MODEL` (a **Sonnet** tier — see "spend discipline"), which
  returns one of seven relationship types — `same_event`, `implies`,
  `mutually_exclusive`, `partition_member`, `conditional`, `correlated`,
  `unrelated` — a direction (for the asymmetric ones), a confidence, and a
  rationale. An unknown label is rejected (`LLMError`), never coerced into the
  graph.
- **Trust gradient (why a bad edge is dangerous).** A wrong edge fed to the
  convex solver becomes a confident-looking but spurious "arbitrage," so edges
  enter at a tier matched to confidence:
  - `confidence ≥ EDGE_TRUSTED_CONFIDENCE` (0.85) → **candidate** for `trusted`
    (the solver's hard constraints), promoted **only if** an *independent* second
    model (`PAIR_VERIFY_MODEL`, deliberately different from `PAIR_MODEL`) agrees
    on the relationship type. Agreement → `trusted` (`agreement_status='agreed'`);
    disagreement → `review`. The 2× spend lands only on this highest-blast-radius
    band — and it's the strongest (**Opus**) tier, since this one decision gates
    the hard-constraint set.
  - `≥ EDGE_SOFT_CONFIDENCE` (0.6) → `soft` (a soft solver constraint).
  - below that → `review` (the manual-review queue: the query
    `trust_tier='review' AND review_status='pending'`).
- **Spend discipline — model tiers, the time gate, and the batch seam.** Stage B
  is the cost driver, so spend is shaped three ways (full rationale in
  [`LLM-COST-MIGRATION.md`](./LLM-COST-MIGRATION.md)):
  - **Tiering.** The primary pass is Sonnet (`PAIR_MODEL`), not Opus —
    Opus-on-every-pair was the biggest line item. Opus (`PAIR_VERIFY_MODEL`) is
    spent *only* as the independent verifier on the ≥0.85 band. Stage A semantics
    stay on Sonnet (`EXTRACTION_MODEL`).
  - **Time-to-resolution gate (`pair_candidates.route_pair`).** An edge's useful
    life is `min(life of A, life of B)` — it dies when *either* endpoint settles
    (the graph is pruned 1h after resolution). So each candidate is gated on
    `remaining_life` (from `markets.closes_at`, carried through
    `get_active_markets_with_semantics`): `< EDGE_REMAINING_LIFE_FLOOR_SECONDS`
    (24h) → **skip** (negative-ROI, never spend); `< EDGE_BATCH_THRESHOLD_SECONDS`
    (48h) → **sync**; `≥` → **batch**. Unknown close time → sync (never silently
    skip).
  - **Two transports, one prompt set.** Latency-insensitive work (long-horizon
    pairs; a *bulk* Phase-A backfill ≥ `BATCH_BULK_SEMANTICS_THRESHOLD`) goes to
    the **Anthropic Message Batches** path (`llm/batch.py`, flat 50% off,
    ~1h–24h). Short-fuse / steady-state work stays on the synchronous OpenRouter
    path (`llm/client.py`). Both share the message builders + result parsers in
    `llm/`, so a batched request is byte-identical to its sync twin. **A pair's
    primary and verify always travel the same transport** (a batched primary's
    verify is itself batched, joining the next wave). The two providers use
    **different model-id spellings** — OpenRouter dotted (`anthropic/claude-…`),
    Anthropic dashed (`claude-…`); the `BATCH_*` model constants mirror the sync
    ones. The batch path is **optional**: with no `ANTHROPIC_API_KEY`, batch-routed
    work degrades to sync (still produced, at full price).
- **Batch state machine (submit-now / retrieve-later).** A batch crosses cycles:
  cycle *N* submits and persists `(batch_id, purpose, the work it covers, prompt
  version)` to the durable `llm_batches` table; a later cycle's **reconcile** phase
  polls each open batch and, once it has *ended*, writes its results (semantics /
  edges) and deletes the row. In-flight markets/pairs are excluded from
  re-selection (`get_inflight_*`) so nothing is double-submitted. Persisting the id
  is essential — a restart would otherwise orphan a submitted batch (Anthropic
  keeps results 29 days, so a persisted id is recoverable). **Orphan backstop:** the
  only two deletion paths are "results written" and "abandoned past
  `BATCH_MAX_AGE_SECONDS` (48h)", so a row can never outlive its batch and silently
  strand its covered items — a wedged batch or a terminally-failing poll/results
  (e.g. a purged/404'd id) is dropped once stale and its items re-spend (at-least-
  once, never silent loss). Single-writer DuckDB is unaffected: batch retrieval
  writes from the same process as the sync path.
- **Cost attribution.** Every call (sync or batch) logs an `llm usage` line tagged
  by `mode` (sync|batch), `purpose` (stage_a|pair_primary|pair_verify) and model,
  so spend can be summed per stage — the number that justifies (or doesn't) the
  batch path.
- **Outputs are durable, non-regenerable artifacts** (§5, §9) — never wiped on
  re-run; a re-classification updates the edge but **never** clobbers a human
  review decision (`review_status`/`reviewed_*` are left untouched on conflict).
- **Soft-fails without a key.** No `OPENROUTER_API_KEY` → `rt.llm is None` → the
  cycle is a logged no-op and plain ingest runs unchanged (`ANTHROPIC_API_KEY`
  alone does not enable extraction — it only adds the batch transport on top of the
  OpenRouter base). **One bad item never sinks a pass:** the clients raise
  `LLMError` on any transport/parse/schema failure (and a single failed *batch*
  request surfaces as a skippable per-item error) — the loop logs-and-skips that
  single market or pair.
- Dry-run inspector (DB read-only, no model calls, no spend; ingest must be
  stopped): `python -m simplex_ingest.loops.extraction` — prints the work plan
  incl. the gate breakdown (skip/sync/batch) and in-flight batch count.

---

## 4. Discovery predicates (`discovery_predicates.py`)

Pure functions over per-series aggregated stats — no I/O. `aggregate(events)`
groups a `get_events` sweep into `SeriesStats` (per-event `EventStats` +
series-summed volume, tradeable markets only). `evaluate(stats)` returns a
`Verdict`; `rank_key(stats)` returns the ordinal sort key.

| ID | Predicate | Threshold |
|---|---|---|
| **P1 — Partition** | ≥1 open event flagged `mutually_exclusive` with ≥ `PREDICATE_PARTITION_MIN_MARKETS` tradeable markets | 3 |
| **P2 — Hierarchy** | ≥1 open event with ≥ `PREDICATE_HIERARCHY_MIN_MARKETS` *distinct* markets (distinct `yes_sub_title`/`subtitle`, falling back to strike) | 2 |
| **P3 — Tradeability** | series-summed tradeable volume ≥ `PREDICATE_MIN_VOLUME_24H` | 1000 |

**Admit iff `(P1 OR P2) AND P3`.** P1/P2 give the solver internal structure to
chew on; P3 ensures the WS feed carries signal.

**Ranking (eviction at cap):** sort admitted series **descending** by the strict
ordinal tuple `(passes_P1, n_partition_events, n_hierarchy_events, volume_24h)` —
no weights. Keep the top `MAX_TRACKED_SERIES`. Note the consequence: ranking
prioritizes *structural event count* before volume, so a very high-volume series
with few partition events can rank below lower-volume but structurally-richer
series. This is intentional (structure is what the coherence solver consumes);
revisit the key if the trading layer wants volume weighted higher.

Tested exhaustively (`tests/test_discovery_predicates.py`): per-predicate
boundaries, the 8-row composite truth table, and ranking property tests
(total order, strictly-worse never displaces better, ticker-irrelevance).

---

## 5. Data model (DuckDB — `schema.sql`)

Idempotent DDL, run on every startup. `db.py` owns one connection behind a write
lock; `raw_events` writes are buffered and flushed in batches.

| Table | Role | Notes |
|-------|------|------|
| `tracked_series` | Self-managed set of series to ingest. | Rewritten atomically each discovery cycle (admit/rank/cap). Replaces the YAML allowlist. PK `series_ticker`; `admitted_at` preserved across re-admits. |
| `markets` | Catalog; `subscribed` flags the active WS set. | Rows never deleted — closed markets keep history, only `subscribed` flips. |
| `raw_events` | **Append-only normalized event log — the source of truth (within the retention window).** | `received_ts` (our monotonic ingest clock) + DuckDB `rowid` order replay; `sequence` is the exchange per-subscription seq for gap detection. **Pruned** to the last `DATA_RETENTION_CYCLES` discovery cycles (not kept forever). |
| `snapshots` | 10 s materialized grid — the marginal time series the solver reads. | Regenerable from `raw_events` **within the retention window**; pruned on the same cadence. PK `(ts, platform, market_id)` gives idempotency under window re-runs. |
| `book_state` | Serialized in-memory books for fast restart. | A replay accelerator, not a source of truth. |
| `audit_results` | Per-market book-vs-REST reconciliation outcomes. | `no_diff`/`small_diff`/`large_diff`/`error`; `action_taken` none/`book_reset`. |
| `market_semantics` | **Stage-3 per-market semantic cache (LLM-derived).** | PK `market_id`. Cached forever; re-extracted only when `extraction_version` bumps. `entities`/`dependencies` are JSON arrays. **Non-regenerable** (see below). |
| `market_edges` | **Stage-3 pairwise typed relationship graph (LLM-derived).** | PK `(platform, market_id_a, market_id_b)`, canonical `a < b`. `trust_tier` ∈ `trusted`/`soft`/`review`; `agreement_status`; review-queue columns. The review queue is the query `trust_tier='review' AND review_status='pending'` — no separate table. **Non-regenerable** (see below). |
| `llm_batches` | **In-flight Anthropic Message Batches (Stage-3 async spend path).** | PK `batch_id`. Durable submit-now/retrieve-later state: `purpose` (semantics/pair_primary/pair_verify), `payload` (custom_id → market_id / pair / {pair, primary}), prompt version. A row exists only while the batch is open — deleted once its results are written. Persisting the id is what lets a restart recover a submitted batch. |

**Two retention classes.** The time-series tables (`raw_events`, `snapshots`,
`book_state`, `audit_results`) are a **rolling window**: the discovery loop prunes
them to the last `DATA_RETENTION_CYCLES` cycles each cycle (§3, §9.1). `raw_events`
is still the source of truth *within* that window, and `snapshots`/`book_state`
remain regenerable from it there — but the window is finite, so long-term history
is intentionally not retained (the planned R2 export, §11, is where a durable
historical corpus would live). The Stage-3 tables are the **other** class: they
are LLM-derived (costly, non-deterministic, and — review decisions — partly
human-curated) and cannot be reconstructed from `raw_events`, so they are
**exempt from the time-series window** — they live by a different rule keyed on
the *market's lifecycle*, not a fixed clock: a market's semantics/edges are kept
for as long as the market is live and are deleted only
`GRAPH_PRUNE_AFTER_RESOLVED_SECONDS` (1 h) after the market **resolves** (becomes
terminal on Kalshi). A resolved market never reopens, so its graph is dead weight
for the live engine and deleting it carries no re-spend risk (unlike a market
merely dropped from the tracked set, which may be re-admitted — those are kept).
The durability requirement therefore applies to the graph of *live* markets;
long-term history of resolved markets belongs in the R2 export (§11), not the
operational DB.

**Timestamps:** DuckDB `TIMESTAMP` is naive; all writes normalize to naive UTC
(`util.naive_utc`). Aware UTC is used in memory.

---

## 6. Order book reconstruction

Kalshi publishes a **bids-only binary book**: `yes` levels are YES buy orders,
`no` levels are NO buy orders. A YES ask is the mirror of a NO bid, so
(`orderbook.py`):

```
yes_bid = max(yes prices)
yes_ask = 1 − max(no prices)
yes_mid = (yes_bid + yes_ask) / 2
```

Prices are dollars (0.01–0.99); sizes are contract counts. Levels are kept in a
`SortedDict` keyed on 6-dp-quantized price (avoids float key drift).
`depth_within(band)` sums `price × size` and counts levels within `band` dollars
of the touch on each side — this is the headline anti-stale-quote signal. The
snapshot columns hard-code **`3c`** (`DEPTH_BAND_PRICE_UNITS = 0.03`); changing
the constant shifts the meaning but not the names.

Reconstruction discipline lives in **`reconstruct.py`** (`BookReconstructor.apply`),
not scattered across the snapshot loop: a **snapshot anchors** the book;
**deltas** apply only while anchored and only for the next contiguous `seq`; a
duplicate/old `seq` is dropped; a forward gap returns `RESET`. The snapshot
builder just forwards drained events and, on `RESET`, calls the reset seam
(`runtime.request_reset` — the one place that resets the in-memory book *and*
enqueues the WS re-subscribe). Reset paths: sequence gap, structural canary, or
audit large-diff.

---

## 7. Kalshi specifics (`kalshi/`)

- **Auth (`auth.py`):** RSA-PSS / SHA256, MGF1(SHA256), salt = digest length.
  Three headers (`KALSHI-ACCESS-KEY`, `-TIMESTAMP` in Unix ms, `-SIGNATURE`
  base64). Signed message is `timestamp + METHOD + path` (path only, no query).
  Signing is harmless on the public read endpoints used today and is already in
  place for the authenticated write endpoints a trader will need.
- **REST (`rest.py`):** `/series`, `/events` (cursor-paginated, with nested
  markets), `/markets`, `/markets/{ticker}/orderbook`. Client-side token-bucket
  rate-limited; retries 429/5xx/transport errors with jittered backoff.
- **WS (`kalshi/subscriber.py`):** envelope `{type, sid, seq, msg}`, `seq`
  monotonic per subscription. Parses `orderbook_snapshot` / `orderbook_delta` /
  `trade` / `market_lifecycle_v2` into `NormalizedEvent`s; never raises on a bad
  message (logs and drops). Hosts/paths per `KALSHI_ENV` live in `config.py`.
- **Fixed-point (`kalshi/fixedpoint.py`):** the one module that decodes Kalshi's
  `*_fp` / `*_dollars` numeric surface (price/size levels, contract volume).
  Shared by the WS parser, the REST audit, the catalog poller, and the discovery
  predicates — so a units question is verified and fixed in one place.

The exchange-agnostic seam is `subscriber.py::BaseSubscriber`: a new venue is one
new file implementing `ws_url` / `connect_headers` / `subscribe_message` /
`is_control` / `parse`. The loops, storage, and `NormalizedEvent` shape are
venue-neutral (`platform` tag on every row).

---

## 8. Cross-cutting concerns

- **Configuration split.** **Six** values come from the environment/`.env` —
  four required (`KALSHI_API_KEY_ID`, `KALSHI_API_SECRET`, `KALSHI_ENV`,
  `SIMPLEX_DATA_DIR`) plus two optional, **`OPENROUTER_API_KEY`** (enables the
  Stage-3 extraction layer, synchronous path) and **`ANTHROPIC_API_KEY`** (adds
  the discounted Message Batches transport on top — absent it, batch-routed work
  degrades to sync) (see `config.py`, `ingest/.env.example`). The LLM keys join the
  env surface because they are *secrets* (they can't be constants); they're the
  bounded exception to "four values," and the precedent the future trader's
  exchange-write secret reuses (§11). **Everything else tunable** (intervals, depth
  band, audit thresholds, rate limits, ports, reconnect bounds, predicate
  thresholds, **LLM model ids (sync *and* batch) / confidence cutoffs /
  entity-overlap threshold / the time-to-resolution gate thresholds / batch
  max_tokens**, log level) stays a documented constant in `constants.py`. No env
  lookups for behavior — edit a constant and redeploy.
- **Rate limiting (`util.TokenBucket`).** General catalog/discovery REST:
  `REST_CALLS_PER_SECOND`/`REST_BURST` (8/8). Audit has its **own** bucket
  (`AUDIT_REST_*`, 4/4) so an audit pass can't starve the pollers. The extraction
  loop's OpenRouter client has its **own** bucket too
  (`LLM_CALLS_PER_SECOND`/`LLM_BURST`, 2/4) — model spend, not REST budget, is its
  real constraint.
- **Supervision & restart (`supervisor.py`).** Per-loop jittered backoff bounded
  by `SUPERVISOR_RESTART_*`; backoff resets after a 60 s healthy run.
- **Liveness (`runtime.Heartbeats`, `health.py`).** Loops idle through the shared
  `util.idle_sleep` (wake-on-shutdown + periodic heartbeat in one place, so a loop
  can't silently stop beating); `/health` reports per-loop liveness and 200/503.
- **Logging (`log.py`).** Structured JSON to stdout, one object per line, tagged
  with `loop` and contextual `extra` fields. Level via `LOG_LEVEL`.
- **Shutdown.** SIGTERM/SIGINT → cancel supervisor + the discovery-grace task →
  flush `raw_events` → checkpoint books → close REST + LLM clients + DB → exit 0.

---

## 9. Key invariants (do not break without updating this file)

1. **`raw_events` is the source of truth — within a bounded retention window.**
   It is append-only, but **not** kept forever: the discovery loop prunes it (and
   `snapshots`/`book_state`/`audit_results`) to the last `DATA_RETENTION_CYCLES`
   discovery cycles each cycle (`DATA_RETENTION_SECONDS` ≈ 3 h with the defaults),
   so the volume stays bounded instead of growing without limit. `snapshots` and
   `book_state` are regenerable from `raw_events` **only within that window**;
   older history is gone. Retention is anchored to the hourly discovery cycle (the
   market-set recompute), so it prunes even on a failed/empty sweep. The durable
   LLM graph (§5) is exempt — it is the one keep-forever store.
2. **DuckDB is single-writer, single-process.** One process holds the read-write
   lock on `simplex.duckdb`; a second read-write open fails (a read-only open
   fails too while a writer holds it). This is why ingest is one process and why
   the deploy is a **single instance** (see [`DEPLOY.md`](./DEPLOY.md) §scaling).
   It is also the constraint that forces a storage split once a second writer
   (e.g. a trader) appears — see §11.
3. **Discovery owns the tracked set.** No manual allowlist, pins, or bans;
   predicates rule. Discovery never wipes the working set on a transient empty
   sweep or REST error.
4. **Writes are idempotent / safe to re-run.** `markets`/`snapshots`/`book_state`/
   `market_semantics`/`market_edges` upsert on their PKs; `tracked_series` is
   swapped in a transaction. An edge re-classification updates the edge but
   **never** touches its human-review columns (`review_status`/`reviewed_*`).
5. **Malformed external input must not raise into a loop** — log and drop/skip.
   The WS subscribers drop a bad message; the extraction LLM client raises a
   typed `LLMError` and the loop logs-and-skips that one market/pair.
6. **The Stage-3 extraction outputs are non-regenerable; durable while the market
   is live, pruned after it resolves.** `market_semantics` and `market_edges` are
   LLM-derived (and partly human-curated) — unlike `snapshots`/`book_state` they
   cannot be rebuilt from `raw_events`, so they are never wiped on a transient
   error or re-run. The one deliberate deletion is lifecycle-driven: once a market
   **resolves** (terminal on Kalshi — outcome known, `markets.resolved_at` set
   from `settlement_ts`), its graph is dead weight and is pruned
   `GRAPH_PRUNE_AFTER_RESOLVED_SECONDS` (1 h) later by the discovery loop. Live
   markets' graph must still be covered by backup/export (§5, §11); resolved
   markets' history belongs only in the export.

---

## 10. Repository layout

The repo is a **monorepo**: each deployable service is a self-contained
subdirectory (Railway Root Directory + Watch Paths, §11). Today there is one
service, `ingest/`; future services are siblings.

```
ingest/                      # the ingest service — self-contained, deployable
  Dockerfile entrypoint.sh railway.json   # deploy layer (see DEPLOY.md)
  .dockerignore .env.example pyproject.toml
  src/simplex_ingest/
    app.py  __main__.py      # entry: wire loops, signals, health, shutdown
    config.py  constants.py  # 4-value env surface  /  all tuning knobs
    schema.sql  db.py        # DuckDB: shared conn, write lock, batched writes
    events.py  orderbook.py  # NormalizedEvent  /  in-memory book + depth + canaries
    reconstruct.py           # BookReconstructor: per-market replay state machine
    discovery_predicates.py  # pure admit/rank predicates (no I/O)
    pair_candidates.py       # pure Stage-B candidate-pair generation (no I/O)
    subscriber.py            # BaseSubscriber (one new file per future venue)
    runtime.py supervisor.py health.py util.py log.py
    kalshi/  auth.py rest.py subscriber.py fixedpoint.py   # fixedpoint: fp/dollars decoding
    llm/     __init__.py client.py   # OpenRouter client: extract_market / classify_pair + taxonomy
    loops/   discovery.py catalog.py websocket.py snapshots.py audit.py extraction.py
  tests/                     # pytest+hypothesis: predicates, DB atomicity, loops, extraction,
                             #   clients (httpx mock), full end-to-end pipeline, opt-in live smoke
docs/      ARCHITECTURE.md DEPLOY.md   # repo-level (shared across services)
CLAUDE.md  README.md
# future siblings (not yet created): trader/  viz/  libs/simplex_core/
```

`pip install -e ".[test]"`, `pytest`, and `python -m simplex_ingest…` all run
from inside `ingest/`. Package name and imports are unchanged by the move.

---

## 11. Planned evolution — target architecture

The current single-process design is correct for read-only ingest. Two future
additions reshape it; capture the reasoning here so we build toward it
deliberately.

### Stage 4 — the solver
Reads the `snapshots` marginal grid **and the Stage-3 `market_edges`** (the
trust-tiered structural relationships — `trusted` edges as hard constraints,
`soft` edges softly), computes the max-entropy joint and coherence/deviation
scores. It **must begin as the 7th in-process loop** (the extraction loop is the
6th, shipped) — not because in-process is merely simpler, but because the
single-writer DuckDB lock (§9.2) means a *separate* solver process **cannot open
`simplex.duckdb` at all** while ingest holds it (read-only opens fail too). So a
standalone solver service is not an option until the storage inflection below
gives it an independent place to read from. In-process the solver only reads what
this process already writes, with no DB-sharing problem; it splits out **after**
the OLTP/OLAP split (e.g. reading snapshots + edges from R2/Parquet or a
replica), not before. Stage 3 already proved this pattern in-process and
established the external-model-secret precedent (`OPENROUTER_API_KEY`) the trader
reuses.

### LLM-in-the-loop autonomous trading
This is the highest-risk addition and should be a **separate process/service**
with an independent deploy cadence — never coupled to ingest. Design intent:

- **The LLM proposes; a deterministic risk layer disposes.** The model never
  calls the order API directly. Between it and the exchange sits non-LLM
  guardrails: max order size, per-market position caps, daily-loss / drawdown
  **kill switch**, a global halt flag, and **idempotency keys** so a retry can't
  double-submit. Paper/dry-run mode is the default until trust is earned.
- **Decision provenance, not just fills.** The trade datastore records the
  decision context — the prompt, the market snapshot, the solver's deviation
  estimate, and the model's rationale *at decision time* — alongside orders and
  fills. This is both the audit trail and the "why did it do that" record.
- **Reconciliation.** A loop reconciles local order/position state against
  Kalshi's own fills/positions endpoints; the local append-only trade log is the
  source of truth for "what we did," reconciled against the exchange for "what
  actually filled."

### The storage inflection (OLTP / OLAP split)
The single-writer constraint (§9.2) means the trader cannot write to the ingest's
DuckDB while ingest is writing. When a second writer appears:

- **OLTP — orders, positions, decisions:** needs ACID + concurrent access + to be
  the operational source of truth → **Postgres** (Railway-managed).
- **OLAP — market data, snapshots, raw_events, plus the durable Stage-3
  `market_semantics`/`market_edges`:** append-heavy, analytical, read by the
  solver → stays DuckDB/Parquet, with the planned **R2 export** as the
  historical/backtest corpus once the volume's size cap bites. (The light
  human-review mutations on `market_edges` are the one OLTP-ish wrinkle; they move
  to Postgres only if a review UI ever needs concurrent writes.)

### Deployment shape as it grows
The monorepo (above) already encodes the pattern: **one repo, per-service Root
Directory + Watch Paths**, each service building from its own sibling folder. The
fleet grows additively into a set of single-instance services — `ingest`
(autodeploy-on-push is fine, low blast radius; Watch Paths keep unrelated pushes
from rebuilding it), the in-process `solver` (ships inside `ingest/` until the
storage split), and `trader` (deliberate deploys, **not** autodeploy: never
redeploy a process holding open orders) — plus managed Postgres and object
storage attached per-service. Trading timescale is seconds-to-minutes (the 10 s
grid), so co-location / ultra-low latency is **not** a requirement; Railway
remains a fit. The deploy-side consequences are detailed in
[`DEPLOY.md`](./DEPLOY.md) §13.

**The deferred `libs/simplex_core` split.** Extracting shared code into a
`libs/simplex_core` package is deliberately deferred to **trader-time** (the
first second consumer), because the core/ingest boundary is partly a guess until
the trader's needs are concrete. The recent deepening cut most of the seams
(`BaseSubscriber`, `kalshi/`, `fixedpoint`, `config`), but the split is **not yet
a clean `git mv`** — a few first-party imports still cross the intended boundary
and must be cut first (see "pre-extraction cleanup" below). The intended target
boundary:

- **core (→ `libs/simplex_core`)** — venue-agnostic plumbing reused by any
  consumer: `kalshi/{auth,rest,fixedpoint}`, `subscriber.py` (`BaseSubscriber`),
  `config.py`, `log.py`, `util.py`, the `Loop`/`Heartbeats` half of `runtime.py`,
  `supervisor.py`, `health.py`, `events.py`.
- **ingest-specific (stays in `ingest/`)** — `loops/` (incl. `extraction.py`),
  `discovery_predicates.py`, `pair_candidates.py`, `llm/` (the extraction
  taxonomy/prompts are coherence-specific), `db.py` + `schema.sql`, `constants.py`,
  `orderbook.py`, `reconstruct.py`, `app.py`, and the `BookStore`/`request_reset`
  half of `runtime.py`.

**Pre-extraction cleanup (known boundary leaks, do these when the split lands).**
A core module must not import ingest-specific code; today three kinds of leak do:

1. **`constants` imported by core** — `supervisor.py`, `health.py`, and
   `kalshi/rest.py` read tuning (restart bounds, heartbeat timeout, REST rate
   limits) directly from `constants.py` (ingest-specific). Cut by **injecting**
   those values (constructor args / a small config object) so the core modules
   take parameters and `ingest/` supplies them from `constants.py`.
2. **`runtime.py` mixes core and ingest** — it holds the generic `Loop`/
   `Heartbeats` (core) *and* `BookStore` + `request_reset`, which depend on
   `orderbook` and `reconstruct` (ingest-specific). Split the file: `Loop`/
   `Heartbeats` go to core, `BookStore`/`request_reset` stay ingest-side.
3. **`kalshi/fixedpoint.py` imports `orderbook._q`** — the price-key quantizer.
   `_q` is a fixed-point concern; relocate it into `fixedpoint` and have
   `orderbook` import it from there (reverse the dependency).

When that split lands, services depending on `libs/simplex_core` switch to a
repo-root build context (a Dockerfile only `COPY`s from inside its context) with
Watch Paths scoped to their folder + `libs/**` — see [`DEPLOY.md`](./DEPLOY.md)
§13.
