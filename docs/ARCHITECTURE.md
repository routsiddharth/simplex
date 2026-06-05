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

It is built in stages. **Only Stage 1 (ingest) exists today.**

| Stage | Scope | State |
|------|-------|-------|
| 1. Ingest | Live Kalshi books → normalized event log → 10s snapshot grid, with self-managing catalog discovery, book reconstruction, checkpointing, and an hourly REST reconciliation audit. | **Built** |
| 2. Deploy | Containerized, 24/7 on a mounted volume (Railway). | **Built** — see [`DEPLOY.md`](./DEPLOY.md) |
| 3. Solver | Max-entropy joint over related markets given snapshot marginals + structural constraints; coherence/deviation scoring. | Planned |
| 4. Graph + viz | Relationship graph across markets; surface largest incoherences over time. | Planned |
| 5. Export | Continuous R2 export of `snapshots` / `raw_events`. | Planned |
| — | LLM-in-the-loop autonomous trading + trade datastore. | Planned — see §11 |
| — | Other venues (Polymarket, Manifold) via the `BaseSubscriber` seam. | Planned |

Everything downstream of the `snapshots` table is the coherence engine proper
and is **not built**. Stage 1 exists to produce the clean, regular, gap-checked
marginal time series the solver will consume.

---

## 2. Process model

One long-running Python process. Inside it, **five supervised async loops** share
a single embedded **DuckDB** file on a mounted volume. Everything runs on one
asyncio event loop; the only work dispatched to threads is DuckDB I/O (via
`asyncio.to_thread`), and every DB statement is serialized behind a single write
lock — because **DuckDB is single-writer** (see §9).

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
```

A `supervisor` runs each loop's `run()` forever; a crash is logged and the loop
restarted with jittered backoff (reset after a sustained healthy run) without
taking the others down. SIGTERM/SIGINT triggers a clean shutdown: cancel loops →
flush buffered `raw_events` → checkpoint books → close DB → exit 0.

`/health` (port `$PORT`, default 8080) returns 200 **only** when all five loops
have emitted a heartbeat within `HEALTH_HEARTBEAT_TIMEOUT_SECONDS` (90 s).

Code map: entry `app.py` (`run()` wires runtime, loops, signals, health,
shutdown) / `__main__.py`. Shared state in `runtime.py` (`BookStore`,
`Heartbeats`). Loop supervisor in `supervisor.py`. Loops in `loops/`.

---

## 3. The five loops

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
- Replays `raw_events` in ingest order with **strict per-market sequence
  checking**; maintains an in-memory `OrderBook` per active market. Every tick
  emits one row per active market to `snapshots` (top-of-book, within-band
  depth, window trade volume, last trade, status), carrying the book forward
  (LOCF) for quiet markets. Idempotent on `(ts, platform, market_id)`.
- **Anchoring & gaps:** deltas apply only after a snapshot anchors the book; a
  `seq` gap discards that market's book and requests a fresh snapshot.
- **Checkpoints** every `CHECKPOINT_INTERVAL_SECONDS` (60 s) to `book_state`
  (and on clean shutdown) so a restart resumes from the checkpoint + replay
  forward instead of from scratch. `replay_floor_ts` skips events already folded
  into a checkpoint.
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
| `raw_events` | **Append-only normalized event log — the source of truth.** | `received_ts` (our monotonic ingest clock) + DuckDB `rowid` order replay; `sequence` is the exchange per-subscription seq for gap detection. Never deleted. |
| `snapshots` | 10 s materialized grid — the marginal time series the solver reads. | Regenerable from `raw_events`. PK `(ts, platform, market_id)` gives idempotency under window re-runs. |
| `book_state` | Serialized in-memory books for fast restart. | A replay accelerator, not a source of truth. |
| `audit_results` | Per-market book-vs-REST reconciliation outcomes. | `no_diff`/`small_diff`/`large_diff`/`error`; `action_taken` none/`book_reset`. |

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

Reconstruction discipline in the snapshot builder: a **snapshot anchors** the
book; **deltas** apply only while anchored and only for the next contiguous
`seq`; a duplicate/old `seq` is dropped; a forward gap triggers reset + fresh
snapshot. Reset paths: sequence gap, structural canary, or audit large-diff.

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

The exchange-agnostic seam is `subscriber.py::BaseSubscriber`: a new venue is one
new file implementing `ws_url` / `connect_headers` / `subscribe_message` /
`is_control` / `parse`. The loops, storage, and `NormalizedEvent` shape are
venue-neutral (`platform` tag on every row).

---

## 8. Cross-cutting concerns

- **Configuration split.** Only **four** values come from the environment/`.env`
  (`KALSHI_API_KEY_ID`, `KALSHI_API_SECRET`, `KALSHI_ENV`, `SIMPLEX_DATA_DIR`;
  see `config.py`, `.env.example`). **Everything tunable** (intervals, depth
  band, audit thresholds, rate limits, ports, reconnect bounds, predicate
  thresholds, log level) is a documented constant in `constants.py`. No env
  lookups for behavior — edit a constant and redeploy.
- **Rate limiting (`util.TokenBucket`).** General catalog/discovery REST:
  `REST_CALLS_PER_SECOND`/`REST_BURST` (8/8). Audit has its **own** bucket
  (`AUDIT_REST_*`, 4/4) so an audit pass can't starve the pollers.
- **Supervision & restart (`supervisor.py`).** Per-loop jittered backoff bounded
  by `SUPERVISOR_RESTART_*`; backoff resets after a 60 s healthy run.
- **Liveness (`runtime.Heartbeats`, `health.py`).** Each loop beats around its
  idle sleeps; `/health` reports per-loop liveness and 200/503.
- **Logging (`log.py`).** Structured JSON to stdout, one object per line, tagged
  with `loop` and contextual `extra` fields. Level via `LOG_LEVEL`.
- **Shutdown.** SIGTERM/SIGINT → cancel supervisor + the discovery-grace task →
  flush `raw_events` → checkpoint books → close REST clients + DB → exit 0.

---

## 9. Key invariants (do not break without updating this file)

1. **`raw_events` is the source of truth.** It is append-only and never deleted;
   `snapshots` and `book_state` are regenerable from it.
2. **DuckDB is single-writer, single-process.** One process holds the read-write
   lock on `simplex.duckdb`; a second read-write open fails (a read-only open
   fails too while a writer holds it). This is why ingest is one process and why
   the deploy is a **single instance** (see [`DEPLOY.md`](./DEPLOY.md) §scaling).
   It is also the constraint that forces a storage split once a second writer
   (e.g. a trader) appears — see §11.
3. **Discovery owns the tracked set.** No manual allowlist, pins, or bans;
   predicates rule. Discovery never wipes the working set on a transient empty
   sweep or REST error.
4. **Writes are idempotent / safe to re-run.** `markets`/`snapshots`/`book_state`
   upsert on their PKs; `tracked_series` is swapped in a transaction.
5. **Subscribers must not raise on malformed input** — log and drop.

---

## 10. Repository layout

```
src/simplex_ingest/
  app.py  __main__.py        # entry: wire loops, signals, health, shutdown
  config.py  constants.py    # 4-value env surface  /  all tuning knobs
  schema.sql  db.py          # DuckDB: shared conn, write lock, batched writes
  events.py  orderbook.py    # NormalizedEvent  /  in-memory book + depth + canaries
  discovery_predicates.py    # pure admit/rank predicates (no I/O)
  subscriber.py              # BaseSubscriber (one new file per future venue)
  runtime.py supervisor.py health.py util.py log.py
  kalshi/    auth.py rest.py subscriber.py
  loops/     discovery.py catalog.py websocket.py snapshots.py audit.py
tests/                       # pytest+hypothesis: predicates, DB atomicity, loops
docs/      ARCHITECTURE.md DEPLOY.md
Dockerfile entrypoint.sh railway.json   # deploy layer (see DEPLOY.md)
```

---

## 11. Planned evolution — target architecture

The current single-process design is correct for read-only ingest. Two future
additions reshape it; capture the reasoning here so we build toward it
deliberately.

### Stage 3 — the solver
Reads the `snapshots` marginal grid and structural relationships, computes the
max-entropy joint and coherence/deviation scores. It can begin as a **6th loop
in-process** (no DB-sharing problem — it only reads what this process writes) or
split into its own service later. Splitting early forces the storage decision
below sooner; in-process keeps it simple but couples deploy/restart.

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
- **OLAP — market data, snapshots, raw_events:** append-heavy, analytical → stays
  DuckDB/Parquet, with the planned **R2 export** as the historical/backtest
  corpus once the volume's size cap bites.

### Deployment shape as it grows
A small fleet of single-instance services — `ingest` (autodeploy-on-push is fine,
low blast radius), `solver`, `trader` (deliberate deploys, **not** autodeploy:
never redeploy a process holding open orders) — plus managed Postgres and object
storage. Trading timescale is seconds-to-minutes (the 10 s grid), so co-location
/ ultra-low latency is **not** a requirement; Railway remains a fit. The
deploy-side consequences are detailed in [`DEPLOY.md`](./DEPLOY.md).
