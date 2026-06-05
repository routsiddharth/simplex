# Simplex

**A real-time probabilistic coherence engine for Kalshi.** Simplex computes the
maximum-entropy joint distribution implied by all active markets and surfaces the
largest deviations from logical consistency.

Prediction markets price many overlapping questions independently. Those prices
imply constraints on a shared underlying world — a partition's markets should sum
to 1, a conditional should respect its parent, mutually exclusive outcomes can't
both be likely — but nothing forces the order books to honor them. Simplex
continuously ingests the live books, finds the **maximum-entropy joint
distribution** consistent with the marginals the market is quoting, and ranks
where reality (the quotes) diverges most from that maximally-noncommittal joint.
Those divergences are the incoherences: mispriced relationships, stale legs,
latent arbitrage.

---

## Status

Simplex is being built in stages. **Stage 1 (ingest) is complete and running;**
the coherence engine itself is the next stage.

| Stage | Scope | State |
|------|-------|-------|
| **1. Ingest** | Live Kalshi order books → normalized event log → 10s snapshot grid, with catalog discovery, book reconstruction, checkpointing, and a hourly REST reconciliation audit. | **Built** |
| 2. Deploy | Containerized, runs 24/7 on a mounted volume (Railway / Fly.io). | **Built** (`Dockerfile`, `railway.json`, `README-DEPLOY.md`) |
| 3. Solver | Max-entropy joint distribution over related markets given snapshot marginals + structural constraints (partitions, hierarchy, mutual exclusivity); coherence/deviation scoring. | Planned |
| 4. Graph + viz | Relationship graph across markets; surface the largest incoherences over time. | Planned |
| 5. Export | Continuous R2 export of `snapshots` / `raw_events` for offline analysis & backup. | Planned |
| — | Other exchanges (Polymarket, Manifold) via the `BaseSubscriber` seam. | Planned |

Everything downstream of the `snapshots` table (stages 3–4) is the coherence
engine proper and is not built yet. Stage 1 exists to produce the clean,
regular, gap-checked marginal time series the solver will consume.

---

## How stage 1 works

One long-running Python process, five supervised async loops sharing a single
embedded **DuckDB** file on a mounted volume:

```
      Kalshi REST              Kalshi REST                Kalshi WebSocket
          │                        │                             │
  ┌───────▼────────────┐  ┌────────▼──────────────┐  ┌───────────▼────────────┐
  │ discovery (hourly) │  │ catalog poller (5 min)│  │ websocket subscriber   │
  │ predicates → admit │  │ tracked_series → open │  │ per-market orderbook   │
  │ / rank / cap →     │─▶│ markets → active set  │─▶│ subs + trade+lifecycle │
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

- **Discovery loop** sweeps all open Kalshi series hourly and rewrites the
  `tracked_series` table — admitting a series iff it has a **partition** (a
  mutually-exclusive event with ≥3 markets) **or** a **hierarchy** (an event with
  ≥2 distinct markets) **and** clears a volume floor, then ranks and caps the
  tracked set. No manual allowlist: the system discovers and rotates what it
  ingests on its own.
- **Catalog poller** reads the `tracked_series` table, expands each series to its
  open markets via REST, and maintains the active subscription set.
- **WebSocket subscriber** holds one persistent connection; subscribes
  `orderbook_delta` **one market per subscription** (isolated per-market sequence
  numbers → clean gap detection), plus bulk `trade` and `market_lifecycle_v2`.
  Reconnects with exponential backoff + jitter, unbounded.
- **Snapshot builder** replays `raw_events` in order with strict sequence
  checking, maintains an in-memory order book per market, and every 10s emits one
  row per active market: top-of-book, depth within 3¢ of the touch (USD + level
  counts), window volume, last trade, status. Last-observation-carried-forward
  for quiet markets. Idempotent on `(ts, platform, market_id)`. Checkpoints books
  so a restart resumes instead of replaying from scratch.
- **Book audit** (hourly, within a configurable UTC window) reconciles each
  in-memory book against a fresh REST orderbook and forces a reset on large
  divergence.

`raw_events` is the source of truth; `snapshots` is a regenerable derived grid.
A crash in any one loop is restarted by a supervisor without taking the others
down. SIGTERM flushes, checkpoints, closes the DB, and exits 0. `/health`
(port 8080) returns 200 only when all five loops are alive.

### Data model (DuckDB — `src/simplex_ingest/schema.sql`)

| Table | Role |
|-------|------|
| `tracked_series` | The self-managed set of series to ingest, rewritten each discovery cycle (admit/rank/cap by predicates). Replaces the old YAML allowlist. |
| `markets` | Catalog; `subscribed` flags the active set. Rows are never deleted. |
| `raw_events` | Append-only normalized event log (snapshots, deltas, trades, lifecycle) with exchange sequence numbers. Source of truth. |
| `snapshots` | 10s materialized grid — the marginal time series the solver will read. |
| `book_state` | Serialized in-memory books for fast restart. |
| `audit_results` | Per-market book-vs-REST reconciliation outcomes. |

---

## Quick start

Requires Python 3.13 and Kalshi API credentials (an API key ID + RSA private key).

```bash
python3 -m venv venv && source venv/bin/activate
pip install -e .

cp .env.example .env        # then fill in the four values (see Configuration)

# Run the ingest — the discovery loop self-populates the tracked series set on
# boot; there is no manual allowlist step.
python -m simplex_ingest               # five loops start; GET :8080/health
```

The discovery loop sweeps all open Kalshi events hourly and admits each series
that passes structural + tradeability **predicates** — a partition (mutex event
≥3 markets) or hierarchy (event ≥2 distinct markets), gated by a volume floor —
then ranks and caps the tracked set, persisting it to the `tracked_series`
table. To inspect what it would track against the live catalog without writing
anything, run a one-shot dry run:

```bash
python -m simplex_ingest.loops.discovery   # prints admitted + rejected series
```

Run a one-time sanity check with `pip install -e ".[test]" && pytest`.

---

## Configuration

Two homes, deliberately separated:

**`.env` — secrets & deployment only (four values):**

| Variable | Meaning |
|---|---|
| `KALSHI_API_KEY_ID` | Kalshi API key ID (UUID) |
| `KALSHI_API_SECRET` | RSA private key (PEM); inline value or a path — the loader normalizes either |
| `KALSHI_ENV` | `prod` or `demo` |
| `SIMPLEX_DATA_DIR` | where the DuckDB file lives (`./data` local, `/data` in prod) |

**`src/simplex_ingest/constants.py` — every tuning knob** (intervals, depth band,
audit window/thresholds, rate limits, reconnect bounds, health port, log level),
each documented inline. No environment lookups; edit and redeploy to tune.

---

## Deployment

Containerized and built to run 24/7 (the persistent WebSocket means it must not
scale to zero). See **[`README-DEPLOY.md`](./README-DEPLOY.md)** for Railway
setup — secrets, the 10 GB volume at `/data`, disabling serverless, and the one
gotcha (`PORT=8080`). The image runs as a non-root user; SIGTERM is handled
cleanly. The same image suits any single-machine + mounted-volume host (Fly.io).

---

## Repository layout

```
src/simplex_ingest/
  app.py  __main__.py        # entry point: wire loops, signals, shutdown
  config.py  constants.py    # 4-value env surface  /  tuning knobs
  schema.sql  db.py          # DuckDB: shared conn, write lock, batched writes
  events.py  orderbook.py    # normalized events  /  in-memory book + depth + canaries
  subscriber.py              # BaseSubscriber (one new file per future exchange)
  runtime.py supervisor.py health.py util.py log.py
  discovery_predicates.py    # pure admit/rank predicates (no I/O)
  kalshi/    auth.py rest.py subscriber.py
  loops/     catalog.py websocket.py snapshots.py audit.py discovery.py
tests/                       # pytest suite: predicates, DB atomicity, loop
Dockerfile  entrypoint.sh  railway.json  README-DEPLOY.md
```

Adding another exchange later is a new `subscriber.py` implementing
`BaseSubscriber`; the loops and storage are venue-agnostic.

---

## Design notes

- **Kalshi specifics** (RSA-PSS auth, WS envelope, `seq` semantics, the bids-only
  binary order book where `yes_ask = 1 − best_no_bid`) were confirmed against
  Kalshi's live docs before implementing.
- **Depth within 3¢** (not top-of-book) is the headline liquidity signal — it
  distinguishes a thick book from a lone stale quote, the dominant false positive
  for any downstream coherence/arbitrage screen.
- **Predicate-based auto-discovery** replaced the hand-curated YAML allowlist and
  its weighted-score discovery script: binary admit/reject predicates separate
  "track this series" from "rank it," and the tracked set rotates itself hourly
  with no manual input.
- **Tests** cover the load-bearing new pieces — the pure predicate module
  (exhaustive boundary + property tests), the transactional `tracked_series`
  swap, and the discovery loop's behavior (cap, eviction, failure resilience).
  Run with `pip install -e ".[test]" && pytest`.
