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

Simplex is being built in stages. **Stages 1–3 are complete and running;** the
solver is the next stage.

| Stage | Scope | State |
|------|-------|-------|
| **1. Ingest** | Live Kalshi order books → normalized event log → 10s snapshot grid, with catalog discovery, book reconstruction, checkpointing, and a hourly REST reconciliation audit. | **Built** |
| 2. Deploy | Containerized, runs 24/7 on a mounted volume (Railway / Fly.io). | **Built** (`ingest/Dockerfile`, `ingest/railway.json`, [`docs/DEPLOY.md`](./docs/DEPLOY.md)) |
| **3. Extraction** | LLM layer over the catalog: per-market semantic representations + pairwise **typed relationship edges** (the market graph), trust-tiered (hard / soft / manual-review) for the solver. | **Built** |
| 4. Solver | Max-entropy joint distribution over related markets given snapshot marginals + the Stage-3 **edge constraints**; coherence/deviation scoring. | Planned |
| 5. Graph viz | Surface the largest incoherences over the edge graph and over time. | Planned |
| 6. Export | Continuous R2 export of `snapshots` / `raw_events` (+ the durable extraction tables) for offline analysis & backup. | Planned |
| — | Other exchanges (Polymarket, Manifold) via the `BaseSubscriber` seam. | Planned |

The solver and everything downstream of it are not built yet. Stage 1 produces
the clean, regular, gap-checked marginal time series the solver will consume;
Stage 3 produces the **structural** half — the typed relationship edges it will
combine with those marginals (and these LLM-derived tables, unlike the snapshot
grid, are **not** regenerable from `raw_events`).

---

## How it works

One long-running Python process, six supervised async loops sharing a single
embedded **DuckDB** file on a mounted volume (the ingest's five, plus the Stage-3
extraction loop):

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
- **Extraction loop** (every 5 min, Stage 3) turns each active market's
  description into a structured semantic record (`market_semantics`, cached
  forever), then classifies cheaply-picked candidate pairs into typed
  relationship edges (`market_edges`) — `same_event`, `implies`,
  `mutually_exclusive`, `partition_member`, `conditional`, `correlated`,
  `unrelated`. Edges enter at a **trust tier** matched to confidence: `trusted`
  (the solver's hard constraints, promoted only when an independent second model
  agrees), `soft`, or `review` (a manual-review queue). Needs an OpenRouter key;
  without one it idles and plain ingest runs unchanged.

`raw_events` is the source of truth; `snapshots` is a regenerable derived grid.
The Stage-3 `market_semantics`/`market_edges` are a third class — LLM-derived,
durable, and **not** regenerable from `raw_events`. A crash in any one loop is
restarted by a supervisor without taking the others down. SIGTERM flushes,
checkpoints, closes the DB, and exits 0. `/health` (port 8080) returns 200 only
when all six loops are alive.

### Data model (DuckDB — `ingest/src/simplex_ingest/schema.sql`)

| Table | Role |
|-------|------|
| `tracked_series` | The self-managed set of series to ingest, rewritten each discovery cycle (admit/rank/cap by predicates). Replaces the old YAML allowlist. |
| `markets` | Catalog; `subscribed` flags the active set. Rows are never deleted. |
| `raw_events` | Append-only normalized event log (snapshots, deltas, trades, lifecycle) with exchange sequence numbers. Source of truth. |
| `snapshots` | 10s materialized grid — the marginal time series the solver will read. |
| `book_state` | Serialized in-memory books for fast restart. |
| `audit_results` | Per-market book-vs-REST reconciliation outcomes. |
| `market_semantics` | Stage-3 per-market semantic cache (LLM-derived; non-regenerable). |
| `market_edges` | Stage-3 pairwise typed relationship graph, trust-tiered for the solver (LLM-derived; non-regenerable). |

---

## Quick start

Requires Python 3.13 and Kalshi API credentials (an API key ID + RSA private key).

```bash
cd ingest                   # the ingest service is self-contained under ingest/
python3 -m venv venv && source venv/bin/activate
pip install -e .

cp .env.example .env        # fill the 4 Kalshi values (see Configuration); OPENROUTER_API_KEY optional

# Run the ingest — the discovery loop self-populates the tracked series set on
# boot; there is no manual allowlist step.
python -m simplex_ingest               # six loops start; GET :8080/health
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

**`.env` — secrets & deployment only (five values: four required + one optional):**

| Variable | Meaning |
|---|---|
| `KALSHI_API_KEY_ID` | Kalshi API key ID (UUID) |
| `KALSHI_API_SECRET` | RSA private key (PEM); inline value or a path — the loader normalizes either |
| `KALSHI_ENV` | `prod` or `demo` |
| `SIMPLEX_DATA_DIR` | where the DuckDB file lives (`./data` local, `/data` in prod) |
| `OPENROUTER_API_KEY` | **optional** — enables the Stage-3 LLM extraction layer; absent, that loop idles |

**`ingest/src/simplex_ingest/constants.py` — every tuning knob** (intervals, depth band,
audit window/thresholds, rate limits, reconnect bounds, **LLM model ids /
confidence cutoffs**, health port, log level), each documented inline. No
environment lookups; edit and redeploy to tune.

---

## Deployment

Containerized and built to run 24/7 (the persistent WebSocket means it must not
scale to zero). See **[`docs/DEPLOY.md`](./docs/DEPLOY.md)** for Railway setup —
the service builds from `ingest/` (**Root Directory = `ingest`**, Watch Paths
`ingest/**`), secrets, the 10 GB volume at `/data`, disabling serverless, and the
one gotcha (`PORT=8080`). The image runs as a non-root user; SIGTERM is handled
cleanly. The same image suits any single-machine + mounted-volume host (Fly.io).

---

## Repository layout

The repo is a **monorepo**; the ingest service is self-contained under `ingest/`,
with `docs/`, `README.md`, and `CLAUDE.md` at the repo root. Future services
(`trader`, `viz`) land as siblings.

```
ingest/                      # the ingest service — self-contained, deployable
  Dockerfile  entrypoint.sh  railway.json  .dockerignore  .env.example  pyproject.toml
  src/simplex_ingest/
    app.py  __main__.py      # entry point: wire loops, signals, shutdown
    config.py  constants.py  # 4-value env surface  /  tuning knobs
    schema.sql  db.py        # DuckDB: shared conn, write lock, batched writes
    events.py  orderbook.py  # normalized events  /  in-memory book + depth + canaries
    reconstruct.py           # per-market order-book replay state machine
    subscriber.py            # BaseSubscriber (one new file per future exchange)
    runtime.py supervisor.py health.py util.py log.py
    discovery_predicates.py  # pure admit/rank predicates (no I/O)
    kalshi/    auth.py rest.py subscriber.py fixedpoint.py
    loops/     catalog.py websocket.py snapshots.py audit.py discovery.py
  tests/                     # pytest suite: predicates, DB atomicity, loop
docs/      ARCHITECTURE.md DEPLOY.md   # repo-level
README.md  CLAUDE.md
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
