# Simplex — end-to-end, as built

Current-state snapshot of what the system does, front to back, **after** the LLM cost
migration ([`LLM-COST-MIGRATION.md`](./LLM-COST-MIGRATION.md)) and the monorepo migration.
This is a navigable overview; [`ARCHITECTURE.md`](./ARCHITECTURE.md) and
[`DEPLOY.md`](./DEPLOY.md) remain the source of truth for depth and ops.

## What it is

**Simplex** is a real-time probabilistic coherence engine for Kalshi prediction markets.
**Stages 1–3 are built and running:** one Python process, six supervised async loops, an
embedded DuckDB on a mounted volume. It ingests live market data and builds an LLM-extracted
**graph** — per-market semantics plus a trust-tiered, typed relationship-edge graph — that a
future solver (Stage 4) will read as constraints against live prices to surface
incoherences. The solver and everything downstream (graph viz, R2 export, LLM-in-the-loop
trading) are **planned, not built**.

Monorepo layout: the ingest service is self-contained under `ingest/` (its own `Dockerfile`,
`pyproject.toml`, `src/`, `tests/`); `docs/`, `CLAUDE.md`, `README.md` live at the repo root.

## The pipeline, end to end

### 1. Ingest (Stages 1–2) — six loops feeding DuckDB

| Loop | Cadence | Does |
|---|---|---|
| **discovery** | hourly | Self-manages the `tracked_series` set via predicates (no manual allowlist); prunes time-series tables to the retention window; reconciles market resolution against Kalshi (`resolved_at` ← `settlement_ts`) and prunes the LLM graph 1 h after a market resolves. |
| **catalog** | 5 min | Tracked series → the active (subscribed) market set, bounded by `MAX_ACTIVE_MARKETS` (kept highest-volume first) on top of the `MAX_TRACKED_SERIES` cap; logs the live volume distribution to tune `CATALOG_MIN_MARKET_VOLUME`. |
| **websocket** | persistent | Orderbook / trade / lifecycle deltas → `raw_events` (`orjson` decode). **Sharded** across `WS_SHARD_COUNT` (4) connections, partitioned by series (≤ Kalshi's 5-connection cap), each pacing its subscribe burst; a coordinator routes book-resets to the owning shard. All shards feed the one `raw_events` writer. |
| **snapshot** | 10 s | A continuous **applier** drains `raw_events` into the books (yielding, so it can't starve the WS keepalive); a 10 s **sampler** reads the current books → the `snapshots` grid. Books checkpointed to `book_state`. |
| **audit** | hourly | In-memory book vs REST orderbook reconciliation (sweeps every market at `AUDIT_ORDERBOOK_DEPTH=100`, `AUDIT_REST_CALLS_PER_SECOND=4`). Beats per-market mid-sweep so a large catalog keeps `/health` green. |
| **extraction** | 5 min | Catalog markets → `market_semantics` + trust-tiered `market_edges` via LLMs, spend-shaped (see below). Soft-fails/idles without `OPENROUTER_API_KEY`. |

All six are supervised by `supervisor.py`, each idling through `util.idle_sleep`
(heartbeat + early wake on shutdown). DB calls go through `asyncio.to_thread` (DuckDB is sync,
lock-serialized).

### 2. Storage & retention

- **DuckDB, single-writer, single-process**, on the `/data` volume (`SIMPLEX_DATA_DIR=/data`
  → `/data/simplex.duckdb`). This is *why* the deploy is one always-on instance — never scale
  out. A second writer (future trader) forces a storage split (Postgres OLTP).
- `raw_events` is the source of truth **within the retention window** — append-only, pruned to
  the last `DATA_RETENTION_CYCLES` discovery cycles (≈3 h). `snapshots`/`book_state`/
  `audit_results` prune on the same cadence; `snapshots`/`book_state` are regenerable from
  `raw_events` only *within* that window.
- The **LLM graph** (`market_semantics`/`market_edges`) is kept while a market is live and
  pruned `GRAPH_PRUNE_AFTER_RESOLVED_SECONDS` (1 h) after it **resolves** — a resolved market
  is terminal, so its graph is dead weight; deletion is safe (it never reopens → no re-spend).

### 3. Stage 3 extraction — the LLM graph builder

Two phases per cycle, both idempotent/resumable, capped at `EXTRACTION_BATCH_SIZE=50`:

**Phase A — per-market semantics** (`extract_market`). Each active market with no
current-version `market_semantics` row is distilled into a comparison-ready structure
(underlying event, resolves-yes/no conditions, timing, entities, dependencies). Cached
**forever** per market (until `EXTRACTION_PROMPT_VERSION` bumps). Its `entities` feed
candidate selection.

**Candidate selection** (`pair_candidates.py`). Cheap, structural — same `event_ticker`, same
`series_ticker`, or entity overlap — never all O(n²) pairs.

**Phase B — pairwise classification** (`classify_pair`). Each candidate pair is classified
into a typed relationship (`same_event`, `implies`, `mutually_exclusive`, `partition_member`,
`conditional`, `correlated`, `unrelated`) + direction + confidence, written to `market_edges`
with a trust tier:
- confidence ≥ `EDGE_TRUSTED_CONFIDENCE` (0.85) **and** an independent second model agrees on
  the relationship type → **`trusted`** (the solver's hard constraints);
- ≥ `EDGE_SOFT_CONFIDENCE` (0.6) → **`soft`**;
- otherwise → **`review`** (manual queue).

### 4. Spend shaping (the cost migration, now live in code)

The expensive part is Phase B, so spend is shaped two ways:

**Model tiering (cheap primary, premium gate).**
> **Temporary override (2026-06-07):** to hold spend near zero during ingest
> stabilization (and while the discounted Anthropic batch path is down for lack of
> account credits), the OpenRouter sync models are pointed at the cheapest reliable
> structured-output tiers — `EXTRACTION_MODEL` / `PAIR_MODEL` =
> `deepseek/deepseek-v4-flash-20260423`, `PAIR_VERIFY_MODEL` =
> `google/gemini-3.1-flash-lite-20260507` (kept a *different family* from the
> primary so the trust gate stays independent). Restore the Sonnet/Opus tiers below
> when extraction quality matters again. The `BATCH_*` (Anthropic-direct) ids run
> their own cost tiering — Haiku on Stage A + Stage B primary, Sonnet on the verify
> gate (re-tier 2026-06-07) — independent of these sync ids. The sync design intent
> is described as-designed below.
- `EXTRACTION_MODEL` (as designed) = `anthropic/claude-sonnet-4.6` — Stage A.
- `PAIR_MODEL` (as designed) = `anthropic/claude-sonnet-4.6` — Stage B **primary** (Sonnet runs on every
  pair; Opus was demoted off the primary — that was the cost driver).
- `PAIR_VERIFY_MODEL` (as designed) = `anthropic/claude-opus-4.8` — the **trust gate** only. The premium
  budget is spent *exclusively* re-checking high-confidence pairs before they become hard
  constraints — the highest-blast-radius decision. Different model from the primary, so
  agreement is genuine independence.

**Time-to-resolution gate.** `remaining_life = min(time-to-resolution of the two endpoints)`,
computed from `markets.closes_at` carried into the candidate path:
- `remaining_life < EDGE_REMAINING_LIFE_FLOOR_SECONDS` (24 h) → **don't spend at all** (an
  edge that can't outlive its payback window is negative-ROI). This floor bites even with no
  batch provider.
- `24 h ≤ remaining_life < EDGE_BATCH_THRESHOLD_SECONDS` (48 h) → **synchronous** (OpenRouter).
- `remaining_life ≥ 48 h` → **batch** (discounted Anthropic path).

### 5. The provider seam — sync (OpenRouter) + optional batch (Anthropic-direct)

- **Sync path** (`llm/client.py`, `OpenRouterClient`) — `POST {OPENROUTER_BASE_URL}/chat/
  completions`, rate-limited + retried. Latency-sensitive / short-horizon / critical-path work.
- **Batch path** (`llm/batch.py`, Anthropic-direct) — long-horizon, latency-insensitive work
  routed to the **Message Batches API**: flat **50% off**, completes within ~1 h. Uses dashed
  model ids — never cross the dotted/dashed spellings. `BATCH_EXTRACTION_MODEL` and
  `BATCH_PAIR_MODEL = claude-haiku-4-5` run Stage A + the Stage B primary on the cheap
  Haiku tier; the trust gate `BATCH_PAIR_VERIFY_MODEL = claude-sonnet-4-6` spends the
  stronger Sonnet *only* on the high-confidence band (re-tier 2026-06-07), and being a
  *different* model from the Haiku primary, a `trusted` edge still reflects two
  independent models agreeing rather than one model agreeing with itself. `BATCH_MAX_TOKENS=1500` is set because it's a mandatory
  Anthropic field (the sync path intentionally leaves `max_tokens` unset — see
  [`LLM-COST-MIGRATION.md`](./LLM-COST-MIGRATION.md) §2).
- The batch seam is **optional**: built only when `ANTHROPIC_API_KEY` is set. Absent it, the
  batch route degrades to sync — the edge is still built, just at full price.

## Configuration & secrets

Behavior lives in `constants.py`, not env. **Six** values come from `.env` (`config.py`):
the four Kalshi/deploy values + two optional secrets — `OPENROUTER_API_KEY` (unlocks Stage 3
sync) and `ANTHROPIC_API_KEY` (adds the discounted batch path). No other env knobs.

## Deploy

Railway, single always-on instance, Docker, healthcheck on `/health` (200 iff all six loops
heartbeat). DuckDB on the `/data` volume. See [`DEPLOY.md`](./DEPLOY.md).

## What is NOT built yet

- **Stage 4 — the solver** (in-process first): consumes `trusted`/`soft` edges as constraints
  against live prices to find incoherences. This is the next milestone and the first piece
  that produces something *actionable*.
- Downstream: graph viz, R2 export, LLM-in-the-loop trading, and the eventual `trader`
  service (which introduces the second writer → storage split). See `ARCHITECTURE.md` §11.

## Tests

`cd ingest && source venv/bin/activate && pytest` → 246 hermetic tests (units, loop behavior,
batch routing/reconcile, spend gate, full end-to-end over a local WS server). `--run-live`
adds 4 opt-in smoke tests against the Railway deploy.
