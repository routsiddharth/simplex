# Ingest stabilization log — 2026-06-06/07

Handoff/working log for the WebSocket-reconnect-death-loop remediation, picking up
from [`LIVE-ISSUES-2026-06-06.md`](./LIVE-ISSUES-2026-06-06.md) (the audit) and
[`INGEST-FIX-PLAN.md`](./INGEST-FIX-PLAN.md) (the plan). This records **what
shipped, the live data we collected, what it proved, what's in flight right now,
and the next steps** — written to continue in a fresh session.

---

## 0. Current state at a glance

| | |
|---|---|
| **On `origin/main`** | `9529b6c` — everything below is pushed: Steps 1–4/6 + metrics (`0dae582`), audit-symmetry (`7a4be4a`), liquidity floor + bulk upsert (`64ea5f2`), cheap-model cost measure (`1044034`), docs (`9529b6c`). Working tree clean. |
| **Tests** | `240 passed, 4 skipped` (live smoke skipped) |
| **Live status (last measured ~21:58 UTC 06-06, on the `7a4be4a` build)** | **still flapping** (`reconnects` ~1/min, `keepalive ping timeout`); grid 30–80 s; `mean_sample_ms ≈ 25 000`. The floor+bulk fix (`64ea5f2`) had **not** deployed yet at that measurement. |
| **Verdict** | The plan's root-cause (drain bursts → yields cure it) was **wrong**. The real bottleneck is the **per-tick snapshot DB write** at ~5.5 k markets. The floor+bulk change targets the *actual* cause; **awaiting the redeploy to confirm.** |

**Immediate next action:** let `origin/main` deploy, wait ~30–45 min, then
re-measure `ws metrics reconnects` (should go flat) and `snapshot metrics
mean_sample_ms` (should be < ~3 000).

---

## 1. What shipped (commits on `main`)

### `0dae582` — ingest stability fixes (Steps 1–4, 6 + metrics)
- **Step 1 — yields.** Snapshot applier yields every `SNAPSHOT_APPLY_YIELD_EVERY=500`
  events; sampler yields every `SNAPSHOT_ROW_YIELD_EVERY=500` markets (between
  markets).
- **Step 2 — applier/sampler split.** `SnapshotBuilder.run()` runs a continuous
  **applier** (drains `raw_events`→books) + a 10 s **sampler** (reads books→grid).
  `tick()` kept as the synchronous full-cycle test entry point.
- **Step 3 — orjson** in the WS reader hot path (dep added to `pyproject.toml`).
- **Step 0/7 — metrics.** `snapshot metrics` (events/s, mean/last sample ms,
  books, active) and `ws metrics` (frames/s, reconnects, reconciles, resets),
  rolled up every `METRICS_LOG_INTERVAL_SECONDS=60`.
- **Step 6 — canary.** Out-of-range (0¢/100¢) levels excluded at apply time in
  `reconstruct.py`, logging the actual price once per market.
- **Step 4 — bounding (partial).** `MAX_ACTIVE_MARKETS=6000` ceiling (greedy by
  volume) + `catalog volume distribution` logging. Floor left at 0 pending data.

### `7a4be4a` — audit-symmetry fix (Step 6 follow-up)
Step 6 dropped 0¢/100¢ levels from the **in-memory** book but the hourly audit
still decoded them from REST (`level_map`) → permanent structural diff →
`audit large diff; resetting book` every hour for primaries. Moved the band
predicate into the decode home (`kalshi/fixedpoint.in_tradeable_band`), applied to
**both** the WS apply path and the REST audit decode.

### `64ea5f2` — liquidity floor + bulk upsert
- `CATALOG_MIN_MARKET_VOLUME` **0 → 100** (set from the live distribution below).
- `db.upsert_snapshots`: multi-row `INSERT … ON CONFLICT` instead of row-by-row
  `executemany` (~2.4× faster, dependency-free).
- Tests: liquidity-floor exclusion; existing catalog tests given above-floor
  volume. Docs updated.

### Cheapest-model cost measure (2026-06-07)
Temporary, to hold LLM spend near zero during stabilization (the Anthropic batch
discount is unavailable — no account credits — so everything degrades to
full-price sync OpenRouter). The three **OpenRouter sync** model ids in
`constants.py`:
- `EXTRACTION_MODEL` / `PAIR_MODEL` → `deepseek/deepseek-v4-flash-20260423`
  (~$0.10/M in, structured-output ✓ — near-cheapest reliable on OpenRouter).
- `PAIR_VERIFY_MODEL` → `google/gemini-3.1-flash-lite-20260507` — a *different
  family* (kept distinct so the trust gate stays an independent cross-check, not
  same-model self-agreement, which would flood the `trusted` tier).
- `BATCH_*` (Anthropic-direct) ids **unchanged** (different provider/host; failing
  on credits regardless of model).

**Restore** `anthropic/claude-sonnet-4.6` (extraction + pair primary) and
`anthropic/claude-opus-4.8` (verify) when extraction quality matters again. Slugs
confirmed live against `https://openrouter.ai/api/v1/models` on 2026-06-07; re-confirm
at deploy time (dated slugs can be retired). Quality risk: a cheap model may raise
the `pair classification skipped` (invalid-JSON) rate — acceptable "for now".

---

## 2. Live data collected (the evidence)

All from `railway logs` against `simplex-production` (KALSHI_ENV=prod,
`api.elections.kalshi.com`), 06-06 ~21:48–21:58 UTC, **on the `7a4be4a` build**
(confirmed live: `ws metrics` / `snapshot metrics` / `catalog volume distribution`
only exist in the new code).

### 2a. WebSocket — STILL FLAPPING
```
ws metrics    reconnects 30 → 35 over ~7 min  (~1/min, unchanged from pre-fix)
              frames_per_s 3.4–63.8   reconciles=0   resets 2334→2355
ws connection error … reason='keepalive ping timeout' (CloseCode 1011)  ×11
ws initial subscribe ×12      ws reconciled = 0  (incremental path still never runs)
```
The yields did **not** stop the keepalive timeouts.

### 2b. Snapshot — the real bottleneck
```
snapshot metrics   mean_sample_ms ≈ 25 000  (last 24 978–31 018)
                   events_per_s = 0–30      frames_per_s = 3–63
                   active ≈ 5527            books ≈ 5554
grid timestamps:   53:40 → 54:10 → 54:50 → 56:10   (Δ 30 s, 40 s, 80 s — never 10 s)
```
`events_per_s`/`frames_per_s` are **tiny** → drain/parse were never the
bottleneck. The **sampler takes ~25 s** to build+write the grid for ~5.5 k
markets → loop stays saturated → keepalive starves → reconnect.

### 2c. Catalog volume distribution (the Step 4 data)
```
catalog volume distribution   n=5527  p50=112.5  p90=6269  p99=109842  max=4.47M
  lt_1=1825   lt_10=2000   lt_100=2600   lt_1000=3987
```
- 1 825 markets (33 %) have **< 1** contract of volume — dead.
- 2 600 (47 %) < 100 · 3 987 (72 %) < 1 000.
→ A floor of **100** drops the ~47 % dead tail while keeping anything with real
activity. (Chosen over 10 = too timid, 1000 = risks gutting primary partitions
the coherence solver needs.)

### 2d. Local benchmarks (why the write is the cost, and how far bulk gets us)
Validated on a tmp DuckDB at production scale (5 527 rows):
```
row-build (top_of_book+depth+canaries) ×2927 ........   19 ms   (negligible)
snapshot write — executemany (old) ..................  10 476 ms
snapshot write — multi-row VALUES (new, bulk) .......   4 257 ms  (~2.4×)
snapshot write — columnar UNNEST ....................   4 171 ms  (no better)
pure-DuckDB generate (no Python params) .............      12 ms
```
**Finding:** parameterized bulk insert bottoms out at **~0.77 ms/row** regardless
of method (VALUES = UNNEST = executemany-per-row, all converge) — it's the
Python→DuckDB value-transfer cost. Only a **columnar dep (pandas/pyarrow,
zero-copy)** beats it (would be ~100–200 ms), deliberately **deferred** to keep
the image lean. With floor=100 (~2 927 rows) the bulk write is **~2.2 s**; the
row-build is ~20 ms → total sampler ~2–3 s, well under the 10 s budget.

### 2e. Other findings
- **Batch path (P2-1):** `ANTHROPIC_API_KEY` is set but Anthropic returns
  `400 … "Your credit balance is too low … purchase credits"` (`batch submit
  failed`, n=302 semantics + n=1000 pair_primary). **Billing issue, not code** —
  the account has no credits. *Verify the batch-submit failure falls back to sync
  rather than dropping those items each cycle.*
- **P2-2 still present:** `pair classification skipped` recurs (model appends prose
  after JSON). Untouched — out of the stability scope.
- **Step 6 working:** zero `out_of_range_price` canary spam and zero
  `dropping out-of-range level` in the window (markets either fixed or not active).

---

## 3. What we're doing right now

Shipping the **two fixes the live data pointed to** (uncommitted, prepared):
1. **Liquidity floor 0 → 100** — cut the ~47 % dead-weight markets.
2. **Bulk snapshot upsert** — the per-tick write was the ~25 s cost; multi-row
   INSERT is ~2.4× faster, no new dep.

Expected effect: sampler write ~25 s → ~2–3 s, grid back to a clean 10 s, loop no
longer saturated → keepalive should survive → **flapping should stop**. This is a
hypothesis to confirm on the next deploy.

---

## 4. Next steps (in order)

1. **Push the uncommitted commit** (`git push origin main`) and let it deploy.
2. **Re-measure** after ~30–45 min up:
   - `ws metrics reconnects` — should go **flat** (~0/hr). This is the pass/fail.
   - `snapshot metrics mean_sample_ms` — should be **< ~3 000**.
   - `snapshots emitted` timestamps — a clean **10 s** apart.
   - `catalog refreshed active_markets` — should drop to ~2 900 (floor cut).
   - `catalog volume distribution` — re-read; re-tune the floor if needed.
3. **If flapping stops** → Steps 1–4/6 are sufficient; **Step 5 not needed**.
4. **If `mean_sample_ms` still high but markets are bounded** → consider the
   columnar-write escalation (pyarrow) before sharding.
5. **If flapping persists with a fast sampler** (loop not saturated but still
   timing out) → the firehose genuinely exceeds one core's parse budget →
   **implement Step 5** (below), or the Step 8 ceiling.
6. **P2-1:** add Anthropic credits (their account) if the discounted batch path is
   wanted; confirm sync fallback on submit-failure meanwhile.
7. **P2-2 (optional):** tolerant JSON parsing / one-shot reformat retry in Stage B.

---

## 5. Step 5 — WS sharding (deferred, designed)

**Status:** deferred by decision, pending the §4 re-measure. The plan sequences it
"after Steps 1–2, only if reconnects still occur." The §2 data shows reconnects
*do* still occur — **but** because of the sampler write (now being fixed in §3),
not raw WS parse load (`frames_per_s` is low, ~45). So sharding is likely
**unnecessary**; confirm after the floor+bulk deploy before building it.

**Why it might still be needed:** a single connection carries the whole
orderbook-delta firehose; any drop re-anchors *every* book (`ws initial subscribe`
re-subscribes all ~5.5 k, `reconciles=0`). If, after §3, the loop is no longer
saturated yet the connection still drops, that points to genuine per-connection
throughput limits.

**Design (when/if built):**
- Run **N** WS connections (shards) as tasks on the one event loop, orderbook
  subscriptions partitioned **by series** (a series's markets stay on one shard so
  its `seq` streams are co-located).
- All shards feed the **single in-process `raw_events` writer** — preserves the
  DuckDB single-writer invariant (#2). No second process.
- Route **resets** and **reconciles** to the owning shard (the reset queue and the
  catalog resubscribe signal need a shard index: market → shard).
- A drop then re-anchors **one shard's** books, not all.
- **Open decision (architect):** shard count `N`. Size it from the post-§3
  `ws metrics frames_per_s` per shard; default ~4, documented as tunable. This is
  the one genuinely unspecified knob.
- **Ceiling (Step 8, document-only):** if one core still can't parse the firehose
  at the desired market count, the only invariant-safe path to multi-core is
  reader *processes* parsing in parallel, handing frames over IPC to one writer
  process. Don't build until forced.

The applier/sampler split + bulk-write + liquidity floor were added to
`ARCHITECTURE.md` (snapshot builder + catalog + WS scaling note); the sharding
design above is the WS "scaling note" §11 reservation.

---

## 6. Files touched (for the next session)

- `ingest/src/simplex_ingest/loops/snapshots.py` — applier/sampler split, yields, metrics.
- `ingest/src/simplex_ingest/loops/websocket.py` — orjson, ws metrics.
- `ingest/src/simplex_ingest/loops/catalog.py` — MAX_ACTIVE_MARKETS ceiling, volume-distribution log.
- `ingest/src/simplex_ingest/reconstruct.py` — out-of-range exclusion (uses shared predicate).
- `ingest/src/simplex_ingest/kalshi/fixedpoint.py` — `in_tradeable_band`, `level_map` filter.
- `ingest/src/simplex_ingest/db.py` — bulk `upsert_snapshots` *(uncommitted)*.
- `ingest/src/simplex_ingest/constants.py` — new constants; `CATALOG_MIN_MARKET_VOLUME=100` *(uncommitted)*.
- Tests: `test_snapshot_replay.py`, `test_reconstruct.py`, `test_fixedpoint.py`,
  `test_audit_diff.py`, `test_catalog_loop.py`, `test_catalog_reads_db.py`.
- Docs: `ARCHITECTURE.md`, `DEPLOY.md`, `END-TO-END.md`, this log.
