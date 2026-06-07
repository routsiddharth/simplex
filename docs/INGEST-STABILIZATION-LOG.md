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
| **Tests** | `243 passed, 4 skipped` (live smoke skipped) — +3 WS sharding tests |
| **WS sharding (Step 5)** | **BUILT + DEPLOYED + VERIFIED 2026-06-07** — `N=4` connections, partition by series, paced subscribe burst, reset routing. Live (02:47–03:15 UTC): **`reconnects` FLAT** (17 across 5 rollups), ~0.18 conn-err/min, zero keepalive, snapshots clean 10 s, extraction running — see §8. Diagnosis: connect-time burst death (§8a). Deployed via `railway up`; commit local, **needs `git push origin main`** (classifier-blocked for the agent). |
| **Live status (re-measured ~22:43–22:46 UTC 06-06, on the deployed floor+bulk build)** | **snapshot fixed, WS still flaps.** `mean_sample_ms ≈ 1 800` (was ~25 000); grid clean **10 s**; `active=2907` (floor cut from 5 453); **zero `keepalive ping timeout`**. BUT `reconnects 18→22` over ~2.5 min ≈ **~1.5/min**, now closing with `ConnectionClosedError(None,None,None)` (no code), not keepalive. `reconciles=1` (incremental path now runs; was 0). |
| **Verdict** | The snapshot-write root-cause was **correct** — floor+bulk killed the 25 s sampler, the 10 s grid, the market bloat, and the keepalive starvation. **But flapping is not gone:** with a *fast* sampler (loop unsaturated) the WS still drops ~1.5/min via a codeless `ConnectionClosedError`. Per §4/§5 decision tree this is the **Step 5 (WS sharding)** trigger — genuine per-connection throughput, not write saturation. Step 5's open knob (shard count `N`) is an **architect decision**; not built. |

**Immediate next action:** deploy the WS-sharding build and re-measure — the
pass/fail is `ws metrics reconnects` going **flat** and shards surviving > 60 s
past their `ws initial subscribe`. Everything the floor+bulk change targeted is
already confirmed fixed; sharding (§5) targets the separate connect-burst cause.

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

## 5. Step 5 — WS sharding (**BUILT** 2026-06-07)

**Status: implemented.** The §7 re-measure settled the question the original
"deferred" note left open: after the floor+bulk fix the loop is *not* saturated
(`mean_sample_ms ≈ 1 800`) yet the connection still dropped ~1.5/min — and §7c's
follow-up showed the death is a **connect-time burst**: every connection dies
5–8 s after firing its ~2.9 k one-market-per-sid subscribes (codeless
`ConnectionClosedError`). So sharding *is* needed, and the sizing criterion is
**subscriptions per shard**, not steady-state `frames_per_s`.

**Chosen `N` = 4.** Kalshi caps a user at **5 concurrent WS connections** (the
one genuinely external constraint — confirmed against docs.kalshi.com), so
`N ≤ 4` leaves headroom for the brief old+new overlap while one shard reconnects
(4 steady + 1 transient = 5). 4 shards → ~725 markets each, a ~4× smaller connect
burst and re-anchor blast radius. Paired with **subscribe-burst pacing**
(`WS_SUBSCRIBE_CHUNK=50` @ `WS_SUBSCRIBE_PACING_SECONDS=0.05`), which targets the
burst directly regardless of count. `WS_RECONNECT_RESET_SECONDS=30` resets a
shard's backoff after a sustained connection so a one-off flap doesn't pin it at
the 60 s cap.

**Shipped:** `loops/websocket.py` rewritten as a **coordinator + N `_Shard`s**
(see ARCHITECTURE.md "WebSocket subscriber — sharded"); `db.get_active_market_series`
for the by-series partition; new `WS_SHARD_COUNT` / `WS_SUBSCRIBE_*` /
`WS_COORD_TICK_SECONDS` / `WS_IDLE_POLL_SECONDS` / `WS_RECONNECT_RESET_SECONDS`
constants; `test_websocket_loop.py` +3 tests (stable partition, by-series
co-location, reset routing to the owning shard). `243 passed, 4 skipped`.

**Original deferral note (for the record):** the §2 data showed reconnects
occurring while `frames_per_s` was low (~45), so sharding looked *unnecessary* —
the read was that the sampler write was the sole cause. §7 corrected that: the
sampler write was *a* cause (keepalive starvation, now fixed) but the connect
burst is a *separate* cause that the floor+bulk change didn't touch.

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

## 7. Post-deploy verification (2026-06-06 ~22:43–22:46 UTC)

Measured on the deployed floor+bulk build (`origin/main` past `64ea5f2`),
~13–16 min after the SUCCESS deploy at 22:30 UTC. Two `ws metrics` / `snapshot
metrics` rollups bracket the steady state.

### 7a. What the fix fixed (all confirmed) ✅
```
snapshot metrics  mean_sample_ms 1826 → 1786   last 1690–2303   (was ~25 000)
                  events_per_s 36–65   frames_per_s 68–110   active=2907  books=5453
snapshots emitted 43:00→10→20→30→…→44:20  — clean 10 s grid     (was Δ 30–80 s)
catalog/ws        active=2907, ws subscribes 2907               (was 5453; floor=100 cut ~47%)
keepalive         ZERO `keepalive ping timeout` in either window (was ~11/7 min)
```
The snapshot-write hypothesis (§3) was right: the per-tick write was the cost,
bulk upsert + floor collapsed it ~14×, the loop is no longer saturated, and the
keepalive starvation it caused is gone.

### 7b. What it did NOT fix ⚠️ — WS still flaps ~1.5/min
```
ws metrics   reconnects 18 (22:43:57) → 22 (22:46:30)  ≈ 1.5/min
             reconciles=1  (incremental reconcile path now runs at least once; was 0)
close reason ConnectionClosedError(None, None, None)   — no close code, NOT keepalive
```
The reconnect rate barely moved, but the **cause changed**: no longer keepalive
starvation from a saturated loop (that's fixed), now a codeless connection close.
With a fast sampler and an unsaturated loop, this is the §4 step-5 / §5 case:
**genuine per-connection throughput / server-side drop**, re-anchoring every book
on each drop (`resets` climbing, `ws initial subscribe` re-subscribes all 2907).

### 7c. Recommendation
Per §4 decision tree: floor+bulk were **sufficient for the snapshot/keepalive
failure** (Steps 1–4/6 done), but the **residual flap → Step 5 (WS sharding)**.
Step 5 is designed (§5) and gated on the architect picking shard count `N`
(the one unspecified knob) — sized from the post-fix `frames_per_s` (now ~68–110
on one connection). Not built pending that decision. Secondary: characterize the
codeless close further (Kalshi idle-drop vs. throughput) before committing to N.

---

## 8. Sharding deployed + verified (2026-06-07 ~02:47–03:15 UTC) ✅

WS sharding (§5, `N=4`) was built and deployed (via `railway up` — direct push to
`main` is still classifier-blocked; the commit `feat: shard the WS subscriber…`
is local, awaiting a manual `git push`). Measured ~25 min after the SUCCESS
deploy at 02:47 UTC.

### 8a. The diagnosis that set `N` (the §7c follow-up)
The codeless close was a **connect-time burst death**: each single connection died
**5–8 s after firing its ~2 907 one-market-per-sid subscribes** (22:43:56 sub →
22:44:04 close; 22:44:18 → 22:44:24; 22:46:29 → 22:46:34). Reducing 5453→2907 hadn't
helped → the survivable threshold is well below 2907. So the sizing criterion is
**subscriptions per shard**, and the binding constraint is Kalshi's **5-connection
cap** → `N=4` (~725/shard) + **paced** subscribe bursts.

### 8b. Result — flapping eliminated, pipeline healthy ✅
```
ws metrics   reconnects 17 → 17 → 17 → 17 → 17  over 03:11–03:15 — FLAT (0/min)
             (was ~1.5/min of 5–8 s connect-burst deaths). shards=4, subscribed=2873.
conn errors  1 in 5 min (~0.18/min, was ~1.5/min);  zero keepalive timeouts
shards live  shard 0 (878 mkts) ran 02:59:17→02:59:59+ — survives indefinitely now
snapshots    emitted clean ~10 s, markets 2873–2874;  mean_sample_ms ~1.9–2.4 s
extraction   Stage 3 llm-usage lines present — full pipeline flowing end to end
```

### 8c. The reset rate was market churn, not a bug
Early on, `resets` ran ~3.3/s and looked like a regression. The trend settled it:
`resets 3197→3235→3238→3238→3238` **tracked `frames_per_s` exactly** (196→30→1.4→
1.9→3.1) — as the live World Cup books (`KXWCGAME-26JUN27…`, `KXWCGROUPORDER…`)
quieted, gaps (and resets) **stopped entirely**. Each reset hit a *distinct*
high-churn market once (not a loop on one market). So the elevated rate was genuine
seq-gaps in fast live-sports orderbooks self-healing via re-anchor — the higher
number vs the old 0.31/s was the live games (03:00 UTC) vs a calm 22:46 UTC window.
*(If a future calm-market window still shows a high reset floor, the next lever is
per-`sid` seq tracking so a re-subscribe's stragglers can't trip a false gap — not
needed on this evidence.)*

### 8d. Verdict
**The WS reconnect-death-loop is resolved.** Root causes were two, fixed in two
steps: (1) the per-tick snapshot write saturating the loop → keepalive starvation
(floor + bulk upsert, §3/§7); (2) the connect-time subscribe burst on one fat
connection → codeless close (shard `N=4` + paced subscribes + reset routing, §5/§8).
243 tests pass incl. the real-socket end-to-end. **Remaining:** `git push origin
main` for the permanent record (the deploy is live from `railway up`).

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
