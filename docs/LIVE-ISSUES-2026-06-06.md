# Live deployment audit — 2026-06-06

Audit of the running Railway deploy (`simplex-production-f4ee.up.railway.app`,
project `simplex` / env `production` / service `simplex`) against
[`END-TO-END.md`](./END-TO-END.md). Evidence is from the live `/health` endpoint,
`railway logs`, and `railway variables`, cross-checked against the source.

## TL;DR

- **The document is architecturally accurate.** Every loop, cadence, constant,
  the spend gate, the provider seam, and the storage/retention model described in
  `END-TO-END.md` match the code as deployed. Data *does* flow front-to-back:
  `raw_events` → `snapshots` (markets=5448) → `market_semantics` (Stage A) →
  `market_edges` (Stage B, `edge classified … tier="trusted"`).
- **But the live system is materially degraded relative to "as intended."** The
  WebSocket connection is flapping roughly once a minute, the snapshot grid is
  running at ~30–80 s instead of the specced 10 s, ~94 markets are stuck in a
  permanent book-reset loop, and the 50 %-discount batch path is not active.
- **`/health` returns `200 {"healthy": true}` for all six loops** — this is
  *misleading*: the WS loop heartbeats on every reconnect and every message, so it
  reads "alive" even while flapping continuously. Green health ≠ healthy data.

Severity legend: **P0** = corrupts/blocks the core data product; **P1** =
significant degradation; **P2** = cost/robustness; **note** = informational.

---

## P0-1 — WebSocket reconnects ~every 45–60 s; steady state never reached

**Evidence.** Eight full `ws initial subscribe` events in ~6 minutes:

```
16:19:15, 16:20:22, 16:21:09, 16:21:36, 16:22:24, 16:23:26, 16:24:17, 16:25:36
```

Each is preceded by a `ws connection error … reason='keepalive ping timeout'`
(`CloseCode.INTERNAL_ERROR: 1011`) — observed at 16:22:58 and 16:24:43 in the raw
stream. After each reconnect the loop re-subscribes **all ~5448 markets** from
scratch (`_subscribe_initial`), which immediately produces a cascade of
`sequence gap; resetting book` warnings (e.g. `got=4389 expected=1865`) as every
market re-anchors.

**Consequence.**
- `ws reconciled` (the incremental add/remove steady-state path,
  `websocket.py:210`) fired **0 times in the last hour**, even though the active
  set changed (`active_markets` 5448 → 5376 across two catalog refreshes). The
  connection never lives long enough to do an incremental reconcile — it only ever
  does full `initial subscribe`. The incremental path is effectively dead code in
  production.
- In-memory books are perpetually re-anchoring, so they rarely reach the steady
  state the snapshot grid and the audit loop assume.

**Root cause (likely).** A single WS connection carries ~5448 one-market-per-sid
`orderbook_delta` subscriptions plus two bulk channels. The `_reader` coroutine
(`websocket.py:109`) parses and buffers the entire firehose on the event loop;
under that load the `websockets` keepalive ping/pong isn't serviced within
`WS_PING_TIMEOUT_SECONDS=10`, so the connection is torn down with 1011. It then
re-subscribes all 5448 markets, re-saturates, and repeats. See also P1-1 (the
market count that drives this).

**Fix directions** (architectural — confirm with the architect before acting):
- Shard the orderbook subscriptions across multiple WS connections (e.g. N
  connections each carrying ~M markets) so no single reader is saturated.
- Offload JSON parse / buffering off the event-loop hot path, or raise WS ping
  tolerance only as a stopgap (does not address the underlying saturation).
- Bound the *market* firehose, not just the series count (P1-1).

---

## P1-1 — Market cardinality is unbounded by the series cap

**Evidence.** `discovery cycle complete … tracked=30 admitted=1868 series_seen=2693`
and `catalog refreshed … active_markets=5448 series=30`. `MAX_TRACKED_SERIES=30`
is doing its job, but 30 election-primary series (e.g. `KXCAPRIMARY`,
`KXFLPRIMARY`, `KXMDPRIMARY`, plus World-Cup/friendly-game series) expand to
**~5448 active markets**.

**Consequence.** The architecture bounds the WS firehose *by series count*, on the
implicit assumption that a series is a modest handful of markets. High-cardinality
series (primaries have hundreds of candidate sub-markets each) break that
assumption and produce the load that drives P0-1, the slow snapshot tick (P1-2),
and the audit-reset volume (P1-3).

**Fix directions.** Add a per-series and/or global *market* ceiling alongside
`MAX_TRACKED_SERIES`, or rank/evict by market cardinality, or apply a real
liquidity floor (`CATALOG_MIN_MARKET_VOLUME` is `0.0` today, so every open market
in a tracked series is admitted). Whatever the choice, `ARCHITECTURE.md` §discovery
should document the market-count bound, not only the series bound.

---

## P1-2 — Snapshot grid is ~30–80 s in production, not the documented 10 s

**Evidence.** Consecutive `snapshots emitted` grid timestamps over 10 minutes:

```
16:19:20  16:20:40  16:21:10  16:21:40  16:22:30  16:23:00  16:23:50
16:24:20  16:25:10  16:25:40  16:27:00  16:27:30  16:28:00  16:28:50  16:29:20
```

Gaps are 30 s, 50 s, 80 s — **never 10 s**. Intermediate 10 s boundaries are
skipped entirely (data gaps in the grid).

**Root cause.** `SnapshotBuilder.tick()` (`loops/snapshots.py:67`) synchronously,
per cycle: reloads the active set, seeds new markets, drains the `raw_events`
firehose, computes window volumes, then builds and upserts **one row per active
market (5448)**. When a tick exceeds `SNAPSHOT_INTERVAL_SECONDS=10`, the next
boundary is computed from `now` after the slow tick (`snapshots.py:52`), so the
loop skips the boundaries it ran past.

**Why it matters.** `constants.py` (`SNAPSHOT_INTERVAL_SECONDS`) and the
extraction/solver design state *"the downstream coherence solver was specced
against a 10 s grid."* Stage 4's core input is silently 3–8× coarser and gappy.

**Fix directions.** Speed up the tick (batch the DB write better, decouple event
drain from grid emit, avoid full-set rebuild each tick), or formally re-spec the
grid cadence and update `constants.py` + the solver assumption. Either way, do not
leave the documented 10 s diverging from a live ~45 s.

---

## P1-3 — ~94 markets stuck in a permanent `out_of_range_price` reset loop

**Evidence.** Over 15 minutes, ≥500 `canary tripped; resetting book
issues=["out_of_range_price"]` warnings across **94 distinct markets**:

```
68 × KXCAPRIMARY   24 × KXMDPRIMARY   2 × KXFLPRIMARY
```

These same California/Maryland primary markets trip the canary **every snapshot
tick** and are reset, re-anchored, and trip again — they never produce a valid
book.

**Root cause (to confirm).** `OrderBook.check_canaries` (`orderbook.py:116`) trips
`out_of_range_price` when any resting level is `< CANARY_PRICE_MIN_USD` (1¢) or
`> CANARY_PRICE_MAX_USD` (99¢). The tight clustering in two primary series points
to a market-class-specific cause: either these markets legitimately carry a level
at 0¢/100¢ (which the canary then rejects forever), or a fixed-point/parse quirk
for this ticker shape. Because the canary forces a reset, these markets are in an
infinite reset loop and contribute no usable snapshot data.

**Fix directions.** Capture one offending raw level for a `KXCAPRIMARY-*` market
and decide: relax the canary band (allow 0¢/100¢ resting levels), fix the decode,
or exclude such levels rather than discarding the whole book. Add a test for the
offending ticker shape.

---

## P1-4 — High `audit large diff` book-reset rate

**Evidence.** During the hourly audit sweep, 38–63 `audit large diff; resetting
book` warnings per sampled minute, with structural-mismatch counts up to
`structural=87` (and several `structural=0, max_pct≈40` size-drift escalations).

**Assessment.** A large fraction of audited books structurally disagree with the
REST orderbook by many levels — consistent with P0-1: books are perpetually
re-anchoring after each WS reconnect, so the audit frequently catches them
mid-rebuild and forces yet another reset, which re-subscribes and adds more
firehose load. This is likely a *symptom* of P0-1 rather than an independent fault,
but it compounds the load. Re-evaluate once P0-1 is fixed; if it persists with a
stable connection, it indicates a genuine reconstruct/decode divergence.

---

## P2-1 — The 50 %-discount batch path is inactive in production

**Evidence.** `railway variables` shows `OPENROUTER_API_KEY` set but **no
`ANTHROPIC_API_KEY`**. A routing log reads `pair routing … candidates=1225
skip=0 sync=0` with the remainder routed to batch, yet there were **zero
`batch submitted` / `batch reconciled`** events in 2 h — i.e. every batch-routed
pair degrades to full-price synchronous OpenRouter (the documented fallback,
`extraction.py:484`).

**Assessment.** This matches `END-TO-END.md`'s "batch seam is optional" framing, so
it is not a *correctness* bug — but it means the headline savings of the LLM cost
migration (flat 50 % off on long-horizon pairs) are **not being captured live**,
and with a 5448-market catalog the long-horizon pair volume is large (1225
candidates in a single cycle). If the discount is intended to be on in production,
set `ANTHROPIC_API_KEY`. If it is intentionally off, note that in `DEPLOY.md` so the
absent batch logs aren't mistaken for a fault.

---

## P2-2 — Stage B pairs occasionally dropped on invalid model JSON

**Evidence.** `pair classification skipped err="response was not valid JSON: Extra
data: line 8 column 1 (char 401)"`. The model (Sonnet primary) sometimes appends
text after the JSON object; the loop logs-and-skips that pair (per the soft-fail
design).

**Assessment.** Not fatal — the cycle continues — but each skip is an edge that
silently never gets built (and won't be retried unless the pair is re-selected).
Consider tolerant parsing (extract the first JSON object / strip trailing prose)
or a one-shot reformat retry before dropping.

---

## Note — `KALSHI_ENV=prod`

The deploy runs against the **production** Kalshi environment
(`api.elections.kalshi.com` confirmed in logs), whereas `.env.example` defaults to
`demo`. Expected for a live deploy; flagged only so it's a conscious choice.

---

## What is healthy / correct

- All six loops heartbeat; `/health` = `200`.
- Discovery self-manages the tracked set (predicates, cap=30) and completes cycles
  cleanly (`discovery cycle complete`).
- Catalog expands series → markets and refreshes on cadence (`catalog refreshed`).
- Snapshots are emitted for the full active set (`markets=5448`) — modulo the
  cadence problem in P1-2.
- Stage A semantics extraction runs (`semantics extracted`, `llm usage … mode="sync"
  purpose="stage_a"`) and Stage B produces typed, trust-tiered edges
  (`edge classified … tier="trusted" rel="unrelated"`).
- All `constants.py` values match what `END-TO-END.md` documents (audit depth 100,
  REST 4/s, batch size 50, model tiers, confidence thresholds, gate floors, prune
  windows, batch token cap).

## Suggested priority order

1. **P0-1 / P1-1** together — bound the market firehose and shard/relieve the WS
   connection so it stops flapping. This is the keystone fix; P1-3 and P1-4 are
   partly downstream of it.
2. **P1-3** — unstick the 94 primary markets (decide canary vs decode).
3. **P1-2** — restore the 10 s grid (or re-spec it) so Stage 4 gets its specced
   input.
4. **P2-1 / P2-2** — enable the discount path if intended; harden Stage B parsing.
