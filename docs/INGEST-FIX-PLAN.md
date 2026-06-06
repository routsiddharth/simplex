# Ingest stability fix plan — 2026-06-06

Remediation plan for the WebSocket reconnect death-loop and its downstream
symptoms, written against the **actual** code (not the earlier
[`FIX_PROPOSAL.md`](./FIX_PROPOSAL.md), whose root-cause diagnosis does not match
this codebase — see "Why the earlier proposal was off" below). Supersedes that doc.

Companion to the findings in [`LIVE-ISSUES-2026-06-06.md`](./LIVE-ISSUES-2026-06-06.md).

---

## Plain-language summary

The whole system is **one worker doing six jobs by taking fast turns**. Two of
those jobs got huge — listening to the price feed from ~5,447 markets, and
rebuilding a snapshot of all of them every 10 seconds in one burst. While the
worker is stuck on a giant job, it misses the "you still there?" handshake the
Kalshi connection needs every few seconds, so Kalshi hangs up. Reconnecting means
re-subscribing to all 5,447 markets — another giant job — so it misses the
handshake again. That's the death loop, ~once a minute.

The fix is to **make the giant jobs pausable** (do a little, answer the handshake,
do a little more) and to **stop rebuilding everything from scratch** every tick.
We keep the complete price recording (`raw_events`) as the source of truth and
rebuild from it exactly as today — we only change how the worker paces itself.

---

## Current state (live, 2026-06-06 ~19:05 UTC)

| Metric | Value | Target |
|---|---|---|
| Tracked **series** (topics) | **30** (`MAX_TRACKED_SERIES`) | n/a |
| Series scanned / admitted by predicates | 2,699 / 1,870 | n/a |
| Active **markets** (post fan-out) | **~5,447** | bounded |
| WS full reconnects | ~1 / minute | ~0 |
| Snapshot grid cadence | ~30–80 s | 10 s |
| Markets stuck in reset loop | ~94 (CA/MD primaries) | 0 |

---

## Root cause (corrected)

**One process runs all six loops on a single asyncio event loop. Two loops do long
synchronous (non-yielding) CPU bursts that monopolize that loop for longer than the
10 s WebSocket ping deadline, so the keepalive handshake is missed and Kalshi closes
the connection.**

The two offenders:

1. **The snapshot tick** (`loops/snapshots.py:67` `tick`). Every 10 s it
   synchronously: drains up to 5,000 buffered events and applies them to the books
   (`_drain_events`, `:155`), then builds **one row per active market (~5,447)**
   (`:88-105`). No `await` inside those loops → a multi-second uninterruptible
   stretch every tick. This is the prime suspect for both the missed handshake and
   the ~45 s grid.

2. **The WS reader** (`loops/websocket.py:109` `_reader`). Parses every incoming
   frame (`json.loads` + `sub.parse`) and buffers it. At a ~5,447-market message
   rate this is heavy per-frame CPU on the same loop.

**Read and book-apply are already decoupled** via `raw_events`: the WS reader only
*appends* to `raw_events` (batched, `RAW_EVENT_BATCH_SIZE=200`); the snapshot loop
*replays* `raw_events` to build the books. So the cure is **not** "add a queue
between read and apply" — that queue exists. The cure is to stop the two loops from
hogging the shared event loop, and to shrink the work.

The high market count is the **trigger** (30 topics fan out to ~5,447 markets
because `MAX_TRACKED_SERIES` caps *series*, not markets, and
`CATALOG_MIN_MARKET_VOLUME=0.0` admits every open market). The non-yielding bursts
are the **flaw**. Cutting markets alone hides the flaw at a threshold; it returns
when markets scale back up.

## Invariants we will NOT break

1. **`raw_events` stays the source of truth.** Books remain regenerable by replaying
   `raw_events`. No dropping deltas, no making in-memory books the authoritative
   state. (CLAUDE.md #1.)
2. **DuckDB stays single-writer, single-process.** No second writer; therefore no
   multi-process ingestion that writes `raw_events` from more than one process.
   (CLAUDE.md #2.) This bounds the scaling options in Step 8.
3. **No softening correctness checks** to quiet symptoms — fix the books, not the
   detectors (sequence-gap, audit, price-range).
4. **Behavior in `constants.py`, not env.** New knobs are constants. (CLAUDE.md #4.)

---

## The fix — ordered by leverage

### Step 0 — Measure first (decides how far to go)

Add timing to the hot paths and read it off the live logs before changing topology:
- snapshot **tick duration** (wall-clock per `tick`),
- **events applied / sec** and **WS frames parsed / sec**,
- reconnect count, resync count.

**Done when:** we can state the current tick duration and message rate, so Steps 5
and 8 are sized by evidence, not guesswork. (Cheap; pairs with Step 7.)

### Step 1 — Make the long loops yield (the actual cure for the reconnect loop)

Insert cooperative yields so no synchronous stretch exceeds the ping deadline:
- In `snapshots._drain_events` (`:155`): `await asyncio.sleep(0)` every N events
  (e.g. N=500) inside the apply loop.
- In `snapshots.tick` row-build (`:88-105`): yield every N markets (e.g. N=500),
  **between markets, never mid-market**, so each emitted row is internally
  consistent.
- Confirm the WS `_reader` yields per frame (it does, via `async for`); the lever
  there is per-frame cost (Step 4), not a yield.

A single yield lets the websockets keepalive task and the pong reader get scheduled,
so the handshake is never missed. **This makes the timeout structurally impossible
on one core**, independent of market count.

**Risk:** low. Pure scheduling change; no data-model change. **Done when:** no
`keepalive ping timeout` in an hour of logs at the current market count.

### Step 2 — Split the snapshot tick: continuous applier + pure sampler

Separate *keeping books current* from *emitting the grid*, both still fed by
`raw_events`:
- **Applier:** drain new `raw_events` rows in small, yielding chunks continuously
  (not one burst per tick), keeping `book_store` near-current.
- **Sampler:** the 10 s tick now only *reads* current `book_store` into row tuples
  (chunked + yielding between markets) and hands the list to `asyncio.to_thread`
  for the DuckDB upsert. No draining inside the tick.

Because everything runs on one event loop and each `r.apply(e)` and per-market read
is synchronous (no `await` mid-operation), the sampler can never observe a
half-applied book — no locks needed.

**Result:** grid cadence decouples from ingestion load → clean 10 s grid even while
a market is mid-resync. **Risk:** medium (restructures `SnapshotBuilder`; covered by
`test_end_to_end.py` + new tick-duration assertion). **Done when:** grid timestamps
are a clean 10 s apart in the live logs.

### Step 3 — Faster frame parsing

Swap `json.loads` → `orjson.loads` in the WS reader (and parser hot path). ~3–5×
faster decode frees the loop sooner. **Risk:** low (add dep to `ingest/pyproject.toml`;
drop-in). **Done when:** parsed-frames/sec headroom improves in Step 0's metrics.

### Step 4 — Bound markets by liquidity, not by rank (tourniquet + standing budget)

This is a load reducer, **not** the cure — but it keeps the system inside one
core's budget and removes dead weight:
- Raise `CATALOG_MIN_MARKET_VOLUME` from `0.0` to a real floor so near-zero-liquidity
  primary candidate markets (most of the 5,447, and most of the 94 stuck ones) are
  not subscribed. Liquidity floor beats a top-N cap: it keeps the markets that carry
  coherence signal and drops the junk.
- Add an explicit **total active-market ceiling** (new constant, e.g.
  `MAX_ACTIVE_MARKETS`) applied after series→market expansion, filled greedily by
  the value the coherence engine cares about (volume / time-to-resolution). Cap
  *markets*, not series.

**Risk:** low-medium (changes coverage — confirm the floor with the architect).
**Done when:** active-market count sits at a deliberate budget, and dead markets are
excluded. Note: a liquidity floor likely clears most of Step 6's stuck markets as a
side effect.

### Step 5 — Shrink reconnect blast radius (optional, after Steps 1–2)

Today a reconnect re-subscribes **all** markets at once (`_subscribe_initial`,
`websocket.py:171`). After Steps 1–2 the connection should stop dropping, but if
reconnects still occur, run **multiple WS connections sharded by series** as tasks
on the same loop, all feeding the one in-process `raw_events` writer. A drop then
re-anchors one shard, not all books. Still one process, one DuckDB writer — invariant
#2 holds. **Risk:** medium. **Done when:** a single reconnect touches only its shard.

### Step 6 — Fix the price-range canary (independent correctness bug)

~94 CA/MD primary markets trip `out_of_range_price` every tick
(`orderbook.py:116`, bounds `CANARY_PRICE_MIN_USD=0.01` / `MAX=0.99`) and reset
forever. **First, log the actual rejected price level** for a few of these markets,
then fix the real cause (do not just widen the band — that would violate invariant
#3 if 0¢/100¢ levels are genuinely junk). Likely a near-decided market resting at
0¢/100¢, or a one-sided book; handle the legitimate case explicitly and add a test
for the offending ticker shape. **Risk:** low. **Done when:** these markets stop
looping and produce valid books (or are correctly excluded).

### Step 7 — Observability (do alongside Step 0)

Emit per-loop metrics: **snapshot tick duration**, applied-events/sec, parsed-
frames/sec, reconnect count, resync count, and (if Step 5 lands) per-shard backlog.
Tick duration is the single most useful signal — it shows the worker getting stuck
*before* the connection dies. **Done when:** these are visible in the logs / a
dashboard.

### Step 8 — The scaling ceiling (document now, build only if forced)

If, after Steps 1–4, one core still cannot parse the firehose at the desired market
count, the **only invariant-safe** path to true multi-core ingestion is: reader
*processes* parsing in parallel, handing frames over IPC to **one** writer process
that owns DuckDB. We cannot have N processes writing `raw_events` (invariant #2).
Past that point the choice is: accept the market budget (Step 4), or accept the
storage split (Postgres OLTP) the architecture already reserves for the trader.
**Do not build this now** — just record it as the known ceiling.

---

## What NOT to do

- **Don't add a message broker** (Kafka/Redis/NATS) for the read→apply handoff. It's
  intra-process and `raw_events` already is the handoff.
- **Don't add an `asyncio.Queue` expecting more throughput.** Producer and consumer
  share one core; a queue changes scheduling, not capacity. Yielding (Step 1) is the
  right tool.
- **Don't make in-memory books the source of truth / drop deltas on backpressure.**
  That breaks invariant #1 and the replay/crash-recovery model.
- **Don't soften the sequence-gap, audit, or price checks** to quiet symptoms.
- **Don't tune the market cap as the fix.** It's a tourniquet; Step 1 is the cure.

---

## Minimum viable fix

**Step 1 (yield) + Step 2 (split tick)** stop the connection dropping and restore the
10 s grid — the system becomes correct rather than lucky. **Step 4 (liquidity floor)**
keeps it inside budget. **Step 6** is a quick independent correctness fix. Steps 3, 5,
7, 8 are headroom, blast-radius, and visibility.

## Doc updates required (standing rule — same change, not later)

- [`ARCHITECTURE.md`](./ARCHITECTURE.md): the applier/sampler split, the
  market-count bound (not just series), and the Step 8 scaling ceiling.
- [`DEPLOY.md`](./DEPLOY.md): any new constants (`MAX_ACTIVE_MARKETS`, liquidity
  floor, yield chunk sizes), new dependency (`orjson`), and the new metrics.
- [`END-TO-END.md`](./END-TO-END.md): correct the snapshot section once the grid is
  a true 10 s.

---

## Why the earlier proposal was off (for reference)

[`FIX_PROPOSAL.md`](./FIX_PROPOSAL.md) diagnosed the root cause as "read and book-
apply coupled in the same loop, mutating books per message." In this codebase the WS
reader does **not** touch books — it appends to `raw_events`; the snapshot loop
applies. So its headline "insert an `asyncio.Queue` between read and apply" rebuilds
a decoupling that already exists, and an intra-process queue wouldn't add the
throughput it claims. It also proposed an in-memory-books-as-truth + drop-deltas
model that conflicts with invariant #1, and a multi-process shard model that
conflicts with invariant #2. Its *priorities* (cap first, measure first, don't soften
checks, don't add a broker, the price-check hypotheses) are sound and are carried
forward above; its central mechanism is replaced by Steps 1–2.
