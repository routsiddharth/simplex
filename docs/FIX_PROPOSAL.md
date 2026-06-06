# Simplex Ingestion Architecture — Fix Proposal

## Context

The Kalshi WebSocket ingestion path is in a reconnect death-loop. The
visible symptoms are a keepalive ping timeout roughly once a minute,
perpetual order-book resets, a gappy ~45s snapshot grid (target is a
clean 10s grid), and an hourly audit that constantly reports books
out of sync.

These are not separate bugs. They share a root cause and a few
secondary couplings. This doc lays out the cause, the target
architecture, and a staged implementation plan ordered by leverage.

## Root cause

**Message reading and book application run on the same asyncio event
loop with no decoupling.** The read loop parses JSON and mutates book
state synchronously per message. That work is CPU-bound and does not
yield. When discovery subscribes ~5,448 markets on one connection, the
backlog of buffered messages monopolizes the loop, the keepalive
coroutine never gets scheduled, the pong deadline passes, and the
connection dies. It reconnects, re-subscribes all markets, re-anchors
all books, gets overwhelmed again, and dies — about once a minute. It
never reaches steady state, so the efficient incremental path never
runs.

The high market count is the *trigger*. The architectural flaw is the
*coupling*. Lowering the market count alone hides the bug at a
threshold and it returns the moment markets scale back up.

### Why each symptom follows

- **Ping timeout / reconnect loop:** event-loop starvation, as above.
- **Constant book resets:** every reconnect re-anchors every book,
  triggering sequence-gap resets. The reset logic is correct; the
  reconnect frequency is the problem.
- **Gappy 45s grid:** the 10s tick rebuilds all markets *and* drains
  the event backlog synchronously inside the tick, blowing the 10s
  budget. Two jobs fighting for one slot.
- **Audit false disagreement:** books are perpetually mid-rebuild, so
  they diverge from Kalshi's canonical state; the audit correctly
  reports this and forces more resets, adding load. Downstream symptom,
  not an audit bug.

### The discovery cap is the trigger to fix first

Discovery caps *series* at 30 but never caps *markets*. Series fan-out
is wildly non-uniform — a binary series is 1 market; an election
primary expands to hundreds of candidate markets. Capping series is the
wrong unit. Cap on **active subscription count** after expansion.

## Open question that must be answered first

**Does `apply_message` (or whatever applies a message to a book) do a
per-message DuckDB write, or any other blocking/synchronous I/O — a
sync DB call, a sync HTTP resync, a sync file write?**

If yes, that blocking call on the apply path is a hidden event-loop
blocker and removing/batching it may recover more headroom than the
queue decoupling does. The agent should answer this before building,
because it changes the relative priority of the steps below.

The plan below works either way, but if there is per-message I/O,
batching it (Step 5) jumps in priority.

## Target architecture

Organizing principle: **ingestion, state, and consumption run on
independent clocks.** Every current symptom is two of those three
sharing a clock. Decouple the clocks and the symptoms become
impossible, not merely less likely.

```
sockets → [bounded queue] → consumer (parse / seqcheck / mutate; single-writer)
                                 │
                                 ├──→ in-memory books ──→ sampler (10s) ──→ solver
                                 ├──→ batch writer ──→ DuckDB
                                 └──→ resync requester (on gap / backpressure)
```

Three clocks: socket-driven readers, queue-driven consumer,
timer-driven sampler. Nothing on one clock can starve a thing on
another.

### Stage 1 — Readers (dumb on purpose)

N sharded WebSocket connections, each owning a disjoint slice of
markets. A reader does exactly three things: receive frame, attach
`(shard_id, recv_timestamp)`, push onto a bounded queue. No book
mutation, no I/O, minimal parsing (only enough to route). The reader
must be incapable of blocking on anything but the socket and the queue.
This is what makes the keepalive coroutine un-starvable — the read loop
yields every iteration regardless of backlog.

Shard **by series**, not round-robin by market, so a series' candidate
markets land on one connection (keeps related books together for
resync; isolates noisy series). Size each shard by a subscription
budget, not a series count.

### Stage 2 — Consumer (single-writer state owner)

One consumer task per shard (or a small pool) drains the queue: parse,
sequence-check, mutate the in-memory book.

Invariant: **single-writer-per-book.** Exactly one coroutine ever
mutates a given market's book, and it never `await`s in the middle of a
mutation (mutate synchronously; await only at the queue boundary). This
is what makes audit false-positives impossible — no reader can observe
a half-applied book.

**Resync / backpressure policy:** when a market falls behind (shard
queue depth crosses a threshold, or a sequence gap appears), do NOT
replay buffered deltas. Drop them, request a fresh snapshot for that
market, and resume incrementals from the snapshot's sequence number.
Order-book snapshots are complete truth, so missed deltas are never
needed. This bounds both memory and staleness — the property a naive
unbounded queue lacks.

### Stage 3 — Sampler (own clock)

The 10s grid tick does exactly one thing: take a coherent snapshot of
current in-memory book state. It does not drain backlog, parse, or
rebuild. O(markets) memory read. Grid cadence becomes fully independent
of ingestion throughput, so the solver gets its clean 10s grid even
while a shard is mid-resync.

Attach `last_update_ts` to every book; the sampler emits
`(price, staleness)` per market, not bare price. **The solver decides
how to treat stale markets** — for a max-entropy joint-distribution
solve, a frozen price fed in as live produces confident coherence
violations that are staleness artifacts, not real arbitrage. Let the
solver down-weight or exclude markets past a staleness threshold rather
than forward-fill silently. Make the threshold an explicit knob.

### Persistence (beside Stage 2, never inside it)

Batch DuckDB writes. The consumer hands completed snapshots or batched
deltas to a separate writer task on an interval. Never a write per
message on the apply path.

## Implementation plan (ordered by leverage)

**Step 0 — Answer the open question.** Inspect the apply path for
per-message blocking I/O. Report findings before proceeding.

**Step 1 — Cap total post-expansion markets (tourniquet, do first).**
Add a hard ceiling on total subscriptions after series→market
expansion. Start conservative (500–800, tune by what survives). Make
the cap subscription-count-based, not series-based. Prefer a
budget-aware fill: rank markets (by liquidity / volume /
time-to-resolution / whatever the coherence engine values) and fill the
budget greedily, dropping low-information and near-resolved markets
first. This stops the bleeding immediately and is nearly a one-line
change conceptually.

**Step 2 — Decouple read from apply (the real fix).** Insert a bounded
`asyncio.Queue` between the read loop and a consumer task. Reader pushes
raw/minimally-parsed frames; consumer parses + mutates. Bound the queue
and apply the resync drop-policy from Stage 2 when it fills. This makes
the ping timeout structurally impossible rather than improbable.
Highest structural value. No new dependency — `asyncio.Queue` is the
correct tool here; do NOT introduce Kafka/Redis for an intra-process
handoff.

**Step 3 — Make the 10s tick a snapshot-sampler.** Maintain book state
continuously in the consumer; the tick reads in-memory state instead of
rebuilding + draining backlog. Emit `(price, staleness)`. This protects
the solver's clean-grid assumption independently of ingestion load, and
it is the part most likely to silently re-break later if skipped.

**Step 4 — Fix the price-range check (independent, quick).** ~94
California/Maryland primary markets trip an out-of-range price check
every tick and reset forever. Before assuming the cause: **log the
actual rejected value and the book state for a few of these markets.**
Likely causes, in order of suspicion:
  1. The check is exclusive (`0 < price < 100`) when 0 and 100 are
     legal Kalshi prices (a near-decided market sits at 0/100). Fix to
     inclusive bounds.
  2. A one-sided book — bid or ask absent (None/null) — and the check
     does arithmetic on the missing side (e.g. `ask - bid`). "Out of
     range" can be a missing-side symptom, not a literal range
     violation. Handle the empty side explicitly.
  Note these 94 overlap heavily with the election fan-out, so the
  budget-aware cap in Step 1 likely removes most of them as a side
  effect. Still fix the check — it's a correctness bug.

**Step 5 — Batch persistence / get I/O off the loop.** If Step 0 found
per-message DuckDB writes or sync I/O on the apply path, batch them
into a separate interval-driven writer task. If Step 0 found this,
**raise this step's priority to right after Step 2** (or even alongside
it) — it may be the single biggest headroom win.

**Step 6 — Route discovery through sharding (defense in depth).** If
WebSocket sharding exists in the Stage 1 ingest design but the
discovery path isn't using it, wire it up. Shard by series with a
per-shard subscription budget. Shrinks reconnect blast radius (a
reconnect re-anchors one shard, not all books). Design shards
**share-nothing** so the shard boundary can later become a process
boundary if CPU-bound parsing ever exceeds one core (the GIL means
threads won't help; processes would, with a broker like NATS / Redis
Streams at the shard seam). Do not build multiprocessing now — just
keep the door open.

**Step 7 — Observability (do early; it pays for itself).** Add
`prometheus_client` gauges: per-shard queue depth, messages/sec
applied, reconnect count, resync count, grid-tick duration. The single
most useful signal is **queue depth per shard** — it shows a shard
falling behind *before* it dies, which is exactly the signal currently
missing. Add `structlog` structured logging so gap-reset and
out-of-range events carry market id, shard id, and the actual rejected
value (this directly serves Step 4's diagnosis). Consider doing the
structlog part before Step 4.

## What NOT to do

- **Do not fix this by softening correctness checks.** Loosening
  sequence-gap detection, widening the price range past what's real, or
  silently forward-filling stale prices all make the symptom quieter
  and the books wronger. The architecture above makes symptoms vanish
  *because the books become correct* — the only "fixed" that survives
  real money on the coherence signal.
- **Do not introduce a message broker (Kafka/Redis/NATS) for the
  read→apply handoff.** That's an intra-process problem; a bounded
  `asyncio.Queue` is correct. A broker only earns its place if/when
  shards become separate processes.
- **Do not migrate off DuckDB preemptively.** 100K events/day is within
  range with batched writes. Only consider QuestDB/TimescaleDB for the
  *ingest/recent-window* path if batched DuckDB writes still can't keep
  up — and keep DuckDB for analytical solver queries even then.
- **Do not soften the audit.** It's correctly reporting wrong books.
  Fix the books (Step 2), not the audit.

## Minimum viable correctness

If only a subset ships first: **Step 1 (cap) + Step 2 (read/apply
decouple) + Step 3 (sampler)** is the set that makes the system correct
rather than lucky. Step 4 is a quick independent correctness fix. Steps
5–7 are headroom, blast-radius, and visibility — high value, but the
system stops dying after 1–3.