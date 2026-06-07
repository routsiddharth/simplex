# LLM cost migration — Stage 3 extraction (handoff)

Status: **implemented** (steps 0–2 + the batch subsystem; the standalone
`max_tokens` cap on the sync path was deliberately *not* applied — see §2). The
durable design now lives in canonical docs: extraction spend shaping +
provider/transport seam + the batch state machine in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) §3/§5, and the `ANTHROPIC_API_KEY` secret in
[`DEPLOY.md`](./DEPLOY.md) §6. This file is retained as the rationale/handoff record;
the canonical docs are the source of truth. One deviation worth noting: the batch
path *does* send `max_tokens` (`BATCH_MAX_TOKENS`) because it is a **mandatory**
field on the Anthropic Messages/Batches API — that is the API requirement of §2,
not the cost-saving sync cap, which remains intentionally unset.

## Goal

Cut the cost of the Stage 3 LLM extraction layer without losing graph quality. The
extraction spend is real and bursty (a market-discovery sweep floods the layer with
work, then it idles). The previous billing wall manifested as OpenRouter `402 Payment
Required` errors in production — that was a *symptom*; this migration addresses the
underlying spend.

## Where the LLM calls live (read these first)

- `ingest/src/simplex_ingest/llm/client.py` — `OpenRouterClient`. Two call shapes:
  - `extract_market(...)` → Stage A, per-market semantics (`MarketSemantics`).
  - `classify_pair(...)` → Stage B, typed relationship between two markets
    (`PairClassification`). Transport is `_chat(...)` →
    `POST {OPENROUTER_BASE_URL}/chat/completions`.
- `ingest/src/simplex_ingest/loops/extraction.py` — `ExtractionLoop`. Per cycle
  (`EXTRACTION_INTERVAL_SECONDS = 300`, `EXTRACTION_BATCH_SIZE = 50`):
  - `_extract_semantics()` → Stage A for markets missing current-version semantics.
  - `_classify_pairs()` → Stage B over candidate pairs.
  - `_classify_one(ma, mb)` → the **trust-tiering** logic (primary call, then an
    independent verify call only when `confidence >= EDGE_TRUSTED_CONFIDENCE`).
- `ingest/src/simplex_ingest/pair_candidates.py` — `candidate_pairs(...)`. Cheap
  structural candidate selection (same event / same series / entity overlap). **Currently
  time-blind** — no notion of time-to-resolution. This is the seam the gate plugs into.
- `ingest/src/simplex_ingest/constants.py` — model ids + thresholds (see below).
- `ingest/src/simplex_ingest/config.py` — the env surface (currently
  `OPENROUTER_API_KEY` is the only LLM secret; batch adds `ANTHROPIC_API_KEY`).

### The three call purposes (this distinction drives every routing decision)

| Purpose | Call | Current model (constant) | Notes |
|---|---|---|---|
| **Stage A — per-market semantics** | `extract_market` | `EXTRACTION_MODEL = "anthropic/claude-sonnet-4.6"` | Cached **forever** per market (until `EXTRACTION_PROMPT_VERSION` bumps). Cheap, low steady-state volume; its `entities` output feeds candidate selection. |
| **Stage B — pairwise classify (primary)** | `classify_pair` | `PAIR_MODEL = "anthropic/claude-opus-4.8"` | **The cost driver.** Opus, runs over every candidate pair. |
| **Stage B — verify (trust gate)** | `classify_pair` | `PAIR_VERIFY_MODEL = "anthropic/claude-sonnet-4.6"` | Second, independent classification, **only** when primary `confidence >= EDGE_TRUSTED_CONFIDENCE (0.85)`. Agreement on `relationship_type` → `trusted` tier; disagreement → `review`. |

Relevant constants (`constants.py`): `EXTRACTION_MODEL`, `PAIR_MODEL`,
`PAIR_VERIFY_MODEL`, `EXTRACTION_PROMPT_VERSION`, `EDGE_TRUSTED_CONFIDENCE = 0.85`,
`EDGE_SOFT_CONFIDENCE = 0.6`, `OPENROUTER_BASE_URL`, plus the `LLM_*` rate/retry knobs.

## The changes (agreed direction)

### 1. Demote Opus from the Stage B primary → Sonnet (decided)

`PAIR_MODEL` is currently Opus-on-every-pair — the single biggest line item. Switch the
**primary** classification to Sonnet (`anthropic/claude-sonnet-4.6`). Optionally keep Opus
only as an *escalation* for the ambiguous middle band, or as the verifier. This changes
the cost *rate*, which can beat the batch discount on its own.

> **Resolved (2026-06-07, architect's call):** on the Anthropic batch path the
> primary pass *and* Stage A semantics run on **Haiku** (`BATCH_EXTRACTION_MODEL` /
> `BATCH_PAIR_MODEL` = `claude-haiku-4-5`), with **Sonnet**
> (`BATCH_PAIR_VERIFY_MODEL` = `claude-sonnet-4-6`) reserved for the Stage B
> verify/trust gate — quality spend concentrated on the low-volume hard-constraint
> band. Validate against a sample if taxonomy quality looks off at Haiku capability.

### 2. `max_tokens` — sync path left uncapped (as decided); batch sets the API-mandated field

> **Implemented decision (supersedes the original plan below):** the architect declined the
> standalone sync cap. The synchronous OpenRouter `_chat` therefore still sends **no
> `max_tokens`** — the premature-`402` root cause persists on the sync path *by design*
> (mitigated instead by the spend gate + Opus→Sonnet demotion lowering per-call reserve
> pressure). The batch path **does** set `max_tokens` (`BATCH_MAX_TOKENS = 1500`), because
> it is a **mandatory** field on the Anthropic Messages/Batches API — that is the API
> requirement, not the cost-saving cap that was declined.

Original rationale (retained for context): `_chat` sends no `max_tokens`, so OpenRouter
reserves the model's full 64K output (`65536`) of credit per call — the source of the
premature `402` ("requested up to 65536 tokens, but can only afford 63354") even though
real outputs are a few hundred tokens. It's model-independent (Sonnet has the same 64K
ceiling, so the Opus→Sonnet swap alone doesn't fix it). The plan had been to fold a ≈1500
cap into the batch rewrite; in the end only the (mandatory) batch field was set, and the
sync cap was intentionally not applied.

### 3. Batch (Anthropic-direct) vs OpenRouter (synchronous) split

Route non-latency-sensitive work to the **Anthropic Message Batches API** (flat **50%**
discount, completes within ~1h, results retrievable 29 days). Keep latency-sensitive /
critical-path work on the existing synchronous OpenRouter client.

The governing quantity is **`remaining_life = min(time-to-resolution of the two
endpoints)`** at the moment a pair becomes a candidate.

| Call | Mode | Why |
|---|---|---|
| Stage A — incremental (new markets, steady state) | **OpenRouter (sync)** | Cheap, low volume, on the critical path (gates that market's Stage B). |
| Stage A — bulk (first backfill / `EXTRACTION_PROMPT_VERSION` bump) | **Batch** | Huge, bursty, zero latency sensitivity. |
| Stage B primary — long-horizon pair | **Batch** | The prize: the cost driver, latency irrelevant against weeks of life. |
| Stage B primary — short-fuse pair | **OpenRouter (sync)**, or skip | Need the edge before its near-term endpoint resolves — or it's below the payback floor. |
| Stage B verify | **Follows its primary** | Batched primary → verify joins the next wave; sync primary → sync verify. Never split a pair's primary/verify across modes. |

### 4. Time-to-resolution gate (prerequisite for #3, and a saving on its own)

There is **no time-to-resolution consideration anywhere in the spend path today**
(`pair_candidates.py` is purely structural; extraction selects on active + missing
semantics). Add a gate so:

- `remaining_life < floor` → **don't spend at all** (an edge that can't outlive its
  payback window is negative-ROI regardless of how cheaply you buy it).
- `floor ≤ remaining_life < batch_threshold` → **sync** (pay full price to get it in time).
- `remaining_life ≥ batch_threshold` → **batch**.

This requires carrying `close_time` / `settlement_ts` (discovery already reads these from
Kalshi) into the market dicts extraction sees and into `candidate_pairs`. It is a shared
prerequisite for both the gate and the batch routing.

> Why this matters economically: the codebase already prunes a market's graph 1h after it
> **resolves** (`GRAPH_PRUNE_AFTER_RESOLVED_SECONDS`) — i.e. it already treats LLM spend as
> an investment amortized over a market's live lifetime. An edge's useful life is
> `min(life of A, life of B)` (a relationship between two live prices dies when either
> endpoint settles). The gate is just that same amortization logic applied at *spend* time
> instead of only at *disposal* time.

## Recommended sequencing (don't build batch first)

0. **Measure.** `_chat` already receives the usage block in `data` and discards it
   (`content, _ = await self._chat(...)`). Capture input/output tokens tagged by purpose
   (Stage A / B-primary / B-verify) + model. This number decides whether the batch
   subsystem is worth building.
1. **Time plumbing** — carry `close_time`/`settlement_ts` into the candidate path. Shared
   prerequisite for the gate and the split.
2. **Zero-infra wins (inside OpenRouter):** (a) demote Opus→Sonnet primary; (b) the
   time-to-resolution gate. No new provider, no state machine.
3. **Re-measure.** The residual long-horizon Opus/Sonnet spend is the *only* thing batch
   buys. Decide go/no-go on the batch subsystem from a real number.
4. **Batch subsystem (only if justified).**

## Batch subsystem — implementation notes (for step 4)

- **New provider seam.** Anthropic-direct client (different from OpenRouter). Model-id
  spelling differs: Anthropic uses dashed (`claude-opus-4-8`, `claude-sonnet-4-6`);
  OpenRouter uses dotted with a prefix (`anthropic/claude-opus-4.8`). Don't cross them.
- **Second secret.** `ANTHROPIC_API_KEY` in `config.py` (consistent with the "secrets are
  the one env exception" rule). Update `DEPLOY.md`’s env table.
- **`max_tokens` is mandatory** on the Anthropic Messages/Batches API (see #2).
- **Durable cross-cycle state.** A batch is submit-now / retrieve-later, so the loop
  becomes: cycle N submits, cycle N+k polls and reconciles. Persist the pending-batch
  state (batch_id + the pairs it covers + prompt version) **in DuckDB, not memory**, or a
  restart orphans a submitted batch. Anthropic keeps results 29 days, so recovery is
  feasible if the id was persisted.
- **Single-writer DuckDB** is unaffected — batch retrieval writes edges from the same
  process, same as the sync path.
- **Doc sync:** `ARCHITECTURE.md` (new provider seam + spend routing in the data flow) and
  `DEPLOY.md` (the `ANTHROPIC_API_KEY` secret).

## Open decisions (architect's call)

- `floor` and `batch_threshold` values (constants; pick `batch_threshold` comfortably above
  worst-case batch turnaround — hours, not 1h).
- Sonnet vs Haiku for the demoted primary.
- Ship the gate and the demotion together or separately (separate gives cleaner attribution
  of which one saved what).

## What's been done so far

- Mapped the three call purposes and the trust-tiering logic (above).
- Confirmed `_chat` sends no `max_tokens` (root cause of the premature `402`).
- Confirmed candidate selection / extraction are time-blind (the gate is net-new).
- **No code changes made.** Hermetic test suite is green at the time of writing
  (`cd ingest && source venv/bin/activate && pytest` → `198 passed, 4 skipped`).
