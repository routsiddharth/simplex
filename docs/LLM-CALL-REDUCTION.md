# Reducing LLM API calls — Stage 3 extraction (plan)

Companion to [`LLM-COST-MIGRATION.md`](./LLM-COST-MIGRATION.md), which shaped
*per-call* spend (model tiers + the batch seam + the time gate). This doc attacks
the other axis — **the number of calls** — because that, not per-call length, is
the burn. It reasons through the options, names their failure modes, and
recommends a sequence. Design decisions are the architect's; this is the analysis
behind them.

> **Standing rule:** whichever we adopt ships with a matching update to
> [`ARCHITECTURE.md`](./ARCHITECTURE.md) §3 (Extraction) and, if a constant's
> documented behavior changes, this doc + `constants.py`.

## Implementation status

- **✅ Increment 1 (2026-06-08) — the burn-stopper, no migration / no new deps.**
  §4.4 tighter predicates (`PAIR_ENTITY_OVERLAP_MIN` 2→3, `PAIR_ON_SAME_SERIES=False`),
  §5.1 per-day budget (`EXTRACTION_MAX_PAIRS_PER_DAY`, value-ordered + deferral
  logging), §1.5 bucket-split instrumentation (the `pair routing` log now carries
  `bucket_event/series/entity`, `todo`, `deferred`, `budget_remaining`). Shipped with
  tests; safe to auto-deploy.
- **⏳ Increment 2 — needs plumbing/migration.** §4.3 liquidity gate (needs market
  volume on the row), §3.2 structural edges (needs the Kalshi `mutually_exclusive`
  signal stored per market).
- **⏳ Increment 3 — the principled fix.** §4.2 hosted embeddings (schema migration:
  vector column) + §3.1 cluster-classify. Prototype cluster-classify quality first.
- **✋ Dropped for now.** §3.3 local NLI — conflicts with the chosen hosted/pure
  deploy (local weights); also the weakest lever. Revisit only if volume grows.

---

## 1. The problem, measured (2026-06-07, live deploy)

From production logs, World-Cup-only scope (`KXWC`):

| Quantity | Value | Source |
|---|---:|---|
| Active markets | **1,301** | `catalog refreshed … active_markets=1301` |
| Candidate pairs | **121,290** | `pair routing candidates=121290` |
| Gate routing | `skip=0  sync=0  batch=121290` | every pair → batch |
| Anthropic balance | **exhausted** | batch submits 400 `credit balance too low` |

### 1.1 The number that should alarm us: average degree ≈ 186

`2 × 121,290 / 1,301 ≈ 186`. **Each market is treated as logically related to ~186
others.** That is not plausible. A given World Cup market is genuinely related to
maybe a handful to a few dozen others — the other outcomes of its match, the other
markets in its group, the tournament-outright siblings that name the same team.
186 means the candidate generator is producing **mostly noise**: pairs that will
come back `unrelated` (a wasted primary call) or `correlated`-but-useless.

The cost isn't a quadratic we can't avoid — `121,290` is only ~14% of the
`C(1301,2) = 845,650` possible pairs, so the predicate *is* filtering. It's
filtering with the **wrong instrument**. That reframes the whole problem: the win
is a *better candidate filter*, not a cheaper model.

### 1.2 Why the time gate gives zero relief

`route_pair` skips edges whose endpoints settle before the edge can pay off. The
World Cup runs for weeks — every `closes_at` is >48h out — so `skip=0`, `sync=0`,
all 121k route to batch. The one volume-bounding mechanism we built is structurally
inert for a long-lived event set. **Anything that depends on short remaining-life
cannot help a tournament**; we need a volume bound that doesn't.

### 1.3 Backfill vs. steady state — what's actually recurring

The 121k is a **one-time backfill**: most World Cup markets are already listed, so
this is the cost of classifying the standing graph once. The *recurring* cost is
small — new markets as knockout brackets get listed, plus a full re-spend on any
`EXTRACTION_PROMPT_VERSION` bump or any batch abandoned past
`BATCH_MAX_AGE_SECONDS` (48h). **Implication:** the levers that matter for the
spike (batching, clustering, structural) differ from what matters at rest (dedup,
gate). Don't optimize the steady state and miss the spike, or vice-versa.

### 1.4 Cost of one full pass

Batched 50%-off prices, ~550 in / ~150 out per pair: primary on Haiku ≈ **$80**;
Sonnet verify on the high-confidence band (World Cup pairs are heavily linked → high
≥0.85 hit rate) ≈ **$70–140**. **≈ $150–220 per complete pass.** Verify is roughly
half the cost and is paid *on top of* every confident primary.

### 1.5 Measure the bucket split first

`candidate_pairs` unions three buckets — same `event_ticker`, same `series_ticker`,
≥`PAIR_ENTITY_OVERLAP_MIN` shared entities. We log only the total. The split
decides the lever order, and §1.1 predicts the **entity bucket** dominates (degree
186 can't come from same-event — a match has ~3–20 markets; or same-series — 47
series × ~`C(28,2)` ≈ 18k pairs). To confirm: add a per-bucket counter to
`candidate_pairs` and log it, or run the read-only inspector during a maintenance
window (DuckDB is single-writer):

```bash
python -m simplex_ingest.loops.extraction   # prints the work plan
```

---

## 2. The right mental model: two independent axes

Every call exists because we made two separate decisions, and **each has its own
menu**. Conflating them is why the current design over-spends.

```
        FILTERING                              TYPING
  "is this pair worth                  "what is the relationship,
   a typing decision?"                  given it's worth deciding?"
  ──────────────────────               ──────────────────────────
  • entity-overlap heuristic (now)     • LLM per pair (now)
  • embedding similarity               • LLM per cluster   ← big
  • hybrid (entity ∧ similarity)       • structural rules (free)
  • liquidity / value gate             • NLI cross-encoder (cheap, lossy)
  • structural pre-removal
```

- **Filtering** decides *how many* pairs reach a model. Today it's one blunt
  heuristic, producing degree-186 noise.
- **Typing** decides *what does the classification* and *at what granularity*.
  Today it's one LLM call per surviving pair — the most expensive cell in the grid.

Plus a third, **orthogonal** concern — *spend control* (budget caps, batching,
dedup) — that bounds whatever the two axes produce.

The cheapest correct design picks a good filter **and** a coarse-grained typer.
The two compound: a 10× better filter on top of a 30× cheaper typer is 300×.

---

## 3. Typing axis — what classifies, and at what granularity

### 3.1 LLM-per-cluster (not per-pair) — the structural reframe  ★★ highest leverage

**The insight.** Pairwise is the wrong unit. Logically-related markets come in
**clusters** (a match's outcomes; a group's standings; a team's prop markets). The
relationship structure of a cluster is a small graph. Instead of `C(k,2)` pairwise
calls to recover it, make **one** structured-output call: *"here are k related
markets; return the edge list among them."*

**Call-count math.** If markets fall into ~`C` clusters of average size `k`
(`1301 = C·k`), intra-cluster structure costs `C` calls instead of `Σ C(kᵢ,2)`. At
`k≈15` → `C≈87` clusters → **~87 calls vs ~9,000 pairwise** for the same edges.
Cross-cluster edges are rarer and handled by a thin pairwise pass on the filtered
set (§4). This is the single biggest reduction available because it attacks the
quadratic at its root.

**How.**
1. Cluster markets (by `event_ticker`, then by embedding proximity within
   series — see §4.2). Cap cluster size (e.g. ≤20) so output stays bounded.
2. One call per cluster: structured-output schema returning a list of
   `{a, b, relationship_type, direction, confidence, rationale}`.
3. Write the edges exactly as the pairwise path does — same tiers, same dedup.
4. Cross-cluster candidates (markets in different clusters that share entities /
   high similarity) go to the existing pairwise path; these are sparse.

**Trade-offs / failure modes.**
- **Output length & attention.** Large clusters → long edge lists → quality
  degrades and `max_tokens` balloons. Cap cluster size; split big clusters.
- **Quadratic-in-output.** A k=20 cluster asks for up to 190 edges in one
  response — long. The sweet spot is small clusters (k≈8–12) where one call
  cleanly covers a match or group.
- **Partial failure.** A malformed edge in the list mustn't sink the cluster —
  parse per-edge, log-and-skip the bad one (mirror the existing discipline).
- Most intra-cluster edges are *also* structurally derivable (§3.3); for those,
  cluster-classify is a fallback for the non-obvious relationships only.

This is the approach most worth prototyping; it changes the asymptotics, not just
the constant.

### 3.2 Structural rules — free, ground-truth, no model  ★ high impact, but bounded

**Idea.** Some edges need no model: Kalshi groups mutually-exclusive outcomes under
one event (a match's home/draw/away; a group's final standings). Within such a
partition the relationship is `mutually_exclusive` / `partition_member` by
construction. Emit those directly as `trusted` (`agreement_status='structural'`),
**no primary call and no verify call** — and remove them from the LLM set.

**Why it also cuts the expensive half.** Structurally-known partitions are exactly
the high-confidence pairs that currently trigger the Sonnet *verify* tier. Handling
them as ground truth removes them from both the primary *and* the verify path —
and Kalshi's grouping is better than a 0.85 guess.

**The safety boundary (reason carefully here).** This is only sound where the
*event semantics* guarantee exclusivity — i.e. Kalshi explicitly models the event
as a mutually-exclusive partition. It is **not** sound to pattern-match tickers and
*assume* a partition; that reintroduces the comparison-by-code brittleness Stage B
exists to avoid (ARCHITECTURE §3: "comparison-by-code is brittle across a domain
this varied"). So: derive structural edges only from an explicit Kalshi
partition/exclusivity signal, per known event templates — and when in doubt, fall
through to the model. The discovery predicates already detect partitions
(`PREDICATE_PARTITION_MIN_MARKETS`); reuse that signal, don't invent a new
heuristic.

**Bound on impact.** Limited to within-event partitions. If same-event pairs are a
small slice of the 121k (likely, per §1.1), this is high-*quality* but modest-
*volume* relief — pair it with a filtering fix, don't expect it to solve the count
alone.

### 3.3 NLI cross-encoder — cheap typing, but lossy and a deploy cost

**Idea.** "Does A entail / contradict / not-relate-to B" is close to Natural
Language Inference (entailment / contradiction / neutral ≈ `implies` /
`mutually_exclusive` / `unrelated`). A fine-tuned cross-encoder (e.g. DeBERTa-MNLI)
runs locally, free, fast.

**Why it's weaker than it looks.** The taxonomy is **7-way and direction-aware**;
NLI is **3-way and roughly symmetric**. The mapping is lossy — `correlated`,
`conditional`, `partition_member`, `same_event` don't have clean NLI analogues. So
an NLI model is a good **screen** ("related vs not" — a *filtering* tool, really)
but a poor **typer**. Best role: a free relatedness screen feeding the LLM, which
makes it a §4 filter, not a typing replacement.

**Deploy cost.** Adds model weights + a torch/ONNX runtime to a currently pure-
Python single-process service (ARCHITECTURE §2 invariant). A small ONNX model via
`onnxruntime` (~50MB, no torch) is the least-intrusive form if pursued.

### 3.4 LLM-per-pair (status quo)
Keep for the residual: cross-cluster pairs that survive filtering and aren't
structurally known. After §3.1/§3.2/§4 this is a small set, classified at the
current cheap tier. Verify stays on it for the ≥0.85 band — but that band is now
dominated by genuine cross-cluster relationships worth the spend, not partition
members we already knew.

---

## 4. Filtering axis — which pairs are worth a decision

### 4.1 The status quo and why it's wrong
The `≥ PAIR_ENTITY_OVERLAP_MIN` (=2) entity bucket is a recall-maximizer that
creates **near-cliques among every market naming the same teams**, and tournament
markets are entity-dense (group-order markets enumerate four teams each; outright
markets name many). That's the degree-186 noise (§1.1). Same-series pairing has the
same flavor — it pairs a partition (better handled structurally) or only weakly-
related markets.

### 4.2 Embedding similarity — the principled filter  ★★

**Idea.** Replace "shares ≥2 entity strings" with "is semantically close." Embed
each market's Stage-A record once; keep only pairs above a similarity threshold.

**How (new stage A.5).**
1. After Stage A, embed `underlying_event + resolves_yes_when + entities`; cache the
   vector on `market_semantics` (a new column). 1,301 × ~200 tok ≈ 260k tokens.
2. Pairwise cosine **locally** (1301² ≈ 1.7M comparisons — milliseconds in NumPy;
   FAISS only if the market set scales to 5k–6k, the catalog ceiling).
3. Candidates = pairs over a tunable threshold (a knob like the confidence
   thresholds). This also produces the **clusters** §3.1 needs (threshold a
   similarity graph → connected components).

**Model choice.**
- *Hosted, cheap:* `text-embedding-3-small` (~$0.02/M) or an OpenRouter embedding —
  ~$0.005 for the whole set; keeps the no-local-weights deploy.
- *Local, zero-marginal:* a `sentence-transformers` MiniLM (~90MB + torch ~800MB),
  or ONNX MiniLM (~50MB, no torch). Removes the per-cycle API call at the cost of
  image size and the single-process purity.

**The failure mode that matters — template similarity ≠ logical relatedness.**
Kalshi markets are templated, so "Argentina to win match 12" and "Brazil to win
match 47" embed *close* (same wording) while being logically **unrelated**.
Naïve embedding similarity would re-create noise of a different flavor. Two
mitigations, both worth doing:
- **Embed entity-aware text**, weighting the specific entities/resolution over the
  template boilerplate.
- **Hybrid blocking:** require *share ≥1 entity **and** exceed similarity*. This
  simultaneously kills template false-positives and collapses the entity bucket's
  density (it's no longer "any 2 shared strings" but "shares an entity *and* is
  actually close"). This hybrid is the recommended candidate rule.

**Impact.** Plausibly 121k → 5k–15k *genuinely* related pairs; quality up, not just
count down. Subsumes §4.4 — when this lands, the raw entity-overlap predicate is
removed, not kept.

### 4.3 Liquidity / value gate
A coherence edge between markets nobody trades carries no solver signal.
`CATALOG_MIN_MARKET_VOLUME` (100) floors *market* admission; extend to *pairs*:
require both endpoints to clear a higher `PAIR_MIN_VOLUME` before spending a call.
Sheds the thin-prop long tail (`KXWCTEAMGOALS-…` variants). Stacks on everything;
re-admission costs one call if volume arrives later — net win.

### 4.4 Tighter predicates (stopgap, subsumed by 4.2)
With no new code, dial `constants.py`: `PAIR_ENTITY_OVERLAP_MIN` 2 → 3, and gate the
`same_series` bucket behind entity overlap. Ships today, ~2–5× reduction depending
on the split. **This is a stopgap you remove when 4.2 lands** — it's the crude
version of the same goal. Don't invest in tuning it; use it to buy time.

---

## 5. Spend control — orthogonal bounds (always-on insurance)

### 5.1 Absolute spend budget — the bound the gate can't provide (§1.2)
A hard cap the long tournament can't sidestep: `EXTRACTION_MAX_PAIRS_PER_DAY` (and
/or per-cycle), with `todo` ordered by **value** (combined volume × useful life) so
the budget buys the highest-signal edges first. Log deferrals (no silent
truncation — mirror the catalog's `dropped_over_ceiling`). This converts the
$150–220 backfill burst into a rate you set; it doesn't reduce total work but makes
spend predictable and front-loads value. **Ship this first** — it's the safety net
that stops a credit top-up from instantly draining again, independent of every
other change.

### 5.2 Per-call batching — secondary, and mind the ceiling
Anthropic batches charge **per token, not per request**, so "fewer requests" isn't
itself the saving — the saving is **amortizing the shared prefix**. The real win is
restructuring around an *anchor market*: one call classifying market A against
`[B, C, D, …]` emits A's semantic block **once** instead of once per pair.

*Token math:* per-pair input ≈ 250 (system) + 2×130 (blocks) = 510. Anchor-batching
A against 20 neighbors ≈ 250 + 130 + 20×130 = 2,980 for 20 classifications =
**~149 tok/classification — a ~3.4× input reduction.** But note **§3.1
cluster-classify dominates this** — same prefix-amortization *and* it cuts the
*number* of decisions, not just their unit cost. Anchor-batching is the lesser
form; prefer cluster-classify. Quality ceiling either way: many classifications in
one context dilutes attention — keep the count modest (≤~10) and A/B against
single-pair quality.

### 5.3 Stop paying twice
- Confirm `unrelated`/`review` edges are written and counted as done (they appear
  to be — `get_classified_pairs`; verify). No re-classify within a prompt version.
- Treat `EXTRACTION_PROMPT_VERSION` bumps as a deliberate, budgeted event — a bump
  re-spends all 121k.
- **Batch-abandonment alert.** A batch dropped past `BATCH_MAX_AGE_SECONDS` (48h)
  re-spends its items. If reconcile is flaky that's a silent recurring cost — log
  and alert on abandonment so a re-spend loop can't hide.

---

## 6. How the levers compose (worked example)

They multiply; some are redundant. Start from 121,290:

```
121,290  candidate pairs (entity-overlap heuristic)
  → −structural partitions (§3.2)            say −15%  → ~103,000   (free, trusted edges)
  → hybrid embedding filter (§4.2)            ÷ ~10    → ~10,000    (genuinely related)
  → liquidity gate (§4.3)                     ÷ ~1.5   → ~6,700     (tradeable)
  → cluster-classify intra-cluster (§3.1)              → ~hundreds of LLM calls
       + thin pairwise on cross-cluster residual       → ~1,000 calls
  → verify only the cross-cluster ≥0.85 band           → a few hundred Sonnet calls
```

From ~121k primary + tens-of-thousands verify, to **~order-1,000 LLM calls total**
for the backfill — a ~100× reduction, with *higher*-quality trusted edges (Kalshi
ground truth) and a predictable budget cap underneath it all. Percentages are
placeholders until §1.5 gives the real bucket split — but the *shape* (filter hard,
type coarsely, structure for free) holds regardless.

**Redundancies to avoid double-counting:** §4.4 is subsumed by §4.2 (remove it when
4.2 lands). §3.3-as-typer is dominated by §3.1; §3.3 only earns its keep as a free
*filter*. Transitive closure within a partition is just §3.1 done at cluster
granularity — don't build a separate closure pass.

---

## 7. Recommended sequencing

Each phase is independently shippable; watch `candidates=` and the `llm usage`
lines after each.

**Phase 0 — bound the spend now (hours).** §5.1 budget + §5.3 dedup/alerting +
§1.5 bucket-split instrumentation. Stops the bleed independent of everything else
and tells us which filter lever matters most.

**Phase 1 — free + cheap volume cuts (1–2 days).** §3.2 structural edges (better
hard constraints, removes them from both primary and verify) + §4.4 stopgap
predicates + §4.3 liquidity gate. Gets us off the worst of the curve while Phase 2
is built.

**Phase 2 — the principled fix (3–5 days).** §4.2 hybrid embedding filter (replaces
§4.4) + §3.1 cluster-classify. This is the durable design: it changes the
asymptotics, so the layer keeps working when the catalog scales past one tournament.
Start with the **hosted** embedding model to preserve the deploy story.

**Phase 3 — only if volume grows (later).** §3.3 NLI screen and/or FAISS (§4.2) when
the market set scales back to the 5k–6k catalog ceiling. Re-evaluate the
single-process / local-weights trade-off then.

Rationale for the order: Phase 0 is pure insurance, Phase 1 is cheap and improves
quality, Phase 2 is the real fix but needs the most design. Don't skip to Phase 2
without Phase 0 — the budget cap is what makes iterating safe while credits are
tight.

---

## 8. What each change touches

| Lever | Files / constants | Docs |
|---|---|---|
| §3.1 cluster-classify | new `cluster_classify.py`, `llm/` (cluster builder/parser), `loops/extraction.py` | ARCH §3 |
| §3.2 structural | new `structural_edges.py`, `pair_candidates.py`, `loops/extraction.py` | ARCH §3, §4 |
| §3.3 NLI | new model dep + runtime; revisit §2/§11 invariant | ARCH §2, §11 |
| §4.2 embeddings | `llm/` embed client, `market_semantics` schema (+vector col), `pair_candidates.py` | ARCH §3, §5; this doc |
| §4.3 liquidity | `constants.py` (`PAIR_MIN_VOLUME`), `route_pair` | ARCH §3 |
| §4.4 predicates | `constants.py` (`PAIR_ENTITY_OVERLAP_MIN`, same-series knob), `pair_candidates.py` | ARCH §3 |
| §5.1 budget | `constants.py` (`EXTRACTION_MAX_PAIRS_PER_DAY`), `loops/extraction.py` | ARCH §3 |
| §5.2 batching | `llm/client.py`, `llm/batch.py` | ARCH §3 |
| §5.3 re-spend | `loops/extraction.py`, alerting | ARCH §3 |

---

## 9. Open decisions (architect's call)

1. **Local model weights, yes or no?** §4.2-hosted and §3.3/§4.2-local both work;
   the line is whether a torch/ONNX dependency is acceptable in the single-process
   deploy. Recommendation: start hosted, keep local as a scaling option.
2. **Cluster-classify vs. keep pairwise.** §3.1 is the biggest win but the largest
   design change and carries an output-quality ceiling. Prototype on one match's
   cluster and compare edge quality to the pairwise baseline before committing.
3. **Filter thresholds** (similarity, `PAIR_MIN_VOLUME`, overlap-min) — need a small
   labeled World-Cup pair sample to tune recall/precision; start recall-favoring.
4. **Budget size** (§5.1) — the daily call/$ ceiling acceptable while stabilizing.
5. **Restore Sonnet/Opus tiers?** Orthogonal to call count, but the current
   deepseek/gemini override (`LLM-COST-MIGRATION.md`) interacts with quality —
   decide alongside Phase 2.
