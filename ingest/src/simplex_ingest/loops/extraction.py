"""Extraction loop — the LLM semantic + relationship-edge layer (Stage 3).

Sixth supervised loop. Every EXTRACTION_INTERVAL_SECONDS it runs three phases over
the active (subscribed) catalog, all idempotent and resumable:

* **Reconcile** — poll any in-flight Anthropic batches (the async spend path); for
  ones that have *ended*, write their results (semantics / edges) and, for a
  primary-classification batch, submit the follow-on verify batch.
* **Phase A — per-market semantics.** Active markets with no current-version
  ``market_semantics`` row are extracted (``extract_market``) and cached. Market
  descriptions don't change after listing, so a market is extracted once and
  cached forever (until EXTRACTION_PROMPT_VERSION bumps). A steady-state trickle
  goes synchronously; a bulk backfill (≥ BATCH_BULK_SEMANTICS_THRESHOLD) is
  submitted to the discounted batch path.
* **Phase B — pairwise edges.** Candidate pairs among markets that now have
  semantics (cheaply picked by :mod:`..pair_candidates`) are first run through the
  time-to-resolution **spend gate** (:func:`..pair_candidates.route_pair`):
  - remaining life < floor → **skip** (the edge can't outlive its payback window);
  - floor ≤ life < batch_threshold → **sync** classify now (need it before the
    near endpoint resolves);
  - life ≥ batch_threshold → **batch** (latency irrelevant, take the 50% off).
  A classified pair is written to ``market_edges`` with a trust tier:
  - confidence ≥ EDGE_TRUSTED_CONFIDENCE **and** an independent second model
    (PAIR_VERIFY_MODEL) agrees on the relationship type → ``trusted`` (the
    solver's hard constraints). Disagreement → ``review``.
  - ≥ EDGE_SOFT_CONFIDENCE → ``soft`` (soft constraint).
  - below that → ``review`` (manual-review queue).

**Two transports, one set of prompts.** The synchronous OpenRouter client and the
asynchronous Anthropic batch client share the message builders + result parsers in
:mod:`..llm`, so a batched request is byte-identical to its sync twin and the trust
tiering is the same either way. A pair's primary and verify always travel the same
mode — a batched primary's verify is itself batched (it joins the next wave); a
sync primary's verify is sync.

**Soft-fail without a key.** If no ``OPENROUTER_API_KEY`` is configured, ``rt.llm``
is ``None`` and a cycle is a logged no-op — plain ingest runs unchanged. The
optional ``ANTHROPIC_API_KEY`` only adds the batch path; without it, batch-routed
work degrades to the synchronous path. The loop still heartbeats so ``/health``
stays green.

**One bad item never sinks the pass.** Every transport/parse/schema failure (sync
or per-request batch failure) is logged and that single market or pair is skipped,
mirroring the WS subscribers' log-and-drop discipline.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

from .. import constants as C
from ..llm import (
    PURPOSE_PAIR_PRIMARY,
    PURPOSE_PAIR_VERIFY,
    PURPOSE_SEMANTICS,
    LLMError,
    MarketSemantics,
    PairClassification,
    build_classify_messages,
    build_extract_messages,
    log_usage,
    parse_classification,
    parse_semantics,
)
from ..log import get_logger
from ..pair_candidates import (
    ROUTE_BATCH,
    ROUTE_SKIP,
    ROUTE_SYNC,
    candidate_pairs,
    remaining_life_seconds,
    route_pair,
)
from ..util import idle_sleep, naive_utc, now_utc

log = get_logger("extraction")

# Map the model's first/second-relative direction onto the canonical a/b columns.
# The loop always presents the canonical pair (a < b) as FIRST=a, SECOND=b.
_DIRECTION_TO_AB = {
    "first_implies_second": "a_implies_b",
    "second_implies_first": "b_implies_a",
    "symmetric": "symmetric",
    "none": "none",
}

# Batch-row purposes (the reconcile-dispatch key persisted in llm_batches.purpose).
# Distinct namespace from the usage-attribution PURPOSE_* tags, though the pair
# values coincide.
_BATCH_SEMANTICS = "semantics"
_BATCH_PRIMARY = "pair_primary"
_BATCH_VERIFY = "pair_verify"


def _semantics_row(market: dict, sem: MarketSemantics, model: str) -> tuple:
    return (
        market["market_id"],
        C.PLATFORM,
        sem.underlying_event,
        sem.resolves_yes_when,
        sem.resolves_no_when,
        sem.resolution_timing,
        json.dumps(list(sem.entities)),
        json.dumps(list(sem.dependencies)),
        model,
        C.EXTRACTION_PROMPT_VERSION,
        naive_utc(now_utc()),
        json.dumps(dataclasses.asdict(sem)),
    )


def _edge_row(
    a: str, b: str, cls: PairClassification, tier: str, agreement: str,
    model: str, verify_model: str | None,
) -> tuple:
    return (
        C.PLATFORM,
        a,
        b,
        cls.relationship_type,
        _DIRECTION_TO_AB.get(cls.direction, "none"),
        cls.confidence,
        tier,
        agreement,
        cls.rationale,
        model,
        verify_model,
        C.EXTRACTION_PROMPT_VERSION,
        naive_utc(now_utc()),
        json.dumps(dataclasses.asdict(cls)),
    )


def _tier_for(confidence: float) -> str:
    """Sub-trusted trust tier from a primary confidence (the band that needs no
    independent verify): ``soft`` or ``review``."""
    return "soft" if confidence >= C.EDGE_SOFT_CONFIDENCE else "review"


def _pair_priority(ma: dict, mb: dict, now) -> float:
    """Value key for the per-day budget ordering (docs/LLM-CALL-REDUCTION.md §5.1):
    classify longest-remaining-life pairs first — an edge's spend amortizes over its
    live life, so longer-lived edges are the higher-value buy. Unknown close time
    sorts highest (classify conservatively rather than defer a possibly-useful edge).
    """
    lives = [
        x for x in (remaining_life_seconds(ma, now), remaining_life_seconds(mb, now))
        if x is not None
    ]
    return float("inf") if not lives else min(lives)


class ExtractionLoop:
    name = "extraction"

    def __init__(self, rt) -> None:
        self.rt = rt

    @property
    def _batch(self):
        """The Anthropic batch client, or None when ANTHROPIC_API_KEY is absent."""
        return getattr(self.rt, "batch", None)

    async def run(self) -> None:
        if self.rt.llm is None:
            log.warning("OPENROUTER_API_KEY not set; extraction layer disabled (loop idles)")

        while not self.rt.shutdown.is_set():
            try:
                await self.extract_cycle()
            except Exception:
                log.exception("extraction cycle failed")
            self.rt.heartbeats.beat(self.name)
            await idle_sleep(
                self.rt.shutdown, self.rt.heartbeats, self.name, C.EXTRACTION_INTERVAL_SECONDS
            )

    async def extract_cycle(self) -> None:
        if self.rt.llm is None:
            return  # soft-fail: no key, nothing to do (still heartbeats in run())
        n_recon = await self._reconcile_batches()
        n_sem = await self._extract_semantics()
        n_edges = await self._classify_pairs()
        log.info(
            "extraction cycle complete",
            extra={"reconciled": n_recon, "semantics": n_sem, "edges": n_edges},
        )

    # -- batch helpers ------------------------------------------------------

    @staticmethod
    def _messages_params(model: str, system: str, user: str) -> dict:
        """Anthropic Messages params for one batched request. ``max_tokens`` is a
        mandatory field on the Batches API (see C.BATCH_MAX_TOKENS)."""
        return {
            "model": model,
            "max_tokens": C.BATCH_MAX_TOKENS,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "temperature": C.LLM_TEMPERATURE,
        }

    async def _submit_batch(
        self, purpose: str, model: str, requests: list[dict], payload: dict
    ) -> str | None:
        """Submit a batch and durably record it. Logs-and-skips on submit failure
        (the items aren't marked in-flight, so they're retried next cycle)."""
        if not requests:
            return None
        try:
            batch_id = await self._batch.submit(requests)
        except LLMError as exc:
            log.warning(
                "batch submit failed",
                extra={"purpose": purpose, "n": len(requests), "err": str(exc)},
            )
            return None
        # Log the id BEFORE persisting: submit already committed spend at Anthropic,
        # so if the DB write below is lost (raises / crash) an operator can still
        # recover or cancel the batch from this line.
        log.info("batch submitted", extra={"purpose": purpose, "batch": batch_id, "n": len(requests)})
        try:
            await asyncio.to_thread(
                self.rt.db.insert_batch,
                batch_id,
                provider="anthropic",
                purpose=purpose,
                model=model,
                version=C.EXTRACTION_PROMPT_VERSION,
                request_count=len(requests),
                submitted_at=now_utc(),
                payload=payload,
            )
        except Exception:
            # Don't let a failed persist abort the rest of the cycle: the batch is
            # orphaned at the provider (logged above) and its items simply re-spend
            # next cycle (at-least-once). Contained, not silent.
            log.exception(
                "batch state persist failed; batch orphaned at provider (items will re-spend)",
                extra={"purpose": purpose, "batch": batch_id},
            )
            return None
        return batch_id

    async def _reconcile_batches(self) -> int:
        """Poll in-flight batches; write results for any that have ended.

        Orphan backstop: a batch still open past ``BATCH_MAX_AGE_SECONDS`` — a
        wedged batch, or one whose poll/results keep failing terminally (e.g. a
        purged/404'd id) — is dropped so its covered items re-spend next cycle. The
        only deletion paths are "results written" and "abandoned past max age", so
        a row can never outlive its batch and silently strand its items.

        Returns the number of rows (semantics + edges) written this cycle."""
        if self._batch is None:
            return 0
        open_batches = await asyncio.to_thread(self.rt.db.get_open_batches)
        now = naive_utc(now_utc())
        written = 0
        for b in open_batches:
            submitted = b["submitted_at"]
            stale = submitted is not None and (now - submitted).total_seconds() > C.BATCH_MAX_AGE_SECONDS

            try:
                status = await self._batch.poll(b["batch_id"])
            except LLMError as exc:
                await self._maybe_abandon(b, stale, "poll failed", exc)
                continue

            if status.get("processing_status") != "ended":
                await self._maybe_abandon(b, stale, "still in progress", None)
                continue

            try:
                results = await self._batch.results(b["batch_id"])
            except LLMError as exc:
                await self._maybe_abandon(b, stale, "results fetch failed", exc)
                continue

            if b["purpose"] == _BATCH_SEMANTICS:
                written += await self._reconcile_semantics_batch(b, results)
            elif b["purpose"] == _BATCH_PRIMARY:
                written += await self._reconcile_primary_batch(b, results)
            elif b["purpose"] == _BATCH_VERIFY:
                written += await self._reconcile_verify_batch(b, results)
            await asyncio.to_thread(self.rt.db.delete_batch, b["batch_id"])
        return written

    async def _maybe_abandon(self, b: dict, stale: bool, reason: str, exc: Exception | None) -> None:
        """Drop an un-reconcilable batch once it's past max age (items re-spend);
        otherwise leave it for a later cycle to retry."""
        if stale:
            log.warning(
                "batch abandoned past max age, dropping for re-spend",
                extra={"batch": b["batch_id"], "purpose": b["purpose"], "reason": reason,
                       "err": str(exc) if exc else None},
            )
            await asyncio.to_thread(self.rt.db.delete_batch, b["batch_id"])
        elif exc is not None:
            log.warning("batch reconcile retrying", extra={"batch": b["batch_id"], "reason": reason, "err": str(exc)})

    async def _reconcile_semantics_batch(self, b: dict, results: list) -> int:
        payload = b["payload"]  # custom_id -> market_id
        rows: list[tuple] = []
        for r in results:
            mid = payload.get(r.custom_id)
            if mid is None:
                continue
            if r.content is None:
                log.warning("batch semantics skipped", extra={"market": mid, "err": r.error})
                continue
            try:
                sem = parse_semantics(r.content)
            except LLMError as exc:
                log.warning("batch semantics skipped", extra={"market": mid, "err": str(exc)})
                continue
            rows.append(_semantics_row({"market_id": mid}, sem, b["model"]))
            log_usage(mode="batch", purpose=PURPOSE_SEMANTICS, model=b["model"], usage=r.usage)
        await asyncio.to_thread(self.rt.db.upsert_market_semantics, rows)
        if rows:
            log.info("batch semantics reconciled", extra={"batch": b["batch_id"], "n": len(rows)})
        return len(rows)

    async def _reconcile_primary_batch(self, b: dict, results: list) -> int:
        """Write sub-trusted edges directly; defer high-confidence ones to a verify
        batch (same mode — never split a pair's primary/verify across transports)."""
        payload = b["payload"]  # custom_id -> [a, b]
        markets = await asyncio.to_thread(self.rt.db.get_active_markets_with_semantics)
        by_id = {m["market_id"]: m for m in markets}
        rows: list[tuple] = []
        verify_requests: list[dict] = []
        verify_payload: dict = {}
        for r in results:
            pair = payload.get(r.custom_id)
            if pair is None:
                continue
            a, bb = pair[0], pair[1]
            if r.content is None:
                log.warning("batch primary skipped", extra={"pair": [a, bb], "err": r.error})
                continue
            try:
                cls = parse_classification(r.content)
            except LLMError as exc:
                log.warning("batch primary skipped", extra={"pair": [a, bb], "err": str(exc)})
                continue
            log_usage(mode="batch", purpose=PURPOSE_PAIR_PRIMARY, model=b["model"], usage=r.usage)
            if cls.confidence >= C.EDGE_TRUSTED_CONFIDENCE and a in by_id and bb in by_id:
                cid = f"r{len(verify_requests)}"
                system, user = build_classify_messages(market_a=by_id[a], market_b=by_id[bb])
                verify_requests.append(
                    {"custom_id": cid, "params": self._messages_params(C.BATCH_PAIR_VERIFY_MODEL, system, user)}
                )
                # Carry the primary model forward so the edge's `model` attribution is
                # exact even if BATCH_PAIR_MODEL changes while the verify is in flight.
                verify_payload[cid] = {
                    "pair": [a, bb], "primary": dataclasses.asdict(cls), "primary_model": b["model"],
                }
            elif cls.confidence >= C.EDGE_TRUSTED_CONFIDENCE:
                # High confidence but an endpoint left the active set (resolved /
                # unsubscribed) → can't get an independent opinion → keep it soft.
                rows.append(_edge_row(a, bb, cls, "soft", "single", b["model"], None))
            else:
                rows.append(_edge_row(a, bb, cls, _tier_for(cls.confidence), "single", b["model"], None))
        await asyncio.to_thread(self.rt.db.upsert_edges, rows)
        await self._submit_batch(_BATCH_VERIFY, C.BATCH_PAIR_VERIFY_MODEL, verify_requests, verify_payload)
        if rows or verify_requests:
            log.info(
                "batch primary reconciled",
                extra={"batch": b["batch_id"], "written": len(rows), "to_verify": len(verify_requests)},
            )
        return len(rows)

    async def _reconcile_verify_batch(self, b: dict, results: list) -> int:
        payload = b["payload"]  # custom_id -> {pair, primary}
        rows: list[tuple] = []
        for r in results:
            ctx = payload.get(r.custom_id)
            if ctx is None:
                continue
            a, bb = ctx["pair"][0], ctx["pair"][1]
            primary = PairClassification(**ctx["primary"])
            primary_model = ctx.get("primary_model", C.BATCH_PAIR_MODEL)
            if r.content is None:
                # Couldn't get the independent opinion → keep it soft (mirrors the
                # sync path's verify-failure branch).
                rows.append(_edge_row(a, bb, primary, "soft", "single", primary_model, None))
                log.warning("batch verify unavailable, kept soft", extra={"pair": [a, bb], "err": r.error})
                continue
            try:
                verify = parse_classification(r.content)
            except LLMError as exc:
                rows.append(_edge_row(a, bb, primary, "soft", "single", primary_model, None))
                log.warning("batch verify unparseable, kept soft", extra={"pair": [a, bb], "err": str(exc)})
                continue
            log_usage(mode="batch", purpose=PURPOSE_PAIR_VERIFY, model=b["model"], usage=r.usage)
            if verify.relationship_type == primary.relationship_type:
                rows.append(_edge_row(a, bb, primary, "trusted", "agreed", primary_model, b["model"]))
            else:
                rows.append(_edge_row(a, bb, primary, "review", "disagreed", primary_model, b["model"]))
        await asyncio.to_thread(self.rt.db.upsert_edges, rows)
        if rows:
            log.info("batch verify reconciled", extra={"batch": b["batch_id"], "n": len(rows)})
        return len(rows)

    # -- Phase A ------------------------------------------------------------

    async def _extract_semantics(self) -> int:
        markets = await asyncio.to_thread(
            self.rt.db.get_markets_missing_semantics, C.EXTRACTION_PROMPT_VERSION
        )
        if not markets:
            return 0
        if self._batch is not None:
            inflight = await asyncio.to_thread(self.rt.db.get_inflight_semantics_markets)
            markets = [m for m in markets if m["market_id"] not in inflight]
        if not markets:
            return 0
        # Bulk backfill → batch (if configured); steady-state trickle → sync.
        if self._batch is not None and len(markets) >= C.BATCH_BULK_SEMANTICS_THRESHOLD:
            await self._submit_semantics_batch(markets[: C.BATCH_SUBMIT_MAX_REQUESTS])
            return 0  # nothing written this cycle; the "batch submitted" log carries the count
        return await self._extract_semantics_sync(markets[: C.EXTRACTION_BATCH_SIZE])

    async def _extract_semantics_sync(self, markets: list[dict]) -> int:
        rows: list[tuple] = []
        for market in markets:
            self.rt.heartbeats.beat(self.name)
            try:
                sem = await self.rt.llm.extract_market(
                    C.EXTRACTION_MODEL,
                    title=market.get("title"),
                    description=market.get("description"),
                    resolution_criteria=market.get("resolution_criteria"),
                    purpose=PURPOSE_SEMANTICS,
                )
            except LLMError as exc:
                log.warning(
                    "semantics extraction skipped",
                    extra={"market": market["market_id"], "err": str(exc)},
                )
                continue
            rows.append(_semantics_row(market, sem, C.EXTRACTION_MODEL))
            log.info("semantics extracted", extra={"market": market["market_id"]})
        await asyncio.to_thread(self.rt.db.upsert_market_semantics, rows)
        return len(rows)

    async def _submit_semantics_batch(self, markets: list[dict]) -> None:
        requests: list[dict] = []
        payload: dict = {}
        for i, market in enumerate(markets):
            cid = f"r{i}"
            system, user = build_extract_messages(
                title=market.get("title"),
                description=market.get("description"),
                resolution_criteria=market.get("resolution_criteria"),
            )
            requests.append(
                {"custom_id": cid, "params": self._messages_params(C.BATCH_EXTRACTION_MODEL, system, user)}
            )
            payload[cid] = market["market_id"]
        await self._submit_batch(_BATCH_SEMANTICS, C.BATCH_EXTRACTION_MODEL, requests, payload)

    # -- Phase B ------------------------------------------------------------

    async def _classify_pairs(self) -> int:
        markets = await asyncio.to_thread(self.rt.db.get_active_markets_with_semantics)
        if len(markets) < 2:
            return 0
        by_id = {m["market_id"]: m for m in markets}
        candidates, buckets = candidate_pairs(markets, return_breakdown=True)
        done = await asyncio.to_thread(self.rt.db.get_classified_pairs, C.EXTRACTION_PROMPT_VERSION)
        inflight = (
            await asyncio.to_thread(self.rt.db.get_inflight_pairs) if self._batch is not None else set()
        )
        todo = [p for p in candidates if p not in done and p not in inflight]
        if not todo:
            return 0

        now = naive_utc(now_utc())

        # Per-day spend budget — the volume bound the time gate can't provide for a
        # long-lived event set (docs/LLM-CALL-REDUCTION.md §5.1). Consumed = pairs
        # already classified today + pairs in flight today; spend the remainder
        # value-first (longest remaining life amortizes best) and defer the rest to
        # later days (logged, never silently dropped).
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        done_today = await asyncio.to_thread(self.rt.db.count_edges_classified_since, day_start)
        budget = max(0, C.EXTRACTION_MAX_PAIRS_PER_DAY - done_today - len(inflight))
        todo.sort(key=lambda p: _pair_priority(by_id[p[0]], by_id[p[1]], now), reverse=True)
        deferred = max(0, len(todo) - budget)
        todo = todo[:budget]

        # Spend gate: bucket each budgeted candidate by remaining life.
        sync_pairs: list[tuple[str, str]] = []   # short-fuse — classify now
        batch_pairs: list[tuple[str, str]] = []  # long-horizon — discounted async
        for a, b in todo:
            route = route_pair(by_id[a], by_id[b], now)
            if route == ROUTE_SKIP:
                continue
            (batch_pairs if route == ROUTE_BATCH else sync_pairs).append((a, b))

        # Surface the gate AND the budget in the deploy logs: skip (below the life
        # floor) and deferred (over today's budget) are both real coverage caps, so
        # they must be observable, not silent. The bucket_* counts say which
        # candidate-generation bucket drives the volume.
        n_skip = len(todo) - len(sync_pairs) - len(batch_pairs)
        log.info(
            "pair routing",
            extra={"candidates": len(candidates), "todo": len(todo), "deferred": deferred,
                   "budget_remaining": budget, "skip": n_skip,
                   "sync": len(sync_pairs), "batch": len(batch_pairs),
                   "bucket_event": buckets["event"], "bucket_series": buckets["series"],
                   "bucket_entity": buckets["entity"]},
        )

        if self._batch is not None:
            await self._submit_primary_batch(batch_pairs[: C.BATCH_SUBMIT_MAX_REQUESTS], by_id)
            sync_todo = sync_pairs
        else:
            # No batch provider: long-horizon pairs degrade to sync, but short-fuse
            # pairs go first (they must be classified before their near endpoint
            # resolves), so a long backlog can't starve them.
            sync_todo = sync_pairs + batch_pairs

        rows: list[tuple] = []
        for a, b in sync_todo[: C.EXTRACTION_BATCH_SIZE]:
            self.rt.heartbeats.beat(self.name)
            try:
                cls, tier, agreement, verify_model = await self._classify_one(by_id[a], by_id[b])
            except LLMError as exc:
                log.warning("pair classification skipped", extra={"pair": [a, b], "err": str(exc)})
                continue
            rows.append(_edge_row(a, b, cls, tier, agreement, C.PAIR_MODEL, verify_model))
            log.info(
                "edge classified",
                extra={"pair": [a, b], "rel": cls.relationship_type, "tier": tier},
            )
        await asyncio.to_thread(self.rt.db.upsert_edges, rows)
        return len(rows)

    async def _submit_primary_batch(self, pairs: list[tuple[str, str]], by_id: dict) -> None:
        requests: list[dict] = []
        payload: dict = {}
        for i, (a, b) in enumerate(pairs):
            cid = f"r{i}"
            system, user = build_classify_messages(market_a=by_id[a], market_b=by_id[b])
            requests.append(
                {"custom_id": cid, "params": self._messages_params(C.BATCH_PAIR_MODEL, system, user)}
            )
            payload[cid] = [a, b]
        await self._submit_batch(_BATCH_PRIMARY, C.BATCH_PAIR_MODEL, requests, payload)

    async def _classify_one(
        self, ma: dict, mb: dict
    ) -> tuple[PairClassification, str, str, str | None]:
        """Classify one pair synchronously and route it to a trust tier.

        A high-confidence label is only promoted to ``trusted`` if an independent
        second model agrees on the relationship type; disagreement (or a verify
        failure) keeps it out of the hard-constraint set."""
        cls = await self.rt.llm.classify_pair(
            C.PAIR_MODEL, market_a=ma, market_b=mb, purpose=PURPOSE_PAIR_PRIMARY
        )
        if cls.confidence >= C.EDGE_TRUSTED_CONFIDENCE:
            try:
                verify = await self.rt.llm.classify_pair(
                    C.PAIR_VERIFY_MODEL, market_a=ma, market_b=mb, purpose=PURPOSE_PAIR_VERIFY
                )
            except LLMError:
                # Can't get an independent opinion → can't trust it; keep it soft.
                return cls, "soft", "single", None
            if verify.relationship_type == cls.relationship_type:
                return cls, "trusted", "agreed", C.PAIR_VERIFY_MODEL
            return cls, "review", "disagreed", C.PAIR_VERIFY_MODEL
        if cls.confidence >= C.EDGE_SOFT_CONFIDENCE:
            return cls, "soft", "single", None
        return cls, "review", "single", None


def inspect_once() -> None:
    """Dry-run inspector: print the extraction *work plan* from the local DB.

    Reads the DuckDB file read-only — no model calls, no spend, no writes — and
    prints how many active markets still need semantics and how many candidate
    pairs Stage B would consider. Run with::

        python -m simplex_ingest.loops.extraction

    Note: DuckDB is single-writer, so this only works while the ingest process is
    **stopped** (a read-only open fails while the writer holds the lock)."""
    import duckdb

    from ..config import get_settings

    settings = get_settings()
    path = settings.db_path
    con = duckdb.connect(str(path), read_only=True)

    missing = con.execute(
        """
        SELECT count(*) FROM markets m
        LEFT JOIN market_semantics s ON s.market_id = m.market_id
        WHERE m.subscribed = TRUE
          AND (s.market_id IS NULL OR s.extraction_version IS DISTINCT FROM ?)
        """,
        [C.EXTRACTION_PROMPT_VERSION],
    ).fetchone()[0]

    rows = con.execute(
        """
        SELECT m.market_id, m.series_ticker, m.event_ticker, s.entities, m.closes_at
        FROM markets m JOIN market_semantics s ON s.market_id = m.market_id
        WHERE m.subscribed = TRUE
        """
    ).fetchall()
    markets = [
        {
            "market_id": r[0],
            "series_ticker": r[1],
            "event_ticker": r[2],
            "entities": json.loads(r[3]) if r[3] else [],
            "closes_at": r[4],
        }
        for r in rows
    ]
    candidates = candidate_pairs(markets)
    now = naive_utc(now_utc())
    routes = {ROUTE_SKIP: 0, ROUTE_SYNC: 0, ROUTE_BATCH: 0}
    by_id = {m["market_id"]: m for m in markets}
    for a, b in candidates:
        routes[route_pair(by_id[a], by_id[b], now)] += 1
    classified = con.execute(
        "SELECT count(*) FROM market_edges WHERE extraction_version = ?",
        [C.EXTRACTION_PROMPT_VERSION],
    ).fetchone()[0]
    open_batches = con.execute("SELECT count(*) FROM llm_batches").fetchone()[0]
    con.close()

    print(f"DB: {path}")
    print(f"prompt version: {C.EXTRACTION_PROMPT_VERSION}")
    print(f"\nPhase A — active markets needing semantics: {missing}")
    print(f"Phase B — markets with semantics: {len(markets)}")
    print(f"          candidate pairs: {len(candidates)}")
    print(f"          gate → skip: {routes[ROUTE_SKIP]}  sync: {routes[ROUTE_SYNC]}  batch: {routes[ROUTE_BATCH]}")
    print(f"          already classified (this version): {classified}")
    print(f"in-flight batches: {open_batches}")


if __name__ == "__main__":
    inspect_once()
