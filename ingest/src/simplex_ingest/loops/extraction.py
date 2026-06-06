"""Extraction loop — the LLM semantic + relationship-edge layer (Stage 3).

Sixth supervised loop. Every EXTRACTION_INTERVAL_SECONDS it runs two phases over
the active (subscribed) catalog, both idempotent and resumable:

* **Phase A — per-market semantics.** Active markets with no current-version
  ``market_semantics`` row are sent to the model one at a time
  (``extract_market``) and the structured result is cached. Market descriptions
  don't change after listing, so a market is extracted once and cached forever
  (until EXTRACTION_PROMPT_VERSION bumps).
* **Phase B — pairwise edges.** Candidate pairs among markets that now have
  semantics (cheaply picked by :mod:`..pair_candidates` — same event/series or
  entity overlap, never all O(n²) pairs) are classified into a typed relationship
  (``classify_pair``) and written to ``market_edges`` with a trust tier:
  - confidence ≥ EDGE_TRUSTED_CONFIDENCE **and** an independent second model
    (PAIR_VERIFY_MODEL) agrees on the relationship type → ``trusted`` (the
    solver's hard constraints). Disagreement → ``review``.
  - ≥ EDGE_SOFT_CONFIDENCE → ``soft`` (soft constraint).
  - below that → ``review`` (manual-review queue).

**Soft-fail without a key.** If no ``OPENROUTER_API_KEY`` is configured,
``rt.llm`` is ``None`` and a cycle is a logged no-op — plain ingest runs
unchanged. The loop still heartbeats so ``/health`` stays green.

**One bad item never sinks the pass.** The model client raises
:class:`..llm.LLMError` on any transport/parse/schema failure; the loop logs and
skips that single market or pair, mirroring the WS subscribers' log-and-drop
discipline.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

from .. import constants as C
from ..llm import LLMError, MarketSemantics, PairClassification
from ..log import get_logger
from ..pair_candidates import candidate_pairs
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


def _semantics_row(market: dict, sem: MarketSemantics) -> tuple:
    return (
        market["market_id"],
        C.PLATFORM,
        sem.underlying_event,
        sem.resolves_yes_when,
        sem.resolves_no_when,
        sem.resolution_timing,
        json.dumps(list(sem.entities)),
        json.dumps(list(sem.dependencies)),
        C.EXTRACTION_MODEL,
        C.EXTRACTION_PROMPT_VERSION,
        naive_utc(now_utc()),
        json.dumps(dataclasses.asdict(sem)),
    )


def _edge_row(
    a: str, b: str, cls: PairClassification, tier: str, agreement: str,
    verify_model: str | None,
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
        C.PAIR_MODEL,
        verify_model,
        C.EXTRACTION_PROMPT_VERSION,
        naive_utc(now_utc()),
        json.dumps(dataclasses.asdict(cls)),
    )


class ExtractionLoop:
    name = "extraction"

    def __init__(self, rt) -> None:
        self.rt = rt

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
        n_sem = await self._extract_semantics()
        n_edges = await self._classify_pairs()
        log.info("extraction cycle complete", extra={"semantics": n_sem, "edges": n_edges})

    # -- Phase A ------------------------------------------------------------

    async def _extract_semantics(self) -> int:
        markets = await asyncio.to_thread(
            self.rt.db.get_markets_missing_semantics, C.EXTRACTION_PROMPT_VERSION
        )
        rows: list[tuple] = []
        for market in markets[: C.EXTRACTION_BATCH_SIZE]:
            self.rt.heartbeats.beat(self.name)
            try:
                sem = await self.rt.llm.extract_market(
                    C.EXTRACTION_MODEL,
                    title=market.get("title"),
                    description=market.get("description"),
                    resolution_criteria=market.get("resolution_criteria"),
                )
            except LLMError as exc:
                log.warning(
                    "semantics extraction skipped",
                    extra={"market": market["market_id"], "err": str(exc)},
                )
                continue
            rows.append(_semantics_row(market, sem))
            log.info("semantics extracted", extra={"market": market["market_id"]})
        await asyncio.to_thread(self.rt.db.upsert_market_semantics, rows)
        return len(rows)

    # -- Phase B ------------------------------------------------------------

    async def _classify_pairs(self) -> int:
        markets = await asyncio.to_thread(self.rt.db.get_active_markets_with_semantics)
        if len(markets) < 2:
            return 0
        by_id = {m["market_id"]: m for m in markets}
        candidates = candidate_pairs(markets)
        done = await asyncio.to_thread(
            self.rt.db.get_classified_pairs, C.EXTRACTION_PROMPT_VERSION
        )
        todo = [p for p in candidates if p not in done]

        rows: list[tuple] = []
        for a, b in todo[: C.EXTRACTION_BATCH_SIZE]:
            self.rt.heartbeats.beat(self.name)
            try:
                cls, tier, agreement, verify_model = await self._classify_one(by_id[a], by_id[b])
            except LLMError as exc:
                log.warning("pair classification skipped", extra={"pair": [a, b], "err": str(exc)})
                continue
            rows.append(_edge_row(a, b, cls, tier, agreement, verify_model))
            log.info(
                "edge classified",
                extra={"pair": [a, b], "rel": cls.relationship_type, "tier": tier},
            )
        await asyncio.to_thread(self.rt.db.upsert_edges, rows)
        return len(rows)

    async def _classify_one(
        self, ma: dict, mb: dict
    ) -> tuple[PairClassification, str, str, str | None]:
        """Classify one pair and route it to a trust tier.

        A high-confidence label is only promoted to ``trusted`` if an independent
        second model agrees on the relationship type; disagreement (or a verify
        failure) keeps it out of the hard-constraint set."""
        cls = await self.rt.llm.classify_pair(C.PAIR_MODEL, market_a=ma, market_b=mb)
        if cls.confidence >= C.EDGE_TRUSTED_CONFIDENCE:
            try:
                verify = await self.rt.llm.classify_pair(C.PAIR_VERIFY_MODEL, market_a=ma, market_b=mb)
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
        SELECT m.market_id, m.series_ticker, m.event_ticker, s.entities
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
        }
        for r in rows
    ]
    candidates = candidate_pairs(markets)
    classified = con.execute(
        "SELECT count(*) FROM market_edges WHERE extraction_version = ?",
        [C.EXTRACTION_PROMPT_VERSION],
    ).fetchone()[0]
    con.close()

    print(f"DB: {path}")
    print(f"prompt version: {C.EXTRACTION_PROMPT_VERSION}")
    print(f"\nPhase A — active markets needing semantics: {missing}")
    print(f"Phase B — markets with semantics: {len(markets)}")
    print(f"          candidate pairs: {len(candidates)}")
    print(f"          already classified (this version): {classified}")
    print(f"          would classify this run: ~{min(len(candidates), C.EXTRACTION_BATCH_SIZE)} per cycle")


if __name__ == "__main__":
    inspect_once()
