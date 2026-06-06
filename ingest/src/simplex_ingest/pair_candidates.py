"""Cheap candidate-pair generation for the extraction layer's Stage B.

Pure function over per-market dicts — no I/O, no LLM, no DuckDB (parallel to
:mod:`discovery_predicates`). The job is to pick which market *pairs* are worth
spending an LLM call on, so Stage B never has to classify all O(n²) pairs.

A pair (a, b) is a candidate iff **any** of:

* **same event** — identical ``event_ticker`` (Kalshi already grouped them), or
* **same series** — identical ``series_ticker`` (same question family), or
* **entity overlap** — their Stage-A ``entities`` share at least
  ``PAIR_ENTITY_OVERLAP_MIN`` normalized entities.

Generation is bucketed (group-by-event, group-by-series, inverted entity index)
rather than a full pairwise scan, so it stays well under O(n²) on the bounded
tracked set. Output pairs are canonical (``a < b``) and de-duplicated.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from itertools import combinations

from . import constants as C

# Spend-gate routes for a candidate pair (see docs/LLM-COST-MIGRATION.md §4 and
# the EDGE_REMAINING_LIFE_FLOOR / EDGE_BATCH_THRESHOLD constants). The loop maps
# these to: drop the pair / classify it synchronously / classify it via batch.
ROUTE_SKIP = "skip"
ROUTE_SYNC = "sync"
ROUTE_BATCH = "batch"


def _canon(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a < b else (b, a)


def remaining_life_seconds(market: dict, now: datetime) -> float | None:
    """Seconds until ``market`` resolves, from its ``closes_at``; None if unknown.

    ``now`` and ``closes_at`` are both naive UTC (DuckDB's TIMESTAMP convention).
    A market with no recorded close time returns None — the caller decides how to
    treat unknown life (the gate routes it sync rather than silently skipping)."""
    closes = market.get("closes_at")
    if closes is None:
        return None
    return (closes - now).total_seconds()


def route_pair(
    ma: dict,
    mb: dict,
    now: datetime,
    *,
    floor_seconds: float | None = None,
    batch_threshold_seconds: float | None = None,
) -> str:
    """Route a candidate pair by its remaining life = min(life of A, life of B).

    Returns one of ``ROUTE_SKIP`` / ``ROUTE_SYNC`` / ``ROUTE_BATCH``:

    * below ``floor_seconds`` of remaining life → SKIP (negative-ROI: the edge
      can't outlive its payback window);
    * below ``batch_threshold_seconds`` → SYNC (pay full price to get it in time);
    * at/above the threshold → BATCH (latency irrelevant, take the discount).

    An edge dies when *either* endpoint settles, so the binding quantity is the
    nearer close time. If neither endpoint's close time is known, route SYNC
    (conservative — never silently skip a possibly-useful edge on missing data)."""
    if floor_seconds is None:
        floor_seconds = C.EDGE_REMAINING_LIFE_FLOOR_SECONDS
    if batch_threshold_seconds is None:
        batch_threshold_seconds = C.EDGE_BATCH_THRESHOLD_SECONDS
    lives = [
        x for x in (remaining_life_seconds(ma, now), remaining_life_seconds(mb, now))
        if x is not None
    ]
    if not lives:
        return ROUTE_SYNC
    life = min(lives)
    if life < floor_seconds:
        return ROUTE_SKIP
    if life < batch_threshold_seconds:
        return ROUTE_SYNC
    return ROUTE_BATCH


def _entities(market: dict) -> set[str]:
    """Normalized entity set for overlap comparison (lowercased, stripped)."""
    out: set[str] = set()
    for e in market.get("entities") or []:
        s = str(e).strip().lower()
        if s:
            out.add(s)
    return out


def candidate_pairs(
    markets: list[dict], *, entity_overlap_min: int | None = None
) -> list[tuple[str, str]]:
    """Canonical, de-duplicated candidate pairs from ``markets``.

    Each market dict needs ``market_id`` and, optionally, ``series_ticker`` /
    ``event_ticker`` / ``entities``. ``entity_overlap_min`` defaults to
    ``constants.PAIR_ENTITY_OVERLAP_MIN``.
    """
    if entity_overlap_min is None:
        entity_overlap_min = C.PAIR_ENTITY_OVERLAP_MIN

    pairs: set[tuple[str, str]] = set()

    by_event: dict[str, set[str]] = defaultdict(set)
    by_series: dict[str, set[str]] = defaultdict(set)
    by_entity: dict[str, set[str]] = defaultdict(set)

    for m in markets:
        mid = m.get("market_id")
        if not mid:
            continue
        if m.get("event_ticker"):
            by_event[m["event_ticker"]].add(mid)
        if m.get("series_ticker"):
            by_series[m["series_ticker"]].add(mid)
        for e in _entities(m):
            by_entity[e].add(mid)

    # Hierarchy buckets: every within-bucket pair is a candidate.
    for bucket in (*by_event.values(), *by_series.values()):
        for a, b in combinations(sorted(bucket), 2):
            pairs.add((a, b))

    # Entity overlap: a pair co-occurs in one entity bucket per shared entity, so
    # counting co-occurrences yields |entities(a) ∩ entities(b)| directly.
    shared: dict[tuple[str, str], int] = defaultdict(int)
    for mids in by_entity.values():
        if len(mids) < 2:
            continue
        for a, b in combinations(sorted(mids), 2):
            shared[(a, b)] += 1
    for pair, overlap in shared.items():
        if overlap >= entity_overlap_min:
            pairs.add(pair)

    return sorted(pairs)
