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
from itertools import combinations

from . import constants as C


def _canon(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a < b else (b, a)


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
