"""Structural + tradeability predicates for series discovery.

Pure functions over per-series aggregated stats — no I/O, no Kalshi client, no
DuckDB. The discovery loop fetches one ``rest.get_events`` sweep, hands the raw
event dicts to :func:`aggregate`, then admits/ranks the resulting
:class:`SeriesStats` with :func:`evaluate` / :func:`rank_key`.

Replaces the old weighted log-sum score (``scripts/discover_series.py``) with
binary admit/reject predicates:

* **P1 — Partition.** The series has >= 1 open event flagged
  ``mutually_exclusive`` with >= ``PREDICATE_PARTITION_MIN_MARKETS`` tradeable
  markets. A clean mutex partition is the richest coherence constraint.
* **P2 — Hierarchy.** The series has >= 1 open event with
  >= ``PREDICATE_HIERARCHY_MIN_MARKETS`` *distinct* markets (distinct
  ``yes_sub_title`` / ``subtitle``, falling back to strike). Distinctness is the
  point: two markets sharing a subtitle are degenerate, not a hierarchy.
* **P3 — Tradeability.** Series-summed volume >= ``PREDICATE_MIN_VOLUME_24H``.

A series is admitted iff ``(P1 OR P2) AND P3``: P1/P2 give the solver internal
structure to chew on, P3 ensures the WS feed actually carries signal.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import constants as C
from .util import market_volume

# Kalshi market statuses considered tradeable / eligible (mirrors catalog.py).
TRADEABLE_STATUSES = frozenset({"active"})


def _market_label(market: dict) -> str | None:
    """A market's distinguishing label within its event, or None if it has none.

    Used for P2's *distinct*-markets count: two markets with the same label are
    one bucket, not two. Subtitle fields win; absent those, a strike value still
    distinguishes (so a subtitle market and a strike market count as two)."""
    for key in ("yes_sub_title", "subtitle"):
        v = market.get(key)
        if v:
            return str(v)
    for key in ("cap_strike", "floor_strike", "strike", "expiration_value"):
        v = market.get(key)
        if v is not None:
            return f"{key}={v}"
    return None


@dataclass(frozen=True)
class EventStats:
    """Per-event facts the predicates need — already filtered to tradeable markets."""

    mutually_exclusive: bool
    n_tradeable_markets: int
    n_distinct_markets: int


@dataclass(frozen=True)
class SeriesStats:
    """Aggregated stats for one series, the unit the predicates operate on."""

    ticker: str
    events: tuple[EventStats, ...] = ()
    volume_24h: float = 0.0


@dataclass(frozen=True)
class Verdict:
    """Outcome of evaluating a series against the predicate set."""

    admit: bool
    passes: dict[str, bool]
    n_partition_events: int
    n_hierarchy_events: int
    reason: str


def evaluate(stats: SeriesStats) -> Verdict:
    """Apply P1/P2/P3 to a series. Admit iff ``(P1 OR P2) AND P3``."""
    n_partition = sum(
        1
        for e in stats.events
        if e.mutually_exclusive
        and e.n_tradeable_markets >= C.PREDICATE_PARTITION_MIN_MARKETS
    )
    n_hierarchy = sum(
        1 for e in stats.events if e.n_distinct_markets >= C.PREDICATE_HIERARCHY_MIN_MARKETS
    )
    p1 = n_partition >= 1
    p2 = n_hierarchy >= 1
    p3 = stats.volume_24h >= C.PREDICATE_MIN_VOLUME_24H
    admit = (p1 or p2) and p3
    passes = {"P1": p1, "P2": p2, "P3": p3}
    failed = [name for name, ok in passes.items() if not ok]
    reason = ("admit" if admit else "reject") + (
        f" (failed: {','.join(failed)})" if failed else ""
    )
    return Verdict(
        admit=admit,
        passes=passes,
        n_partition_events=n_partition,
        n_hierarchy_events=n_hierarchy,
        reason=reason,
    )


def rank_key(stats: SeriesStats) -> tuple:
    """Strict ordinal sort key (descending) for eviction at the tracked cap.

    ``(passes_P1, n_partition_events, n_hierarchy_events, volume_24h)`` — no
    weights, no series_ticker (irrelevant perturbations must not reorder)."""
    v = evaluate(stats)
    return (v.passes["P1"], v.n_partition_events, v.n_hierarchy_events, stats.volume_24h)


def aggregate(events: list[dict]) -> dict[str, SeriesStats]:
    """Group a flat ``get_events`` sweep into per-series stats. Pure; no I/O.

    Only tradeable markets count toward per-event market/label counts and the
    summed volume; non-tradeable (settled/closed) markets are ignored."""
    events_by_series: dict[str, list[EventStats]] = {}
    volume_by_series: dict[str, float] = {}
    for event in events:
        series = event.get("series_ticker")
        if not series:
            continue
        markets = [
            m for m in (event.get("markets") or []) if m.get("status") in TRADEABLE_STATUSES
        ]
        labels = {lbl for m in markets if (lbl := _market_label(m)) is not None}
        events_by_series.setdefault(series, []).append(
            EventStats(
                mutually_exclusive=bool(event.get("mutually_exclusive")),
                n_tradeable_markets=len(markets),
                n_distinct_markets=len(labels),
            )
        )
        volume_by_series[series] = volume_by_series.get(series, 0.0) + sum(
            market_volume(m) for m in markets
        )
    return {
        series: SeriesStats(
            ticker=series,
            events=tuple(evs),
            volume_24h=volume_by_series.get(series, 0.0),
        )
        for series, evs in events_by_series.items()
    }
