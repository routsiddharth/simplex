"""Unit tests for the discovery predicate module — covered like a library."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from simplex_ingest import constants as C
from simplex_ingest.discovery_predicates import (
    EventStats,
    SeriesStats,
    aggregate,
    evaluate,
    rank_key,
)

HIGH_VOL = C.PREDICATE_MIN_VOLUME_24H + 1.0  # comfortably passes P3


# --------------------------------------------------------------------------
# P1 — Partition
# --------------------------------------------------------------------------

@pytest.mark.parametrize("n_markets", [0, 1, 2, 3, 4])
def test_p1_market_count_threshold(make_series_stats, make_event_stats, n_markets):
    # Mutex event with N markets; P2 isolated out via n_distinct=0.
    stats = make_series_stats(
        events=[make_event_stats(mutex=True, n_tradeable=n_markets, n_distinct=0)],
        volume=HIGH_VOL,
    )
    v = evaluate(stats)
    assert v.passes["P1"] is (n_markets >= C.PREDICATE_PARTITION_MIN_MARKETS)


def test_p1_requires_mutex_flag(make_series_stats, make_event_stats):
    # 10 markets but not mutually exclusive: not a partition.
    stats = make_series_stats(
        events=[make_event_stats(mutex=False, n_tradeable=10, n_distinct=0)],
        volume=HIGH_VOL,
    )
    assert evaluate(stats).passes["P1"] is False


def test_p1_any_event_satisfies(make_series_stats, make_event_stats):
    stats = make_series_stats(
        events=[
            make_event_stats(mutex=True, n_tradeable=1, n_distinct=0),
            make_event_stats(mutex=True, n_tradeable=3, n_distinct=0),
        ],
        volume=HIGH_VOL,
    )
    v = evaluate(stats)
    assert v.passes["P1"] is True
    assert v.n_partition_events == 1


def test_p1_non_tradeable_markets_excluded(make_event, make_market):
    # aggregate() filters non-active markets before counting toward P1.
    event = make_event(
        series="KXP1",
        mutex=True,
        markets=[
            make_market("KXP1-a", status="active", subtitle="a"),
            make_market("KXP1-b", status="active", subtitle="b"),
            make_market("KXP1-c", status="settled", subtitle="c"),
            make_market("KXP1-d", status="closed", subtitle="d"),
        ],
    )
    stats = aggregate([event])["KXP1"]
    # Only 2 active markets -> below the 3-market partition floor.
    assert stats.events[0].n_tradeable_markets == 2
    assert evaluate(SeriesStats("KXP1", stats.events, HIGH_VOL)).passes["P1"] is False


# --------------------------------------------------------------------------
# P2 — Hierarchy
# --------------------------------------------------------------------------

@pytest.mark.parametrize("n_distinct", [0, 1, 2, 3])
def test_p2_distinct_count_threshold(make_series_stats, make_event_stats, n_distinct):
    stats = make_series_stats(
        events=[make_event_stats(mutex=False, n_tradeable=n_distinct, n_distinct=n_distinct)],
        volume=HIGH_VOL,
    )
    assert evaluate(stats).passes["P2"] is (n_distinct >= C.PREDICATE_HIERARCHY_MIN_MARKETS)


def test_p2_identical_subtitles_degenerate(make_event, make_market):
    event = make_event(
        series="KXH",
        markets=[
            make_market("KXH-a", subtitle="Yes"),
            make_market("KXH-b", subtitle="Yes"),
        ],
    )
    stats = aggregate([event])["KXH"]
    assert stats.events[0].n_distinct_markets == 1  # collapsed
    assert evaluate(SeriesStats("KXH", stats.events, HIGH_VOL)).passes["P2"] is False


def test_p2_distinct_subtitles_pass(make_event, make_market):
    event = make_event(
        series="KXH",
        markets=[make_market("KXH-a", subtitle="Up"), make_market("KXH-b", subtitle="Down")],
    )
    stats = aggregate([event])["KXH"]
    assert stats.events[0].n_distinct_markets == 2
    assert evaluate(SeriesStats("KXH", stats.events, HIGH_VOL)).passes["P2"] is True


def test_p2_subtitle_and_strike_both_count(make_event, make_market):
    # One market distinguished by subtitle, the other by strike -> two buckets.
    event = make_event(
        series="KXH",
        markets=[
            make_market("KXH-a", subtitle="Above"),
            make_market("KXH-b", cap_strike=100),
        ],
    )
    stats = aggregate([event])["KXH"]
    assert stats.events[0].n_distinct_markets == 2
    assert evaluate(SeriesStats("KXH", stats.events, HIGH_VOL)).passes["P2"] is True


# --------------------------------------------------------------------------
# P3 — Tradeability
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "volume,expected",
    [
        (C.PREDICATE_MIN_VOLUME_24H - 1, False),
        (C.PREDICATE_MIN_VOLUME_24H, True),
        (C.PREDICATE_MIN_VOLUME_24H + 1, True),
    ],
)
def test_p3_volume_boundary(make_series_stats, make_event_stats, volume, expected):
    stats = make_series_stats(
        events=[make_event_stats(mutex=True, n_tradeable=3, n_distinct=3)],
        volume=volume,
    )
    assert evaluate(stats).passes["P3"] is expected


def test_p3_volume_summed_across_markets(make_event, make_market):
    # Each market below the floor individually; the series sum clears it.
    third = C.PREDICATE_MIN_VOLUME_24H / 3 + 1
    event = make_event(
        series="KXV",
        mutex=True,
        markets=[make_market(f"KXV-{i}", volume=third, subtitle=str(i)) for i in range(3)],
    )
    stats = aggregate([event])["KXV"]
    assert stats.volume_24h >= C.PREDICATE_MIN_VOLUME_24H
    assert evaluate(stats).passes["P3"] is True


def test_p3_volume_field_fallbacks(make_event):
    # volume_fp missing -> volume -> volume_24h_fp, mirroring util.market_volume.
    event = make_event(
        series="KXV",
        markets=[
            {"ticker": "KXV-a", "status": "active", "volume": 600, "subtitle": "a"},
            {"ticker": "KXV-b", "status": "active", "volume_24h_fp": 600, "subtitle": "b"},
        ],
    )
    stats = aggregate([event])["KXV"]
    assert stats.volume_24h == 1200.0


# --------------------------------------------------------------------------
# Composite admit logic
# --------------------------------------------------------------------------

def _stats_for(make_series_stats, make_event_stats, p1, p2, p3):
    events = []
    if p1:
        events.append(make_event_stats(mutex=True, n_tradeable=3, n_distinct=0))
    if p2:
        events.append(make_event_stats(mutex=False, n_tradeable=2, n_distinct=2))
    if not events:
        events.append(make_event_stats())  # all predicates fail
    return make_series_stats(events=events, volume=(HIGH_VOL if p3 else 0.0))


@pytest.mark.parametrize("p1", [True, False])
@pytest.mark.parametrize("p2", [True, False])
@pytest.mark.parametrize("p3", [True, False])
def test_admit_truth_table(make_series_stats, make_event_stats, p1, p2, p3):
    stats = _stats_for(make_series_stats, make_event_stats, p1, p2, p3)
    v = evaluate(stats)
    assert v.passes == {"P1": p1, "P2": p2, "P3": p3}
    assert v.admit is ((p1 or p2) and p3)


def test_reason_lists_every_failed_predicate(make_series_stats, make_event_stats):
    stats = _stats_for(make_series_stats, make_event_stats, p1=False, p2=False, p3=False)
    reason = evaluate(stats).reason
    for name in ("P1", "P2", "P3"):
        assert name in reason


# --------------------------------------------------------------------------
# Ranking — property tests
# --------------------------------------------------------------------------

_event_st = st.builds(
    EventStats,
    mutually_exclusive=st.booleans(),
    n_tradeable_markets=st.integers(min_value=0, max_value=10),
    n_distinct_markets=st.integers(min_value=0, max_value=10),
)
_series_st = st.builds(
    lambda evs, vol, i: SeriesStats(ticker=f"S{i}", events=tuple(evs), volume_24h=vol),
    evs=st.lists(_event_st, max_size=5),
    vol=st.floats(min_value=0.0, max_value=1e7, allow_nan=False, allow_infinity=False),
    i=st.integers(min_value=0, max_value=10_000_000),
)


@given(st.lists(_series_st, max_size=20))
def test_ranking_is_total_order(stats_list):
    keys = [rank_key(s) for s in sorted(stats_list, key=rank_key, reverse=True)]
    assert keys == sorted(keys, reverse=True)


@given(st.lists(_series_st, min_size=1, max_size=20))
def test_strictly_worse_never_displaces_better(stats_list):
    k = 5
    worst = SeriesStats(ticker="WORST", events=(), volume_24h=-1.0)  # below any real key
    top_after = sorted(stats_list + [worst], key=rank_key, reverse=True)[:k]
    assert all(s is not worst for s in top_after) or len(stats_list) < k


@given(_series_st, st.text(min_size=1, max_size=12))
def test_rank_key_ignores_ticker(s, new_ticker):
    s2 = SeriesStats(ticker=new_ticker, events=s.events, volume_24h=s.volume_24h)
    assert rank_key(s) == rank_key(s2)


# --------------------------------------------------------------------------
# Edge cases
# --------------------------------------------------------------------------

def test_empty_events_rejected(make_series_stats):
    assert evaluate(make_series_stats(events=(), volume=HIGH_VOL)).admit is False


def test_giant_non_mutex_event_passes_p2(make_series_stats, make_event_stats):
    stats = make_series_stats(
        events=[make_event_stats(mutex=False, n_tradeable=1000, n_distinct=1000)],
        volume=HIGH_VOL,
    )
    v = evaluate(stats)
    assert v.passes["P1"] is False
    assert v.passes["P2"] is True
    assert v.admit is True


def test_aggregate_skips_seriesless_events(make_event):
    event = {"event_ticker": "E", "mutually_exclusive": True, "markets": []}
    assert aggregate([event]) == {}
