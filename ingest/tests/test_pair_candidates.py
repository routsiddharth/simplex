"""Tests for the pure candidate-pair generator + the spend gate (Stage B input)."""

from __future__ import annotations

from datetime import datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from simplex_ingest import constants as C
from simplex_ingest.pair_candidates import (
    ROUTE_BATCH,
    ROUTE_SKIP,
    ROUTE_SYNC,
    candidate_pairs,
    remaining_life_seconds,
    route_pair,
)


def _m(mid, series=None, event=None, entities=()):
    return {"market_id": mid, "series_ticker": series, "event_ticker": event, "entities": list(entities)}


def test_same_event_is_candidate():
    markets = [_m("A", event="E1"), _m("B", event="E1"), _m("C", event="E2")]
    assert candidate_pairs(markets) == [("A", "B")]


def test_same_series_is_candidate():
    markets = [_m("A", series="S1"), _m("B", series="S1"), _m("C", series="S2")]
    assert candidate_pairs(markets) == [("A", "B")]


def test_entity_overlap_meets_threshold():
    # Different event/series, but share 2 entities -> candidate at default min=2.
    markets = [
        _m("A", series="S1", entities=["trump", "2024", "x"]),
        _m("B", series="S2", entities=["trump", "2024", "y"]),
    ]
    assert candidate_pairs(markets, entity_overlap_min=2) == [("A", "B")]


def test_entity_overlap_below_threshold_excluded():
    # Share only one entity; below the default min of 2 -> not a candidate.
    markets = [
        _m("A", series="S1", entities=["trump", "x"]),
        _m("B", series="S2", entities=["trump", "y"]),
    ]
    assert candidate_pairs(markets, entity_overlap_min=2) == []


def test_entity_match_is_case_insensitive():
    markets = [
        _m("A", series="S1", entities=["Trump", "Biden"]),
        _m("B", series="S2", entities=["trump", "BIDEN"]),
    ]
    assert candidate_pairs(markets, entity_overlap_min=2) == [("A", "B")]


def test_pairs_are_canonical_and_deduped():
    # A and B are both same-series AND share enough entities — still one pair,
    # canonically ordered.
    markets = [
        _m("B", series="S1", entities=["x", "y"]),
        _m("A", series="S1", entities=["x", "y"]),
    ]
    assert candidate_pairs(markets, entity_overlap_min=2) == [("A", "B")]


def test_no_self_pairs_and_empty_inputs():
    assert candidate_pairs([]) == []
    assert candidate_pairs([_m("A", series="S1")]) == []
    # Markets missing every grouping key produce nothing.
    assert candidate_pairs([_m("A"), _m("B")]) == []


def test_three_in_one_event_yields_all_three_pairs():
    markets = [_m("A", event="E"), _m("B", event="E"), _m("C", event="E")]
    assert candidate_pairs(markets) == [("A", "B"), ("A", "C"), ("B", "C")]


# -- time-to-resolution spend gate -----------------------------------------

_NOW = datetime(2026, 6, 6, 12, 0, 0)


def _mc(mid, closes_in_hours=None):
    """A market dict carrying a closes_at relative to _NOW (None = unknown)."""
    closes = None if closes_in_hours is None else _NOW + timedelta(hours=closes_in_hours)
    return {"market_id": mid, "closes_at": closes}


def test_remaining_life_uses_closes_at():
    assert remaining_life_seconds(_mc("A", closes_in_hours=2), _NOW) == 2 * 3600
    assert remaining_life_seconds(_mc("A", closes_in_hours=None), _NOW) is None


def test_route_skip_below_floor():
    # Nearer endpoint resolves in 1h — under the 24h floor → don't spend.
    a = _mc("A", closes_in_hours=1)
    b = _mc("B", closes_in_hours=100)
    assert route_pair(a, b, _NOW) == ROUTE_SKIP


def test_route_sync_between_floor_and_threshold():
    # 36h remaining: above the 24h floor, below the 48h batch threshold → sync.
    a = _mc("A", closes_in_hours=36)
    b = _mc("B", closes_in_hours=200)
    assert route_pair(a, b, _NOW) == ROUTE_SYNC


def test_route_batch_above_threshold():
    a = _mc("A", closes_in_hours=200)
    b = _mc("B", closes_in_hours=400)
    assert route_pair(a, b, _NOW) == ROUTE_BATCH


def test_route_binds_on_nearer_endpoint():
    # B is long-lived but A is short-fuse → the pair binds on A → skip.
    a = _mc("A", closes_in_hours=2)
    b = _mc("B", closes_in_hours=1000)
    assert route_pair(a, b, _NOW) == ROUTE_SKIP


def test_route_unknown_life_defaults_to_sync():
    # No close time on either endpoint → conservative: classify, don't skip.
    assert route_pair(_mc("A"), _mc("B"), _NOW) == ROUTE_SYNC
    # One known (long) endpoint still routes off the one we know.
    assert route_pair(_mc("A"), _mc("B", closes_in_hours=400), _NOW) == ROUTE_BATCH


def test_route_thresholds_match_constants():
    # The exact boundary values come from constants (24h floor / 48h batch).
    assert C.EDGE_REMAINING_LIFE_FLOOR_SECONDS == 86400
    assert C.EDGE_BATCH_THRESHOLD_SECONDS == 172800
    just_under_floor = _mc("A", closes_in_hours=23.9)
    assert route_pair(just_under_floor, _mc("B", closes_in_hours=400), _NOW) == ROUTE_SKIP


@given(
    st.lists(
        st.fixed_dictionaries(
            {
                "market_id": st.text(alphabet="ABCDE", min_size=1, max_size=2),
                "series_ticker": st.sampled_from([None, "S1", "S2"]),
                "event_ticker": st.sampled_from([None, "E1", "E2"]),
                "entities": st.lists(st.sampled_from(["p", "q", "r"]), max_size=3),
            }
        ),
        max_size=8,
    )
)
def test_output_is_canonical_sorted_unique_and_order_invariant(markets):
    # De-dup by market_id (the generator keys on it; duplicate ids are degenerate).
    seen: dict[str, dict] = {}
    for m in markets:
        seen.setdefault(m["market_id"], m)
    uniq = list(seen.values())

    out = candidate_pairs(uniq)
    # canonical (a < b), sorted, unique
    assert out == sorted(set(out))
    assert all(a < b for a, b in out)
    # order of the input markets must not change the result
    assert candidate_pairs(list(reversed(uniq))) == out
