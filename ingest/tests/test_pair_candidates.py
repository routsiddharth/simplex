"""Tests for the pure candidate-pair generator (Stage B input)."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from simplex_ingest.pair_candidates import candidate_pairs


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
