"""Async tests for the extraction loop, over a fake LLM + a throwaway DuckDB."""

from __future__ import annotations

import json
import logging
from datetime import datetime

from simplex_ingest import constants as C
from simplex_ingest.llm import MarketSemantics, PairClassification
from simplex_ingest.loops.extraction import ExtractionLoop

T0 = datetime(2026, 1, 1)


# -- seeding helpers --------------------------------------------------------

def _market_row(mid, *, title=None, description="desc", series=None, event=None):
    return (
        mid, C.PLATFORM, title or f"{mid} title", description, "rules",
        series, event, T0, None, None, "active", "{}", T0,
    )


def _seed_markets(db, rows, active):
    db.upsert_markets(rows)
    db.set_active_set(set(active))


# -- Phase A: semantics extraction + cache ---------------------------------

async def test_phase_a_extracts_and_caches(tmp_db, make_runtime, make_fake_llm):
    _seed_markets(tmp_db, [_market_row("M1"), _market_row("M2")], active=["M1", "M2"])
    llm = make_fake_llm()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=llm))

    n = await loop._extract_semantics()
    assert n == 2
    assert tmp_db.get_markets_missing_semantics(C.EXTRACTION_PROMPT_VERSION) == []

    # Second pass: nothing missing -> no further extract calls.
    before = len(llm.extract_calls)
    n2 = await loop._extract_semantics()
    assert n2 == 0
    assert len(llm.extract_calls) == before


async def test_phase_a_skips_malformed_keeps_others(tmp_db, make_runtime, make_fake_llm, caplog):
    _seed_markets(tmp_db, [_market_row("M1"), _market_row("M2")], active=["M1", "M2"])
    # M2's title raises -> it is skipped, M1 still stored.
    llm = make_fake_llm(raise_titles={"M2 title"})
    loop = ExtractionLoop(make_runtime(tmp_db, llm=llm))

    with caplog.at_level(logging.WARNING):
        n = await loop._extract_semantics()
    assert n == 1
    missing = {m["market_id"] for m in tmp_db.get_markets_missing_semantics(C.EXTRACTION_PROMPT_VERSION)}
    assert missing == {"M2"}
    assert any("semantics extraction skipped" in r.message for r in caplog.records)


async def test_version_bump_reextracts(tmp_db, make_runtime, make_fake_llm, monkeypatch):
    _seed_markets(tmp_db, [_market_row("M1")], active=["M1"])
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm()))
    await loop._extract_semantics()
    assert tmp_db.get_markets_missing_semantics(C.EXTRACTION_PROMPT_VERSION) == []

    # A newer prompt version treats the cached row as stale.
    assert len(tmp_db.get_markets_missing_semantics(C.EXTRACTION_PROMPT_VERSION + 1)) == 1


# -- Phase B: pair classification + trust tiers ----------------------------

def _setup_pair(tmp_db, rel_pair, classifications, make_fake_llm, make_runtime):
    """Two subscribed markets in the same event, with semantics, ready to pair."""
    a, b = rel_pair
    tmp_db.upsert_markets([_market_row(a, event="E"), _market_row(b, event="E")])
    for mid in (a, b):
        tmp_db.upsert_market_semantics([(
            mid, C.PLATFORM, "ev", "yes", "no", "soon", json.dumps([]), json.dumps([]),
            C.EXTRACTION_MODEL, C.EXTRACTION_PROMPT_VERSION, T0, "{}",
        )])
    tmp_db.set_active_set({a, b})
    llm = make_fake_llm(classifications=classifications)
    return ExtractionLoop(make_runtime(tmp_db, llm=llm)), llm


async def test_trusted_requires_agreement(tmp_db, make_runtime, make_fake_llm):
    cls = {("A", "B"): {
        C.PAIR_MODEL: PairClassification("same_event", "none", 0.95, "primary"),
        C.PAIR_VERIFY_MODEL: PairClassification("same_event", "none", 0.80, "verify"),
    }}
    loop, llm = _setup_pair(tmp_db, ("A", "B"), cls, make_fake_llm, make_runtime)
    n = await loop._classify_pairs()
    assert n == 1
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "trusted"
    assert edge["agreement_status"] == "agreed"
    assert edge["verify_model"] == C.PAIR_VERIFY_MODEL
    # both the primary and the verify model were called
    assert {m for _, _, m in llm.classify_calls} == {C.PAIR_MODEL, C.PAIR_VERIFY_MODEL}


async def test_high_confidence_disagreement_goes_to_review(tmp_db, make_runtime, make_fake_llm):
    cls = {("A", "B"): {
        C.PAIR_MODEL: PairClassification("same_event", "none", 0.95, "primary"),
        C.PAIR_VERIFY_MODEL: PairClassification("implies", "first_implies_second", 0.9, "verify"),
    }}
    loop, _ = _setup_pair(tmp_db, ("A", "B"), cls, make_fake_llm, make_runtime)
    await loop._classify_pairs()
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "review"
    assert edge["agreement_status"] == "disagreed"
    assert tmp_db.pending_review_edges()[0]["market_id_a"] == "A"


async def test_mid_confidence_is_soft_single_pass(tmp_db, make_runtime, make_fake_llm):
    cls = {("A", "B"): PairClassification("correlated", "none", 0.7, "meh")}
    loop, llm = _setup_pair(tmp_db, ("A", "B"), cls, make_fake_llm, make_runtime)
    await loop._classify_pairs()
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "soft"
    assert edge["agreement_status"] == "single"
    # no verify call for a sub-trusted confidence
    assert [m for _, _, m in llm.classify_calls] == [C.PAIR_MODEL]


async def test_low_confidence_goes_to_review_queue(tmp_db, make_runtime, make_fake_llm):
    cls = {("A", "B"): PairClassification("unrelated", "none", 0.2, "dunno")}
    loop, _ = _setup_pair(tmp_db, ("A", "B"), cls, make_fake_llm, make_runtime)
    await loop._classify_pairs()
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "review"
    assert edge["review_status"] == "pending"


async def test_pairs_not_reclassified_next_cycle(tmp_db, make_runtime, make_fake_llm):
    cls = {("A", "B"): PairClassification("correlated", "none", 0.7, "meh")}
    loop, llm = _setup_pair(tmp_db, ("A", "B"), cls, make_fake_llm, make_runtime)
    await loop._classify_pairs()
    calls = len(llm.classify_calls)
    n2 = await loop._classify_pairs()  # already classified at this version
    assert n2 == 0
    assert len(llm.classify_calls) == calls  # no new model calls


async def test_classify_skips_on_llm_error(tmp_db, make_runtime, make_fake_llm, caplog):
    loop, _ = _setup_pair(tmp_db, ("A", "B"), {}, make_fake_llm, make_runtime)
    loop.rt.llm.raise_pairs = {("A", "B")}
    with caplog.at_level(logging.WARNING):
        n = await loop._classify_pairs()
    assert n == 0
    assert tmp_db.get_edges_for_pairs([("A", "B")]) == {}
    assert any("pair classification skipped" in r.message for r in caplog.records)


# -- soft-fail without a key -----------------------------------------------

async def test_disabled_without_llm_is_noop(tmp_db, make_runtime):
    _seed_markets(tmp_db, [_market_row("M1")], active=["M1"])
    loop = ExtractionLoop(make_runtime(tmp_db, llm=None))
    await loop.extract_cycle()  # must not raise
    # nothing extracted
    assert len(tmp_db.get_markets_missing_semantics(C.EXTRACTION_PROMPT_VERSION)) == 1


async def test_full_cycle_extracts_then_classifies(tmp_db, make_runtime, make_fake_llm):
    # Two markets in the same event, no semantics yet. One full cycle should
    # extract both, then classify the single resulting candidate pair.
    _seed_markets(
        tmp_db,
        [_market_row("A", event="E"), _market_row("B", event="E")],
        active=["A", "B"],
    )
    sem = {
        "A title": MarketSemantics("e", "y", "n", "t", ("trump",), ()),
        "B title": MarketSemantics("e", "y", "n", "t", ("trump",), ()),
    }
    cls = {("A", "B"): PairClassification("same_event", "none", 0.7, "soft")}
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(semantics=sem, classifications=cls)))
    await loop.extract_cycle()
    assert tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]["trust_tier"] == "soft"
