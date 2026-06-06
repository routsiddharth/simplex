"""Async tests for the extraction loop, over a fake LLM + a throwaway DuckDB."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from simplex_ingest import constants as C
from simplex_ingest.llm import LLMError, MarketSemantics, PairClassification
from simplex_ingest.loops.extraction import ExtractionLoop

T0 = datetime(2026, 1, 1)


# -- seeding helpers --------------------------------------------------------

def _market_row(mid, *, title=None, description="desc", series=None, event=None, closes=None):
    return (
        mid, C.PLATFORM, title or f"{mid} title", description, "rules",
        series, event, T0, closes, None, "active", "{}", T0,
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


# -- spend gate + batch (async) path ----------------------------------------
# closes_at is read against the loop's real now_utc(); using a date far in the
# future (FAR) / far in the past (PAST) makes the route deterministic without
# having to inject the clock: FAR → long horizon (batch), PAST → below the floor
# (skip). Pairs with no close time route sync (covered by the tests above).

FAR = datetime(2030, 1, 1)
PAST = datetime(2000, 1, 1)


def _classify(rel="correlated", direction="none", conf=0.7, why="x"):
    return json.dumps(
        {"relationship_type": rel, "direction": direction, "confidence": conf, "rationale": why}
    )


def _semantics_json():
    return json.dumps(
        {"underlying_event": "e", "resolves_yes_when": "y", "resolves_no_when": "n",
         "resolution_timing": "t", "entities": [], "dependencies": []}
    )


def _seed_pair_closes(tmp_db, a, b, closes):
    """Two same-event subscribed markets with semantics + a given close time."""
    tmp_db.upsert_markets([_market_row(a, event="E", closes=closes), _market_row(b, event="E", closes=closes)])
    for mid in (a, b):
        tmp_db.upsert_market_semantics([(
            mid, C.PLATFORM, "ev", "yes", "no", "soon", json.dumps([]), json.dumps([]),
            C.EXTRACTION_MODEL, C.EXTRACTION_PROMPT_VERSION, T0, "{}",
        )])
    tmp_db.set_active_set({a, b})


async def test_long_horizon_pair_routes_to_batch(tmp_db, make_runtime, make_fake_llm, make_fake_batch):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    n = await loop._classify_pairs()
    assert n == 0                                   # nothing written synchronously
    assert tmp_db.get_edges_for_pairs([("A", "B")]) == {}
    assert len(batch.submitted) == 1
    open_batches = tmp_db.get_open_batches()
    assert len(open_batches) == 1
    assert open_batches[0]["purpose"] == "pair_primary"
    assert list(open_batches[0]["payload"].values()) == [["A", "B"]]


async def test_short_fuse_pair_is_skipped(tmp_db, make_runtime, make_fake_llm, make_fake_batch, caplog):
    _seed_pair_closes(tmp_db, "A", "B", PAST)       # below the floor → don't spend
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    with caplog.at_level(logging.INFO):
        n = await loop._classify_pairs()
    assert n == 0
    assert tmp_db.get_edges_for_pairs([("A", "B")]) == {}
    assert batch.submitted == []                    # no spend at all
    assert tmp_db.get_open_batches() == []
    # The coverage cap must be observable, not silent.
    routing = next(r for r in caplog.records if r.message == "pair routing")
    assert routing.skip == 1 and routing.sync == 0 and routing.batch == 0


async def test_batch_route_degrades_to_sync_without_client(tmp_db, make_runtime, make_fake_llm):
    # Long horizon, but no ANTHROPIC_API_KEY (batch=None) → classify synchronously.
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    cls = {("A", "B"): PairClassification("correlated", "none", 0.7, "x")}
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(classifications=cls), batch=None))
    n = await loop._classify_pairs()
    assert n == 1
    assert tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]["trust_tier"] == "soft"


async def test_inflight_pair_not_resubmitted(tmp_db, make_runtime, make_fake_llm, make_fake_batch):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()
    assert len(batch.submitted) == 1
    await loop._classify_pairs()                    # batch still draining
    assert len(batch.submitted) == 1                # not resubmitted


async def test_reconcile_primary_writes_soft_edge(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, make_batch_result
):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()
    b = tmp_db.get_open_batches()[0]
    cid = next(iter(b["payload"]))
    batch.set_results(b["batch_id"], [make_batch_result(cid, content=_classify(conf=0.7))])

    n = await loop._reconcile_batches()
    assert n == 1
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "soft"
    assert edge["agreement_status"] == "single"
    assert edge["model"] == C.BATCH_PAIR_MODEL
    assert tmp_db.get_open_batches() == []          # batch consumed


async def test_batch_primary_error_skips_pair(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, make_batch_result
):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()
    b = tmp_db.get_open_batches()[0]
    cid = next(iter(b["payload"]))
    batch.set_results(b["batch_id"], [make_batch_result(cid, error="errored: boom")])

    n = await loop._reconcile_batches()
    assert n == 0
    assert tmp_db.get_edges_for_pairs([("A", "B")]) == {}
    assert tmp_db.get_open_batches() == []          # consumed → pair retryable next cycle


async def test_batch_trusted_requires_verify_agreement(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, make_batch_result
):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()

    # High-confidence primary → deferred to a verify batch, no edge yet.
    pb = tmp_db.get_open_batches()[0]
    pcid = next(iter(pb["payload"]))
    batch.set_results(pb["batch_id"], [make_batch_result(pcid, content=_classify("same_event", conf=0.95))])
    n = await loop._reconcile_batches()
    assert n == 0
    assert tmp_db.get_edges_for_pairs([("A", "B")]) == {}
    vb = tmp_db.get_open_batches()
    assert len(vb) == 1 and vb[0]["purpose"] == "pair_verify"
    vcid = next(iter(vb[0]["payload"]))
    assert vb[0]["payload"][vcid]["pair"] == ["A", "B"]

    # Verify agrees on the relationship type → trusted.
    batch.set_results(vb[0]["batch_id"], [make_batch_result(vcid, content=_classify("same_event", conf=0.9))])
    n2 = await loop._reconcile_batches()
    assert n2 == 1
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "trusted"
    assert edge["agreement_status"] == "agreed"
    assert edge["model"] == C.BATCH_PAIR_MODEL
    assert edge["verify_model"] == C.BATCH_PAIR_VERIFY_MODEL


async def test_batch_verify_disagreement_goes_to_review(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, make_batch_result
):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()
    pb = tmp_db.get_open_batches()[0]
    pcid = next(iter(pb["payload"]))
    batch.set_results(pb["batch_id"], [make_batch_result(pcid, content=_classify("same_event", conf=0.95))])
    await loop._reconcile_batches()
    vb = tmp_db.get_open_batches()[0]
    vcid = next(iter(vb["payload"]))
    # Verify disagrees → review/disagreed.
    batch.set_results(vb["batch_id"], [make_batch_result(vcid, content=_classify("implies", conf=0.9))])
    await loop._reconcile_batches()
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "review"
    assert edge["agreement_status"] == "disagreed"


async def test_batch_verify_unavailable_keeps_soft(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, make_batch_result
):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()
    pb = tmp_db.get_open_batches()[0]
    pcid = next(iter(pb["payload"]))
    batch.set_results(pb["batch_id"], [make_batch_result(pcid, content=_classify("same_event", conf=0.95))])
    await loop._reconcile_batches()
    vb = tmp_db.get_open_batches()[0]
    vcid = next(iter(vb["payload"]))
    # Verify request itself failed → can't get an independent opinion → keep soft.
    batch.set_results(vb["batch_id"], [make_batch_result(vcid, error="expired")])
    await loop._reconcile_batches()
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "soft"
    assert edge["agreement_status"] == "single"


async def test_bulk_semantics_routes_to_batch_then_reconciles(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, make_batch_result, monkeypatch
):
    monkeypatch.setattr(C, "BATCH_BULK_SEMANTICS_THRESHOLD", 3)
    rows = [_market_row(f"M{i}") for i in range(3)]
    _seed_markets(tmp_db, rows, active=[f"M{i}" for i in range(3)])
    batch = make_fake_batch()
    llm = make_fake_llm()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=llm, batch=batch))

    n = await loop._extract_semantics()
    assert n == 0                                   # nothing written synchronously
    assert llm.extract_calls == []                  # no sync model calls
    assert len(batch.submitted) == 1
    b = tmp_db.get_open_batches()[0]
    assert b["purpose"] == "semantics"
    assert set(b["payload"].values()) == {"M0", "M1", "M2"}

    results = [make_batch_result(cid, content=_semantics_json()) for cid in b["payload"]]
    batch.set_results(b["batch_id"], results)
    nr = await loop._reconcile_batches()
    assert nr == 3
    assert tmp_db.get_markets_missing_semantics(C.EXTRACTION_PROMPT_VERSION) == []


async def test_steady_state_semantics_stays_sync(tmp_db, make_runtime, make_fake_llm, make_fake_batch):
    # Below the bulk threshold (default 100): trickle stays on the sync path.
    _seed_markets(tmp_db, [_market_row("M1"), _market_row("M2")], active=["M1", "M2"])
    batch = make_fake_batch()
    llm = make_fake_llm()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=llm, batch=batch))
    n = await loop._extract_semantics()
    assert n == 2
    assert batch.submitted == []
    assert len(llm.extract_calls) == 2


# -- orphan recovery (a batch that never reconciles must not strand its items) --

async def test_stale_in_progress_batch_is_abandoned_for_respend(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, monkeypatch
):
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()
    b = tmp_db.get_open_batches()[0]
    batch.set_status(b["batch_id"], "in_progress")          # wedged — never ends

    # Jump the clock past BATCH_MAX_AGE_SECONDS so reconcile sees it as stale.
    monkeypatch.setattr(
        "simplex_ingest.loops.extraction.now_utc", lambda: datetime(2099, 1, 1, tzinfo=timezone.utc)
    )
    n = await loop._reconcile_batches()
    assert n == 0
    assert tmp_db.get_open_batches() == []                  # dropped
    assert tmp_db.get_inflight_pairs() == set()             # pair free to re-route/re-spend


async def test_stale_failing_poll_is_abandoned(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, monkeypatch
):
    # A terminal poll error (e.g. a purged/404'd batch id) past max age → drop,
    # don't orphan forever.
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()

    async def _boom(_bid):
        raise LLMError("404 batch not found")

    monkeypatch.setattr(batch, "poll", _boom)
    monkeypatch.setattr(
        "simplex_ingest.loops.extraction.now_utc", lambda: datetime(2099, 1, 1, tzinfo=timezone.utc)
    )
    await loop._reconcile_batches()
    assert tmp_db.get_open_batches() == []                  # dropped for re-spend


async def test_young_failing_poll_is_retained(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, monkeypatch
):
    # A transient poll error on a *young* batch must NOT drop it — it retries next
    # cycle (the batch may yet complete; dropping would double-spend needlessly).
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))
    await loop._classify_pairs()

    async def _boom(_bid):
        raise LLMError("transient 503")

    monkeypatch.setattr(batch, "poll", _boom)
    await loop._reconcile_batches()
    assert len(tmp_db.get_open_batches()) == 1              # retained, will retry


async def test_batch_submit_persist_failure_does_not_abort_cycle(
    tmp_db, make_runtime, make_fake_llm, make_fake_batch, monkeypatch, caplog
):
    # If the durable insert fails after submit, the cycle must not crash; the batch
    # is logged as orphaned (recoverable from logs) and items re-spend next cycle.
    _seed_pair_closes(tmp_db, "A", "B", FAR)
    batch = make_fake_batch()
    loop = ExtractionLoop(make_runtime(tmp_db, llm=make_fake_llm(), batch=batch))

    def _boom(*a, **k):
        raise RuntimeError("disk full")

    monkeypatch.setattr(tmp_db, "insert_batch", _boom)
    with caplog.at_level(logging.INFO):
        n = await loop._classify_pairs()                   # must not raise
    assert n == 0
    assert tmp_db.get_open_batches() == []                 # nothing persisted
    assert any(r.message == "batch submitted" for r in caplog.records)        # id logged first
    assert any("orphaned at provider" in r.message for r in caplog.records)
