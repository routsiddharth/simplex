"""Audit diff classification. Structural mismatches (a level present in one book
but absent in the other) are the primary signal; small per-level size drift is
expected (the frozen book is ~100ms older than REST) and tolerated, but a *gross*
size drift on a shared level escalates as a backstop against a magnitude/decode
bug the structural test cannot see (``AUDIT_SMALL_DIFF_MAX_SIZE_PCT``)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from simplex_ingest import constants as C
from simplex_ingest.loops.audit import BookAuditLoop, _diff_side


def test_identical_books_have_no_diff():
    assert _diff_side({0.40: 100.0}, {0.40: 100.0}) == (0, 0.0)


def test_size_drift_on_shared_level_is_not_structural():
    structural, max_pct = _diff_side({0.40: 100.0}, {0.40: 150.0})
    assert structural == 0                         # same level exists in both
    assert max_pct > 0.0                            # drift recorded, not structural


def test_size_drift_pct_is_relative_to_larger_size():
    structural, max_pct = _diff_side({0.40: 100.0}, {0.40: 110.0})
    assert structural == 0
    assert round(max_pct, 4) == round(10 / 110 * 100, 4)


def test_level_present_in_one_book_is_structural():
    structural, max_pct = _diff_side({0.40: 100.0, 0.39: 50.0}, {0.40: 100.0})
    assert structural == 1                         # 0.39 only in mem
    assert max_pct == 0.0


def test_mixed_structural_and_drift():
    structural, max_pct = _diff_side(
        {0.40: 100.0, 0.38: 20.0}, {0.40: 90.0, 0.37: 10.0}
    )
    assert structural == 2                         # 0.38 only mem, 0.37 only rest
    assert round(max_pct, 4) == round(10 / 100 * 100, 4)   # shared 0.40 drift


# -- _audit_market classification & reset decision --------------------------

def _pairs(levels: dict[float, float]) -> list[list[str]]:
    """Render a {price: size} dict as Kalshi REST [price, size] string pairs."""
    return [[str(p), str(s)] for p, s in levels.items()]


def _audit_loop(frozen_yes, frozen_no, rest_yes, rest_no):
    """A BookAuditLoop wired to a fake book_store (returns the given frozen
    book, or None) and a fake audit_rest (returns the given REST levels).
    Returns (loop, rt) so callers can inspect reset side effects."""
    frozen = None
    if frozen_yes is not None or frozen_no is not None:
        frozen = SimpleNamespace(yes=dict(frozen_yes or {}), no=dict(frozen_no or {}))

    book_store = SimpleNamespace(
        _reset=[],
        freeze=lambda market: frozen,
    )
    book_store.reset_book = lambda market: book_store._reset.append(market)

    async def get_orderbook(market, depth):
        return {"yes_dollars": _pairs(rest_yes), "no_dollars": _pairs(rest_no)}

    rt = SimpleNamespace(
        book_store=book_store,
        audit_rest=SimpleNamespace(get_orderbook=get_orderbook),
        reset_requests=asyncio.Queue(),
    )
    return BookAuditLoop(rt), rt


async def _classify(frozen_yes, frozen_no, rest_yes, rest_no):
    loop, rt = _audit_loop(frozen_yes, frozen_no, rest_yes, rest_no)
    _ts, _market, status, structural, max_pct, action, _details = await loop._audit_market("M")
    reset = bool(rt.book_store._reset) and not rt.reset_requests.empty()
    return status, action, structural, max_pct, reset


async def test_audit_identical_books_no_diff_no_reset():
    status, action, structural, max_pct, reset = await _classify(
        {0.40: 100.0}, {0.60: 50.0}, {0.40: 100.0}, {0.60: 50.0}
    )
    assert (status, action) == ("no_diff", "none")
    assert structural == 0 and max_pct == 0.0 and reset is False


async def test_audit_minor_drift_is_small_diff_no_reset():
    # Shared level, sub-ceiling size drift (~9%) -> expected freeze->REST movement.
    status, action, _s, max_pct, reset = await _classify(
        {0.40: 100.0}, {}, {0.40: 110.0}, {}
    )
    assert (status, action) == ("small_diff", "none")
    assert 0 < max_pct < C.AUDIT_SMALL_DIFF_MAX_SIZE_PCT and reset is False


async def test_audit_few_structural_mismatches_is_small_diff():
    # One level present only in memory: structural=1 (<= max levels) -> small.
    status, action, structural, _p, reset = await _classify(
        {0.40: 100.0, 0.39: 10.0}, {}, {0.40: 100.0}, {}
    )
    assert (status, action) == ("small_diff", "none")
    assert structural == 1 and reset is False


async def test_audit_many_structural_mismatches_force_reset():
    # 5 yes levels absent from REST -> structural=5 (> max levels) -> large + reset.
    mem = {0.40: 100.0, 0.39: 10.0, 0.38: 10.0, 0.37: 10.0, 0.36: 10.0}
    status, action, structural, _p, reset = await _classify(mem, {}, {}, {})
    assert (status, action) == ("large_diff", "book_reset")
    assert structural == C.AUDIT_STRUCTURAL_DIFF_MAX_LEVELS + 1 and reset is True


async def test_audit_beats_per_market_during_sweep():
    """A long REST sweep must keep the loop's heartbeat fresh: ``run_pass``
    beats once per market, not just at end-of-cycle, so ``/health`` stays green
    while a large catalog is swept. Regression for the Railway healthcheck
    rollback (the audit sweep over a many-outcome series ran past the heartbeat
    timeout, 503-ing the deploy through its grace)."""
    beats: list[str] = []
    markets = ["M1", "M2", "M3", "M4"]

    async def get_orderbook(market, depth):
        return {"yes_dollars": [], "no_dollars": []}

    rt = SimpleNamespace(
        book_store=SimpleNamespace(freeze=lambda m: None),
        audit_rest=SimpleNamespace(get_orderbook=get_orderbook),
        heartbeats=SimpleNamespace(beat=lambda name: beats.append(name)),
        db=SimpleNamespace(
            get_active_market_ids=lambda: list(markets),
            insert_audit_results=lambda rows: None,
        ),
    )
    await BookAuditLoop(rt).run_pass()
    assert beats == ["audit"] * len(markets)


async def test_audit_filters_out_of_band_rest_levels_no_reset():
    """Step 6 symmetry: the in-memory book drops 0c/100c levels at apply time, so
    the audit's REST decode must drop them too. With matching in-band levels and
    only out-of-band extras reported by REST, the books agree — no phantom
    structural diff, no perpetual reset (the regression this guards against)."""
    status, action, structural, _p, reset = await _classify(
        {0.40: 100.0}, {0.60: 50.0},
        {0.40: 100.0, 0.00: 99.0, 1.00: 5.0}, {0.60: 50.0, 1.00: 80.0},
    )
    assert (status, action) == ("no_diff", "none")
    assert structural == 0 and reset is False


async def test_audit_gross_size_only_desync_forces_reset():
    # The restored backstop: same single level (structural=0), but its size is
    # off by a large factor (1 -> 1000, ~99.9%) -> a magnitude/decode bug the
    # structural test can't see. This must escalate to a reset.
    status, action, structural, max_pct, reset = await _classify(
        {0.40: 1.0}, {}, {0.40: 1000.0}, {}
    )
    assert structural == 0
    assert max_pct > C.AUDIT_SMALL_DIFF_MAX_SIZE_PCT
    assert (status, action) == ("large_diff", "book_reset")
    assert reset is True
