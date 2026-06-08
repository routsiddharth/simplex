"""BookReconstructor — the per-market state machine, tested through its own
interface. No DuckDB, no loop, no Runtime: anchor / delta / seq-gap / replay-floor
are now exercised with plain apply() calls. This is the deepening win."""

from __future__ import annotations

from datetime import datetime

from simplex_ingest.events import EventType
from simplex_ingest.reconstruct import ApplyResult, BookReconstructor

_T0 = datetime(2026, 1, 1, 0, 0, 0)


def _ev(et, payload, seq, rts=_T0):
    return {"received_ts": rts, "event_type": et.value, "payload": payload, "sequence": seq}


def _snap(yes, no, seq, rts=_T0):
    return _ev(EventType.ORDERBOOK_SNAPSHOT, {"yes": yes, "no": no}, seq, rts)


def _delta(side, price, delta, seq, rts=_T0):
    return _ev(EventType.ORDERBOOK_DELTA, {"side": side, "price": price, "delta": delta}, seq, rts)


# -- anchoring --------------------------------------------------------------

def test_snapshot_anchors_the_book():
    r = BookReconstructor("M")
    assert r.apply(_snap([[0.40, 100]], [[0.55, 80]], seq=5)) is ApplyResult.OK
    assert r.anchored
    assert r.last_sequence == 5
    assert r.top_of_book()[0] == 0.40


def test_delta_before_anchor_is_ignored():
    r = BookReconstructor("M")
    assert r.apply(_delta("yes", 0.40, 10, seq=1)) is ApplyResult.OK
    assert not r.anchored
    assert r.top_of_book() == (None, None, None)


# -- sequence discipline ----------------------------------------------------

def test_contiguous_delta_applies():
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 100]], [[0.55, 80]], seq=5))
    assert r.apply(_delta("yes", 0.41, 30, seq=6)) is ApplyResult.OK
    assert r.last_sequence == 6
    assert r.top_of_book()[0] == 0.41


def test_duplicate_or_old_seq_is_dropped():
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 100]], [], seq=5))
    r.apply(_delta("yes", 0.41, 30, seq=6))
    before = dict(r.book.yes)
    assert r.apply(_delta("yes", 0.41, 30, seq=6)) is ApplyResult.OK   # replayed seq
    assert dict(r.book.yes) == before                                  # not double-applied


def test_sequence_gap_is_tolerated_and_applied():
    # Kalshi's orderbook seq is per-CONNECTION, so a forward "gap" is almost always
    # other markets' deltas bumping the shared counter, not a missed delta for this
    # market. We resync + apply rather than reset (resetting caused a perpetual reset
    # storm). Genuine corruption is caught by canaries + the REST audit instead.
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 100]], [], seq=5))
    r.apply(_delta("yes", 0.41, 30, seq=6))
    assert r.apply(_delta("yes", 0.42, 10, seq=8)) is ApplyResult.OK  # forward gap tolerated
    assert r.last_sequence == 8                                        # resynced forward
    assert r.top_of_book()[0] == 0.42                                 # the delta was applied


# -- checkpoint replay floor ------------------------------------------------

def test_replay_floor_skips_already_folded_events():
    r = BookReconstructor("M")
    r.replay_floor_ts = datetime(2026, 1, 2)
    # Event at/under the floor was already folded into the checkpoint -> skipped.
    assert r.apply(_snap([[0.40, 100]], [], seq=5, rts=datetime(2026, 1, 1))) is ApplyResult.OK
    assert not r.anchored
    # Event after the floor applies.
    assert r.apply(_snap([[0.40, 100]], [], seq=6, rts=datetime(2026, 1, 3))) is ApplyResult.OK
    assert r.anchored


# -- trade / lifecycle ------------------------------------------------------

def test_trade_sets_last_price_and_lifecycle_sets_status():
    r = BookReconstructor("M")
    r.apply(_ev(EventType.TRADE, {"price": 0.42}, seq=None))
    assert r.last_trade_price == 0.42
    r.apply(_ev(EventType.LIFECYCLE, {"event_type": "settled"}, seq=None))
    assert r.status == "settled"


# -- checkpoint round-trip + reset ------------------------------------------

def test_restore_checkpoint_rehydrates_state():
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 100]], [[0.55, 80]], seq=5))
    cp = {"book": r.serialize(), "last_sequence": 5, "last_ts": _T0}
    r2 = BookReconstructor("M")
    r2.restore_checkpoint(cp)
    assert r2.anchored
    assert r2.last_sequence == 5
    assert 0.40 in r2.book.yes


def test_reset_clears_and_deanchors():
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 100]], [[0.55, 80]], seq=5))
    r.reset()
    assert not r.anchored
    assert r.last_sequence is None
    assert r.book.is_empty


def test_reset_canaries_flags_crossed_book():
    r = BookReconstructor("M")
    # yes_bid 0.60 > yes_ask (1 - 0.50) = 0.50 -> crossed.
    r.apply(_snap([[0.60, 10]], [[0.50, 10]], seq=5))
    assert "crossed_book" in r.reset_canaries()


def test_negative_size_is_not_a_reset_trigger():
    # negative_size is the side-effect of tolerating per-connection seq gaps: a
    # delta drives a level below zero. The book self-heals (clamps to zero) and we
    # do NOT reset on it (2026-06-08) — only crossed_book / out_of_range do, and
    # accumulated drift is corrected by the hourly REST audit. (Detection itself is
    # covered by test_orderbook.test_canary_negative_size_from_delta.)
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 10]], [], seq=5))
    r.apply(_delta("yes", 0.40, -50, seq=6))   # drives 0.40 below zero
    assert 0.40 not in r.book.yes              # level clamped away (self-heal)
    # reset_canaries() = check_canaries() & _RESET_CANARIES; negative_size is in the
    # former but excluded from the latter, so the intersection is empty.
    assert r.reset_canaries() == set()


# -- out-of-range levels (the CA/MD primary reset-loop fix) -----------------

def test_snapshot_drops_out_of_range_levels_no_canary():
    """A near-decided primary market resting at 0c/100c must not trip the
    out_of_range_price canary forever: those levels are excluded at apply time so
    the book is valid and stable, built only from the in-band levels."""
    r = BookReconstructor("M")
    # 1.00 (100c) on no, 0.00 (0c) on yes are non-tradeable extremes; 0.97/0.02 ok.
    r.apply(_snap([[0.02, 10], [0.00, 99]], [[1.00, 50], [0.97, 5]], seq=5))
    assert r.anchored
    assert 0.0 not in r.book.yes and 0.02 in r.book.yes      # extreme dropped, valid kept
    assert 1.0 not in r.book.no and 0.97 in r.book.no
    assert r.reset_canaries() == set()                        # no perpetual reset


def test_out_of_range_delta_is_dropped():
    r = BookReconstructor("M")
    r.apply(_snap([[0.40, 100]], [[0.55, 80]], seq=5))
    # A delta at 100c must not resurrect an out-of-range level.
    assert r.apply(_delta("no", 1.00, 30, seq=6)) is ApplyResult.OK
    assert 1.0 not in r.book.no
    assert r.reset_canaries() == set()
    assert r.last_sequence == 6                                # seq still advances


def test_in_band_boundary_levels_are_kept():
    r = BookReconstructor("M")
    # Exactly 1c and 99c are tradeable and must survive the filter.
    r.apply(_snap([[0.01, 10]], [[0.99, 10]], seq=5))
    assert 0.01 in r.book.yes and 0.99 in r.book.no
