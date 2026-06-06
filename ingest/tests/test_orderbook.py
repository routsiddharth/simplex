"""OrderBook reconstruction math: the load-bearing financial logic.

Covers the bids-only mirror (yes_ask = 1 - best_no_bid), within-band depth, the
structural canaries, delta application, and the checkpoint serialize round-trip.
"""

from __future__ import annotations

import math

from simplex_ingest.orderbook import OrderBook, _q


def _book(yes=(), no=()):
    b = OrderBook()
    b.apply_snapshot(list(yes), list(no))
    return b


# -- top of book ------------------------------------------------------------

def test_top_of_book_mirrors_no_side():
    # yes_bid = max yes price; yes_ask = 1 - max no price.
    b = _book(yes=[(0.40, 100), (0.38, 50)], no=[(0.55, 80), (0.52, 20)])
    yes_bid, yes_ask, yes_mid = b.top_of_book()
    assert yes_bid == 0.40
    assert yes_ask == _q(1.0 - 0.55)            # 0.45
    assert yes_mid == _q((0.40 + 0.45) / 2)     # 0.425


def test_top_of_book_handles_empty_sides():
    assert OrderBook().top_of_book() == (None, None, None)
    only_yes = _book(yes=[(0.30, 10)])
    assert only_yes.top_of_book() == (0.30, None, None)


def test_apply_snapshot_drops_nonpositive_levels():
    b = _book(yes=[(0.40, 100), (0.41, 0), (0.42, -5)])
    assert list(b.yes.keys()) == [0.40]


# -- depth within band ------------------------------------------------------

def test_depth_within_band_counts_only_near_touch():
    # band = 0.03. yes: 0.40 (best), 0.38 (within), 0.36 (outside).
    b = _book(yes=[(0.40, 100), (0.38, 50), (0.36, 70)],
              no=[(0.55, 80), (0.53, 40), (0.51, 10)])
    bid_depth, ask_depth, bid_levels, ask_levels = b.depth_within(0.03)
    assert bid_levels == 2                       # 0.40, 0.38 (0.37 floor)
    assert math.isclose(bid_depth, _q(0.40 * 100 + 0.38 * 50))
    assert ask_levels == 2                        # no 0.55, 0.53 (0.52 floor)
    # ask depth is priced in yes space: (1 - no_price) * size
    assert math.isclose(ask_depth, _q((1 - 0.55) * 80 + (1 - 0.53) * 40))


# -- canaries ---------------------------------------------------------------

def test_canary_crossed_book():
    # yes_bid 0.60 > yes_ask (1 - 0.50) = 0.50 -> crossed.
    b = _book(yes=[(0.60, 10)], no=[(0.50, 10)])
    assert "crossed_book" in b.check_canaries(0.01, 0.99)


def test_canary_out_of_range_price():
    b = _book(yes=[(1.50, 10)])                   # above the 0.99 ceiling
    assert "out_of_range_price" in b.check_canaries(0.01, 0.99)


def test_canary_negative_size_from_delta():
    b = _book(yes=[(0.40, 10)])
    b.apply_delta("yes", 0.40, -25)               # drives the level negative
    issues = b.check_canaries(0.01, 0.99)
    assert "negative_size" in issues
    assert 0.40 not in b.yes                       # level removed on underflow


def test_canaries_drain_each_call():
    b = _book(yes=[(0.40, 10)])
    b.apply_delta("yes", 0.40, -25)
    assert "negative_size" in b.check_canaries(0.01, 0.99)
    assert "negative_size" not in b.check_canaries(0.01, 0.99)  # not sticky


# -- delta application ------------------------------------------------------

def test_apply_delta_add_update_remove():
    b = _book(yes=[(0.40, 100)])
    b.apply_delta("yes", 0.41, 30)                 # new level
    assert b.yes[0.41] == 30
    b.apply_delta("yes", 0.40, 50)                 # accumulate
    assert b.yes[0.40] == 150
    b.apply_delta("yes", 0.40, -150)               # exact zero -> removed
    assert 0.40 not in b.yes


# -- serialize round-trip ---------------------------------------------------

def test_serialize_round_trip():
    b = _book(yes=[(0.40, 100), (0.38, 50)], no=[(0.55, 80)])
    clone = OrderBook.deserialize(b.serialize())
    assert dict(clone.yes) == dict(b.yes)
    assert dict(clone.no) == dict(b.no)
    assert clone.top_of_book() == b.top_of_book()
