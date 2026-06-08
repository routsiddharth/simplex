"""Per-market order-book reconstruction — the §6 discipline as one deep module.

A :class:`BookReconstructor` owns one market's reconstruction state
(``book``, ``last_sequence``, ``anchored``, ``replay_floor_ts``) and folds
``raw_events`` payloads into the book behind a small interface:

    reconstructor.apply(event) -> ApplyResult        # OK | RESET
    reconstructor.top_of_book() / depth_within(band) # reads for the snapshot grid
    reconstructor.reset_canaries()                   # structural anomalies -> reset
    reconstructor.restore_checkpoint(cp)             # resume after a restart

The reconstruction invariants live here, not scattered across the snapshot loop:
a snapshot anchors the book; deltas apply only while anchored; a duplicate/replayed
``seq`` (≤ the last applied) is dropped; a **forward gap is tolerated** — resync and
apply — because Kalshi's orderbook ``seq`` is per-*connection*, not per-market, so a
forward jump is almost always other markets' deltas bumping the shared counter, not
a missed delta for this market (see docs/INGEST-STABILIZATION-LOG.md §9).
``replay_floor_ts`` skips events already folded into a checkpoint. Book **resets**
no longer come from seq gaps; they come from the structural canaries and the hourly
REST audit (the snapshot/audit loops call ``runtime.request_reset`` directly).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum, auto
from typing import Any

from . import constants as C
from .events import EventType
from .kalshi.fixedpoint import in_tradeable_band
from .log import get_logger
from .orderbook import OrderBook

log = get_logger("reconstruct")

# Canaries that warrant a book reset (vs the informational stale-market check).
# `negative_size` is deliberately EXCLUDED (2026-06-08): it is the side-effect of
# tolerating per-connection seq gaps (a delta decremented a level we never saw
# incremented), the book already self-heals by clamping that level to zero, and
# resetting on it just re-introduced churn for a minor, self-correcting
# inconsistency. Genuine *accumulated* drift is corrected by the hourly REST audit
# instead — the authoritative per-market reconciliation. `crossed_book` stays (a
# crossed book corrupts the snapshot mid directly); `out_of_range_price` stays as a
# defensive backstop though it's already filtered at apply time.
_RESET_CANARIES = {"crossed_book", "out_of_range_price"}


class ApplyResult(Enum):
    OK = auto()       # event folded in (or harmlessly dropped)
    RESET = auto()    # caller should request a book reset. Reserved: apply() no
                      # longer returns this (seq gaps are tolerated; resets come
                      # from canaries/audit), but the snapshot loop still honors it,
                      # so a future genuine per-market desync signal can use it.


class BookReconstructor:
    """One market's reconstruction state machine. Held by the BookStore."""

    __slots__ = (
        "market_id", "book", "last_sequence", "anchored", "replay_floor_ts",
        "last_trade_price", "status", "last_event_received_ts",
        "_logged_out_of_range",
    )

    def __init__(self, market_id: str) -> None:
        self.market_id = market_id
        self.book = OrderBook()
        self.last_sequence: int | None = None
        self.anchored = False
        self.replay_floor_ts: datetime | None = None
        self.last_trade_price: float | None = None
        self.status: str | None = None
        self.last_event_received_ts: datetime | None = None
        # One-shot diagnostic: log the first out-of-range level we drop for this
        # market (with its actual price) so the live logs reveal the offending
        # value, then stay quiet — bounded markets would otherwise flood.
        self._logged_out_of_range = False

    # -- checkpoint restore -------------------------------------------------

    def restore_checkpoint(self, cp: dict[str, Any]) -> None:
        """Resume from a serialized ``book_state`` checkpoint."""
        self.book = OrderBook.deserialize(cp["book"])
        self.last_sequence = cp["last_sequence"]
        self.anchored = not self.book.is_empty
        self.replay_floor_ts = cp["last_ts"]

    # -- mutation -----------------------------------------------------------

    def apply(self, event: dict[str, Any]) -> ApplyResult:
        """Fold one ``raw_events`` row into the book. Returns OK; seq gaps are
        tolerated (resync + apply) — resets come from canaries/audit, not here."""
        rts = event["received_ts"]
        if self.replay_floor_ts is not None and rts is not None and rts <= self.replay_floor_ts:
            return ApplyResult.OK  # already folded into the checkpoint
        self.last_event_received_ts = rts

        et = event["event_type"]
        payload = event["payload"]
        seq = event["sequence"]

        if et == EventType.ORDERBOOK_SNAPSHOT.value:
            yes_in, yes_out = _split_in_band(payload.get("yes", []))
            no_in, no_out = _split_in_band(payload.get("no", []))
            if yes_out or no_out:
                self._note_out_of_range((yes_out or no_out)[0][0],
                                        yes_dropped=len(yes_out), no_dropped=len(no_out))
            self.book.apply_snapshot(yes_in, no_in)
            self.anchored = True
            self.last_sequence = seq
        elif et == EventType.ORDERBOOK_DELTA.value:
            if not self.anchored:
                return ApplyResult.OK  # wait for a snapshot to anchor
            # Kalshi's orderbook `seq` is per-CONNECTION, not per-market — confirmed
            # in production (the gap log's `got` is one counter climbing across every
            # market, while each market's `expected` is its own; see
            # docs/INGEST-STABILIZATION-LOG.md §9). So a forward "gap" is almost
            # always just *other* markets' deltas bumping the shared counter between
            # two of THIS market's deltas — not a missed delta for this market.
            # Resetting on it caused a perpetual reset storm that left books
            # de-anchored and the depth grid empty, so we DON'T reset on a gap: we
            # resync and apply. Genuine corruption is still caught by the structural
            # canaries and the hourly REST audit (the two reset paths that don't
            # depend on per-market seq). A seq we've already passed is still dropped
            # as a duplicate/replay (guards against a re-sent event double-applying).
            if self.last_sequence is not None and seq is not None and seq <= self.last_sequence:
                return ApplyResult.OK  # duplicate / replay
            side = payload.get("side")
            price = payload.get("price")
            delta = payload.get("delta")
            if side in ("yes", "no") and price is not None and delta is not None:
                if in_tradeable_band(price):
                    self.book.apply_delta(side, price, delta)
                else:
                    # A delta at a non-tradeable (0c/100c-ish) price: the level was
                    # never stored (snapshots filter it too), so dropping the delta
                    # keeps the book consistent rather than letting an out-of-range
                    # level resurrect and trip the canary into an endless reset.
                    self._note_out_of_range(price, side=side)
            if seq is not None:
                self.last_sequence = seq
        elif et == EventType.TRADE.value:
            price = payload.get("price")
            if price is not None:
                self.last_trade_price = price
        elif et == EventType.LIFECYCLE.value:
            self.status = _status_from_lifecycle(payload, self.status)
        return ApplyResult.OK

    def reset(self) -> None:
        """Discard the book and de-anchor (a fresh snapshot must re-anchor it)."""
        self.book.reset()
        self.anchored = False
        self.last_sequence = None

    def _note_out_of_range(
        self, sample_price: float, *, side: str | None = None,
        yes_dropped: int = 0, no_dropped: int = 0,
    ) -> None:
        """Log the first out-of-range level dropped for this market, with its
        actual price, then go quiet. Lets a deploy reveal whether these are
        legitimate 0c/100c resting levels (drop is correct) or a decode bug
        (the logged price would be grossly off, e.g. cents-not-dollars)."""
        if self._logged_out_of_range:
            return
        self._logged_out_of_range = True
        log.warning(
            "dropping out-of-range level",
            extra={"market": self.market_id, "sample_price": sample_price,
                   "side": side, "yes_dropped": yes_dropped, "no_dropped": no_dropped,
                   "band": [C.CANARY_PRICE_MIN_USD, C.CANARY_PRICE_MAX_USD]},
        )

    # -- reads --------------------------------------------------------------

    def top_of_book(self) -> tuple[float | None, float | None, float | None]:
        return self.book.top_of_book()

    def depth_within(self, band: float) -> tuple[float, float, int, int]:
        return self.book.depth_within(band)

    def reset_canaries(self) -> set[str]:
        """Structural canaries that warrant a reset (drains the book's anomalies)."""
        return self.book.check_canaries(C.CANARY_PRICE_MIN_USD, C.CANARY_PRICE_MAX_USD) & _RESET_CANARIES

    def serialize(self) -> dict[str, Any]:
        return self.book.serialize()


def _split_in_band(
    levels: list,
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """Partition snapshot ``[price, size]`` pairs into (in-band, out-of-band) by
    the shared :func:`~simplex_ingest.kalshi.fixedpoint.in_tradeable_band`
    predicate — the same one the REST audit decode uses, so the in-memory book
    and the audit's REST book exclude the same levels."""
    in_band: list[tuple[float, float]] = []
    out_of_band: list[tuple[float, float]] = []
    for x in levels:
        pair = tuple(x)
        (in_band if in_tradeable_band(pair[0]) else out_of_band).append(pair)
    return in_band, out_of_band


def _status_from_lifecycle(payload: dict, current: str | None) -> str | None:
    if payload.get("is_deactivated"):
        return "inactive"
    event_type = payload.get("event_type")
    mapping = {
        "activated": "active",
        "created": current or "active",
        "deactivated": "inactive",
        "determined": "determined",
        "settled": "settled",
        "close_date_updated": current,
    }
    return mapping.get(event_type, current)
