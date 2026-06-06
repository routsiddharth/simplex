"""Per-market order-book reconstruction — the §6 discipline as one deep module.

A :class:`BookReconstructor` owns one market's reconstruction state
(``book``, ``last_sequence``, ``anchored``, ``replay_floor_ts``) and folds
``raw_events`` payloads into the book behind a small interface:

    reconstructor.apply(event) -> ApplyResult        # OK | RESET
    reconstructor.top_of_book() / depth_within(band) # reads for the snapshot grid
    reconstructor.reset_canaries()                   # structural anomalies -> reset
    reconstructor.restore_checkpoint(cp)             # resume after a restart

The reconstruction invariants live here, not scattered across the snapshot loop:
a snapshot anchors the book; deltas apply only while anchored and only on the
next contiguous ``seq``; a duplicate/old ``seq`` is dropped; a forward gap returns
``RESET``; ``replay_floor_ts`` skips events already folded into a checkpoint. The
loop just forwards drained events and acts on a ``RESET``.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum, auto
from typing import Any

from . import constants as C
from .events import EventType
from .log import get_logger
from .orderbook import OrderBook

log = get_logger("reconstruct")

# Canaries that warrant a book reset (vs the informational stale-market check).
_RESET_CANARIES = {"crossed_book", "negative_size", "out_of_range_price"}


class ApplyResult(Enum):
    OK = auto()       # event folded in (or harmlessly dropped)
    RESET = auto()    # a sequence gap: caller should request a book reset


class BookReconstructor:
    """One market's reconstruction state machine. Held by the BookStore."""

    __slots__ = (
        "market_id", "book", "last_sequence", "anchored", "replay_floor_ts",
        "last_trade_price", "status", "last_event_received_ts",
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

    # -- checkpoint restore -------------------------------------------------

    def restore_checkpoint(self, cp: dict[str, Any]) -> None:
        """Resume from a serialized ``book_state`` checkpoint."""
        self.book = OrderBook.deserialize(cp["book"])
        self.last_sequence = cp["last_sequence"]
        self.anchored = not self.book.is_empty
        self.replay_floor_ts = cp["last_ts"]

    # -- mutation -----------------------------------------------------------

    def apply(self, event: dict[str, Any]) -> ApplyResult:
        """Fold one ``raw_events`` row into the book. Returns RESET on a seq gap."""
        rts = event["received_ts"]
        if self.replay_floor_ts is not None and rts is not None and rts <= self.replay_floor_ts:
            return ApplyResult.OK  # already folded into the checkpoint
        self.last_event_received_ts = rts

        et = event["event_type"]
        payload = event["payload"]
        seq = event["sequence"]

        if et == EventType.ORDERBOOK_SNAPSHOT.value:
            self.book.apply_snapshot(
                [tuple(x) for x in payload.get("yes", [])],
                [tuple(x) for x in payload.get("no", [])],
            )
            self.anchored = True
            self.last_sequence = seq
        elif et == EventType.ORDERBOOK_DELTA.value:
            if not self.anchored:
                return ApplyResult.OK  # wait for a snapshot to anchor
            if self.last_sequence is not None and seq is not None:
                if seq <= self.last_sequence:
                    return ApplyResult.OK  # duplicate / out of order
                if seq > self.last_sequence + 1:
                    log.warning(
                        "sequence gap; resetting book",
                        extra={"market": self.market_id,
                               "expected": self.last_sequence + 1, "got": seq},
                    )
                    return ApplyResult.RESET
            side = payload.get("side")
            price = payload.get("price")
            delta = payload.get("delta")
            if side in ("yes", "no") and price is not None and delta is not None:
                self.book.apply_delta(side, price, delta)
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
