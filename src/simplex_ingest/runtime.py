"""Shared in-process state passed to the loops.

The snapshot builder owns the order books; the audit loop reads frozen copies
and the WS loop drains reset requests. Everything runs in one event loop, so
plain objects suffice — no locks around the book structures (DB writes are the
only thing dispatched to threads, and those are serialized in :mod:`db`).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime

from .orderbook import OrderBook


@dataclass
class MarketState:
    """Per-market reconstruction state held by the snapshot builder."""

    book: OrderBook = field(default_factory=OrderBook)
    last_sequence: int | None = None      # last applied orderbook seq
    anchored: bool = False                # have we seen a snapshot to anchor on?
    last_trade_price: float | None = None
    status: str | None = None
    last_event_received_ts: datetime | None = None
    replay_floor_ts: datetime | None = None  # skip events <= this (checkpoint boundary)


class BookStore:
    """Map of market_id -> MarketState, owned by the snapshot builder."""

    def __init__(self) -> None:
        self.markets: dict[str, MarketState] = {}

    def get(self, market_id: str) -> MarketState | None:
        return self.markets.get(market_id)

    def get_or_create(self, market_id: str) -> MarketState:
        st = self.markets.get(market_id)
        if st is None:
            st = MarketState()
            self.markets[market_id] = st
        return st

    def drop(self, market_id: str) -> None:
        self.markets.pop(market_id, None)

    def reset_book(self, market_id: str) -> None:
        st = self.markets.get(market_id)
        if st is not None:
            st.book.reset()
            st.anchored = False
            st.last_sequence = None

    def freeze(self, market_id: str) -> OrderBook | None:
        """A deep copy of a market's book (for audit comparison)."""
        st = self.markets.get(market_id)
        if st is None:
            return None
        return OrderBook.deserialize(st.book.serialize())


class Heartbeats:
    """Liveness timestamps per loop, for the /health endpoint."""

    def __init__(self) -> None:
        self._beats: dict[str, float] = {}

    def beat(self, name: str) -> None:
        self._beats[name] = time.monotonic()

    def alive(self, name: str, timeout: float) -> bool:
        last = self._beats.get(name)
        return last is not None and (time.monotonic() - last) <= timeout

    def status(self, names: list[str], timeout: float) -> dict[str, bool]:
        return {n: self.alive(n, timeout) for n in names}
