"""Shared in-process state passed to the loops.

The snapshot builder owns the order books (one :class:`BookReconstructor` per
market); the audit loop reads frozen copies and the WS loop drains reset
requests. Everything runs in one event loop, so plain objects suffice — no locks
around the book structures (DB writes are the only thing dispatched to threads,
and those are serialized in :mod:`db`).
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol, runtime_checkable

from .log import get_logger
from .orderbook import OrderBook
from .reconstruct import BookReconstructor

log = get_logger("runtime")


@runtime_checkable
class Loop(Protocol):
    """The contract every supervised loop honors.

    The supervisor restarts these by ``name`` and drives them via ``run()``;
    ``/health`` keys liveness off the same ``name``. A future in-process loop
    (e.g. the Stage-3 solver) only needs to satisfy this Protocol."""

    name: str

    async def run(self) -> None: ...


class BookStore:
    """Map of market_id -> BookReconstructor, owned by the snapshot builder."""

    def __init__(self) -> None:
        self.markets: dict[str, BookReconstructor] = {}

    def get(self, market_id: str) -> BookReconstructor | None:
        return self.markets.get(market_id)

    def get_or_create(self, market_id: str) -> BookReconstructor:
        st = self.markets.get(market_id)
        if st is None:
            st = BookReconstructor(market_id)
            self.markets[market_id] = st
        return st

    def drop(self, market_id: str) -> None:
        self.markets.pop(market_id, None)

    def reset_book(self, market_id: str) -> None:
        st = self.markets.get(market_id)
        if st is not None:
            st.reset()

    def freeze(self, market_id: str) -> OrderBook | None:
        """A deep copy of a market's book (for audit comparison)."""
        st = self.markets.get(market_id)
        if st is None:
            return None
        return OrderBook.deserialize(st.book.serialize())


def request_reset(rt, market_id: str) -> None:
    """Reset a market's in-memory book **and** ask the WS loop to re-subscribe it,
    forcing a fresh snapshot.

    One protocol, one place: the two steps (clear in-memory state, enqueue the
    re-subscribe) always happen together, so no caller can do half of it. The WS
    loop is the sole drain of ``reset_requests``. Producers are the snapshot
    builder (seq gap / canary) and the audit loop (large structural diff)."""
    rt.book_store.reset_book(market_id)
    try:
        rt.reset_requests.put_nowait(market_id)
    except asyncio.QueueFull:
        log.warning("reset queue full", extra={"market": market_id})


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
