"""In-memory order book for a single binary (yes/no) Kalshi market.

Kalshi publishes *bids only*: ``yes`` levels are YES buy orders, ``no`` levels
are NO buy orders. A YES ask is the mirror of a NO bid: an ask to sell YES at
price p is a NO bid at (1 - p). So:

    yes_bid = max(yes prices)
    yes_ask = 1 - max(no prices)

Prices are dollars (0.01..0.99); sizes are contract counts. Depth within a band
is summed in yes-price space on each side.
"""

from __future__ import annotations

from typing import Any

from sortedcontainers import SortedDict

_Q = 6  # price quantization (decimal places) to avoid float key drift


def _q(price: float) -> float:
    return round(float(price), _Q)


class OrderBook:
    __slots__ = ("yes", "no", "_anomalies")

    def __init__(self) -> None:
        self.yes: SortedDict = SortedDict()  # price -> size (YES bids)
        self.no: SortedDict = SortedDict()   # price -> size (NO bids)
        self._anomalies: set[str] = set()

    # -- mutation -----------------------------------------------------------

    def reset(self) -> None:
        self.yes.clear()
        self.no.clear()
        self._anomalies.clear()

    def apply_snapshot(
        self,
        yes_levels: list[tuple[float, float]],
        no_levels: list[tuple[float, float]],
    ) -> None:
        self.reset()
        for price, size in yes_levels:
            if size > 0:
                self.yes[_q(price)] = float(size)
        for price, size in no_levels:
            if size > 0:
                self.no[_q(price)] = float(size)

    def apply_delta(self, side: str, price: float, delta: float) -> None:
        book = self.yes if side == "yes" else self.no
        key = _q(price)
        new = book.get(key, 0.0) + float(delta)
        if new < 0:
            # A delta drove a level negative => we missed messages / desync.
            self._anomalies.add("negative_size")
            book.pop(key, None)
        elif new == 0:
            book.pop(key, None)
        else:
            book[key] = new

    @property
    def is_empty(self) -> bool:
        return not self.yes and not self.no

    # -- reads --------------------------------------------------------------

    def top_of_book(self) -> tuple[float | None, float | None, float | None]:
        """(yes_bid, yes_ask, yes_mid) in dollars; any may be None."""
        yes_bid = self.yes.peekitem(-1)[0] if self.yes else None
        yes_ask = _q(1.0 - self.no.peekitem(-1)[0]) if self.no else None
        mid = _q((yes_bid + yes_ask) / 2.0) if (yes_bid is not None and yes_ask is not None) else None
        return yes_bid, yes_ask, mid

    def depth_within(self, band: float) -> tuple[float, float, int, int]:
        """(bid_depth_usd, ask_depth_usd, bid_levels, ask_levels) within ``band``
        dollars of the best price on each side."""
        bid_depth = ask_depth = 0.0
        bid_levels = ask_levels = 0

        if self.yes:
            best_bid = self.yes.peekitem(-1)[0]
            for price in self.yes.irange(_q(best_bid - band), best_bid):
                bid_depth += price * self.yes[price]
                bid_levels += 1

        if self.no:
            best_no = self.no.peekitem(-1)[0]
            for no_price in self.no.irange(_q(best_no - band), best_no):
                ask_price = 1.0 - no_price
                ask_depth += ask_price * self.no[no_price]
                ask_levels += 1

        return _q(bid_depth), _q(ask_depth), bid_levels, ask_levels

    def check_canaries(self, price_min: float, price_max: float) -> set[str]:
        """Structural sanity checks, drained per snapshot emit.

        Returns a set drawn from {crossed_book, negative_size, out_of_range_price}.
        negative_size is accumulated transiently as deltas are applied; the others
        are evaluated against the current book here.
        """
        issues = set(self._anomalies)
        self._anomalies.clear()

        yes_bid, yes_ask, _ = self.top_of_book()
        if yes_bid is not None and yes_ask is not None and yes_bid > yes_ask + 1e-9:
            issues.add("crossed_book")

        for price in (*self.yes.keys(), *self.no.keys()):
            if price < price_min - 1e-9 or price > price_max + 1e-9:
                issues.add("out_of_range_price")
                break

        return issues

    # -- (de)serialization for checkpoints ----------------------------------

    def serialize(self) -> dict[str, Any]:
        return {
            "yes": {repr(p): s for p, s in self.yes.items()},
            "no": {repr(p): s for p, s in self.no.items()},
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "OrderBook":
        ob = cls()
        for p, s in (data.get("yes") or {}).items():
            ob.yes[_q(float(p))] = float(s)
        for p, s in (data.get("no") or {}).items():
            ob.no[_q(float(p))] = float(s)
        return ob
