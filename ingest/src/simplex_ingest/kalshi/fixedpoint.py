"""Decoding of Kalshi numeric fields — the one module that knows the exchange's
fixed-point / dollars surface.

Kalshi quotes prices in dollars (0.01..0.99) and sizes/volume as fixed-point
contract counts (the ``*_fp`` fields). Both the WS parser and the REST audit turn
raw ``[[price, size], ...]`` level lists into numbers; the catalog poller and the
discovery predicates read market volume. Keeping every decode here means a units
question (e.g. WS ``count_fp`` vs a REST count) is verified and fixed in **one**
place instead of being re-derived at each call site.
"""

from __future__ import annotations

from typing import Any

from ..orderbook import _q


def to_float(value: Any) -> float | None:
    """One Kalshi numeric field -> float, or None if missing/unparseable."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def levels(raw: Any) -> list[tuple[float, float]]:
    """``[[price, size], ...]`` -> ``[(price, size), ...]``; bad pairs skipped.

    The WS orderbook snapshot decode (yes/no level lists)."""
    out: list[tuple[float, float]] = []
    for pair in raw or []:
        if not isinstance(pair, (list, tuple)) or len(pair) < 2:
            continue
        p, s = to_float(pair[0]), to_float(pair[1])
        if p is not None and s is not None:
            out.append((p, s))
    return out


def level_map(raw: Any) -> dict[float, float]:
    """Same level list as :func:`levels`, collected into a ``{price: size}`` dict
    with **quantized** price keys.

    The REST audit decode: keys are quantized the same way the in-memory
    :class:`~simplex_ingest.orderbook.OrderBook` quantizes, so the two diff
    cleanly."""
    out: dict[float, float] = {}
    for p, s in levels(raw):
        out[_q(p)] = s
    return out


def volume(market: dict) -> float:
    """Best-effort contract volume for a Kalshi market.

    Prefer ``volume_fp``, then plain ``volume``, then the 24h fixed-point figure.
    Shared by the catalog poller (liquidity floor) and the discovery predicates
    (P3 tradeability)."""
    for key in ("volume_fp", "volume", "volume_24h_fp"):
        v = market.get(key)
        if v is not None:
            f = to_float(v)
            if f is not None:
                return f
    return 0.0
