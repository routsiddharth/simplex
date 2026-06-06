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

from .. import constants as C
from ..orderbook import _q

_BAND_EPS = 1e-9


def in_tradeable_band(price: float) -> bool:
    """A resting level is tradeable iff priced within the canary band
    (``CANARY_PRICE_MIN_USD``..``CANARY_PRICE_MAX_USD``, i.e. 1¢–99¢). Levels
    outside it — a near-decided market resting at 0¢/100¢, or a decode glitch —
    are not valid resting orders.

    The single home for "is this a real level": the WS apply path
    (:class:`~simplex_ingest.reconstruct.BookReconstructor`) and the REST audit
    decode (:func:`level_map`) both drop the same levels, so the in-memory book
    and the REST comparison stay apples-to-apples — otherwise a level the book
    excludes but REST still reports would read as a permanent structural diff and
    reset the book forever."""
    return (C.CANARY_PRICE_MIN_USD - _BAND_EPS) <= price <= (C.CANARY_PRICE_MAX_USD + _BAND_EPS)


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
    cleanly. Out-of-band levels are dropped here to match the in-memory book
    (which excludes them at apply time) — see :func:`in_tradeable_band`."""
    out: dict[float, float] = {}
    for p, s in levels(raw):
        if in_tradeable_band(p):
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
