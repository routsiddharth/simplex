"""Kalshi WebSocket subscriber: command builders + message parsing.

Schemas confirmed against docs.kalshi.com (envelope ``{type, sid, seq, msg}``,
``seq`` monotonic per subscription):
  orderbook_snapshot : msg.market_ticker, msg.yes_dollars_fp/no_dollars_fp = [[price$, count_fp]]
  orderbook_delta    : msg.market_ticker, msg.price_dollars, msg.delta_fp, msg.side, msg.ts_ms
  trade              : msg.market_ticker, msg.yes_price_dollars, msg.no_price_dollars,
                       msg.count_fp, msg.taker_side, msg.ts
  market_lifecycle_v2: msg.market_ticker, msg.event_type, msg.open_ts, msg.close_ts,
                       msg.is_deactivated, msg.result
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ..events import EventType, NormalizedEvent
from ..log import get_logger
from ..subscriber import BaseSubscriber
from .auth import KalshiSigner

log = get_logger("ws")

_CONTROL_TYPES = {"subscribed", "unsubscribed", "ok", "error", "subscriptions"}
_WS_SIGNING_PATH = "/trade-api/ws/v2"


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _levels(raw: Any) -> list[tuple[float, float]]:
    """Parse [[price_str, size_str], ...] -> [(price, size), ...]."""
    out: list[tuple[float, float]] = []
    for pair in raw or []:
        if not isinstance(pair, (list, tuple)) or len(pair) < 2:
            continue
        p, s = _f(pair[0]), _f(pair[1])
        if p is not None and s is not None:
            out.append((p, s))
    return out


def _ts_from_ms(ms: Any) -> datetime | None:
    v = _f(ms)
    return datetime.fromtimestamp(v / 1000.0, timezone.utc) if v else None


def _ts_from_s(s: Any) -> datetime | None:
    v = _f(s)
    return datetime.fromtimestamp(v, timezone.utc) if v else None


class KalshiSubscriber(BaseSubscriber):
    platform = "kalshi"

    def __init__(self, signer: KalshiSigner, ws_url: str) -> None:
        self._signer = signer
        self._ws_url = ws_url

    # -- connection / commands ---------------------------------------------

    def ws_url(self) -> str:
        return self._ws_url

    def connect_headers(self) -> dict[str, str]:
        return self._signer.headers("GET", _WS_SIGNING_PATH)

    def subscribe_message(
        self, msg_id: int, channels: list[str], market_tickers: list[str]
    ) -> dict:
        return {
            "id": msg_id,
            "cmd": "subscribe",
            "params": {"channels": channels, "market_tickers": market_tickers},
        }

    def unsubscribe_message(self, msg_id: int, sids: list[int]) -> dict:
        return {"id": msg_id, "cmd": "unsubscribe", "params": {"sids": sids}}

    def update_subscription_message(
        self, msg_id: int, sid: int, market_tickers: list[str], action: str
    ) -> dict:
        # action: "add_markets" | "delete_markets". Kalshi's examples use "sid"
        # for add and "sids" for delete; send both keys to be safe.
        return {
            "id": msg_id,
            "cmd": "update_subscription",
            "params": {
                "sid": sid,
                "sids": [sid],
                "market_tickers": market_tickers,
                "action": action,
            },
        }

    def is_control(self, message: dict) -> bool:
        return message.get("type") in _CONTROL_TYPES

    # -- parsing ------------------------------------------------------------

    def parse(self, message: dict) -> list[NormalizedEvent]:
        try:
            mtype = message.get("type")
            if mtype in _CONTROL_TYPES or mtype is None:
                return []
            msg = message.get("msg") or {}
            seq = message.get("seq")
            received_ts = datetime.now(timezone.utc)
            market_id = msg.get("market_ticker") or msg.get("market_id")
            if not market_id:
                return []

            if mtype == "orderbook_snapshot":
                payload = {
                    "yes": _levels(msg.get("yes_dollars_fp") or msg.get("yes")),
                    "no": _levels(msg.get("no_dollars_fp") or msg.get("no")),
                }
                return [NormalizedEvent(received_ts, self.platform, market_id,
                                        EventType.ORDERBOOK_SNAPSHOT, payload,
                                        ts=_ts_from_ms(msg.get("ts_ms")), sequence=seq)]

            if mtype == "orderbook_delta":
                payload = {
                    "side": msg.get("side"),
                    "price": _f(msg.get("price_dollars") or msg.get("price")),
                    "delta": _f(msg.get("delta_fp") if msg.get("delta_fp") is not None
                                else msg.get("delta")),
                }
                return [NormalizedEvent(received_ts, self.platform, market_id,
                                        EventType.ORDERBOOK_DELTA, payload,
                                        ts=_ts_from_ms(msg.get("ts_ms")), sequence=seq)]

            if mtype == "trade":
                payload = {
                    "price": _f(msg.get("yes_price_dollars")),
                    "no_price": _f(msg.get("no_price_dollars")),
                    "count": _f(msg.get("count_fp")),
                    "taker_side": msg.get("taker_side"),
                    "trade_id": msg.get("trade_id"),
                }
                ts = _ts_from_ms(msg.get("ts_ms")) or _ts_from_s(msg.get("ts"))
                return [NormalizedEvent(received_ts, self.platform, market_id,
                                        EventType.TRADE, payload, ts=ts, sequence=seq)]

            if mtype in ("market_lifecycle_v2", "market_lifecycle"):
                payload = dict(msg)  # event_type, open_ts, close_ts, is_deactivated, result, ...
                return [NormalizedEvent(received_ts, self.platform, market_id,
                                        EventType.LIFECYCLE, payload,
                                        ts=_ts_from_s(msg.get("ts")), sequence=seq)]

            return []
        except Exception:  # never crash the connection on a bad message
            log.warning("failed to parse message", extra={"raw_type": message.get("type")})
            return []
