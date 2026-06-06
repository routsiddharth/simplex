"""WebSocket subscriber loop.

Holds one persistent connection. orderbook_delta is subscribed one-market-per
subscription (so each market's `seq` is isolated for clean gap detection); trade
and market_lifecycle_v2 are bulk subscriptions. The active set is reconciled
incrementally on catalog signal (subscribe/unsubscribe per market for orderbook,
update_subscription add/delete for the bulk channels) — never a full reconnect.

A reset request (from the snapshot builder on a seq gap/canary, or the audit
loop on a large diff) re-subscribes that one market's orderbook to force a fresh
snapshot. Reconnects use exponential backoff with jitter; retries are unbounded.
"""

from __future__ import annotations

import asyncio
import json
import ssl

import certifi
import orjson
from websockets.asyncio.client import connect

from .. import constants as C
from ..log import get_logger
from ..util import Backoff, now_utc

log = get_logger("ws")


class WebSocketLoop:
    name = "websocket"

    def __init__(self, rt) -> None:
        self.rt = rt
        self.sub = rt.subscriber
        self._msg_id = 0
        self._ws = None
        # websockets doesn't pick up certifi's CA bundle by default (httpx does);
        # supply one explicitly so the wss handshake verifies.
        self._ssl = ssl.create_default_context(cafile=certifi.where())
        # Process-lifetime metric counters (NOT reset per connection) — see
        # _maybe_log_metrics. reconnects = full re-subscribes; reconciles =
        # incremental catalog diffs; resets = per-market re-anchors.
        self._frames = 0
        self._reconnects = 0
        self._reconciles = 0
        self._resets = 0
        self._metrics_at = now_utc().timestamp()
        self._frames_at_mark = 0
        self._reset_conn_state()

    def _reset_conn_state(self) -> None:
        self._orderbook_sids: dict[str, int] = {}   # market -> sid
        self._trade_sid: int | None = None
        self._lifecycle_sid: int | None = None
        self._pending: dict[int, tuple[str, str | None]] = {}  # msg_id -> (kind, market)
        self._current: set[str] = set()             # markets we have orderbook subs for

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    async def run(self) -> None:
        backoff = Backoff(
            C.WS_RECONNECT_MIN_SECONDS, C.WS_RECONNECT_MAX_SECONDS, C.WS_RECONNECT_BACKOFF_FACTOR
        )
        while not self.rt.shutdown.is_set():
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("ws connection error", extra={"err": repr(exc)})
            if self.rt.shutdown.is_set():
                break
            # Beat before the (up to WS_RECONNECT_MAX_SECONDS) backoff sleep so a
            # flapping/slow reconnect doesn't trip the health heartbeat timeout
            # while we're disconnected and emitting no message-driven beats.
            self.rt.heartbeats.beat(self.name)
            delay = await backoff.sleep()
            log.info("ws reconnecting", extra={"delay_s": round(delay, 2)})

    async def _connect_and_run(self) -> None:
        url = self.sub.ws_url()
        headers = self.sub.connect_headers()
        log.info("ws connecting", extra={"url": url})
        async with connect(
            url,
            additional_headers=headers,
            ssl=self._ssl if url.startswith("wss") else None,
            open_timeout=C.WS_OPEN_TIMEOUT_SECONDS,
            ping_interval=C.WS_PING_INTERVAL_SECONDS,
            ping_timeout=C.WS_PING_TIMEOUT_SECONDS,
            max_size=None,
        ) as ws:
            self._ws = ws
            self._reset_conn_state()
            self._reconnects += 1
            self.rt.heartbeats.beat(self.name)
            await self._subscribe_initial(ws)

            reader = asyncio.create_task(self._reader(ws), name="ws-reader")
            controller = asyncio.create_task(self._controller(ws), name="ws-controller")
            try:
                done, _ = await asyncio.wait(
                    {reader, controller}, return_when=asyncio.FIRST_EXCEPTION
                )
                for task in done:
                    task.result()  # re-raise to trigger reconnect
            finally:
                for task in (reader, controller):
                    task.cancel()
                await asyncio.gather(reader, controller, return_exceptions=True)
                self._ws = None
                await asyncio.to_thread(self.rt.db.flush_raw_events)

    # -- reading ------------------------------------------------------------

    async def _reader(self, ws) -> None:
        async for raw in ws:
            self.rt.heartbeats.beat(self.name)
            self._frames += 1
            try:
                # orjson decodes ~3-5x faster than stdlib json, freeing the event
                # loop sooner on the firehose (orjson.JSONDecodeError subclasses
                # ValueError, so the handling below is unchanged).
                message = orjson.loads(raw)
            except (ValueError, TypeError):
                log.warning("ws non-JSON message dropped")
                continue
            if not isinstance(message, dict):
                continue
            if self.sub.is_control(message):
                self._handle_control(message)
                continue
            buffered = 0
            for ev in self.sub.parse(message):
                buffered = self.rt.db.buffer_event(ev)
            if buffered >= C.RAW_EVENT_BATCH_SIZE:
                await asyncio.to_thread(self.rt.db.flush_raw_events)

    def _handle_control(self, message: dict) -> None:
        mtype = message.get("type")
        if mtype == "subscribed":
            mid = message.get("id")
            sid = (message.get("msg") or {}).get("sid")
            kind, market = self._pending.pop(mid, (None, None))
            if sid is None or kind is None:
                return
            if kind == "orderbook" and market is not None:
                self._orderbook_sids[market] = sid
            elif kind == "trade":
                self._trade_sid = sid
            elif kind == "lifecycle":
                self._lifecycle_sid = sid
        elif mtype == "error":
            log.warning("ws error reply", extra={"detail": message.get("msg")})
        # "ok" / "unsubscribed" / "subscriptions": nothing to track.

    # -- control loop -------------------------------------------------------

    async def _controller(self, ws) -> None:
        while True:
            await asyncio.sleep(C.RAW_EVENT_FLUSH_SECONDS)
            await asyncio.to_thread(self.rt.db.flush_raw_events)
            self.rt.heartbeats.beat(self.name)
            self._maybe_log_metrics()

            if self.rt.resubscribe_event.is_set():
                self.rt.resubscribe_event.clear()
                await self._reconcile(ws)

            # Drain any pending book-reset requests.
            while True:
                try:
                    market = self.rt.reset_requests.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await self._reset_market(ws, market)

    # -- subscription management -------------------------------------------

    async def _send(self, ws, message: dict) -> None:
        await ws.send(json.dumps(message))

    async def _subscribe_initial(self, ws) -> None:
        active = await asyncio.to_thread(self.rt.db.get_active_market_ids)
        for market in sorted(active):
            await self._subscribe_orderbook(ws, market)
        if active:
            await self._subscribe_bulk(ws, C.WS_CHANNEL_TRADE, "trade", sorted(active))
            await self._subscribe_bulk(ws, C.WS_CHANNEL_LIFECYCLE, "lifecycle", sorted(active))
        self._current = set(active)
        log.info("ws initial subscribe", extra={"markets": len(active)})

    async def _subscribe_orderbook(self, ws, market: str) -> None:
        mid = self._next_id()
        self._pending[mid] = ("orderbook", market)
        await self._send(ws, self.sub.subscribe_message(mid, [C.WS_CHANNEL_ORDERBOOK], [market]))

    async def _subscribe_bulk(self, ws, channel: str, kind: str, markets: list[str]) -> None:
        mid = self._next_id()
        self._pending[mid] = (kind, None)
        await self._send(ws, self.sub.subscribe_message(mid, [channel], markets))

    async def _reconcile(self, ws) -> None:
        desired = await asyncio.to_thread(self.rt.db.get_active_market_ids)
        to_add = desired - self._current
        to_remove = self._current - desired
        if not to_add and not to_remove:
            return

        for market in sorted(to_add):
            await self._subscribe_orderbook(ws, market)
        for market in sorted(to_remove):
            sid = self._orderbook_sids.pop(market, None)
            if sid is not None:
                await self._send(ws, self.sub.unsubscribe_message(self._next_id(), [sid]))
            self.rt.book_store.drop(market)

        await self._update_bulk(ws, self._trade_sid, C.WS_CHANNEL_TRADE, "trade", to_add, to_remove, desired)
        await self._update_bulk(ws, self._lifecycle_sid, C.WS_CHANNEL_LIFECYCLE, "lifecycle", to_add, to_remove, desired)

        self._current = set(desired)
        self._reconciles += 1
        log.info("ws reconciled", extra={"added": len(to_add), "removed": len(to_remove)})

    async def _update_bulk(self, ws, sid, channel, kind, to_add, to_remove, desired) -> None:
        """Incrementally add/remove markets on a bulk channel, or subscribe fresh
        if we don't yet have a sid for it."""
        if sid is None:
            if desired:
                await self._subscribe_bulk(ws, channel, kind, sorted(desired))
            return
        if to_add:
            await self._send(
                ws, self.sub.update_subscription_message(self._next_id(), sid, sorted(to_add), "add_markets")
            )
        if to_remove:
            await self._send(
                ws, self.sub.update_subscription_message(self._next_id(), sid, sorted(to_remove), "delete_markets")
            )

    async def _reset_market(self, ws, market: str) -> None:
        """Force a fresh orderbook_snapshot for one market by re-subscribing it."""
        if market not in self._current:
            return
        sid = self._orderbook_sids.pop(market, None)
        if sid is not None:
            await self._send(ws, self.sub.unsubscribe_message(self._next_id(), [sid]))
        await self._subscribe_orderbook(ws, market)
        self._resets += 1
        log.info("ws reset market", extra={"market": market})

    # -- metrics ------------------------------------------------------------

    def _maybe_log_metrics(self) -> None:
        now = now_utc().timestamp()
        elapsed = now - self._metrics_at
        if elapsed < C.METRICS_LOG_INTERVAL_SECONDS:
            return
        frames = self._frames - self._frames_at_mark
        log.info(
            "ws metrics",
            extra={"frames_per_s": round(frames / elapsed, 1),
                   "reconnects": self._reconnects, "reconciles": self._reconciles,
                   "resets": self._resets},
        )
        self._metrics_at = now
        self._frames_at_mark = self._frames
