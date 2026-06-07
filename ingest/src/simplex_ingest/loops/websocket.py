"""WebSocket subscriber loop — sharded across N connections.

A single connection carrying every one-market-per-sid orderbook subscription
proved unstable at production scale (~2.9 k markets): each connection died a few
seconds after its initial subscribe burst with a codeless close, then reconnected
and re-anchored *every* book. So the loop is sharded:

- **N shards** (``WS_SHARD_COUNT``), each a persistent connection running on the
  one event loop. Orderbook subscriptions are partitioned **by series** (a
  series's markets stay on one shard, so its ``seq`` streams are co-located and a
  drop re-anchors only that shard's books). trade and lifecycle are bulk
  subscriptions per shard, over that shard's market subset.
- Each shard **paces** its (re)subscribe burst (``WS_SUBSCRIBE_CHUNK`` at a time)
  so neither the outbound subscribes nor the inbound snapshot flood overwhelms the
  connection at anchor time — the empirical cause of the death.
- A **coordinator** owns the cross-shard concerns: it (re)partitions the active
  set on the catalog signal, routes each book-reset request to the shard that owns
  that market, beats the health heartbeat, and rolls up metrics.

All shards feed the **single** in-process ``raw_events`` writer — the DuckDB
single-writer invariant is preserved (one process, one writer). Reconnects use
exponential backoff with jitter (reset after a sustained connection); retries are
unbounded.
"""

from __future__ import annotations

import asyncio
import json
import ssl
import zlib

import certifi
import orjson
from websockets.asyncio.client import connect

from .. import constants as C
from ..log import get_logger
from ..util import Backoff, now_utc

log = get_logger("ws")

_LOOP_NAME = "websocket"


def _shard_for(key: str, n: int) -> int:
    """Stable shard index for a partition key (series ticker, or market id when
    the series is unknown). crc32 is deterministic across processes — unlike
    ``hash()`` of a str, which is salted per-process — so the partition is
    reproducible (matters for tests and for reasoning about a deploy)."""
    return zlib.crc32(key.encode("utf-8")) % n


async def _wait_any(events, timeout: float) -> None:
    """Wait until any of ``events`` is set or ``timeout`` elapses, then return.
    Cancels its waiters so no tasks leak across ticks."""
    waiters = [asyncio.ensure_future(e.wait()) for e in events]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


class _Shard:
    """One WS connection scoped to an assigned subset of markets (``desired``).

    The coordinator assigns ``desired`` and signals ``_changed``; the shard
    (re)subscribes to match. Connection/subscription state is per-shard; the
    raw_events writer it feeds is shared."""

    def __init__(self, rt, sub, ssl_ctx, index: int) -> None:
        self.rt = rt
        self.sub = sub
        self._ssl = ssl_ctx
        self.index = index
        self.desired: set[str] = set()
        self._changed = asyncio.Event()          # coordinator -> shard: desired changed
        self._reset_q: asyncio.Queue[str] = asyncio.Queue(maxsize=C.RESET_REQUEST_QUEUE_MAXSIZE)
        self._msg_id = 0
        self._ws = None
        # Process-lifetime metric counters (read by the coordinator; NOT reset per
        # connection). reconnects = full re-subscribes; reconciles = incremental
        # catalog diffs; resets = per-market re-anchors.
        self.frames = 0
        self.reconnects = 0
        self.reconciles = 0
        self.resets = 0
        self._reset_conn_state()

    def set_desired(self, markets: set[str]) -> None:
        self.desired = markets
        self._changed.set()

    def _reset_conn_state(self) -> None:
        self._orderbook_sids: dict[str, int] = {}   # market -> sid
        self._trade_sid: int | None = None
        self._lifecycle_sid: int | None = None
        self._pending: dict[int, tuple[str, str | None]] = {}  # msg_id -> (kind, market)
        self._current: set[str] = set()             # markets we have orderbook subs for
        self._ready = asyncio.Event()               # set once initial subscribe completes

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    # -- run loop -----------------------------------------------------------

    async def run(self) -> None:
        backoff = Backoff(
            C.WS_RECONNECT_MIN_SECONDS, C.WS_RECONNECT_MAX_SECONDS, C.WS_RECONNECT_BACKOFF_FACTOR
        )
        while not self.rt.shutdown.is_set():
            if not self.desired:
                # Nothing assigned yet (cold start, or this shard's series all
                # left): hold no connection, just wait for an assignment. The
                # coordinator beats the health heartbeat while every shard idles.
                self._changed.clear()
                await _wait_any([self._changed, self.rt.shutdown], C.WS_IDLE_POLL_SECONDS)
                continue
            started = now_utc().timestamp()
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("ws connection error", extra={"shard": self.index, "err": repr(exc)})
            if self.rt.shutdown.is_set():
                break
            if now_utc().timestamp() - started >= C.WS_RECONNECT_RESET_SECONDS:
                backoff.reset()  # the connection was sustained; don't carry stale backoff
            # Beat before the backoff sleep so a flapping/slow reconnect doesn't
            # trip the health heartbeat while we're disconnected.
            self.rt.heartbeats.beat(_LOOP_NAME)
            delay = await backoff.sleep()
            log.info("ws reconnecting", extra={"shard": self.index, "delay_s": round(delay, 2)})

    async def _connect_and_run(self) -> None:
        url = self.sub.ws_url()
        headers = self.sub.connect_headers()
        log.info("ws connecting", extra={"shard": self.index, "url": url})
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
            self.reconnects += 1
            self.rt.heartbeats.beat(_LOOP_NAME)
            # Reader first so the inbound snapshot flood is drained as it arrives;
            # the (paced) initial subscribe runs concurrently. All three live in
            # the wait set so a failure in any of them triggers a reconnect.
            reader = asyncio.create_task(self._reader(ws), name=f"ws-reader-{self.index}")
            controller = asyncio.create_task(self._controller(ws), name=f"ws-ctrl-{self.index}")
            subscribe = asyncio.create_task(self._subscribe_initial(ws), name=f"ws-sub-{self.index}")
            tasks = (reader, controller, subscribe)
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                for task in done:
                    task.result()  # re-raise to trigger reconnect (None for a clean subscribe)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._ws = None
                await asyncio.to_thread(self.rt.db.flush_raw_events)

    # -- reading ------------------------------------------------------------

    async def _reader(self, ws) -> None:
        async for raw in ws:
            self.rt.heartbeats.beat(_LOOP_NAME)
            self.frames += 1
            try:
                # orjson decodes ~3-5x faster than stdlib json, freeing the event
                # loop sooner on the firehose (orjson.JSONDecodeError subclasses
                # ValueError, so the handling below is unchanged).
                message = orjson.loads(raw)
            except (ValueError, TypeError):
                log.warning("ws non-JSON message dropped", extra={"shard": self.index})
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
            log.warning("ws error reply", extra={"shard": self.index, "detail": message.get("msg")})
        # "ok" / "unsubscribed" / "subscriptions": nothing to track.

    # -- control loop -------------------------------------------------------

    async def _controller(self, ws) -> None:
        # Don't reconcile/reset until the initial subscribe has populated _current
        # (else a mid-anchor reconcile would double-subscribe).
        await self._ready.wait()
        while True:
            await asyncio.sleep(C.RAW_EVENT_FLUSH_SECONDS)
            await asyncio.to_thread(self.rt.db.flush_raw_events)
            self.rt.heartbeats.beat(_LOOP_NAME)

            if self._changed.is_set():
                self._changed.clear()
                await self._reconcile(ws)

            while True:
                try:
                    market = self._reset_q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await self._reset_market(ws, market)

    # -- subscription management -------------------------------------------

    async def _send(self, ws, message: dict) -> None:
        await ws.send(json.dumps(message))

    async def _subscribe_initial(self, ws) -> None:
        markets = sorted(self.desired)
        # Pace the orderbook subscribe burst: chunk + pause. Firing all ~725 at
        # once is what killed the connection a few seconds later.
        for i in range(0, len(markets), C.WS_SUBSCRIBE_CHUNK):
            for market in markets[i:i + C.WS_SUBSCRIBE_CHUNK]:
                await self._subscribe_orderbook(ws, market)
            if i + C.WS_SUBSCRIBE_CHUNK < len(markets):
                await asyncio.sleep(C.WS_SUBSCRIBE_PACING_SECONDS)
        if markets:
            await self._subscribe_bulk(ws, C.WS_CHANNEL_TRADE, "trade", markets)
            await self._subscribe_bulk(ws, C.WS_CHANNEL_LIFECYCLE, "lifecycle", markets)
        self._current = set(markets)
        self._ready.set()
        log.info("ws initial subscribe", extra={"shard": self.index, "markets": len(markets)})

    async def _subscribe_orderbook(self, ws, market: str) -> None:
        mid = self._next_id()
        self._pending[mid] = ("orderbook", market)
        await self._send(ws, self.sub.subscribe_message(mid, [C.WS_CHANNEL_ORDERBOOK], [market]))

    async def _subscribe_bulk(self, ws, channel: str, kind: str, markets: list[str]) -> None:
        mid = self._next_id()
        self._pending[mid] = (kind, None)
        await self._send(ws, self.sub.subscribe_message(mid, [channel], markets))

    async def _reconcile(self, ws) -> None:
        desired = set(self.desired)
        to_add = desired - self._current
        to_remove = self._current - desired
        if not to_add and not to_remove:
            return

        # Pace adds too — a catalog cycle can admit a whole new series at once.
        add_list = sorted(to_add)
        for i in range(0, len(add_list), C.WS_SUBSCRIBE_CHUNK):
            for market in add_list[i:i + C.WS_SUBSCRIBE_CHUNK]:
                await self._subscribe_orderbook(ws, market)
            if i + C.WS_SUBSCRIBE_CHUNK < len(add_list):
                await asyncio.sleep(C.WS_SUBSCRIBE_PACING_SECONDS)
        for market in sorted(to_remove):
            sid = self._orderbook_sids.pop(market, None)
            if sid is not None:
                await self._send(ws, self.sub.unsubscribe_message(self._next_id(), [sid]))
            self.rt.book_store.drop(market)

        await self._update_bulk(ws, self._trade_sid, C.WS_CHANNEL_TRADE, "trade", to_add, to_remove, desired)
        await self._update_bulk(ws, self._lifecycle_sid, C.WS_CHANNEL_LIFECYCLE, "lifecycle", to_add, to_remove, desired)

        self._current = desired
        self.reconciles += 1
        log.info("ws reconciled", extra={"shard": self.index, "added": len(to_add), "removed": len(to_remove)})

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
        self.resets += 1
        log.info("ws reset market", extra={"shard": self.index, "market": market})


class WebSocketLoop:
    """Coordinator over ``WS_SHARD_COUNT`` shards. Public ``runtime.Loop``."""

    name = _LOOP_NAME

    def __init__(self, rt) -> None:
        self.rt = rt
        self.sub = rt.subscriber
        # websockets doesn't pick up certifi's CA bundle by default (httpx does);
        # supply one explicitly so the wss handshake verifies.
        self._ssl = ssl.create_default_context(cafile=certifi.where())
        self._shards = [
            _Shard(rt, self.sub, self._ssl, i) for i in range(C.WS_SHARD_COUNT)
        ]
        self._owner: dict[str, int] = {}        # market -> shard index
        self._metrics_at = now_utc().timestamp()
        self._frames_mark = 0

    @property
    def _current(self) -> set[str]:
        """Union of every shard's subscribed set (the loop-wide active set)."""
        out: set[str] = set()
        for s in self._shards:
            out |= s._current
        return out

    async def run(self) -> None:
        await self._repartition()
        shard_tasks = [
            asyncio.create_task(s.run(), name=f"ws-shard-{s.index}") for s in self._shards
        ]
        try:
            while not self.rt.shutdown.is_set():
                self.rt.heartbeats.beat(self.name)
                if self.rt.resubscribe_event.is_set():
                    self.rt.resubscribe_event.clear()
                    await self._repartition()
                self._dispatch_resets()
                self._maybe_log_metrics()
                # A shard task should only finish on shutdown; if one died with an
                # exception, surface it so the supervisor restarts the whole loop.
                for t in shard_tasks:
                    if t.done():
                        t.result()
                await _wait_any([self.rt.resubscribe_event, self.rt.shutdown], C.WS_COORD_TICK_SECONDS)
        finally:
            for t in shard_tasks:
                t.cancel()
            await asyncio.gather(*shard_tasks, return_exceptions=True)

    async def _repartition(self) -> None:
        """Recompute the market->shard partition (by series) and hand each shard
        its desired set. Idempotent: shards diff desired against what they hold."""
        market_series = await asyncio.to_thread(self.rt.db.get_active_market_series)
        owner: dict[str, int] = {}
        buckets: list[set[str]] = [set() for _ in self._shards]
        for market, series in market_series.items():
            idx = _shard_for(series or market, len(self._shards))
            buckets[idx].add(market)
            owner[market] = idx
        self._owner = owner
        for shard, bucket in zip(self._shards, buckets):
            shard.set_desired(bucket)

    def _dispatch_resets(self) -> None:
        """Drain the shared reset queue, routing each market to the shard that
        owns it. A reset for a market we no longer track is dropped."""
        while True:
            try:
                market = self.rt.reset_requests.get_nowait()
            except asyncio.QueueEmpty:
                break
            idx = self._owner.get(market)
            if idx is None:
                continue
            try:
                self._shards[idx]._reset_q.put_nowait(market)
            except asyncio.QueueFull:
                log.warning("ws reset queue full; dropping", extra={"shard": idx, "market": market})

    # -- metrics ------------------------------------------------------------

    def _maybe_log_metrics(self) -> None:
        now = now_utc().timestamp()
        elapsed = now - self._metrics_at
        if elapsed < C.METRICS_LOG_INTERVAL_SECONDS:
            return
        total_frames = sum(s.frames for s in self._shards)
        frames = total_frames - self._frames_mark
        log.info(
            "ws metrics",
            extra={
                "frames_per_s": round(frames / elapsed, 1),
                "reconnects": sum(s.reconnects for s in self._shards),
                "reconciles": sum(s.reconciles for s in self._shards),
                "resets": sum(s.resets for s in self._shards),
                "shards": len(self._shards),
                "subscribed": len(self._current),
            },
        )
        self._metrics_at = now
        self._frames_mark = total_frames
