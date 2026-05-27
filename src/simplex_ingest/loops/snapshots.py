"""Snapshot builder loop.

Maintains in-memory order books by replaying `raw_events` in ingest order with
strict per-market sequence checking (a gap discards that market's book and
requests a fresh snapshot). Every SNAPSHOT_INTERVAL_SECONDS it emits one row per
active market to `snapshots` — top-of-book, within-band depth, window volume,
last trade, status — carrying the book forward (LOCF) for quiet markets. Books
are checkpointed to `book_state` every CHECKPOINT_INTERVAL_SECONDS so a restart
resumes instead of replaying from scratch.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime

from .. import constants as C
from ..events import EventType
from ..log import get_logger
from ..util import floor_to_interval, naive_utc, now_utc

log = get_logger("snapshot")

_FETCH_LIMIT = 5000
_RESET_CANARIES = {"crossed_book", "negative_size", "out_of_range_price"}


class SnapshotBuilder:
    name = "snapshot"

    def __init__(self, rt) -> None:
        self.rt = rt
        self._cursor_ts: datetime = datetime.min
        self._cursor_rowid: int = -1
        self._cursor_ready = False
        self._checkpoints: dict[str, dict] = {}
        self._seeded: set[str] = set()
        self._active: set[str] = set()
        self._active_status: dict[str, str | None] = {}
        self._last_checkpoint = 0.0

    async def run(self) -> None:
        # Load checkpoints once; markets are seeded from them lazily as they
        # appear in the active set.
        self._checkpoints = await asyncio.to_thread(self.rt.db.load_book_states)
        log.info("loaded checkpoints", extra={"markets": len(self._checkpoints)})
        self._last_checkpoint = now_utc().timestamp()

        while not self.rt.shutdown.is_set():
            now = now_utc()
            boundary = floor_to_interval(now, C.SNAPSHOT_INTERVAL_SECONDS) + _interval()
            if not await self._sleep_until(boundary):
                break
            try:
                await self.tick(window_end=boundary)
            except Exception:
                log.exception("snapshot tick failed")
            self.rt.heartbeats.beat(self.name)
            await self._maybe_checkpoint()

    async def _sleep_until(self, target: datetime) -> bool:
        """Sleep until ``target`` (UTC), waking on shutdown. False => shutting down."""
        while not self.rt.shutdown.is_set():
            remaining = (target - naive_utc(now_utc())).total_seconds()
            if remaining <= 0:
                return True
            try:
                await asyncio.wait_for(self.rt.shutdown.wait(), timeout=min(remaining, 5.0))
                return False  # shutdown fired
            except asyncio.TimeoutError:
                self.rt.heartbeats.beat(self.name)
        return False

    # -- one grid tick ------------------------------------------------------

    async def tick(self, window_end: datetime) -> None:
        window_start = window_end - _interval()

        markets = await asyncio.to_thread(self.rt.db.get_active_markets)
        self._active = {m["market_id"] for m in markets}
        self._active_status = {m["market_id"]: m["status"] for m in markets}
        await self._seed_new_markets()

        await self._drain_events()

        volumes = await asyncio.to_thread(
            self.rt.db.window_trade_volume, window_start, window_end
        )

        rows: list[tuple] = []
        built = naive_utc(now_utc())
        ts = naive_utc(window_end)
        for market in self._active:
            st = self.rt.book_store.get_or_create(market)
            yes_bid, yes_ask, yes_mid = st.book.top_of_book()
            bid_depth, ask_depth, bid_levels, ask_levels = st.book.depth_within(
                C.DEPTH_BAND_PRICE_UNITS
            )
            status = st.status or self._active_status.get(market)
            rows.append(
                (
                    ts, self.rt.subscriber.platform, market,
                    yes_bid, yes_ask, yes_mid,
                    bid_depth, ask_depth, bid_levels, ask_levels,
                    volumes.get(market, 0.0), st.last_trade_price, status, built,
                )
            )
            self._run_canaries(market, st, window_end)

        await asyncio.to_thread(self.rt.db.upsert_snapshots, rows)
        log.info("snapshots emitted", extra={"ts": ts.isoformat(), "markets": len(rows)})

    def _run_canaries(self, market: str, st, window_end: datetime) -> None:
        issues = st.book.check_canaries(C.CANARY_PRICE_MIN_USD, C.CANARY_PRICE_MAX_USD)
        reset = issues & _RESET_CANARIES
        if reset:
            log.warning("canary tripped; resetting book", extra={"market": market, "issues": sorted(reset)})
            self._request_reset(market)

        # Stale-market canary (informational only).
        last = st.last_event_received_ts
        open_market = (st.status or self._active_status.get(market)) in (None, "active")
        if open_market and last is not None:
            idle = (naive_utc(window_end) - naive_utc(last)).total_seconds()
            if idle > C.STALE_MARKET_SECONDS:
                log.info("stale market", extra={"market": market, "idle_s": round(idle)})

    def _request_reset(self, market: str) -> None:
        self.rt.book_store.reset_book(market)
        try:
            self.rt.reset_requests.put_nowait(market)
        except asyncio.QueueFull:
            log.warning("reset queue full", extra={"market": market})

    # -- seeding + replay ---------------------------------------------------

    async def _seed_new_markets(self) -> None:
        new = [m for m in self._active if m not in self._seeded]
        if not new:
            return
        prices = await asyncio.to_thread(self.rt.db.latest_trade_prices, new)
        floors: list[datetime] = []
        all_checkpointed = True
        for market in new:
            st = self.rt.book_store.get_or_create(market)
            cp = self._checkpoints.pop(market, None)
            if cp:
                from ..orderbook import OrderBook
                st.book = OrderBook.deserialize(cp["book"])
                st.last_sequence = cp["last_sequence"]
                st.anchored = not st.book.is_empty
                st.replay_floor_ts = cp["last_ts"]
                if cp["last_ts"] is not None:
                    floors.append(cp["last_ts"])
            else:
                all_checkpointed = False
            st.last_trade_price = prices.get(market)
            st.status = self._active_status.get(market)
            self._seeded.add(market)

        if not self._cursor_ready:
            if all_checkpointed and floors:
                self._cursor_ts = naive_utc(min(floors))
            else:
                self._cursor_ts = datetime.min
            self._cursor_rowid = -1
            self._cursor_ready = True
            log.info("replay cursor set", extra={"from_ts": self._cursor_ts.isoformat()})

    async def _drain_events(self) -> None:
        if not self._cursor_ready:
            return
        while True:
            batch = await asyncio.to_thread(
                self.rt.db.fetch_events_after, self._cursor_ts, self._cursor_rowid, _FETCH_LIMIT
            )
            if not batch:
                return
            for e in batch:
                self._apply_event(e)
                self._cursor_ts = e["received_ts"]
                self._cursor_rowid = e["rowid"]
            if len(batch) < _FETCH_LIMIT:
                return

    def _apply_event(self, e: dict) -> None:
        market = e["market_id"]
        if market not in self._active:
            return  # only maintain books for the active set
        st = self.rt.book_store.get_or_create(market)
        rts = e["received_ts"]
        if st.replay_floor_ts is not None and rts is not None and rts <= st.replay_floor_ts:
            return  # already folded into the checkpoint
        st.last_event_received_ts = rts

        et = e["event_type"]
        payload = e["payload"]
        seq = e["sequence"]

        if et == EventType.ORDERBOOK_SNAPSHOT.value:
            st.book.apply_snapshot(
                [tuple(x) for x in payload.get("yes", [])],
                [tuple(x) for x in payload.get("no", [])],
            )
            st.anchored = True
            st.last_sequence = seq
        elif et == EventType.ORDERBOOK_DELTA.value:
            if not st.anchored:
                return  # wait for a snapshot to anchor
            if st.last_sequence is not None and seq is not None:
                if seq <= st.last_sequence:
                    return  # duplicate / out of order
                if seq > st.last_sequence + 1:
                    log.warning(
                        "sequence gap; resetting book",
                        extra={"market": market, "expected": st.last_sequence + 1, "got": seq},
                    )
                    self._request_reset(market)
                    return
            side = payload.get("side")
            price = payload.get("price")
            delta = payload.get("delta")
            if side in ("yes", "no") and price is not None and delta is not None:
                st.book.apply_delta(side, price, delta)
            if seq is not None:
                st.last_sequence = seq
        elif et == EventType.TRADE.value:
            price = payload.get("price")
            if price is not None:
                st.last_trade_price = price
        elif et == EventType.LIFECYCLE.value:
            st.status = _status_from_lifecycle(payload, st.status)

    # -- checkpointing ------------------------------------------------------

    async def _maybe_checkpoint(self) -> None:
        if now_utc().timestamp() - self._last_checkpoint < C.CHECKPOINT_INTERVAL_SECONDS:
            return
        await self.checkpoint()

    async def checkpoint(self) -> None:
        rows = []
        for market, st in self.rt.book_store.markets.items():
            rows.append((market, st.last_sequence, st.last_event_received_ts, json.dumps(st.book.serialize())))
        if rows:
            await asyncio.to_thread(self.rt.db.save_book_states, rows)
        self._last_checkpoint = now_utc().timestamp()
        log.info("checkpointed books", extra={"markets": len(rows)})


def _interval():
    from datetime import timedelta
    return timedelta(seconds=C.SNAPSHOT_INTERVAL_SECONDS)


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
