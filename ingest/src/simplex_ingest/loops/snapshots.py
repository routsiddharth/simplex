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
from datetime import datetime, timedelta

from .. import constants as C
from ..log import get_logger
from ..reconstruct import ApplyResult
from ..runtime import request_reset
from ..util import floor_to_interval, idle_sleep, naive_utc, now_utc

log = get_logger("snapshot")

_FETCH_LIMIT = 5000


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
            secs = (naive_utc(boundary) - naive_utc(now_utc())).total_seconds()
            # step=5s: beat more often than the other loops so the tighter grid
            # boundary stays liveness-safe.
            if not await idle_sleep(self.rt.shutdown, self.rt.heartbeats, self.name, secs, step=5.0):
                break  # shutdown fired
            try:
                await self.tick(window_end=boundary)
            except Exception:
                log.exception("snapshot tick failed")
            self.rt.heartbeats.beat(self.name)
            await self._maybe_checkpoint()

    # -- one grid tick ------------------------------------------------------

    async def tick(self, window_end: datetime) -> None:
        window_start = window_end - _interval()

        markets = await asyncio.to_thread(self.rt.db.get_active_markets)
        self._active = {m["market_id"] for m in markets}
        self._active_status = {m["market_id"]: m["status"] for m in markets}
        # Forget markets that left the active set so a later re-admit re-seeds
        # (status/last-trade + any checkpoint) instead of silently restarting
        # with an empty book. The WS loop already dropped their BookStore entry.
        self._seeded &= self._active
        await self._seed_new_markets()

        await self._drain_events()

        volumes = await asyncio.to_thread(
            self.rt.db.window_trade_volume, window_start, window_end
        )

        rows: list[tuple] = []
        built = naive_utc(now_utc())
        ts = naive_utc(window_end)
        for market in self._active:
            r = self.rt.book_store.get_or_create(market)
            yes_bid, yes_ask, yes_mid = r.top_of_book()
            bid_depth, ask_depth, bid_levels, ask_levels = r.depth_within(
                C.DEPTH_BAND_PRICE_UNITS
            )
            status = r.status or self._active_status.get(market)
            rows.append(
                (
                    ts, self.rt.subscriber.platform, market,
                    yes_bid, yes_ask, yes_mid,
                    bid_depth, ask_depth, bid_levels, ask_levels,
                    volumes.get(market, 0.0), r.last_trade_price, status, built,
                )
            )
            self._run_canaries(market, r, window_end)

        await asyncio.to_thread(self.rt.db.upsert_snapshots, rows)
        log.info("snapshots emitted", extra={"ts": ts.isoformat(), "markets": len(rows)})

    def _run_canaries(self, market: str, r, window_end: datetime) -> None:
        reset = r.reset_canaries()
        if reset:
            log.warning("canary tripped; resetting book", extra={"market": market, "issues": sorted(reset)})
            request_reset(self.rt, market)

        # Stale-market canary (informational only).
        last = r.last_event_received_ts
        open_market = (r.status or self._active_status.get(market)) in (None, "active")
        if open_market and last is not None:
            idle = (naive_utc(window_end) - naive_utc(last)).total_seconds()
            if idle > C.STALE_MARKET_SECONDS:
                log.info("stale market", extra={"market": market, "idle_s": round(idle)})

    # -- seeding + replay ---------------------------------------------------

    async def _seed_new_markets(self) -> None:
        new = [m for m in self._active if m not in self._seeded]
        if not new:
            return
        prices = await asyncio.to_thread(self.rt.db.latest_trade_prices, new)
        floors: list[datetime] = []
        for market in new:
            r = self.rt.book_store.get_or_create(market)
            cp = self._checkpoints.pop(market, None)
            if cp:
                r.restore_checkpoint(cp)
                if cp["last_ts"] is not None:
                    floors.append(cp["last_ts"])
            r.last_trade_price = prices.get(market)
            r.status = self._active_status.get(market)
            self._seeded.add(market)

        if not self._cursor_ready:
            # Resume replay from the earliest checkpoint floor so previously-active
            # markets pick up the events written since their last checkpoint. With
            # NO checkpoints (first boot, or only freshly-discovered markets) there
            # is nothing to resume: start at the current tail and let books anchor
            # from the next live WS snapshot. Collapsing to datetime.min here would
            # rescan the entire append-only raw_events history every cold start —
            # unbounded as the DB grows, and long enough to trip the health timeout
            # and crash-loop once it's large.
            self._cursor_ts = naive_utc(min(floors)) if floors else naive_utc(now_utc())
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
        r = self.rt.book_store.get_or_create(market)
        if r.apply(e) is ApplyResult.RESET:
            request_reset(self.rt, market)

    # -- checkpointing ------------------------------------------------------

    async def _maybe_checkpoint(self) -> None:
        if now_utc().timestamp() - self._last_checkpoint < C.CHECKPOINT_INTERVAL_SECONDS:
            return
        await self.checkpoint()

    async def checkpoint(self) -> None:
        rows = []
        for market, r in self.rt.book_store.markets.items():
            rows.append((market, r.last_sequence, r.last_event_received_ts, json.dumps(r.serialize())))
        if rows:
            await asyncio.to_thread(self.rt.db.save_book_states, rows)
        self._last_checkpoint = now_utc().timestamp()
        log.info("checkpointed books", extra={"markets": len(rows)})


def _interval() -> timedelta:
    return timedelta(seconds=C.SNAPSHOT_INTERVAL_SECONDS)
