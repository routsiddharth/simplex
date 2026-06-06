"""WebSocket loop over a REAL local socket.

Drives the production WebSocketLoop against a FakeKalshiWSServer (conftest):
it connects, subscribes the active set one-market-per-orderbook plus the bulk
trade/lifecycle channels, parses inbound frames through the real
KalshiSubscriber, and buffers them to raw_events. Also covers the two control
paths the loop owns: incremental reconcile on the catalog signal, and draining
a book-reset request into a re-subscribe.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from simplex_ingest import constants as C
from simplex_ingest.kalshi.subscriber import KalshiSubscriber
from simplex_ingest.loops.websocket import WebSocketLoop
from simplex_ingest.runtime import BookStore, Heartbeats
from simplex_ingest.util import naive_utc, now_utc


def _ws_runtime(db, server, signer):
    return SimpleNamespace(
        db=db,
        subscriber=KalshiSubscriber(signer, server.url),
        book_store=BookStore(),
        reset_requests=asyncio.Queue(maxsize=256),
        resubscribe_event=asyncio.Event(),
        heartbeats=Heartbeats(),
        shutdown=asyncio.Event(),
    )


def _activate(db, *market_ids):
    now = naive_utc(now_utc())
    rows = [(m, "kalshi", m, None, None, "KXS", "KXS-E", None, None, None,
             "active", None, now) for m in market_ids]
    db.upsert_markets(rows)
    db.set_active_set(set(market_ids))


async def _count_raw(db) -> int:
    await asyncio.to_thread(db.flush_raw_events)
    with db._lock:  # test-only direct read
        return db._con.execute("SELECT count(*) FROM raw_events").fetchone()[0]


async def _wait_for(predicate, timeout=5.0, interval=0.05):
    """Poll an async predicate until truthy or timeout (deterministic-ish wait
    for socket round-trips without sleeping a fixed wall-clock budget)."""
    waited = 0.0
    while waited < timeout:
        if await predicate():
            return True
        await asyncio.sleep(interval)
        waited += interval
    return False


async def test_loop_subscribes_and_streams_events_to_raw_events(tmp_db, ws_server, make_signer):
    rt = _ws_runtime(tmp_db, ws_server, make_signer())
    _activate(tmp_db, "KXM-A", "KXM-B")

    loop = WebSocketLoop(rt)
    task = asyncio.create_task(loop.run())
    try:
        # Each market yields a snapshot + a delta (orderbook) + a trade (bulk).
        got = await _wait_for(lambda: _raw_ge(tmp_db, 6))
        assert got, "expected >=6 raw_events from 2 markets (snapshot+delta+trade each)"

        with tmp_db._lock:
            by_type = dict(tmp_db._con.execute(
                "SELECT event_type, count(*) FROM raw_events GROUP BY event_type"
            ).fetchall())
        assert by_type.get("orderbook_snapshot", 0) >= 2
        assert by_type.get("orderbook_delta", 0) >= 2
        assert by_type.get("trade", 0) >= 2

        # The loop sent one orderbook subscribe per market + bulk trade + lifecycle.
        subscribes = [c for c in ws_server.received if c.get("cmd") == "subscribe"]
        channels = {ch for c in subscribes for ch in (c["params"]["channels"])}
        assert C.WS_CHANNEL_ORDERBOOK in channels
        assert C.WS_CHANNEL_TRADE in channels
        assert C.WS_CHANNEL_LIFECYCLE in channels
    finally:
        rt.shutdown.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_reset_request_resubscribes_the_market(tmp_db, ws_server, make_signer):
    rt = _ws_runtime(tmp_db, ws_server, make_signer())
    _activate(tmp_db, "KXM-A")

    loop = WebSocketLoop(rt)
    task = asyncio.create_task(loop.run())
    try:
        assert await _wait_for(lambda: _raw_ge(tmp_db, 1))
        n_sub_before = len([c for c in ws_server.received if c.get("cmd") == "subscribe"])

        # Ask the loop to reset that market's book -> it re-subscribes orderbook.
        rt.reset_requests.put_nowait("KXM-A")
        got = await _wait_for(
            lambda: _async_true(
                len([c for c in ws_server.received if c.get("cmd") == "subscribe"]) > n_sub_before
            )
        )
        assert got, "reset request should trigger a fresh orderbook subscribe"
        assert any(c.get("cmd") == "unsubscribe" for c in ws_server.received)
    finally:
        rt.shutdown.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_reconcile_adds_new_market_on_catalog_signal(tmp_db, ws_server, make_signer):
    rt = _ws_runtime(tmp_db, ws_server, make_signer())
    _activate(tmp_db, "KXM-A")

    loop = WebSocketLoop(rt)
    task = asyncio.create_task(loop.run())
    try:
        assert await _wait_for(lambda: _async_true("KXM-A" in loop._current))

        # Catalog admits a second market and signals the WS loop to reconcile.
        _activate(tmp_db, "KXM-A", "KXM-B")
        rt.resubscribe_event.set()

        got = await _wait_for(lambda: _async_true("KXM-B" in loop._current))
        assert got, "reconcile should pick up the newly-active market"
    finally:
        rt.shutdown.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# -- small async predicate helpers ------------------------------------------

async def _raw_ge(db, n) -> bool:
    return (await _count_raw(db)) >= n


async def _async_true(value: bool) -> bool:
    return bool(value)
