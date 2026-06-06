"""Snapshot builder: anchoring discipline, sequence-gap reset, the cold-start
replay cursor (must NOT rescan all of raw_events), and checkpoint resume.

Drives the real SnapshotBuilder over a tmp DuckDB with a minimal runtime.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from simplex_ingest.events import EventType, NormalizedEvent
from simplex_ingest.loops.snapshots import SnapshotBuilder
from simplex_ingest.runtime import BookStore, Heartbeats
from simplex_ingest.util import naive_utc, now_utc


def _runtime(db):
    return SimpleNamespace(
        db=db,
        book_store=BookStore(),
        subscriber=SimpleNamespace(platform="kalshi"),
        heartbeats=Heartbeats(),
        shutdown=asyncio.Event(),
        reset_requests=asyncio.Queue(),
    )


def _activate(db, market_id, status="active"):
    now = naive_utc(now_utc())
    row = (market_id, "kalshi", None, None, None, "KXS", "KXS-E",
           None, None, None, status, None, now)
    db.upsert_markets([row])
    db.set_active_set({market_id})


def _insert(db, ev):
    db.buffer_event(ev)
    db.flush_raw_events()


def _snap(market_id, yes, no, seq, rts):
    return NormalizedEvent(rts, "kalshi", market_id, EventType.ORDERBOOK_SNAPSHOT,
                           {"yes": yes, "no": no}, sequence=seq)


def _delta(market_id, side, price, delta, seq, rts):
    return NormalizedEvent(rts, "kalshi", market_id, EventType.ORDERBOOK_DELTA,
                           {"side": side, "price": price, "delta": delta}, sequence=seq)


def _prime_cursor(builder, market_id):
    """Simulate a cursor established at boot (datetime.min) before events stream
    in, so historical rows are replayed — decoupled from the cold-start policy."""
    builder._cursor_ready = True
    builder._cursor_ts = datetime.min
    builder._cursor_rowid = -1
    builder._seeded = {market_id}


async def test_anchor_then_delta_applies(tmp_db):
    rt = _runtime(tmp_db)
    builder = SnapshotBuilder(rt)
    mid = "M1"
    _activate(tmp_db, mid)
    base = naive_utc(now_utc()) - timedelta(hours=1)
    _insert(tmp_db, _snap(mid, [[0.40, 100], [0.38, 50]], [[0.55, 80]], seq=5, rts=base))
    _insert(tmp_db, _delta(mid, "yes", 0.41, 30, seq=6, rts=base + timedelta(seconds=1)))
    _prime_cursor(builder, mid)

    await builder.tick(window_end=now_utc())

    st = rt.book_store.get(mid)
    assert st.anchored
    assert st.last_sequence == 6
    yes_bid, yes_ask, _ = st.book.top_of_book()
    assert yes_bid == 0.41                          # delta added a new top level
    assert yes_ask == round(1 - 0.55, 6)


async def test_sequence_gap_requests_reset(tmp_db):
    rt = _runtime(tmp_db)
    builder = SnapshotBuilder(rt)
    mid = "M2"
    _activate(tmp_db, mid)
    base = naive_utc(now_utc()) - timedelta(hours=1)
    _insert(tmp_db, _snap(mid, [[0.40, 100]], [[0.55, 80]], seq=5, rts=base))
    _insert(tmp_db, _delta(mid, "yes", 0.41, 30, seq=6, rts=base + timedelta(seconds=1)))
    _prime_cursor(builder, mid)
    await builder.tick(window_end=now_utc())
    assert rt.book_store.get(mid).anchored

    # seq jumps 6 -> 8 (expected 7): gap -> reset requested, book de-anchored.
    _insert(tmp_db, _delta(mid, "yes", 0.42, 10, seq=8, rts=base + timedelta(seconds=2)))
    await builder.tick(window_end=now_utc())

    assert rt.reset_requests.get_nowait() == mid
    assert not rt.book_store.get(mid).anchored


async def test_cold_start_cursor_does_not_rescan_history(tmp_db):
    """The HIGH regression: with raw_events history but no checkpoints, the
    cursor starts at ~now (not datetime.min), so we never rescan all history."""
    rt = _runtime(tmp_db)
    builder = SnapshotBuilder(rt)
    mid = "M3"
    _activate(tmp_db, mid)
    old = naive_utc(now_utc()) - timedelta(days=1)
    _insert(tmp_db, _snap(mid, [[0.40, 100]], [[0.55, 80]], seq=1, rts=old))

    before = naive_utc(now_utc())
    await builder.tick(window_end=now_utc())

    assert builder._cursor_ready
    assert builder._cursor_ts >= before - timedelta(seconds=5)   # ~now, not min
    # The day-old snapshot was skipped; the book awaits a live anchor.
    assert not rt.book_store.get(mid).anchored


async def test_checkpoint_resume_restores_and_replays_forward(tmp_db):
    base = naive_utc(now_utc()) - timedelta(hours=1)
    mid = "M4"
    _activate(tmp_db, mid)

    # Builder 1: anchor a book and checkpoint it.
    rt1 = _runtime(tmp_db)
    b1 = SnapshotBuilder(rt1)
    _insert(tmp_db, _snap(mid, [[0.40, 100]], [[0.55, 80]], seq=5, rts=base))
    _prime_cursor(b1, mid)
    await b1.tick(window_end=now_utc())
    assert rt1.book_store.get(mid).anchored
    await b1.checkpoint()

    # A delta written AFTER the checkpoint.
    _insert(tmp_db, _delta(mid, "yes", 0.41, 30, seq=6, rts=base + timedelta(seconds=1)))

    # Builder 2: fresh book store, loads checkpoints like run() does.
    rt2 = _runtime(tmp_db)
    b2 = SnapshotBuilder(rt2)
    b2._checkpoints = tmp_db.load_book_states()
    assert mid in b2._checkpoints

    await b2.tick(window_end=now_utc())

    st = rt2.book_store.get(mid)
    assert st.anchored
    assert 0.40 in st.book.yes                       # restored from checkpoint
    assert 0.41 in st.book.yes                       # post-checkpoint delta replayed
    # Resumed from the checkpoint floor (~base), not from "now".
    assert b2._cursor_ts < naive_utc(now_utc()) - timedelta(minutes=30)


async def test_readmit_reseeds_after_drop(tmp_db):
    """A market dropped from the active set and re-admitted must re-seed rather
    than stay in _seeded with a silently-empty book."""
    rt = _runtime(tmp_db)
    builder = SnapshotBuilder(rt)
    mid = "M5"
    _activate(tmp_db, mid)
    await builder.tick(window_end=now_utc())
    assert mid in builder._seeded

    # Catalog drops it; WS would drop the book. Mimic both.
    tmp_db.set_active_set(set())
    rt.book_store.drop(mid)
    await builder.tick(window_end=now_utc())
    assert mid not in builder._seeded                 # pruned

    # Re-admitted: it must seed again (not be skipped as already-seeded).
    _activate(tmp_db, mid)
    await builder.tick(window_end=now_utc())
    assert mid in builder._seeded
    assert rt.book_store.get(mid) is not None


def _snapshot_count(db, market_id):
    with db._lock:
        return db._con.execute(
            "SELECT COUNT(*) FROM snapshots WHERE market_id = ?", [market_id]
        ).fetchone()[0]


async def test_run_split_applier_and_sampler(tmp_db, monkeypatch):
    """The real run(): the continuous applier keeps the book current from
    raw_events while the sampler emits the grid on its own cadence — exercising
    the two-task split, not just the synchronous tick()."""
    from simplex_ingest import constants as C

    monkeypatch.setattr(C, "SNAPSHOT_INTERVAL_SECONDS", 1)  # emit ~every second
    rt = _runtime(tmp_db)
    mid = "MR"
    _activate(tmp_db, mid)
    builder = SnapshotBuilder(rt)

    task = asyncio.create_task(builder.run())
    await asyncio.sleep(0.2)  # let run() set the replay cursor at ~now
    # Event written AFTER the cursor is established → the applier drains it.
    _insert(tmp_db, _snap(mid, [[0.40, 100]], [[0.55, 80]], seq=1, rts=naive_utc(now_utc())))

    try:
        for _ in range(50):  # up to ~5s for the applier+sampler to do their thing
            await asyncio.sleep(0.1)
            if _snapshot_count(tmp_db, mid) > 0 and rt.book_store.get(mid) and rt.book_store.get(mid).anchored:
                break
    finally:
        rt.shutdown.set()
        await asyncio.wait_for(task, timeout=5)

    assert rt.book_store.get(mid).anchored           # applier kept the book current
    assert _snapshot_count(tmp_db, mid) > 0          # sampler emitted the grid
    assert builder._events_applied >= 1              # metric counter advanced
