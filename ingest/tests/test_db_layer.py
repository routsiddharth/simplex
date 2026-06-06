"""DuckDB access layer — the methods not already covered by the tracked_series
and extraction DB tests: market upsert + active-set flip, snapshot/book_state
round-trips, audit inserts, and the snapshot-builder read queries (trade volume,
latest price, forward event cursor).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from simplex_ingest.events import EventType, NormalizedEvent
from simplex_ingest.util import naive_utc, now_utc


def _market_row(mid, status="active", title="T"):
    now = naive_utc(now_utc())
    return (mid, "kalshi", title, "desc", "rules", "KXS", "KXS-E",
            None, None, None, status, None, now)


def _count(db, sql, params=None):
    with db._lock:
        return db._con.execute(sql, params or []).fetchall()


def test_market_upsert_and_active_set_flip(tmp_db):
    tmp_db.upsert_markets([_market_row("A"), _market_row("B")])
    tmp_db.set_active_set({"A"})
    assert tmp_db.get_active_market_ids() == {"A"}

    # Re-upsert must NOT touch `subscribed` (managed by set_active_set only).
    tmp_db.upsert_markets([_market_row("A", title="renamed")])
    assert tmp_db.get_active_market_ids() == {"A"}
    rows = _count(tmp_db, "SELECT title FROM markets WHERE market_id='A'")
    assert rows[0][0] == "renamed"

    # Flipping the active set unsubscribes the old, subscribes the new; rows kept.
    tmp_db.set_active_set({"B"})
    assert tmp_db.get_active_market_ids() == {"B"}
    ids = {r[0] for r in _count(tmp_db, "SELECT market_id FROM markets")}
    assert ids == {"A", "B"}


def test_snapshot_upsert_is_idempotent_on_pk(tmp_db):
    ts = naive_utc(now_utc())
    tmp_db.upsert_snapshots([(ts, "kalshi", "A", 0.4, 0.45, 0.425, 1.0, 1.0, 1, 1, 5.0, 0.41, "active", ts)])
    tmp_db.upsert_snapshots([(ts, "kalshi", "A", 0.5, 0.55, 0.525, 2.0, 2.0, 2, 2, 6.0, 0.5, "active", ts)])
    rows = _count(tmp_db, "SELECT yes_bid FROM snapshots WHERE market_id='A'")
    assert len(rows) == 1 and rows[0][0] == 0.5  # conflict updated in place


def test_book_state_round_trip(tmp_db):
    ts = naive_utc(now_utc())
    tmp_db.save_book_states([("A", 7, ts, '{"yes": {"0.4": 100}, "no": {}}')])
    out = tmp_db.load_book_states()
    assert out["A"]["last_sequence"] == 7
    assert out["A"]["book"] == {"yes": {"0.4": 100}, "no": {}}


def test_audit_results_insert(tmp_db):
    ts = now_utc()
    tmp_db.insert_audit_results([(ts, "A", "large_diff", 5, 99.0, "book_reset", '{"x": 1}')])
    row = _count(tmp_db, "SELECT status, action_taken FROM audit_results")[0]
    assert row == ("large_diff", "book_reset")


def _trade(mid, price, count, rts):
    return NormalizedEvent(rts, "kalshi", mid, EventType.TRADE, {"price": price, "count": count})


def test_window_trade_volume_and_latest_price(tmp_db):
    base = naive_utc(now_utc())
    for ev in (
        _trade("A", 0.40, 3, base),
        _trade("A", 0.50, 2, base + timedelta(seconds=1)),
        _trade("B", 0.70, 10, base),
    ):
        tmp_db.buffer_event(ev)
    tmp_db.flush_raw_events()

    vols = tmp_db.window_trade_volume(base - timedelta(seconds=1), base + timedelta(seconds=5))
    assert vols["A"] == 5.0 and vols["B"] == 10.0

    prices = tmp_db.latest_trade_prices(["A", "B"])
    assert prices["A"] == 0.50  # most recent A trade by received_ts
    assert prices["B"] == 0.70


def test_fetch_events_after_orders_and_advances_cursor(tmp_db):
    base = naive_utc(now_utc())
    for i in range(3):
        tmp_db.buffer_event(NormalizedEvent(
            base + timedelta(seconds=i), "kalshi", "A",
            EventType.ORDERBOOK_SNAPSHOT, {"yes": [], "no": []}, sequence=i,
        ))
    tmp_db.flush_raw_events()

    batch = tmp_db.fetch_events_after(datetime.min, -1, 10)
    assert [e["sequence"] for e in batch] == [0, 1, 2]

    first = batch[0]
    rest = tmp_db.fetch_events_after(first["received_ts"], first["rowid"], 10)
    assert [e["sequence"] for e in rest] == [1, 2]  # strictly after the cursor
