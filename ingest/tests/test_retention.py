"""Rolling-window retention.

Time-series tables (raw_events/snapshots/audit_results/book_state) are pruned to
the last few discovery cycles; the durable LLM graph (market_semantics/
market_edges) and the catalog are never touched. The discovery loop drives the
prune each cycle, so retention is in sync with the hourly market-set recompute.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace

from simplex_ingest import constants as C
from simplex_ingest.events import EventType, NormalizedEvent
from simplex_ingest.loops.discovery import DiscoveryLoop, _resolution_time
from simplex_ingest.runtime import Heartbeats
from simplex_ingest.util import naive_utc, now_utc


def _market(mid, resolved_at=None, status="active", subscribed=False):
    now = naive_utc(now_utc())
    return (mid, "kalshi", "T", None, None, "KXS", "KXS-E", None, None, resolved_at, status, None, now)


def _sem(mid):
    now = naive_utc(now_utc())
    return (mid, "kalshi", "e", "y", "n", "t", "[]", "[]", "m", 1, now, "{}")


def _edge(a, b):
    now = naive_utc(now_utc())
    return ("kalshi", a, b, "unrelated", "none", 0.1, "review", "single", "r", "m", None, 1, now, "{}")


def _trade(mid, rts):
    return NormalizedEvent(rts, "kalshi", mid, EventType.TRADE, {"price": 0.4, "count": 1})


def _count(db, table):
    with db._lock:
        return db._con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_prune_deletes_old_timeseries_and_keeps_recent(tmp_db):
    now = naive_utc(now_utc())
    old = now - timedelta(hours=5)
    recent = now - timedelta(minutes=30)

    tmp_db.buffer_event(_trade("A", old))
    tmp_db.buffer_event(_trade("A", recent))
    tmp_db.flush_raw_events()
    tmp_db.upsert_snapshots([
        (old, "kalshi", "A", 0.4, 0.45, 0.42, 1, 1, 1, 1, 1, 0.4, "active", old),
        (recent, "kalshi", "A", 0.5, 0.55, 0.52, 1, 1, 1, 1, 1, 0.5, "active", recent),
    ])
    tmp_db.insert_audit_results([
        (old, "A", "no_diff", 0, 0.0, "none", "{}"),
        (recent, "A", "no_diff", 0, 0.0, "none", "{}"),
    ])
    # book_state: an old one (pruned), a recent one (kept), and a NULL-ts one
    # (a checkpoint that never anchored -> kept, since NULL < cutoff is not true).
    tmp_db.save_book_states([
        ("A", 1, old, "{}"),
        ("B", 2, recent, "{}"),
        ("C", None, None, "{}"),
    ])

    deleted = tmp_db.prune_time_series(now - timedelta(hours=3))

    assert deleted == {"raw_events": 1, "snapshots": 1, "audit_results": 1, "book_state": 1}
    assert _count(tmp_db, "raw_events") == 1
    assert _count(tmp_db, "snapshots") == 1
    assert _count(tmp_db, "audit_results") == 1
    with tmp_db._lock:
        ids = {r[0] for r in tmp_db._con.execute("SELECT market_id FROM book_state").fetchall()}
    assert ids == {"B", "C"}  # recent + the never-anchored checkpoint survive


def test_prune_never_touches_the_llm_graph_or_catalog(tmp_db):
    now = naive_utc(now_utc())
    long_ago = now - timedelta(hours=100)
    tmp_db.upsert_market_semantics([
        ("A", "kalshi", "ev", "y", "n", "t", "[]", "[]", "model", 1, long_ago, "{}"),
    ])
    tmp_db.upsert_edges([
        ("kalshi", "A", "B", "unrelated", "none", 0.1, "review", "single", "r", "model", None, 1, long_ago, "{}"),
    ])
    tmp_db.upsert_markets([
        ("A", "kalshi", "T", None, None, "KXS", "KXS-E", None, None, None, "active", None, long_ago),
    ])

    # A cutoff in the far future would delete every time-series row; the durable
    # graph + catalog must be untouched regardless.
    tmp_db.prune_time_series(now + timedelta(hours=100))

    assert _count(tmp_db, "market_semantics") == 1
    assert _count(tmp_db, "market_edges") == 1
    assert _count(tmp_db, "markets") == 1


async def test_discovery_loop_prune_enforces_the_retention_window(tmp_db):
    """The loop computes its cutoff from the live constant, so the window tracks
    the configured cycle count automatically."""
    now = naive_utc(now_utc())
    beyond = now - timedelta(seconds=C.DATA_RETENTION_SECONDS + 600)
    inside = now - timedelta(seconds=C.DATA_RETENTION_SECONDS - 600)
    tmp_db.buffer_event(_trade("A", beyond))
    tmp_db.buffer_event(_trade("A", inside))
    tmp_db.flush_raw_events()

    rt = SimpleNamespace(db=tmp_db, shutdown=asyncio.Event(), heartbeats=Heartbeats())
    await DiscoveryLoop(rt).prune()

    assert _count(tmp_db, "raw_events") == 1  # only the in-window row remains


async def test_discovery_prune_survives_a_db_error(tmp_db, mocker):
    """A prune failure must not crash the loop (it logs and returns)."""
    rt = SimpleNamespace(db=tmp_db, rest=None, shutdown=asyncio.Event(), heartbeats=Heartbeats())
    mocker.patch.object(tmp_db, "prune_time_series", side_effect=RuntimeError("boom"))
    await DiscoveryLoop(rt).prune()  # no exception propagates


# -- resolution-based graph pruning -----------------------------------------

def test_resolution_time_from_kalshi_market():
    # finalized with the authoritative settlement_ts.
    assert _resolution_time(
        {"status": "finalized", "result": "yes", "settlement_ts": "2026-06-06T10:00:00Z"}
    ) is not None
    # determined but not yet finalized -> falls back to close_time.
    assert _resolution_time(
        {"status": "determined", "result": "yes", "close_time": "2026-06-06T09:00:00Z"}
    ) is not None
    # a non-empty result alone marks it resolved.
    assert _resolution_time({"status": "closed", "result": "no", "close_time": "2026-06-06T09:00:00Z"}) is not None
    # live / pending: active, or closed-for-trading but outcome not yet known.
    assert _resolution_time({"status": "active", "result": ""}) is None
    assert _resolution_time({"status": "closed", "result": ""}) is None


def test_mark_resolved_does_not_overwrite(tmp_db):
    now = naive_utc(now_utc())
    first = now - timedelta(hours=1)
    tmp_db.upsert_markets([_market("M", resolved_at=first, status="finalized")])
    tmp_db.mark_resolved([("M", now)])  # a later call must not clobber the first time
    with tmp_db._lock:
        got = tmp_db._con.execute("SELECT resolved_at FROM markets WHERE market_id='M'").fetchone()[0]
    assert got == first


def test_pending_resolution_is_ingraph_unsubscribed_and_unresolved(tmp_db):
    now = naive_utc(now_utc())
    tmp_db.upsert_markets([_market("A"), _market("B"), _market("C", resolved_at=now), _market("D")])
    tmp_db.set_active_set({"B"})  # B is live -> excluded
    for mid in ("A", "C"):
        tmp_db.upsert_market_semantics([_sem(mid)])
    tmp_db.upsert_edges([_edge("D", "Z")])  # D is a graph endpoint; Z has no markets row

    pending = set(tmp_db.graph_markets_pending_resolution(100))
    assert pending == {"A", "D"}  # B live, C already resolved, Z not a known market


def test_prune_resolved_graph_drops_resolved_keeps_recent_and_live(tmp_db):
    now = naive_utc(now_utc())
    tmp_db.upsert_markets([
        _market("R1", resolved_at=now - timedelta(hours=2)),   # resolved well past the 1h grace
        _market("R2", resolved_at=now - timedelta(minutes=10)),  # resolved, still within grace
        _market("L"),                                          # live (no resolved_at)
    ])
    for mid in ("R1", "R2", "L", "X"):
        tmp_db.upsert_market_semantics([_sem(mid)])
    tmp_db.upsert_edges([_edge("L", "R1"), _edge("L", "R2"), _edge("L", "X")])

    g = tmp_db.prune_resolved_graph(now - timedelta(seconds=C.GRAPH_PRUNE_AFTER_RESOLVED_SECONDS))

    assert g == {"market_semantics": 1, "market_edges": 1}  # only R1's
    with tmp_db._lock:
        sem = {r[0] for r in tmp_db._con.execute("SELECT market_id FROM market_semantics").fetchall()}
        edges = {tuple(r) for r in tmp_db._con.execute(
            "SELECT market_id_a, market_id_b FROM market_edges").fetchall()}
    assert sem == {"R2", "L", "X"}
    assert edges == {("L", "R2"), ("L", "X")}  # edge with the resolved R1 endpoint gone


async def test_discovery_reconciles_resolution_then_prunes_graph(tmp_db, make_fake_rest):
    """End-to-end of the new path: a graph market that left the active set is
    looked up on Kalshi, its resolution time persisted, and (being >1h old) its
    graph pruned — all in one discovery prune cycle."""
    now = naive_utc(now_utc())
    tmp_db.upsert_markets([_market("R")])
    tmp_db.set_active_set(set())  # R no longer subscribed
    tmp_db.upsert_market_semantics([_sem("R")])

    settled = now - timedelta(hours=2)
    rest = make_fake_rest(markets={
        "R": {"status": "finalized", "result": "yes", "settlement_ts": settled.isoformat()},
    })
    rt = SimpleNamespace(db=tmp_db, rest=rest, shutdown=asyncio.Event(), heartbeats=Heartbeats())

    await DiscoveryLoop(rt).prune()

    assert rest.market_calls == 1
    with tmp_db._lock:
        resolved_at = tmp_db._con.execute("SELECT resolved_at FROM markets WHERE market_id='R'").fetchone()[0]
        n_sem = tmp_db._con.execute("SELECT count(*) FROM market_semantics").fetchone()[0]
    assert resolved_at is not None          # learned from Kalshi
    assert n_sem == 0                        # pruned (resolved >1h ago)

