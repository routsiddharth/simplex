"""End-to-end pipeline test.

Wires the REAL loops together and walks one item all the way through the system,
the way the live process does — only the two external boundaries are simulated:
Kalshi REST (an in-memory FakeREST) and OpenRouter (a FakeLLM). The WebSocket
boundary is a REAL socket: the production WebSocketLoop connects to a local
Kalshi-shaped server and streams frames in.

    discovery (predicates)        -> tracked_series
        -> catalog (REST expand)  -> markets + active subscription set
        -> websocket (real socket)-> raw_events            [source of truth]
        -> snapshot (reconstruct) -> snapshots             [marginal grid]
        -> extraction (LLM)       -> market_semantics + market_edges  [graph]

Each stage asserts the artifact the next stage consumes, so a break anywhere in
the chain localizes to a stage.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from simplex_ingest import constants as C
from simplex_ingest.kalshi.subscriber import KalshiSubscriber
from simplex_ingest.llm import PairClassification
from simplex_ingest.loops.catalog import CatalogPoller
from simplex_ingest.loops.discovery import DiscoveryLoop
from simplex_ingest.loops.extraction import ExtractionLoop
from simplex_ingest.loops.snapshots import SnapshotBuilder
from simplex_ingest.loops.websocket import WebSocketLoop
from simplex_ingest.runtime import BookStore, Heartbeats
from simplex_ingest.util import now_utc

SERIES = "KXE2E"
MARKETS = [f"{SERIES}-{i}" for i in range(3)]   # KXE2E-0, KXE2E-1, KXE2E-2


def _runtime(db, rest, subscriber, llm):
    return SimpleNamespace(
        db=db,
        rest=rest,
        subscriber=subscriber,
        llm=llm,
        book_store=BookStore(),
        reset_requests=asyncio.Queue(maxsize=256),
        resubscribe_event=asyncio.Event(),
        heartbeats=Heartbeats(),
        shutdown=asyncio.Event(),
    )


async def _raw_count(db) -> int:
    await asyncio.to_thread(db.flush_raw_events)
    with db._lock:
        return db._con.execute("SELECT count(*) FROM raw_events").fetchone()[0]


async def _wait_raw(db, n, timeout=5.0):
    waited = 0.0
    while waited < timeout:
        if await _raw_count(db) >= n:
            return True
        await asyncio.sleep(0.05)
        waited += 0.05
    return False


async def test_full_pipeline_discovery_to_edges(
    tmp_db, make_fake_rest, make_admissible_event, make_signer, make_fake_llm, ws_server
):
    # One admissible series (mutex event, 3 active markets, volume 5000): passes
    # discovery's P1+P3. The same FakeREST answers discovery's full sweep and
    # catalog's per-series expansion.
    event = make_admissible_event(SERIES, volume=5000.0, n_markets=3, mutex=True)
    rest = make_fake_rest(events=[event])
    subscriber = KalshiSubscriber(make_signer(), "")  # ws_url filled in once server is up

    # A trusted edge for one canonical pair (both models agree, high confidence);
    # the other pairs fall to the default (low-confidence -> review).
    llm = make_fake_llm(classifications={
        (MARKETS[0], MARKETS[1]): PairClassification(
            relationship_type="mutually_exclusive", direction="none",
            confidence=0.95, rationale="exhaustive partition",
        ),
    })
    rt = _runtime(tmp_db, rest, subscriber, llm)

    # -- Stage 1: discovery -> tracked_series -------------------------------
    await DiscoveryLoop(rt).discover()
    assert tmp_db.get_tracked_series() == [SERIES]

    # -- Stage 2: catalog -> markets + active set ---------------------------
    await CatalogPoller(rt).refresh()
    assert tmp_db.get_active_market_ids() == set(MARKETS)
    assert rt.resubscribe_event.is_set()

    # -- Stage 3: websocket (real socket) -> raw_events ---------------------
    subscriber._ws_url = ws_server.url  # point the real subscriber at the local server

    # Boot the snapshot builder's cursor BEFORE live frames arrive (mirrors a real
    # cold start: replay forward from the tail, not back through history).
    builder = SnapshotBuilder(rt)
    await builder.tick(window_end=now_utc())
    assert builder._cursor_ready

    ws_task = asyncio.create_task(WebSocketLoop(rt).run())
    try:
        # 3 markets x (snapshot + delta + trade) = 9 frames -> raw_events rows.
        assert await _wait_raw(tmp_db, 9), "WS loop did not stream events into raw_events"
    finally:
        rt.shutdown.set()
        ws_task.cancel()
        await asyncio.gather(ws_task, return_exceptions=True)

    with tmp_db._lock:
        by_type = dict(tmp_db._con.execute(
            "SELECT event_type, count(*) FROM raw_events GROUP BY event_type"
        ).fetchall())
    assert by_type.get("orderbook_snapshot", 0) >= 3
    assert by_type.get("orderbook_delta", 0) >= 3
    assert by_type.get("trade", 0) >= 3

    # -- Stage 4: snapshot -> reconstructed book in the grid ----------------
    await builder.tick(window_end=now_utc())  # drains the live frames forward
    with tmp_db._lock:
        # Latest snapshot row per market; the delta lifted yes_bid to 0.41 and
        # yes_ask mirrors the no side (1 - 0.55 = 0.45).
        snap = tmp_db._con.execute(
            "SELECT yes_bid, yes_ask FROM snapshots WHERE market_id = ? "
            "ORDER BY ts DESC LIMIT 1", [MARKETS[0]],
        ).fetchone()
    assert snap is not None
    assert snap[0] == 0.41
    assert snap[1] == round(1 - 0.55, 6)

    # -- Stage 5: extraction -> semantics + typed edges ---------------------
    await ExtractionLoop(rt).extract_cycle()

    with tmp_db._lock:
        n_sem = tmp_db._con.execute("SELECT count(*) FROM market_semantics").fetchone()[0]
    assert n_sem == 3  # every active market got a semantic record

    edges = tmp_db.get_edges_for_pairs([
        (MARKETS[0], MARKETS[1]), (MARKETS[0], MARKETS[2]), (MARKETS[1], MARKETS[2]),
    ])
    assert len(edges) == 3  # every candidate pair classified
    trusted = edges[(MARKETS[0], MARKETS[1])]
    assert trusted["relationship_type"] == "mutually_exclusive"
    assert trusted["trust_tier"] == "trusted"
    assert trusted["agreement_status"] == "agreed"
    assert trusted["verify_model"] == C.PAIR_VERIFY_MODEL


async def test_pipeline_runs_without_llm_key(
    tmp_db, make_fake_rest, make_admissible_event, make_signer, ws_server
):
    """With no OPENROUTER key (rt.llm is None) the ingest half still flows
    end-to-end; only the extraction stage is a no-op (Stage-3 soft-fail)."""
    event = make_admissible_event(SERIES, volume=5000.0, n_markets=3, mutex=True)
    rest = make_fake_rest(events=[event])
    subscriber = KalshiSubscriber(make_signer(), "")
    rt = _runtime(tmp_db, rest, subscriber, llm=None)

    await DiscoveryLoop(rt).discover()
    await CatalogPoller(rt).refresh()
    assert tmp_db.get_active_market_ids() == set(MARKETS)

    subscriber._ws_url = ws_server.url
    builder = SnapshotBuilder(rt)
    await builder.tick(window_end=now_utc())
    ws_task = asyncio.create_task(WebSocketLoop(rt).run())
    try:
        assert await _wait_raw(tmp_db, 9)
    finally:
        rt.shutdown.set()
        ws_task.cancel()
        await asyncio.gather(ws_task, return_exceptions=True)

    await builder.tick(window_end=now_utc())

    # Extraction is a clean no-op without a key — no rows, no exception.
    await ExtractionLoop(rt).extract_cycle()
    with tmp_db._lock:
        n_sem = tmp_db._con.execute("SELECT count(*) FROM market_semantics").fetchone()[0]
        n_edges = tmp_db._con.execute("SELECT count(*) FROM market_edges").fetchone()[0]
    assert n_sem == 0 and n_edges == 0
