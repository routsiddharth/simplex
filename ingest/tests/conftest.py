"""Shared fixtures for the Simplex ingest test suite.

No test here touches Kalshi or the network: predicate/loop tests drive a
``fake_rest`` over in-memory event dicts, and DB tests use a throwaway DuckDB
file initialized from the real ``schema.sql``.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
import websockets
from cryptography.hazmat.primitives.asymmetric import rsa

from simplex_ingest.db import Database
from simplex_ingest.discovery_predicates import EventStats, SeriesStats
from simplex_ingest.kalshi.auth import KalshiSigner
from simplex_ingest.llm import BatchResult, LLMError, MarketSemantics, PairClassification
from simplex_ingest.runtime import Heartbeats


# -- DuckDB -----------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """A fresh on-disk DuckDB initialized with the production schema."""
    db = Database(tmp_path / "test.duckdb")
    try:
        yield db
    finally:
        db.close()


# -- raw Kalshi-shaped factories (for aggregate() / loop tests) -------------

@pytest.fixture
def make_market():
    def _make(ticker="M", status="active", volume=None, subtitle=None, **extra):
        m: dict = {"ticker": ticker, "status": status}
        if volume is not None:
            m["volume_fp"] = volume
        if subtitle is not None:
            m["yes_sub_title"] = subtitle
        m.update(extra)
        return m

    return _make


@pytest.fixture
def make_event():
    def _make(series="KXTEST", event_ticker=None, mutex=False, markets=None):
        return {
            "series_ticker": series,
            "event_ticker": event_ticker or f"{series}-E",
            "mutually_exclusive": mutex,
            "markets": list(markets or []),
        }

    return _make


@pytest.fixture
def make_admissible_event(make_market, make_event):
    """One mutex event with N distinct active markets summing to ``volume``.

    Passes P1 (mutex, >=3 markets), P2 (distinct subtitles) and — if
    ``volume >= 1000`` — P3. The default is a comfortably-admissible series."""

    def _make(series, volume=5000.0, n_markets=3, mutex=True):
        markets = [
            make_market(
                ticker=f"{series}-{i}",
                status="active",
                volume=volume / n_markets,
                subtitle=f"{series}-sub-{i}",
            )
            for i in range(n_markets)
        ]
        return make_event(series=series, mutex=mutex, markets=markets)

    return _make


# -- predicate-layer factories (build SeriesStats directly) -----------------

@pytest.fixture
def make_event_stats():
    def _make(mutex=False, n_tradeable=0, n_distinct=0):
        return EventStats(
            mutually_exclusive=mutex,
            n_tradeable_markets=n_tradeable,
            n_distinct_markets=n_distinct,
        )

    return _make


@pytest.fixture
def make_series_stats():
    def _make(ticker="KX", events=(), volume=0.0):
        return SeriesStats(ticker=ticker, events=tuple(events), volume_24h=volume)

    return _make


# -- fake REST + runtime (for the discovery loop) ---------------------------

class FakeREST:
    """Minimal stand-in for KalshiREST over in-memory events + orderbooks."""

    def __init__(self, events=None, raise_exc=None, orderbooks=None, markets=None):
        self.events = list(events or [])
        self.raise_exc = raise_exc
        # ticker -> raw orderbook_fp dict ({"yes": [[p, s], ...], "no": [...]})
        self.orderbooks = dict(orderbooks or {})
        # ticker -> Kalshi market dict (for get_market / resolution reconciliation)
        self.markets = dict(markets or {})
        self.calls = 0
        self.orderbook_calls = 0
        self.market_calls = 0

    async def get_events(self, status="open", series_ticker=None,
                         with_nested_markets=False, limit=200):
        self.calls += 1
        if self.raise_exc is not None:
            raise self.raise_exc
        if series_ticker:
            return [e for e in self.events if e.get("series_ticker") == series_ticker]
        return list(self.events)

    async def get_orderbook(self, ticker, depth):
        self.orderbook_calls += 1
        return self.orderbooks.get(ticker, {"yes": [], "no": []})

    async def get_market(self, ticker):
        self.market_calls += 1
        return self.markets.get(ticker)


@pytest.fixture
def make_fake_rest():
    def _make(events=None, raise_exc=None, orderbooks=None, markets=None):
        return FakeREST(events=events, raise_exc=raise_exc, orderbooks=orderbooks, markets=markets)

    return _make


# -- Kalshi WS message factories + a real local WS server -------------------
# These build the on-the-wire envelope KalshiSubscriber.parse() expects, so a
# test can drive the REAL WebSocketLoop through an actual socket.

def ws_snapshot(ticker, yes, no, seq, ts_ms=1_700_000_000_000):
    return {"type": "orderbook_snapshot", "sid": 1, "seq": seq,
            "msg": {"market_ticker": ticker, "yes_dollars_fp": yes,
                    "no_dollars_fp": no, "ts_ms": ts_ms}}


def ws_delta(ticker, side, price, delta, seq, ts_ms=1_700_000_001_000):
    return {"type": "orderbook_delta", "sid": 1, "seq": seq,
            "msg": {"market_ticker": ticker, "side": side, "price_dollars": price,
                    "delta_fp": delta, "ts_ms": ts_ms}}


def ws_trade(ticker, price, count, seq, ts_ms=1_700_000_002_000):
    return {"type": "trade", "sid": 1, "seq": seq,
            "msg": {"market_ticker": ticker, "yes_price_dollars": price,
                    "no_price_dollars": round(1 - price, 2), "count_fp": count,
                    "taker_side": "yes", "ts_ms": ts_ms}}


def ws_lifecycle(ticker, event_type="open", seq=1, ts=1_700_000_000):
    return {"type": "market_lifecycle_v2", "sid": 1, "seq": seq,
            "msg": {"market_ticker": ticker, "event_type": event_type, "ts": ts}}


class FakeKalshiWSServer:
    """A local websockets server speaking the Kalshi control envelope.

    On each ``subscribe`` it replies with a ``subscribed`` frame (assigning a
    fresh sid) and then pushes scripted market-data frames for the requested
    tickers/channels — a snapshot + one delta on the orderbook channel, one
    trade on the trade channel. ``unsubscribe``/``update_subscription`` get the
    matching control acks so the loop's sid bookkeeping stays consistent.
    """

    def __init__(self):
        self.received: list[dict] = []   # every command the loop sent us
        self._server = None
        self.port: int | None = None
        self._sid = 0

    async def start(self):
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    async def _handler(self, ws):
        async for raw in ws:
            try:
                cmd = json.loads(raw)
            except (ValueError, TypeError):
                continue
            self.received.append(cmd)
            await self._respond(ws, cmd)

    async def _respond(self, ws, cmd):
        c, mid = cmd.get("cmd"), cmd.get("id")
        if c == "subscribe":
            self._sid += 1
            await ws.send(json.dumps({"type": "subscribed", "id": mid, "msg": {"sid": self._sid}}))
            params = cmd.get("params") or {}
            channels = params.get("channels") or []
            tickers = params.get("market_tickers") or []
            if "orderbook_delta" in channels:
                for t in tickers:
                    await ws.send(json.dumps(ws_snapshot(t, [[0.40, 100], [0.38, 50]], [[0.55, 80]], seq=1)))
                    await ws.send(json.dumps(ws_delta(t, "yes", 0.41, 30, seq=2)))
            if "trade" in channels:
                for t in tickers:
                    await ws.send(json.dumps(ws_trade(t, 0.41, 5, seq=1)))
            if "market_lifecycle_v2" in channels:
                for t in tickers:
                    await ws.send(json.dumps(ws_lifecycle(t)))
        elif c == "unsubscribe":
            await ws.send(json.dumps({"type": "unsubscribed", "id": mid, "msg": {}}))
        elif c == "update_subscription":
            await ws.send(json.dumps({"type": "ok", "id": mid, "msg": {}}))

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


@pytest.fixture
async def ws_server():
    """A started FakeKalshiWSServer, torn down after the test."""
    server = await FakeKalshiWSServer().start()
    try:
        yield server
    finally:
        await server.stop()


# -- RSA signer (for auth / rest / ws connect-header tests) -----------------

@pytest.fixture
def rsa_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def make_signer(rsa_key):
    def _make(key_id="test-key-id"):
        return KalshiSigner(key_id, rsa_key)

    return _make


@pytest.fixture
def make_runtime():
    """A SimpleNamespace runtime carrying just what the loops under test touch."""

    def _make(db, rest=None, llm=None, batch=None):
        return SimpleNamespace(
            db=db,
            rest=rest,
            llm=llm,
            batch=batch,
            shutdown=asyncio.Event(),
            heartbeats=Heartbeats(),
        )

    return _make


# -- fake LLM client (for the extraction loop) ------------------------------

_DEFAULT_SEMANTICS = MarketSemantics(
    underlying_event="event",
    resolves_yes_when="yes",
    resolves_no_when="no",
    resolution_timing="someday",
    entities=(),
    dependencies=(),
)
_DEFAULT_CLASSIFICATION = PairClassification(
    relationship_type="unrelated", direction="none", confidence=0.1, rationale="default"
)


class FakeLLMClient:
    """In-memory stand-in for OpenRouterClient — no network.

    ``semantics`` maps a market *title* -> MarketSemantics (extract_market only
    gets the text fields, so tests key on title). ``classifications`` maps a
    canonical ``(a, b)`` market-id pair -> either one PairClassification (used for
    every model) or a ``{model: PairClassification}`` dict (to drive the
    independent-verify agreement gate). ``raise_titles`` / ``raise_pairs`` force
    an LLMError for the log-and-skip paths.
    """

    def __init__(self, semantics=None, classifications=None,
                 raise_titles=None, raise_pairs=None):
        self.semantics = semantics or {}
        self.classifications = classifications or {}
        self.raise_titles = set(raise_titles or ())
        self.raise_pairs = set(raise_pairs or ())
        self.extract_calls: list[str] = []
        self.classify_calls: list[tuple] = []

    async def extract_market(self, model, *, title, description, resolution_criteria,
                             purpose="stage_a"):
        self.extract_calls.append(title)
        if title in self.raise_titles:
            raise LLMError("forced extract failure")
        return self.semantics.get(title, _DEFAULT_SEMANTICS)

    async def classify_pair(self, model, *, market_a, market_b, purpose="pair_primary"):
        a, b = market_a["market_id"], market_b["market_id"]
        self.classify_calls.append((a, b, model))
        if (a, b) in self.raise_pairs:
            raise LLMError("forced classify failure")
        spec = self.classifications.get((a, b), _DEFAULT_CLASSIFICATION)
        if isinstance(spec, dict):
            return spec[model]
        return spec

    async def aclose(self):
        pass


@pytest.fixture
def make_fake_llm():
    def _make(semantics=None, classifications=None, raise_titles=None, raise_pairs=None):
        return FakeLLMClient(
            semantics=semantics, classifications=classifications,
            raise_titles=raise_titles, raise_pairs=raise_pairs,
        )

    return _make


# -- fake Anthropic batch client (for the extraction loop's async path) ------

class FakeBatchClient:
    """In-memory stand-in for AnthropicBatchClient — no network.

    ``submit`` records the requests and hands back a deterministic batch id; a
    submitted batch reports ``ended`` by default. A test inspects the submission
    (and the DB-persisted payload) to learn the custom_id → work mapping, then
    feeds results back with :meth:`set_results` before driving reconciliation.
    """

    def __init__(self):
        self.submitted: list[dict] = []        # [{batch_id, requests}]
        self._status: dict[str, str] = {}       # batch_id -> processing_status
        self._results: dict[str, list] = {}     # batch_id -> [BatchResult]
        self._counter = 0
        self.fail_submit = False

    async def submit(self, requests):
        if self.fail_submit:
            raise LLMError("forced submit failure")
        self._counter += 1
        bid = f"batch-{self._counter}"
        self.submitted.append({"batch_id": bid, "requests": list(requests)})
        return bid

    async def poll(self, batch_id):
        return {"processing_status": self._status.get(batch_id, "ended")}

    async def results(self, batch_id):
        return self._results.get(batch_id, [])

    def set_status(self, batch_id, status):
        self._status[batch_id] = status

    def set_results(self, batch_id, results):
        self._results[batch_id] = list(results)

    async def aclose(self):
        pass


@pytest.fixture
def make_fake_batch():
    def _make():
        return FakeBatchClient()

    return _make


@pytest.fixture
def make_batch_result():
    """Build a BatchResult: succeeded (content set) or failed (error set)."""

    def _make(custom_id, *, content=None, usage=None, error=None):
        return BatchResult(custom_id=custom_id, content=content, usage=usage, error=error)

    return _make


# -- live-deployment tests (opt-in) -----------------------------------------
# Tests marked `live` hit the real Railway deployment (network required). They
# are skipped by default so the suite stays hermetic; enable with --run-live.

LIVE_BASE_URL = "https://simplex-production-f4ee.up.railway.app"


def pytest_addoption(parser):
    parser.addoption(
        "--run-live", action="store_true", default=False,
        help="run @pytest.mark.live tests against the live Railway deployment",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "live: hits the live Railway deployment (needs network; opt-in via --run-live)",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-live"):
        return
    skip_live = pytest.mark.skip(reason="needs --run-live (hits live Railway)")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)
