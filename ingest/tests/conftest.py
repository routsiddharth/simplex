"""Shared fixtures for the Simplex ingest test suite.

No test here touches Kalshi or the network: predicate/loop tests drive a
``fake_rest`` over in-memory event dicts, and DB tests use a throwaway DuckDB
file initialized from the real ``schema.sql``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from simplex_ingest.db import Database
from simplex_ingest.discovery_predicates import EventStats, SeriesStats
from simplex_ingest.llm import LLMError, MarketSemantics, PairClassification
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
    """Minimal stand-in for KalshiREST.get_events over in-memory events."""

    def __init__(self, events=None, raise_exc=None):
        self.events = list(events or [])
        self.raise_exc = raise_exc
        self.calls = 0

    async def get_events(self, status="open", series_ticker=None,
                         with_nested_markets=False, limit=200):
        self.calls += 1
        if self.raise_exc is not None:
            raise self.raise_exc
        if series_ticker:
            return [e for e in self.events if e.get("series_ticker") == series_ticker]
        return list(self.events)


@pytest.fixture
def make_fake_rest():
    def _make(events=None, raise_exc=None):
        return FakeREST(events=events, raise_exc=raise_exc)

    return _make


@pytest.fixture
def make_runtime():
    """A SimpleNamespace runtime carrying just what the loops under test touch."""

    def _make(db, rest=None, llm=None):
        return SimpleNamespace(
            db=db,
            rest=rest,
            llm=llm,
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

    async def extract_market(self, model, *, title, description, resolution_criteria):
        self.extract_calls.append(title)
        if title in self.raise_titles:
            raise LLMError("forced extract failure")
        return self.semantics.get(title, _DEFAULT_SEMANTICS)

    async def classify_pair(self, model, *, market_a, market_b):
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
