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
    """A SimpleNamespace runtime carrying just what the discovery loop touches."""

    def _make(db, rest):
        return SimpleNamespace(
            db=db,
            rest=rest,
            shutdown=asyncio.Event(),
            heartbeats=Heartbeats(),
        )

    return _make
