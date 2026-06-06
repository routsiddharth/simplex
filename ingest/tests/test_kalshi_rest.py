"""Async Kalshi REST client: cursor pagination, retry on 429/5xx/transport
error, and that requests are signed. Uses an httpx MockTransport injected into
the client (no network); backoff sleeps are patched to no-ops so retries are
instant.
"""

from __future__ import annotations

import httpx
import pytest

from simplex_ingest.kalshi.rest import KalshiREST
from simplex_ingest.util import Backoff


@pytest.fixture(autouse=True)
def _fast_backoff(mocker):
    async def _noop(self):
        return 0.0

    mocker.patch.object(Backoff, "sleep", _noop)


def _rest(make_signer, handler):
    rest = KalshiREST("https://api.example.com/trade-api/v2", make_signer())
    rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return rest


async def test_get_events_follows_cursor_to_the_end(make_signer):
    pages = [
        {"events": [{"event_ticker": "E1"}], "cursor": "c1"},
        {"events": [{"event_ticker": "E2"}], "cursor": None},
    ]
    cursors_seen = []

    def handler(req):
        cursors_seen.append(req.url.params.get("cursor"))
        return httpx.Response(200, json=pages[len(cursors_seen) - 1])

    rest = _rest(make_signer, handler)
    try:
        out = await rest.get_events()
        assert [e["event_ticker"] for e in out] == ["E1", "E2"]
        assert cursors_seen == [None, "c1"]  # first page no cursor, then follow c1
    finally:
        await rest.aclose()


async def test_retries_on_429_then_succeeds(make_signer):
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={})
        return httpx.Response(200, json={"series": [{"ticker": "KX"}]})

    rest = _rest(make_signer, handler)
    try:
        out = await rest.get_series_list()
        assert calls["n"] == 2
        assert out == [{"ticker": "KX"}]
    finally:
        await rest.aclose()


async def test_signs_every_request(make_signer):
    captured = {}

    def handler(req):
        captured["headers"] = req.headers
        captured["query"] = req.url.query.decode()
        return httpx.Response(200, json={"orderbook_fp": {"yes": [], "no": []}})

    rest = _rest(make_signer, handler)
    try:
        ob = await rest.get_orderbook("KXM", depth=10)
        assert "kalshi-access-signature" in {k.lower() for k in captured["headers"].keys()}
        assert "depth=10" in captured["query"]  # query travels on the wire
        assert ob == {"yes": [], "no": []}
    finally:
        await rest.aclose()


async def test_raises_after_exhausting_retries_on_5xx(make_signer):
    def handler(req):
        return httpx.Response(503, json={})

    rest = _rest(make_signer, handler)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await rest.get_series_list()
    finally:
        await rest.aclose()


async def test_transport_error_retries_then_raises(make_signer):
    def handler(req):
        raise httpx.ConnectError("boom")

    rest = _rest(make_signer, handler)
    try:
        with pytest.raises(httpx.TransportError):
            await rest.get_series_list()
    finally:
        await rest.aclose()


async def test_get_series_returns_none_on_404(make_signer):
    def handler(req):
        return httpx.Response(404, json={"error": "missing"})

    rest = _rest(make_signer, handler)
    try:
        assert await rest.get_series("NOPE") is None
    finally:
        await rest.aclose()
