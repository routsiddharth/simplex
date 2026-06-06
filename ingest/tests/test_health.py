"""/health server: 200 when every loop has a fresh heartbeat, 503 when any is
stale, and the same health document on any path. Drives the real asyncio TCP
server on an ephemeral port over real HTTP.
"""

from __future__ import annotations

import httpx

from simplex_ingest.health import LOOP_NAMES, start_health_server
from simplex_ingest.runtime import Heartbeats


async def _get(server, path="/health"):
    port = server.sockets[0].getsockname()[1]
    async with httpx.AsyncClient() as client:
        return await client.get(f"http://127.0.0.1:{port}{path}")


async def test_all_loops_alive_returns_200():
    hb = Heartbeats()
    for name in LOOP_NAMES:
        hb.beat(name)
    server = await start_health_server(hb, port=0)
    try:
        resp = await _get(server)
        assert resp.status_code == 200
        body = resp.json()
        assert body["healthy"] is True
        assert set(body["loops"]) == set(LOOP_NAMES)
        assert all(body["loops"].values())
    finally:
        server.close()
        await server.wait_closed()


async def test_one_stale_loop_returns_503():
    hb = Heartbeats()
    for name in LOOP_NAMES[:-1]:
        hb.beat(name)  # last loop never beats -> stale
    server = await start_health_server(hb, port=0)
    try:
        resp = await _get(server)
        assert resp.status_code == 503
        body = resp.json()
        assert body["healthy"] is False
        assert body["loops"][LOOP_NAMES[-1]] is False
    finally:
        server.close()
        await server.wait_closed()


async def test_any_path_returns_the_health_document():
    hb = Heartbeats()
    for name in LOOP_NAMES:
        hb.beat(name)
    server = await start_health_server(hb, port=0)
    try:
        resp = await _get(server, path="/anything")
        assert resp.status_code == 200
        assert "loops" in resp.json()
    finally:
        server.close()
        await server.wait_closed()
