"""Minimal /health endpoint.

200 if every loop has heartbeat within HEALTH_HEARTBEAT_TIMEOUT_SECONDS, else
503. Dependency-free asyncio TCP server (no web framework). Any path returns the
same health document; the body lists per-loop liveness.
"""

from __future__ import annotations

import asyncio
import json

from . import constants as C
from .log import get_logger
from .runtime import Heartbeats

log = get_logger("health")

LOOP_NAMES = ["catalog", "websocket", "snapshot", "audit"]


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, heartbeats: Heartbeats) -> None:
    try:
        await reader.readline()  # request line; we don't route on it
        # drain headers
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
        statuses = heartbeats.status(LOOP_NAMES, C.HEALTH_HEARTBEAT_TIMEOUT_SECONDS)
        healthy = all(statuses.values())
        body = json.dumps({"healthy": healthy, "loops": statuses}).encode()
        code = "200 OK" if healthy else "503 Service Unavailable"
        writer.write(
            b"HTTP/1.1 " + code.encode() + b"\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def start_health_server(heartbeats: Heartbeats, port: int = C.HEALTH_PORT) -> asyncio.AbstractServer:
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, heartbeats), host="0.0.0.0", port=port
    )
    log.info("health server listening", extra={"port": port})
    return server
