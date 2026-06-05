"""Process entry point: wire the four loops under one supervisor.

Owns the shared DuckDB connection, the Kalshi clients, the shared runtime state,
the /health server, and clean SIGTERM shutdown (cancel loops -> flush DB ->
checkpoint books -> close DB -> exit 0).
"""

from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass
from typing import Any

from . import constants as C
from .config import Settings, get_settings
from .db import Database
from .health import start_health_server
from .kalshi.auth import KalshiSigner
from .kalshi.rest import KalshiREST
from .kalshi.subscriber import KalshiSubscriber
from .log import configure_logging, get_logger
from .loops.audit import BookAuditLoop
from .loops.catalog import CatalogPoller
from .loops.discovery import DiscoveryLoop
from .loops.snapshots import SnapshotBuilder
from .loops.websocket import WebSocketLoop
from .runtime import BookStore, Heartbeats
from .supervisor import run_supervised
from .util import TokenBucket

log = get_logger("app")


@dataclass
class Runtime:
    settings: Settings
    db: Database
    signer: KalshiSigner
    rest: KalshiREST
    audit_rest: KalshiREST
    subscriber: KalshiSubscriber
    book_store: BookStore
    reset_requests: "asyncio.Queue[str]"
    resubscribe_event: asyncio.Event
    heartbeats: Heartbeats
    shutdown: asyncio.Event


def build_runtime(settings: Settings) -> Runtime:
    settings.ensure_dirs()
    db = Database(settings.db_path)
    signer = KalshiSigner(settings.kalshi_api_key_id, settings.load_private_key())
    rest = KalshiREST(settings.rest_base_url, signer,
                      TokenBucket(C.REST_CALLS_PER_SECOND, C.REST_BURST))
    audit_rest = KalshiREST(settings.rest_base_url, signer,
                            TokenBucket(C.AUDIT_REST_CALLS_PER_SECOND, C.AUDIT_REST_BURST))
    subscriber = KalshiSubscriber(signer, settings.ws_url)
    return Runtime(
        settings=settings,
        db=db,
        signer=signer,
        rest=rest,
        audit_rest=audit_rest,
        subscriber=subscriber,
        book_store=BookStore(),
        reset_requests=asyncio.Queue(),
        resubscribe_event=asyncio.Event(),
        heartbeats=Heartbeats(),
        shutdown=asyncio.Event(),
    )


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown: asyncio.Event) -> None:
    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("shutdown signal received", extra={"signal": sig.name})
        shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig)
        except NotImplementedError:  # e.g. non-main thread / unsupported platform
            signal.signal(sig, lambda *_: shutdown.set())


async def _warn_if_discovery_stalled(rt: Runtime) -> None:
    """After a startup grace, warn once if discovery still hasn't populated.

    Self-terminates on shutdown. An empty tracked set past the grace means the
    discovery loop is failing (REST down / bad creds), not a slow boot — the WS
    set stays idle but the process keeps running."""
    try:
        await asyncio.wait_for(rt.shutdown.wait(), timeout=C.DISCOVERY_STARTUP_GRACE_SECONDS)
        return  # shutdown arrived first
    except asyncio.TimeoutError:
        pass
    tracked = await asyncio.to_thread(rt.db.get_tracked_series)
    if not tracked:
        log.warning(
            "tracked_series still empty after startup grace; WS idle until discovery populates",
            extra={"grace_s": C.DISCOVERY_STARTUP_GRACE_SECONDS},
        )


async def run() -> int:
    settings = get_settings()
    configure_logging(C.LOG_LEVEL)
    log.info("starting simplex ingest", extra={"env": settings.env, "db": str(settings.db_path)})

    rt = build_runtime(settings)
    builder = SnapshotBuilder(rt)
    loops: list[Any] = [
        CatalogPoller(rt),
        WebSocketLoop(rt),
        builder,
        BookAuditLoop(rt),
        DiscoveryLoop(rt),
    ]

    _install_signal_handlers(asyncio.get_running_loop(), rt.shutdown)
    health_server = await start_health_server(rt.heartbeats)

    supervisor_task = asyncio.create_task(run_supervised(loops, rt.shutdown), name="supervisor")
    grace_task = asyncio.create_task(_warn_if_discovery_stalled(rt), name="discovery-grace")

    await rt.shutdown.wait()
    log.info("shutting down")

    # 1) cancel loops
    supervisor_task.cancel()
    grace_task.cancel()
    await asyncio.gather(supervisor_task, grace_task, return_exceptions=True)

    # 2) flush pending writes, 3) checkpoint books, 4) close DB
    try:
        await asyncio.to_thread(rt.db.flush_raw_events)
        await builder.checkpoint()
    except Exception:
        log.exception("error during shutdown flush/checkpoint")
    await rt.rest.aclose()
    await rt.audit_rest.aclose()
    rt.db.close()

    health_server.close()
    await health_server.wait_closed()

    log.info("shutdown complete")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
