"""Discovery loop — the self-managing tracked set.

Fifth supervised loop. Every DISCOVERY_INTERVAL_SECONDS it sweeps all open
Kalshi events once (``rest.get_events`` with nested markets), aggregates them by
series, admits each series that passes the structural + tradeability predicates
(see :mod:`..discovery_predicates`), ranks the admitted set, caps it at
MAX_TRACKED_SERIES, and rewrites the ``tracked_series`` table atomically.

The catalog poller reads ``tracked_series`` on its next tick (within one
CATALOG_REFRESH_SECONDS), so discovery never has to wake the catalog or the WS
loop directly — the existing resubscribe/reconcile path carries the delta.

On startup ``discover()`` runs eagerly (before the first sleep) so a cold boot
has a populated tracked set before catalog's first tick. A transient empty sweep
or a REST error leaves the prior table contents intact — we never wipe the
working set on a blip.
"""

from __future__ import annotations

import asyncio

from .. import constants as C
from ..discovery_predicates import aggregate, evaluate, rank_key
from ..log import get_logger
from ..util import idle_sleep, naive_utc, now_utc

log = get_logger("discovery")


class DiscoveryLoop:
    name = "discovery"

    def __init__(self, rt) -> None:
        self.rt = rt

    async def run(self) -> None:
        # Eager first cycle: populate tracked_series before catalog's first tick.
        try:
            await self.discover()
        except Exception:
            log.exception("initial discovery failed")
        self.rt.heartbeats.beat(self.name)

        while not self.rt.shutdown.is_set():
            await idle_sleep(self.rt.shutdown, self.rt.heartbeats, self.name, C.DISCOVERY_INTERVAL_SECONDS)
            if self.rt.shutdown.is_set():
                break
            try:
                await self.discover()
            except Exception:
                log.exception("discovery cycle failed")
            self.rt.heartbeats.beat(self.name)

    async def discover(self) -> None:
        events = await self.rt.rest.get_events(status="open", with_nested_markets=True)
        if not events:
            # Don't wipe the working set on a transient empty sweep.
            log.warning("discovery sweep returned no events; keeping current tracked set")
            return

        stats = aggregate(events)
        admitted = [s for s in stats.values() if evaluate(s).admit]
        admitted.sort(key=rank_key, reverse=True)
        top = admitted[: C.MAX_TRACKED_SERIES]

        now = naive_utc(now_utc())
        rows = []
        for position, s in enumerate(top, start=1):
            v = evaluate(s)
            rows.append(
                (
                    s.ticker,
                    now,  # admitted_at (used only for rows new to the table)
                    now,  # last_check_at
                    v.passes["P1"],
                    v.passes["P2"],
                    v.passes["P3"],
                    v.n_partition_events,
                    v.n_hierarchy_events,
                    s.volume_24h,
                    position,
                )
            )

        if not rows:
            # Sweep had events but nothing passed the predicates. Surface it loudly
            # rather than silently wiping a previously-good tracked set.
            log.warning(
                "discovery admitted zero series; keeping current tracked set",
                extra={"series_seen": len(stats)},
            )
            return

        await asyncio.to_thread(self.rt.db.replace_tracked_series, rows)
        log.info(
            "discovery cycle complete",
            extra={"series_seen": len(stats), "admitted": len(admitted), "tracked": len(top)},
        )


def discover_once() -> None:
    """One-shot live discovery against real Kalshi, for manual pre-deploy sanity.

    Builds a throwaway REST client from the local ``.env``, runs a single
    aggregate/evaluate/rank pass, and prints the admitted series (with predicate
    flags) plus the highest-ranked rejects. Does not touch DuckDB. Run with::

        python -m simplex_ingest.loops.discovery
    """
    import asyncio as _asyncio

    from ..config import get_settings
    from ..discovery_predicates import SeriesStats
    from ..kalshi.auth import KalshiSigner
    from ..kalshi.rest import KalshiREST

    async def _main() -> None:
        settings = get_settings()
        signer = KalshiSigner(settings.kalshi_api_key_id, settings.load_private_key())
        rest = KalshiREST(settings.rest_base_url, signer)
        try:
            print("Fetching open events (with nested markets)...", flush=True)
            events = await rest.get_events(status="open", with_nested_markets=True)
            print(f"  {len(events)} open events", flush=True)
        finally:
            await rest.aclose()

        stats = aggregate(events)
        scored = sorted(stats.values(), key=rank_key, reverse=True)
        admitted = [s for s in scored if evaluate(s).admit]

        def _line(s: SeriesStats) -> str:
            v = evaluate(s)
            flags = "".join(name[-1] if ok else "-" for name, ok in v.passes.items())
            return (
                f"  {s.ticker:<24} P[{flags}]  "
                f"part={v.n_partition_events} hier={v.n_hierarchy_events} "
                f"vol={s.volume_24h:,.0f}"
            )

        print(f"\n=== Admitted ({len(admitted)}/{len(scored)}), top {C.MAX_TRACKED_SERIES} tracked ===")
        for i, s in enumerate(admitted[: C.MAX_TRACKED_SERIES], 1):
            print(f"{i:>3} {_line(s)}")
        if len(admitted) > C.MAX_TRACKED_SERIES:
            print(f"  ... {len(admitted) - C.MAX_TRACKED_SERIES} more admitted but over cap")

        rejected = [s for s in scored if not evaluate(s).admit]
        print(f"\n=== Top rejects ({len(rejected)} total) ===")
        for s in rejected[:15]:
            print(_line(s))

    _asyncio.run(_main())


if __name__ == "__main__":
    discover_once()
