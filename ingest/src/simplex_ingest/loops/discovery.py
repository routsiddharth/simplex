"""Discovery loop — the self-managing tracked set.

Fifth supervised loop. Every DISCOVERY_INTERVAL_SECONDS it sweeps all open
Kalshi events once (``rest.get_events`` with nested markets), aggregates them by
series, admits each series that passes the structural + tradeability predicates
(see :mod:`..discovery_predicates`), ranks the admitted set, caps it at
MAX_TRACKED_SERIES, and rewrites the ``tracked_series`` table atomically.

**Scoped override:** when ``DISCOVERY_SERIES_ALLOWLIST_PREFIXES`` is non-empty
(currently ``("KXWC",)`` — World Cup 2026 only, an initial smoke-test scope-down),
admission is by ticker-prefix match alone and the predicate/rank/cap path is
bypassed entirely. Set the constant to ``()`` to restore predicate-driven
discovery.

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
from datetime import datetime, timedelta

from .. import constants as C
from ..discovery_predicates import aggregate, evaluate, rank_key
from ..log import get_logger
from ..util import idle_sleep, naive_utc, now_utc, parse_dt

log = get_logger("discovery")

# Kalshi market statuses past the live-trading phase (outcome known / settling /
# settled). A non-empty `result` is the other resolution signal. See the Kalshi
# market lifecycle: active -> closed -> determined -> finalized.
_TERMINAL_STATUSES = {"determined", "disputed", "amended", "finalized", "settled"}


def _resolution_time(market: dict) -> datetime | None:
    """Authoritative resolution time from a Kalshi market dict, or None if it is
    not resolved yet. Resolved = a non-empty ``result`` or a terminal ``status``;
    the timestamp is ``settlement_ts`` (set at finalized), falling back to
    ``close_time`` when determined-but-not-yet-finalized, then to now."""
    result = (market.get("result") or "").strip()
    status = (market.get("status") or "").strip().lower()
    if not result and status not in _TERMINAL_STATUSES:
        return None
    return parse_dt(market.get("settlement_ts")) or parse_dt(market.get("close_time")) or now_utc()


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
        await self.prune()
        self.rt.heartbeats.beat(self.name)

        while not self.rt.shutdown.is_set():
            await idle_sleep(self.rt.shutdown, self.rt.heartbeats, self.name, C.DISCOVERY_INTERVAL_SECONDS)
            if self.rt.shutdown.is_set():
                break
            try:
                await self.discover()
            except Exception:
                log.exception("discovery cycle failed")
            # Retention is anchored to the discovery cycle (the hourly market-set
            # recompute), so prune here regardless of whether the sweep admitted
            # anything — a failed/empty sweep must not let the volume grow.
            await self.prune()
            self.rt.heartbeats.beat(self.name)

    async def discover(self) -> None:
        events = await self.rt.rest.get_events(status="open", with_nested_markets=True)
        if not events:
            # Don't wipe the working set on a transient empty sweep.
            log.warning("discovery sweep returned no events; keeping current tracked set")
            return

        stats = aggregate(events)
        prefixes = C.DISCOVERY_SERIES_ALLOWLIST_PREFIXES
        if prefixes:
            # Scoped smoke-test mode (e.g. World Cup 2026 only): admit EXACTLY the
            # series matching an allowlist prefix and bypass the predicates, the
            # ranking, and the MAX_TRACKED_SERIES cap. The Verdict is still computed
            # for the tracked_series row data (P-flags / counts), but it does not
            # gate admission here. Sort by rank_key only to give `position` a
            # stable, sensible value.
            admitted = [(s, evaluate(s)) for s in stats.values()
                        if s.ticker.startswith(prefixes)]
            admitted.sort(key=lambda sv: rank_key(sv[0]), reverse=True)
            top = admitted
        else:
            # Default path: evaluate each series once; carry the Verdict alongside
            # the stats so the row build below reuses it. Admit on the predicates,
            # rank, and cap at MAX_TRACKED_SERIES.
            admitted = [(s, v) for s in stats.values() if (v := evaluate(s)).admit]
            admitted.sort(key=lambda sv: rank_key(sv[0]), reverse=True)
            top = admitted[: C.MAX_TRACKED_SERIES]

        now = naive_utc(now_utc())
        rows = []
        for position, (s, v) in enumerate(top, start=1):
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

    async def prune(self) -> None:
        """Enforce retention each cycle (in sync with the hourly recompute):

        1. trim the time-series tables to the rolling window (DATA_RETENTION_*),
        2. reconcile resolution for graph markets against Kalshi (source of truth),
        3. delete the LLM graph for markets resolved > GRAPH_PRUNE_AFTER_RESOLVED_*.

        Each step is independently guarded so one failure can't skip the others."""
        # (1) time-series rolling window
        ts_cutoff = naive_utc(now_utc()) - timedelta(seconds=C.DATA_RETENTION_SECONDS)
        try:
            deleted = await asyncio.to_thread(self.rt.db.prune_time_series, ts_cutoff)
            if any(deleted.values()):
                log.info("retention prune complete", extra={"cutoff": ts_cutoff.isoformat(), **deleted})
        except Exception:
            log.exception("retention prune failed")

        # (2) learn resolution times from Kalshi
        try:
            await self.reconcile_resolutions()
        except Exception:
            log.exception("resolution reconciliation failed")

        # (3) drop the graph for markets resolved long enough ago
        g_cutoff = naive_utc(now_utc()) - timedelta(seconds=C.GRAPH_PRUNE_AFTER_RESOLVED_SECONDS)
        try:
            g = await asyncio.to_thread(self.rt.db.prune_resolved_graph, g_cutoff)
            if any(g.values()):
                log.info("resolved-graph prune complete", extra={"cutoff": g_cutoff.isoformat(), **g})
        except Exception:
            log.exception("resolved-graph prune failed")

    async def reconcile_resolutions(self) -> None:
        """Persist `resolved_at` for graph markets that left the active set, using
        Kalshi REST as the source of truth.

        A settled market's event is no longer open, so it won't appear in the
        catalog's open-events sweep — we look it up directly. Only markets in the
        graph with no known resolution time are checked (bounded by
        GRAPH_RESOLUTION_CHECK_BATCH); the result anchors the resolved-graph
        prune above."""
        pending = await asyncio.to_thread(
            self.rt.db.graph_markets_pending_resolution, C.GRAPH_RESOLUTION_CHECK_BATCH
        )
        if not pending:
            return
        resolved: list[tuple[str, datetime]] = []
        for ticker in pending:
            try:
                market = await self.rt.rest.get_market(ticker)
            except Exception:
                log.warning("resolution check failed", extra={"market": ticker})
                continue
            if not market:
                continue
            ts = _resolution_time(market)
            if ts is not None:
                resolved.append((ticker, ts))
        if resolved:
            await asyncio.to_thread(self.rt.db.mark_resolved, resolved)
            log.info("resolution reconciled", extra={"checked": len(pending), "resolved": len(resolved)})


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
        prefixes = C.DISCOVERY_SERIES_ALLOWLIST_PREFIXES
        if prefixes:
            admitted = [s for s in scored if s.ticker.startswith(prefixes)]
        else:
            admitted = [s for s in scored if evaluate(s).admit]

        def _line(s: SeriesStats) -> str:
            v = evaluate(s)
            flags = "".join(name[-1] if ok else "-" for name, ok in v.passes.items())
            return (
                f"  {s.ticker:<24} P[{flags}]  "
                f"part={v.n_partition_events} hier={v.n_hierarchy_events} "
                f"vol={s.volume_24h:,.0f}"
            )

        if prefixes:
            print(f"\n=== Allowlist mode: prefixes {prefixes} — "
                  f"{len(admitted)} series matched (predicates/rank/cap bypassed) ===")
            for i, s in enumerate(admitted, 1):
                print(f"{i:>3} {_line(s)}")
        else:
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
