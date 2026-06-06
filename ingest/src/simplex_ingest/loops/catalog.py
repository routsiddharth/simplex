"""Catalog poller loop.

Every CATALOG_REFRESH_SECONDS: read the tracked series from the `tracked_series`
table (maintained by the discovery loop), expand each series to its open markets
via Kalshi REST, filter by status/liquidity, upsert into `markets`, set the
active subscription set, and signal the WS loop to reconcile. Closed/removed
markets keep their rows (and history); only their `subscribed` flag flips to
false.
"""

from __future__ import annotations

import asyncio
import json

from .. import constants as C
from ..kalshi.fixedpoint import volume
from ..log import get_logger
from ..util import idle_sleep, naive_utc, now_utc, parse_dt

log = get_logger("catalog")

# Kalshi market statuses considered tradeable / eligible for subscription.
_TRADEABLE = {"active"}


def _market_row(market: dict, event: dict, platform: str) -> tuple:
    """Map a Kalshi market (+ its event) to a `markets` upsert row."""
    ticker = market.get("ticker")
    title = market.get("title") or event.get("title") or ticker
    sub = market.get("yes_sub_title") or market.get("subtitle")
    if sub and title and sub not in title:
        title = f"{title} — {sub}"
    created = parse_dt(market.get("open_time") or market.get("created_time"))
    closes = parse_dt(market.get("close_time"))
    resolved = parse_dt(market.get("settlement_ts") or market.get("expiration_time"))
    return (
        ticker,
        platform,
        title,
        market.get("rules_primary"),
        market.get("rules_secondary") or market.get("rules_primary"),
        event.get("series_ticker") or market.get("series_ticker"),
        market.get("event_ticker") or event.get("event_ticker"),
        naive_utc(created) if created else None,
        naive_utc(closes) if closes else None,
        naive_utc(resolved) if resolved else None,
        market.get("status"),
        json.dumps(market, default=str),
        naive_utc(now_utc()),
    )


class CatalogPoller:
    name = "catalog"

    def __init__(self, rt) -> None:
        self.rt = rt

    async def run(self) -> None:
        while not self.rt.shutdown.is_set():
            try:
                await self.refresh()
            except Exception:
                log.exception("catalog refresh failed")
            self.rt.heartbeats.beat(self.name)
            await idle_sleep(self.rt.shutdown, self.rt.heartbeats, self.name, C.CATALOG_REFRESH_SECONDS)

    async def refresh(self) -> None:
        series_list = await asyncio.to_thread(self.rt.db.get_tracked_series)
        if not series_list:
            # Soft fail: discovery hasn't populated yet (or admitted nothing). The
            # WS set stays as-is until the next tick finds a tracked set.
            log.warning("no tracked series; WS will be idle until discovery populates")
            return

        platform = self.rt.subscriber.platform
        # Collect candidates as (volume, ticker, row) so we can both log the live
        # volume distribution and apply the MAX_ACTIVE_MARKETS ceiling greedily by
        # volume after the full series→market fan-out.
        candidates: list[tuple[float, str, tuple]] = []

        for series in series_list:
            events = await self.rt.rest.get_events(
                status="open", series_ticker=series, with_nested_markets=True
            )
            if not events:
                # Discovery only tracks series with open structure, so an empty
                # result here is just a transient gap, not a bad ticker.
                log.info("tracked series has no open events", extra={"series": series})
                continue

            for event in events:
                for market in event.get("markets") or []:
                    if market.get("status") not in _TRADEABLE:
                        continue
                    vol = volume(market)
                    if vol < C.CATALOG_MIN_MARKET_VOLUME:
                        continue
                    ticker = market.get("ticker")
                    if not ticker:
                        continue
                    candidates.append((vol, ticker, _market_row(market, event, platform)))

        self._log_volume_distribution(candidates)

        # Cap *markets* (not just series): a high-cardinality series can fan out
        # to thousands of markets and break the firehose budget. Keep the highest-
        # volume markets — the value the coherence engine cares about — and drop
        # the long low-liquidity tail.
        dropped = 0
        if len(candidates) > C.MAX_ACTIVE_MARKETS:
            candidates.sort(key=lambda c: c[0], reverse=True)
            dropped = len(candidates) - C.MAX_ACTIVE_MARKETS
            candidates = candidates[: C.MAX_ACTIVE_MARKETS]

        active_ids = {ticker for _, ticker, _ in candidates}
        rows = [row for _, _, row in candidates]

        await asyncio.to_thread(self.rt.db.upsert_markets, rows)
        await asyncio.to_thread(self.rt.db.set_active_set, active_ids)
        self.rt.resubscribe_event.set()
        log.info(
            "catalog refreshed",
            extra={"series": len(series_list), "active_markets": len(active_ids),
                   "dropped_over_ceiling": dropped},
        )

    @staticmethod
    def _log_volume_distribution(candidates: list[tuple[float, str, tuple]]) -> None:
        """Emit the active-market volume distribution so a deliberate
        CATALOG_MIN_MARKET_VOLUME floor can be set from live data (the floor
        cannot be sampled out-of-band: DuckDB is single-writer while the process
        holds the lock). Greppable as ``catalog volume distribution``."""
        n = len(candidates)
        if not n:
            return
        vols = sorted(v for v, _, _ in candidates)

        def pct(p: float) -> float:
            return round(vols[min(n - 1, int(p * n))], 1)

        below = {f"lt_{t}": sum(1 for v in vols if v < t) for t in (1, 10, 100, 1000)}
        log.info(
            "catalog volume distribution",
            extra={"n": n, "p50": pct(0.50), "p90": pct(0.90), "p99": pct(0.99),
                   "max": round(vols[-1], 1), **below},
        )
