"""Catalog poller loop.

Every CATALOG_REFRESH_SECONDS: re-read simplex_allowlist.yaml, expand each
series to its open markets via Kalshi REST, filter by status/liquidity, upsert
into `markets`, set the active subscription set, and signal the WS loop to
reconcile. Closed/removed markets keep their rows (and history); only their
`subscribed` flag flips to false.
"""

from __future__ import annotations

import asyncio
import json

from .. import constants as C
from ..allowlist import load_allowlist
from ..log import get_logger
from ..util import now_utc, parse_dt, naive_utc

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


def _market_volume(market: dict) -> float:
    for key in ("volume_fp", "volume", "volume_24h_fp"):
        v = market.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return 0.0


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
            await self._sleep(C.CATALOG_REFRESH_SECONDS)

    async def _sleep(self, seconds: float) -> None:
        # Wake early on shutdown; heartbeat periodically while idle.
        remaining = seconds
        while remaining > 0 and not self.rt.shutdown.is_set():
            step = min(15.0, remaining)
            try:
                await asyncio.wait_for(self.rt.shutdown.wait(), timeout=step)
            except asyncio.TimeoutError:
                self.rt.heartbeats.beat(self.name)
            remaining -= step

    async def refresh(self) -> None:
        entries = load_allowlist()
        if not entries:
            log.error("allowlist is empty; no markets will be subscribed")
            return

        platform = self.rt.subscriber.platform
        active_ids: set[str] = set()
        rows: list[tuple] = []
        kept = 0

        for entry in entries:
            series = entry.ticker
            events = await self.rt.rest.get_events(
                status="open", series_ticker=series, with_nested_markets=True
            )
            if not events:
                # Distinguish a typo'd/resolved series from one with no open markets.
                if await self.rt.rest.get_series(series) is None:
                    log.warning("allowlist series not found on Kalshi", extra={"series": series})
                else:
                    log.info("allowlist series has no open events", extra={"series": series})
                continue

            for event in events:
                for market in event.get("markets") or []:
                    if market.get("status") not in _TRADEABLE:
                        continue
                    if _market_volume(market) < C.CATALOG_MIN_MARKET_VOLUME:
                        continue
                    ticker = market.get("ticker")
                    if not ticker:
                        continue
                    active_ids.add(ticker)
                    rows.append(_market_row(market, event, platform))
                    kept += 1

        await asyncio.to_thread(self.rt.db.upsert_markets, rows)
        await asyncio.to_thread(self.rt.db.set_active_set, active_ids)
        self.rt.resubscribe_event.set()
        log.info(
            "catalog refreshed",
            extra={"series": len(entries), "active_markets": kept},
        )
