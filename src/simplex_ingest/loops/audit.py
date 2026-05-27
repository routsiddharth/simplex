"""Hourly book audit loop.

Wakes every AUDIT_TICK_SECONDS; runs a pass only when the current UTC hour is
inside [AUDIT_WINDOW_START_UTC_HOUR, AUDIT_WINDOW_END_UTC_HOUR). For each
subscribed market it freezes the in-memory book BEFORE the REST call, fetches
the REST orderbook, diffs them, classifies (no/small/large), writes a row to
`audit_results`, and forces a book reset on a large diff. Orderbook fetches use
a dedicated token bucket so an audit can't starve the catalog poller. A single
market's error never aborts the pass.
"""

from __future__ import annotations

import asyncio
import json

from .. import constants as C
from ..log import get_logger
from ..orderbook import _q
from ..util import now_utc

log = get_logger("audit")


def _rest_levels(raw: list | None) -> dict[float, float]:
    out: dict[float, float] = {}
    for pair in raw or []:
        if isinstance(pair, (list, tuple)) and len(pair) >= 2:
            try:
                out[_q(float(pair[0]))] = float(pair[1])
            except (TypeError, ValueError):
                continue
    return out


def _diff_side(mem: dict[float, float], rest: dict[float, float]) -> tuple[int, float]:
    """(levels differing, max per-level size delta as % of the larger size)."""
    diff_levels = 0
    max_pct = 0.0
    for price in set(mem) | set(rest):
        m = mem.get(price, 0.0)
        r = rest.get(price, 0.0)
        delta = abs(m - r)
        if delta > 1e-9:
            diff_levels += 1
            base = max(m, r, 1.0)
            max_pct = max(max_pct, delta / base * 100.0)
    return diff_levels, max_pct


class BookAuditLoop:
    name = "audit"

    def __init__(self, rt) -> None:
        self.rt = rt

    async def run(self) -> None:
        while not self.rt.shutdown.is_set():
            if self._in_window():
                try:
                    await self.run_pass()
                except Exception:
                    log.exception("audit pass failed")
            self.rt.heartbeats.beat(self.name)
            await self._sleep(C.AUDIT_TICK_SECONDS)

    def _in_window(self) -> bool:
        hour = now_utc().hour
        return C.AUDIT_WINDOW_START_UTC_HOUR <= hour < C.AUDIT_WINDOW_END_UTC_HOUR

    async def _sleep(self, seconds: float) -> None:
        remaining = seconds
        while remaining > 0 and not self.rt.shutdown.is_set():
            step = min(15.0, remaining)
            try:
                await asyncio.wait_for(self.rt.shutdown.wait(), timeout=step)
            except asyncio.TimeoutError:
                self.rt.heartbeats.beat(self.name)
            remaining -= step

    async def run_pass(self) -> None:
        markets = await asyncio.to_thread(self.rt.db.get_active_market_ids)
        log.info("audit pass start", extra={"markets": len(markets)})
        results: list[tuple] = []
        for market in sorted(markets):
            try:
                results.append(await self._audit_market(market))
            except Exception as exc:
                log.warning("audit market error", extra={"market": market, "err": repr(exc)})
                results.append(
                    (now_utc(), market, "error", None, None, "none", json.dumps({"err": repr(exc)}))
                )
        await asyncio.to_thread(self.rt.db.insert_audit_results, results)
        log.info("audit pass done", extra={"rows": len(results)})

    async def _audit_market(self, market: str) -> tuple:
        # Freeze the in-memory book BEFORE the REST call.
        frozen = self.rt.book_store.freeze(market)
        ob = await self.rt.audit_rest.get_orderbook(market, C.AUDIT_ORDERBOOK_DEPTH)
        ts = now_utc()

        rest_yes = _rest_levels(ob.get("yes_dollars") or ob.get("yes"))
        rest_no = _rest_levels(ob.get("no_dollars") or ob.get("no"))
        mem_yes = dict(frozen.yes) if frozen else {}
        mem_no = dict(frozen.no) if frozen else {}

        yes_levels, yes_pct = _diff_side(mem_yes, rest_yes)
        no_levels, no_pct = _diff_side(mem_no, rest_no)
        levels_diff = yes_levels + no_levels
        max_pct = max(yes_pct, no_pct)

        if levels_diff == 0:
            status, action = "no_diff", "none"
            log.debug("audit no diff", extra={"market": market})
        elif levels_diff <= C.AUDIT_SMALL_DIFF_MAX_LEVELS and max_pct <= C.AUDIT_SMALL_DIFF_MAX_SIZE_PCT:
            status, action = "small_diff", "none"
            log.info("audit small diff", extra={"market": market, "levels": levels_diff, "max_pct": round(max_pct, 2)})
        else:
            status, action = "large_diff", "book_reset"
            log.warning(
                "audit large diff; resetting book",
                extra={"market": market, "levels": levels_diff, "max_pct": round(max_pct, 2)},
            )
            self.rt.book_store.reset_book(market)
            try:
                self.rt.reset_requests.put_nowait(market)
            except asyncio.QueueFull:
                pass

        details = json.dumps(
            {
                "yes_levels_diff": yes_levels,
                "no_levels_diff": no_levels,
                "mem_yes": len(mem_yes),
                "rest_yes": len(rest_yes),
                "mem_no": len(mem_no),
                "rest_no": len(rest_no),
            }
        )
        return (ts, market, status, levels_diff, round(max_pct, 4), action, details)
