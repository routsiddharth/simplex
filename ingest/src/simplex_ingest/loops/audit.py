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
from ..kalshi.fixedpoint import level_map
from ..log import get_logger
from ..runtime import request_reset
from ..util import idle_sleep, now_utc

log = get_logger("audit")


def _diff_side(mem: dict[float, float], rest: dict[float, float]) -> tuple[int, float]:
    """Compare one side of the frozen in-memory book against the REST book.

    Returns ``(structural_mismatches, max_size_pct)``:

    * ``structural_mismatches`` — prices present in exactly one of the two books
      (a level that exists in memory but not REST, or vice versa). This is the
      corruption signal: a correctly-reconstructed book agrees with REST on
      *which* levels exist.
    * ``max_size_pct`` — the largest per-level size delta (as % of the larger
      size) among prices present in *both* books. Small values are expected
      freeze->REST market movement (the book is frozen ~100ms before the fetch);
      a *gross* value flags a magnitude/decode bug and escalates the pass to
      'large' once it exceeds ``AUDIT_SMALL_DIFF_MAX_SIZE_PCT`` (the structural
      test alone cannot catch a right-levels/wrong-sizes desync).

    Both dicts hold only size>0 levels, so a missing key means the level is
    absent."""
    structural = 0
    max_pct = 0.0
    for price in set(mem) | set(rest):
        in_mem = price in mem
        in_rest = price in rest
        if in_mem != in_rest:
            structural += 1
        else:
            delta = abs(mem[price] - rest[price])
            if delta > 1e-9:
                base = max(mem[price], rest[price], 1.0)
                max_pct = max(max_pct, delta / base * 100.0)
    return structural, max_pct


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
            await idle_sleep(self.rt.shutdown, self.rt.heartbeats, self.name, C.AUDIT_TICK_SECONDS)

    def _in_window(self) -> bool:
        hour = now_utc().hour
        return C.AUDIT_WINDOW_START_UTC_HOUR <= hour < C.AUDIT_WINDOW_END_UTC_HOUR

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

        rest_yes = level_map(ob.get("yes_dollars") or ob.get("yes"))
        rest_no = level_map(ob.get("no_dollars") or ob.get("no"))
        mem_yes = dict(frozen.yes) if frozen else {}
        mem_no = dict(frozen.no) if frozen else {}

        yes_levels, yes_pct = _diff_side(mem_yes, rest_yes)
        no_levels, no_pct = _diff_side(mem_no, rest_no)
        structural = yes_levels + no_levels
        max_pct = max(yes_pct, no_pct)

        if structural == 0 and max_pct == 0.0:
            status, action = "no_diff", "none"
            log.debug("audit no diff", extra={"market": market})
        elif structural <= C.AUDIT_STRUCTURAL_DIFF_MAX_LEVELS and max_pct <= C.AUDIT_SMALL_DIFF_MAX_SIZE_PCT:
            # Few/no structural mismatches and only minor size drift: expected
            # freeze->REST movement, not desync.
            status, action = "small_diff", "none"
            log.info("audit small diff", extra={"market": market, "structural": structural, "max_pct": round(max_pct, 2)})
        else:
            # Many structural mismatches OR a gross size drift (a magnitude/decode
            # bug the structural test can't see) -> the book genuinely disagrees.
            status, action = "large_diff", "book_reset"
            log.warning(
                "audit large diff; resetting book",
                extra={"market": market, "structural": structural, "max_pct": round(max_pct, 2)},
            )
            request_reset(self.rt, market)

        details = json.dumps(
            {
                "yes_structural_diff": yes_levels,
                "no_structural_diff": no_levels,
                "max_size_pct": round(max_pct, 4),
                "mem_yes": len(mem_yes),
                "rest_yes": len(rest_yes),
                "mem_no": len(mem_no),
                "rest_no": len(rest_no),
            }
        )
        return (ts, market, status, structural, round(max_pct, 4), action, details)
