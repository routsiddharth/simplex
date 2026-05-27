"""One-shot series discovery for the Simplex allowlist.

Sweeps all open Kalshi events (with nested markets), groups by series, and scores
each series with a composite heuristic that prioritizes internal structural
density (partition + hierarchy evidence) over headline volume, gated by a minimum
liquidity floor. Prints a ranked list with a one-line reason per series and
writes the top N to simplex_allowlist.yaml. If the allowlist already exists,
prints a diff (newly high-scoring series; current entries that now score low).

Run:  python -m scripts.discover_series
Then review the output and commit simplex_allowlist.yaml.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field

import yaml

from simplex_ingest import constants as C
from simplex_ingest.allowlist import allowlist_path, load_allowlist
from simplex_ingest.config import get_settings
from simplex_ingest.kalshi.auth import KalshiSigner
from simplex_ingest.kalshi.rest import KalshiREST

_TRADEABLE = {"active"}


@dataclass
class SeriesStats:
    ticker: str
    n_events: int = 0
    n_markets: int = 0
    n_mutex_events: int = 0
    n_multi_market_events: int = 0
    volume: float = 0.0
    title: str | None = None
    score: float = field(default=0.0)

    def compute_score(self) -> None:
        count_s = math.log1p(self.n_markets)
        partition_s = math.log1p(self.n_mutex_events)
        hierarchy_s = math.log1p(self.n_multi_market_events)
        liquidity_s = math.log1p(self.volume)
        composite = (
            C.DISCOVERY_WEIGHT_MARKET_COUNT * count_s
            + C.DISCOVERY_WEIGHT_PARTITION * partition_s
            + C.DISCOVERY_WEIGHT_HIERARCHY * hierarchy_s
            + C.DISCOVERY_WEIGHT_LIQUIDITY * liquidity_s
        )
        penalty = 1.0 if self.volume >= C.DISCOVERY_MIN_LIQUIDITY_CONTRACTS else 0.1
        self.score = composite * penalty

    @property
    def below_floor(self) -> bool:
        return self.volume < C.DISCOVERY_MIN_LIQUIDITY_CONTRACTS

    def reason(self) -> str:
        flag = " [below liquidity floor]" if self.below_floor else ""
        return (
            f"{self.n_markets} mkts / {self.n_events} evts, "
            f"{self.n_mutex_events} mutex, {self.n_multi_market_events} multi-mkt, "
            f"vol {self.volume:,.0f}{flag}"
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


async def gather_stats(rest: KalshiREST) -> dict[str, SeriesStats]:
    print("Fetching open events (with nested markets)...", flush=True)
    events = await rest.get_events(status="open", with_nested_markets=True)
    print(f"  {len(events)} open events", flush=True)

    by_series: dict[str, SeriesStats] = {}
    for event in events:
        series = event.get("series_ticker")
        if not series:
            continue
        st = by_series.setdefault(series, SeriesStats(ticker=series))
        st.n_events += 1
        if event.get("mutually_exclusive"):
            st.n_mutex_events += 1
        markets = [m for m in (event.get("markets") or []) if m.get("status") in _TRADEABLE]
        if len(markets) >= 2:
            st.n_multi_market_events += 1
        st.n_markets += len(markets)
        st.volume += sum(_market_volume(m) for m in markets)

    for st in by_series.values():
        st.compute_score()
    return by_series


async def enrich_titles(rest: KalshiREST, top: list[SeriesStats]) -> None:
    for st in top:
        try:
            series = await rest.get_series(st.ticker)
            if series:
                st.title = series.get("title")
        except Exception:
            pass


def print_ranked(ranked: list[SeriesStats]) -> None:
    print("\n=== Ranked series (highest structural density first) ===")
    print(f"{'rank':>4}  {'score':>7}  {'series':<22}  reason")
    for i, st in enumerate(ranked, 1):
        print(f"{i:>4}  {st.score:>7.2f}  {st.ticker:<22}  {st.reason()}")


def print_diff(ranked: list[SeriesStats], top: list[SeriesStats]) -> None:
    current = {e.ticker for e in load_allowlist()}
    if not current:
        return
    top_tickers = {st.ticker for st in top}
    threshold = top[-1].score * C.DISCOVERY_LOW_SCORE_FRACTION if top else 0.0
    by_ticker = {st.ticker: st for st in ranked}

    print("\n=== Diff vs current allowlist ===")
    new_high = [st for st in top if st.ticker not in current]
    if new_high:
        print("New high-scoring series NOT in current allowlist:")
        for st in new_high:
            print(f"  + {st.ticker:<22} {st.score:>7.2f}  {st.reason()}")
    low = []
    for ticker in sorted(current):
        st = by_ticker.get(ticker)
        if st is None:
            low.append((ticker, "no open markets / not found"))
        elif st.score < threshold:
            low.append((ticker, f"score {st.score:.2f} < {threshold:.2f}"))
    if low:
        print("Current entries that now score low:")
        for ticker, why in low:
            print(f"  - {ticker:<22} {why}")
    if not new_high and not low:
        print("  (no changes)")


def write_yaml(top: list[SeriesStats]) -> None:
    path = allowlist_path()
    doc = {
        "series": [
            {
                "ticker": st.ticker,
                "notes": f"{st.title + ' — ' if st.title else ''}{st.reason()}",
            }
            for st in top
        ]
    }
    path.write_text(yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, allow_unicode=True))
    print(f"\nWrote top {len(top)} series to {path}")


async def main() -> None:
    settings = get_settings()
    signer = KalshiSigner(settings.kalshi_api_key_id, settings.load_private_key())
    rest = KalshiREST(settings.rest_base_url, signer)
    try:
        stats = await gather_stats(rest)
        if not stats:
            print("No open series found.")
            return
        ranked = sorted(stats.values(), key=lambda s: s.score, reverse=True)
        top = ranked[: C.DISCOVERY_TOP_N]
        await enrich_titles(rest, top)
        print_ranked(ranked[: max(C.DISCOVERY_TOP_N * 3, 30)])
        print_diff(ranked, top)
        write_yaml(top)
    finally:
        await rest.aclose()


if __name__ == "__main__":
    asyncio.run(main())
