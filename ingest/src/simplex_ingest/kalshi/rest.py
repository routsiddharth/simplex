"""Async Kalshi REST client.

Endpoints (confirmed against docs.kalshi.com, base URL ends with /trade-api/v2):
  GET /series?category=        -> series list (no pagination)
  GET /events?status=open&with_nested_markets=true  -> cursor-paginated
  GET /markets?status=open&series_ticker=           -> cursor-paginated
  GET /markets/{ticker}/orderbook?depth=            -> {orderbook_fp: {...}}

All calls are signed (harmless on public endpoints, ready for authed ones),
client-side rate-limited via a token bucket, and retried with jittered backoff
on 429 / transient 5xx.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

import httpx

from .. import constants as C
from ..log import get_logger
from ..util import Backoff, TokenBucket
from .auth import KalshiSigner

log = get_logger("rest")


class KalshiREST:
    def __init__(
        self,
        base_url: str,
        signer: KalshiSigner,
        bucket: TokenBucket | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._signer = signer
        self._bucket = bucket or TokenBucket(C.REST_CALLS_PER_SECOND, C.REST_BURST)
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base}{path}"
        sign_path = urlsplit(url).path  # path only, no query
        backoff = Backoff(
            C.WS_RECONNECT_MIN_SECONDS, C.WS_RECONNECT_MAX_SECONDS, C.WS_RECONNECT_BACKOFF_FACTOR
        )
        attempt = 0
        while True:
            await self._bucket.acquire()
            headers = self._signer.headers("GET", sign_path)
            try:
                resp = await self._client.get(url, params=params, headers=headers)
            except httpx.TransportError as exc:
                attempt += 1
                if attempt > C.REST_MAX_RETRIES:
                    raise
                log.warning("rest transport error, retrying", extra={"path": path, "err": str(exc)})
                await backoff.sleep()
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                attempt += 1
                if attempt > C.REST_MAX_RETRIES:
                    resp.raise_for_status()
                log.warning(
                    "rest throttled/5xx, retrying",
                    extra={"path": path, "status": resp.status_code, "attempt": attempt},
                )
                await backoff.sleep()
                continue

            resp.raise_for_status()
            return resp.json()

    # -- catalog ------------------------------------------------------------

    async def get_series_list(self, category: str | None = None) -> list[dict[str, Any]]:
        params = {"include_volume": "true"}
        if category:
            params["category"] = category
        data = await self._get("/series", params)
        return data.get("series", []) or []

    async def get_series(self, series_ticker: str) -> dict[str, Any] | None:
        try:
            data = await self._get(f"/series/{series_ticker}", {"include_volume": "true"})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        return data.get("series")

    async def get_events(
        self,
        status: str | None = "open",
        series_ticker: str | None = None,
        with_nested_markets: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """All events matching the filter, following the cursor to the end."""
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": limit}
            if status:
                params["status"] = status
            if series_ticker:
                params["series_ticker"] = series_ticker
            if with_nested_markets:
                params["with_nested_markets"] = "true"
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/events", params)
            out.extend(data.get("events", []) or [])
            cursor = data.get("cursor") or None
            if not cursor:
                return out

    async def get_markets(
        self,
        status: str | None = "open",
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {"limit": limit}
            if status:
                params["status"] = status
            if series_ticker:
                params["series_ticker"] = series_ticker
            if event_ticker:
                params["event_ticker"] = event_ticker
            if cursor:
                params["cursor"] = cursor
            data = await self._get("/markets", params)
            out.extend(data.get("markets", []) or [])
            cursor = data.get("cursor") or None
            if not cursor:
                return out

    async def get_orderbook(self, ticker: str, depth: int) -> dict[str, Any]:
        """Returns the raw orderbook_fp dict ({yes_dollars, no_dollars})."""
        data = await self._get(f"/markets/{ticker}/orderbook", {"depth": depth})
        return data.get("orderbook_fp") or data.get("orderbook") or {}
