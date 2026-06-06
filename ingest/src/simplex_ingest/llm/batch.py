"""Anthropic Message Batches client — the asynchronous, discounted spend path.

The second provider seam (alongside the synchronous :class:`OpenRouterClient`).
Routes non-latency-sensitive extraction work through Anthropic's Message Batches
API: a flat 50% discount, results within ~1h–24h, retrievable for 29 days.

This client is **transport only** — submit / poll / fetch-results. It is
deliberately ignorant of Stages A/B: callers build request ``params`` with the
shared :func:`..llm.client.build_extract_messages` /
:func:`build_classify_messages` and parse results with the shared
``parse_semantics`` / ``parse_classification``, so a batched request is
byte-identical to its synchronous twin and a batched result validates the same way.

Three shapes:

* :meth:`AnthropicBatchClient.submit` — POST a list of requests, returns the
  ``batch_id`` (which the loop persists in DuckDB; a batch is submit-now /
  retrieve-later, so the id must survive a restart).
* :meth:`AnthropicBatchClient.poll` — GET the batch, returns its
  ``processing_status`` + counts.
* :meth:`AnthropicBatchClient.results` — GET the JSONL results of an *ended*
  batch, normalized to :class:`BatchResult` per ``custom_id``.

**Failure discipline** mirrors the sync client: transport / 429 / 5xx are retried
with backoff; a non-2xx after retries raises :class:`..llm.LLMError`. Per-request
*result* failures (a single ``errored``/``expired`` item inside an otherwise-good
batch) are surfaced as a :class:`BatchResult` with ``error`` set, never raised —
the loop logs-and-skips that one item, exactly as on the sync path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import httpx

from ..log import get_logger
from ..util import Backoff, TokenBucket
from .client import LLMError

log = get_logger("llm")


@dataclass(frozen=True)
class BatchResult:
    """One per-request outcome from a completed batch.

    ``content`` is the model's text (to feed the shared parsers) when the request
    succeeded; otherwise ``content`` is None and ``error`` describes why (an
    ``errored`` / ``canceled`` / ``expired`` request). ``usage`` is the token
    block, for cost attribution."""

    custom_id: str
    content: str | None
    usage: dict | None
    error: str | None


def _result_text(message: dict) -> str | None:
    """First text block of an Anthropic message's content array, or None."""
    for block in message.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            return block.get("text")
    return None


class AnthropicBatchClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        bucket: TokenBucket,
        *,
        version: str,
        max_retries: int,
        timeout: float,
        backoff_min: float,
        backoff_max: float,
        backoff_factor: float,
    ) -> None:
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._bucket = bucket
        self._version = version
        self._max_retries = max_retries
        self._backoff = (backoff_min, backoff_max, backoff_factor)
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))

    async def aclose(self) -> None:
        await self._client.aclose()

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "x-api-key": self._api_key,
            "anthropic-version": self._version,
            "content-type": "application/json",
        }

    # -- transport ----------------------------------------------------------

    async def _request(self, method: str, path: str, *, json_body: dict | None = None) -> httpx.Response:
        """One retried Anthropic-direct request. Raises LLMError on terminal failure.

        Returns the raw response (so callers can read JSON or JSONL); 429/5xx and
        transport errors are retried with backoff, 4xx is terminal."""
        url = f"{self._base}{path}"
        backoff = Backoff(*self._backoff)
        attempt = 0
        while True:
            await self._bucket.acquire()
            try:
                resp = await self._client.request(
                    method, url, json=json_body, headers=self._headers
                )
            except httpx.TransportError as exc:
                attempt += 1
                if attempt > self._max_retries:
                    raise LLMError(f"batch transport error after {attempt} attempts: {exc}") from exc
                log.warning("batch transport error, retrying", extra={"path": path, "err": str(exc)})
                await backoff.sleep()
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                attempt += 1
                if attempt > self._max_retries:
                    raise LLMError(
                        f"batch throttled/5xx ({resp.status_code}) after {attempt} attempts"
                    )
                log.warning(
                    "batch throttled/5xx, retrying",
                    extra={"path": path, "status": resp.status_code, "attempt": attempt},
                )
                await backoff.sleep()
                continue

            if resp.status_code >= 400:
                raise LLMError(f"batch request failed: {resp.status_code} {resp.text[:200]}")
            return resp

    # -- submit / poll / results -------------------------------------------

    async def submit(self, requests: list[dict]) -> str:
        """Submit a batch of requests, return its ``batch_id``.

        Each request is ``{"custom_id": str, "params": {...Messages params...}}``;
        callers build ``params`` (model, max_tokens, system, messages, temperature).
        Raises LLMError if the batch can't be created (the loop logs-and-skips the
        whole submission — the items simply aren't marked in-flight, so they're
        retried next cycle)."""
        resp = await self._request("POST", "/messages/batches", json_body={"requests": requests})
        try:
            return str(resp.json()["id"])
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise LLMError(f"malformed batch-create response: {exc}") from exc

    async def poll(self, batch_id: str) -> dict:
        """Return the batch object (``processing_status``, ``request_counts``, …)."""
        resp = await self._request("GET", f"/messages/batches/{batch_id}")
        try:
            return resp.json()
        except json.JSONDecodeError as exc:
            raise LLMError(f"malformed batch-poll response: {exc}") from exc

    async def results(self, batch_id: str) -> list[BatchResult]:
        """Fetch + normalize the JSONL results of an *ended* batch.

        One :class:`BatchResult` per line. Succeeded requests carry ``content`` +
        ``usage``; errored/canceled/expired requests carry ``error`` (and None
        content) so the caller skips just that item."""
        resp = await self._request("GET", f"/messages/batches/{batch_id}/results")
        out: list[BatchResult] = []
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                log.warning("batch result line not JSON", extra={"batch": batch_id, "err": str(exc)})
                continue
            cid = str(row.get("custom_id", ""))
            result = row.get("result") or {}
            rtype = result.get("type")
            if rtype == "succeeded":
                message = result.get("message") or {}
                text = _result_text(message)
                if text is None:
                    out.append(BatchResult(cid, None, message.get("usage"), "no text block in message"))
                else:
                    out.append(BatchResult(cid, text, message.get("usage"), None))
            else:
                # errored | canceled | expired — surface, don't raise.
                err = result.get("error") or {}
                detail = err.get("message") if isinstance(err, dict) else str(err)
                out.append(BatchResult(cid, None, None, f"{rtype}: {detail}"))
        return out
