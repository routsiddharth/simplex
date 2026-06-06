"""AnthropicBatchClient transport tests.

Covers submit/poll/results marshaling + the failure discipline: a created batch
returns its id; poll surfaces processing_status; JSONL results are normalized to
BatchResult (succeeded → content+usage, errored/expired → error); 429 retries;
4xx funnels into LLMError. httpx MockTransport, no network; backoff patched fast.
"""

from __future__ import annotations

import json

import httpx
import pytest

from simplex_ingest.llm import LLMError
from simplex_ingest.llm.batch import AnthropicBatchClient, BatchResult
from simplex_ingest.util import Backoff, TokenBucket


@pytest.fixture(autouse=True)
def _fast_backoff(mocker):
    async def _noop(self):
        return 0.0

    mocker.patch.object(Backoff, "sleep", _noop)


def _client(handler):
    c = AnthropicBatchClient(
        "sk-ant-test", "https://api.anthropic.com/v1", TokenBucket(100, 100),
        version="2023-06-01", max_retries=3, timeout=5.0,
        backoff_min=0.0, backoff_max=0.0, backoff_factor=2.0,
    )
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return c


def _ok_message(text: str, usage=None):
    return {
        "type": "succeeded",
        "message": {"content": [{"type": "text", "text": text}], "usage": usage or {}},
    }


async def test_submit_returns_batch_id():
    seen = {}

    def handler(req):
        seen["method"] = req.method
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        seen["version"] = req.headers.get("anthropic-version")
        seen["key"] = req.headers.get("x-api-key")
        return httpx.Response(200, json={"id": "msgbatch_123", "processing_status": "in_progress"})

    c = _client(handler)
    try:
        bid = await c.submit([{"custom_id": "r0", "params": {"model": "claude-sonnet-4-6"}}])
        assert bid == "msgbatch_123"
        assert seen["method"] == "POST"
        assert seen["url"].endswith("/messages/batches")
        assert seen["body"]["requests"][0]["custom_id"] == "r0"
        assert seen["version"] == "2023-06-01"
        assert seen["key"] == "sk-ant-test"
    finally:
        await c.aclose()


async def test_poll_returns_status():
    def handler(req):
        return httpx.Response(200, json={"processing_status": "ended", "request_counts": {"succeeded": 2}})

    c = _client(handler)
    try:
        status = await c.poll("msgbatch_123")
        assert status["processing_status"] == "ended"
    finally:
        await c.aclose()


async def test_results_normalizes_succeeded_and_errored():
    lines = [
        json.dumps({"custom_id": "r0", "result": _ok_message('{"x": 1}', {"input_tokens": 10, "output_tokens": 5})}),
        json.dumps({"custom_id": "r1", "result": {"type": "errored", "error": {"message": "boom"}}}),
        json.dumps({"custom_id": "r2", "result": {"type": "expired"}}),
        "",  # blank line tolerated
    ]

    def handler(req):
        assert str(req.url).endswith("/messages/batches/msgbatch_123/results")
        return httpx.Response(200, text="\n".join(lines))

    c = _client(handler)
    try:
        results = await c.results("msgbatch_123")
        assert [r.custom_id for r in results] == ["r0", "r1", "r2"]
        ok = results[0]
        assert isinstance(ok, BatchResult)
        assert ok.content == '{"x": 1}'
        assert ok.usage == {"input_tokens": 10, "output_tokens": 5}
        assert ok.error is None
        assert results[1].content is None and "boom" in results[1].error
        assert results[2].content is None and "expired" in results[2].error
    finally:
        await c.aclose()


async def test_retries_on_429_then_succeeds():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={})
        return httpx.Response(200, json={"id": "msgbatch_ok"})

    c = _client(handler)
    try:
        bid = await c.submit([{"custom_id": "r0", "params": {}}])
        assert calls["n"] == 2
        assert bid == "msgbatch_ok"
    finally:
        await c.aclose()


async def test_4xx_raises_llmerror():
    def handler(req):
        return httpx.Response(400, text="bad request")

    c = _client(handler)
    try:
        with pytest.raises(LLMError):
            await c.submit([{"custom_id": "r0", "params": {}}])
    finally:
        await c.aclose()


async def test_malformed_create_response_raises():
    def handler(req):
        return httpx.Response(200, json={"no_id": True})

    c = _client(handler)
    try:
        with pytest.raises(LLMError):
            await c.submit([{"custom_id": "r0", "params": {}}])
    finally:
        await c.aclose()
