"""OpenRouter client (extraction transport).

Covers the marshaling + validation discipline: a valid completion becomes a
typed dataclass; ```json fences are tolerated; every failure mode (malformed
envelope, non-JSON content, unknown enum label, 4xx) funnels into a single
LLMError; 429 retries. httpx MockTransport, no network; backoff patched fast.
"""

from __future__ import annotations

import json

import httpx
import pytest

from simplex_ingest.llm import LLMError, MarketSemantics, PairClassification
from simplex_ingest.llm.client import OpenRouterClient
from simplex_ingest.util import Backoff, TokenBucket

_SEM = {
    "underlying_event": "Team X wins the final",
    "resolves_yes_when": "X wins",
    "resolves_no_when": "X loses",
    "resolution_timing": "by 2026-07-01",
    "entities": ["Team X", "Team Y"],
    "dependencies": ["final result"],
}
_CLS = {"relationship_type": "implies", "direction": "first_implies_second",
        "confidence": 0.92, "rationale": "X winning forces the bracket."}


@pytest.fixture(autouse=True)
def _fast_backoff(mocker):
    async def _noop(self):
        return 0.0

    mocker.patch.object(Backoff, "sleep", _noop)


def _client(handler):
    c = OpenRouterClient(
        "sk-test", "https://openrouter.ai/api/v1", TokenBucket(100, 100),
        temperature=0.0, max_retries=3, timeout=5.0,
        backoff_min=0.0, backoff_max=0.0, backoff_factor=2.0,
    )
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return c


def _completion(content: str):
    return {"choices": [{"message": {"content": content}}]}


def _ok(content: str):
    def handler(req):
        return httpx.Response(200, json=_completion(content))

    return handler


async def test_extract_market_returns_typed_semantics():
    c = _client(_ok(json.dumps(_SEM)))
    try:
        sem = await c.extract_market("m", title="t", description="d", resolution_criteria="r")
        assert isinstance(sem, MarketSemantics)
        assert sem.underlying_event == "Team X wins the final"
        assert sem.entities == ("Team X", "Team Y")
        assert sem.dependencies == ("final result",)
    finally:
        await c.aclose()


async def test_extract_tolerates_json_code_fences():
    fenced = "```json\n" + json.dumps(_SEM) + "\n```"
    c = _client(_ok(fenced))
    try:
        sem = await c.extract_market("m", title="t", description="d", resolution_criteria="r")
        assert sem.resolves_yes_when == "X wins"
    finally:
        await c.aclose()


async def test_classify_pair_returns_typed_classification():
    c = _client(_ok(json.dumps(_CLS)))
    try:
        cls = await c.classify_pair("m", market_a={"market_id": "a"}, market_b={"market_id": "b"})
        assert isinstance(cls, PairClassification)
        assert cls.relationship_type == "implies"
        assert cls.direction == "first_implies_second"
        assert cls.confidence == 0.92
    finally:
        await c.aclose()


async def test_classify_clamps_confidence_to_unit_interval():
    c = _client(_ok(json.dumps({**_CLS, "confidence": 1.7})))
    try:
        cls = await c.classify_pair("m", market_a={"market_id": "a"}, market_b={"market_id": "b"})
        assert cls.confidence == 1.0
    finally:
        await c.aclose()


async def test_unknown_relationship_type_raises_llmerror():
    c = _client(_ok(json.dumps({**_CLS, "relationship_type": "bogus"})))
    try:
        with pytest.raises(LLMError):
            await c.classify_pair("m", market_a={"market_id": "a"}, market_b={"market_id": "b"})
    finally:
        await c.aclose()


async def test_non_json_content_raises_llmerror():
    c = _client(_ok("this is not json"))
    try:
        with pytest.raises(LLMError):
            await c.extract_market("m", title="t", description="d", resolution_criteria="r")
    finally:
        await c.aclose()


async def test_malformed_envelope_raises_llmerror():
    def handler(req):
        return httpx.Response(200, json={"unexpected": "shape"})

    c = _client(handler)
    try:
        with pytest.raises(LLMError):
            await c.extract_market("m", title="t", description="d", resolution_criteria="r")
    finally:
        await c.aclose()


async def test_4xx_raises_llmerror():
    def handler(req):
        return httpx.Response(400, text="bad request")

    c = _client(handler)
    try:
        with pytest.raises(LLMError):
            await c.extract_market("m", title="t", description="d", resolution_criteria="r")
    finally:
        await c.aclose()


async def test_retries_on_429_then_succeeds():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, json={})
        return httpx.Response(200, json=_completion(json.dumps(_SEM)))

    c = _client(handler)
    try:
        sem = await c.extract_market("m", title="t", description="d", resolution_criteria="r")
        assert calls["n"] == 2
        assert sem.underlying_event == "Team X wins the final"
    finally:
        await c.aclose()
