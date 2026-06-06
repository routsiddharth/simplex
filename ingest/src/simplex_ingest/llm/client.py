"""OpenRouter LLM client for the extraction layer (the synchronous spend path).

Two call shapes, both returning validated dataclasses:

* :meth:`OpenRouterClient.extract_market` — Stage A, per-market semantics.
* :meth:`OpenRouterClient.classify_pair` — Stage B, the typed relationship
  between two markets.

The transport is OpenRouter's OpenAI-compatible ``/chat/completions`` (Anthropic
models are reached via a ``anthropic/...`` model id, kept in ``constants.py`` so
it's swappable). The client is rate-limited by a shared :class:`TokenBucket` and
retries 429/5xx with jittered backoff, mirroring :mod:`kalshi.rest`.

The **message building** (system + user prompts) and **response parsing** are
module-level functions (``build_extract_messages`` / ``build_classify_messages`` /
``parse_semantics`` / ``parse_classification``) rather than methods, so the
asynchronous Anthropic batch path (:mod:`..llm.batch`) constructs *byte-identical*
requests and parses results the same way — one prompt+schema definition, two
transports.

**Failure discipline.** Every failure path — transport error, non-2xx after
retries, unparseable body, schema/enum violation — is funneled into a single
:class:`LLMError`. The extraction loop catches it and *logs-and-skips that one
item* (the same "never let one bad input take the pass down" discipline the WS
subscribers follow). The client never raises a raw httpx/JSON error at the loop.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import httpx

from ..log import get_logger
from ..util import Backoff, TokenBucket

log = get_logger("llm")

# The relationship taxonomy. Stage B must return exactly one of these; anything
# else is an LLMError (we don't silently coerce a hallucinated label into the
# graph). Mirrored in schema.sql's market_edges.relationship_type comment.
RELATIONSHIP_TYPES = frozenset(
    {
        "same_event",          # the two markets resolve on the same underlying event
        "implies",             # one outcome logically forces the other (see direction)
        "mutually_exclusive",  # both cannot resolve YES together
        "partition_member",    # members of the same exhaustive partition (sum-to-one)
        "conditional",         # one is conditioned on the other (see direction)
        "correlated",          # statistically linked, no hard logical constraint
        "unrelated",           # no meaningful relationship
    }
)

# Direction for the asymmetric relationships (implies / conditional). "first" /
# "second" refer to the order the markets are presented to the model; the loop
# presents them in canonical (a < b) order, so first==a, second==b.
DIRECTIONS = frozenset({"first_implies_second", "second_implies_first", "symmetric", "none"})

# Call-purpose tags. Threaded into usage logging (cost attribution by stage) and
# used by the loop/batch layer to route work. Stable strings — they appear in the
# structured logs the deploy verification greps.
PURPOSE_SEMANTICS = "stage_a"
PURPOSE_PAIR_PRIMARY = "pair_primary"
PURPOSE_PAIR_VERIFY = "pair_verify"


class LLMError(Exception):
    """Any failure producing/validating a model response. The loop logs + skips."""


@dataclass(frozen=True)
class MarketSemantics:
    """Stage A output — a rich, comparison-ready view of one market."""

    underlying_event: str
    resolves_yes_when: str
    resolves_no_when: str
    resolution_timing: str
    entities: tuple[str, ...]
    dependencies: tuple[str, ...]


@dataclass(frozen=True)
class PairClassification:
    """Stage B output — the typed logical relationship between two markets."""

    relationship_type: str
    direction: str
    confidence: float
    rationale: str


# -- prompts ----------------------------------------------------------------
# Reasoning lives at *pair* time, not extraction time: Stage A makes each market
# rich enough to compare; Stage B does the comparison by model, not by code.

_EXTRACT_SYSTEM = (
    "You analyze a single prediction market and produce a structured semantic "
    "representation of what it is actually about. Be precise and literal about "
    "resolution. Return ONLY a JSON object, no prose, with exactly these keys:\n"
    '  "underlying_event": string — the real-world event the market tracks\n'
    '  "resolves_yes_when": string — the condition that settles the market YES\n'
    '  "resolves_no_when": string — the condition that settles it NO\n'
    '  "resolution_timing": string — when/by what date it resolves\n'
    '  "entities": array of strings — the real-world entities it depends on '
    "(people, teams, places, tickers, offices), normalized\n"
    '  "dependencies": array of strings — concrete real-world facts whose '
    "outcome determines resolution"
)

_CLASSIFY_SYSTEM = (
    "You compare two prediction markets and classify the logical relationship "
    "between them. Reason carefully about resolution conditions, not surface "
    "wording. Return ONLY a JSON object, no prose, with exactly these keys:\n"
    '  "relationship_type": one of '
    "[same_event, implies, mutually_exclusive, partition_member, conditional, "
    "correlated, unrelated]\n"
    '  "direction": one of [first_implies_second, second_implies_first, '
    "symmetric, none] — non-'none' only for implies/conditional; describes the "
    "FIRST vs SECOND market as presented\n"
    '  "confidence": number in [0,1] — your calibrated confidence in the label\n'
    '  "rationale": string — one or two sentences of reasoning\n'
    "Default to unrelated with modest confidence when the markets are not "
    "clearly linked; do not invent constraints."
)


def _market_block(label: str, m: dict) -> str:
    ents = ", ".join(m.get("entities") or []) or "—"
    return (
        f"{label} market:\n"
        f"  title: {m.get('title') or m.get('market_id')}\n"
        f"  underlying_event: {m.get('underlying_event') or '—'}\n"
        f"  resolves_yes_when: {m.get('resolves_yes_when') or '—'}\n"
        f"  resolves_no_when: {m.get('resolves_no_when') or '—'}\n"
        f"  resolution_timing: {m.get('resolution_timing') or '—'}\n"
        f"  entities: {ents}\n"
    )


def build_extract_messages(
    *, title: str | None, description: str | None, resolution_criteria: str | None
) -> tuple[str, str]:
    """(system, user) for a Stage A semantics extraction. Shared by both paths."""
    user = (
        f"Title: {title or '—'}\n\n"
        f"Description / rules:\n{description or '—'}\n\n"
        f"Resolution criteria:\n{resolution_criteria or '—'}"
    )
    return _EXTRACT_SYSTEM, user


def build_classify_messages(*, market_a: dict, market_b: dict) -> tuple[str, str]:
    """(system, user) for a Stage B pair classification. Shared by both paths."""
    user = (
        _market_block("FIRST", market_a)
        + "\n"
        + _market_block("SECOND", market_b)
        + "\nClassify the relationship from FIRST to SECOND."
    )
    return _CLASSIFY_SYSTEM, user


# -- parsing ----------------------------------------------------------------


def _parse_object(content: str) -> dict:
    """Parse the model's JSON object, tolerating ```json fences."""
    s = content.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[-1] if "\n" in s else s
        s = s.removeprefix("json").strip()
        if s.endswith("```"):
            s = s[: s.rfind("```")]
    try:
        obj = json.loads(s)
    except json.JSONDecodeError as exc:
        raise LLMError(f"response was not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise LLMError("response JSON was not an object")
    return obj


def _as_str_list(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise LLMError("expected a JSON array")
    return tuple(str(v).strip() for v in value if str(v).strip())


def parse_semantics(content: str) -> MarketSemantics:
    """Validate a Stage A completion into MarketSemantics. Shared by both paths."""
    obj = _parse_object(content)
    try:
        return MarketSemantics(
            underlying_event=str(obj["underlying_event"]),
            resolves_yes_when=str(obj["resolves_yes_when"]),
            resolves_no_when=str(obj["resolves_no_when"]),
            resolution_timing=str(obj["resolution_timing"]),
            entities=_as_str_list(obj.get("entities")),
            dependencies=_as_str_list(obj.get("dependencies")),
        )
    except KeyError as exc:
        raise LLMError(f"semantics missing required key: {exc}") from exc


def parse_classification(content: str) -> PairClassification:
    """Validate a Stage B completion into PairClassification. Shared by both paths."""
    obj = _parse_object(content)
    rel = str(obj.get("relationship_type", "")).strip()
    if rel not in RELATIONSHIP_TYPES:
        raise LLMError(f"unknown relationship_type {rel!r}")
    direction = str(obj.get("direction", "none")).strip() or "none"
    if direction not in DIRECTIONS:
        raise LLMError(f"unknown direction {direction!r}")
    try:
        confidence = float(obj.get("confidence"))
    except (TypeError, ValueError) as exc:
        raise LLMError(f"confidence not numeric: {exc}") from exc
    confidence = max(0.0, min(1.0, confidence))
    return PairClassification(
        relationship_type=rel,
        direction=direction,
        confidence=confidence,
        rationale=str(obj.get("rationale", "")),
    )


def log_usage(*, mode: str, purpose: str, model: str, usage: dict | None) -> None:
    """Emit one structured cost-attribution line for an LLM call.

    ``usage`` is the provider's token block (OpenAI-compatible ``prompt_tokens`` /
    ``completion_tokens`` for OpenRouter, ``input_tokens`` / ``output_tokens`` for
    Anthropic). Tagged by ``mode`` (sync|batch), ``purpose`` (stage_a|pair_primary|
    pair_verify) and ``model`` so spend can be summed per stage — the number that
    decides batch go/no-go. Greppable phrase: ``llm usage``."""
    usage = usage or {}
    inp = usage.get("prompt_tokens", usage.get("input_tokens"))
    out = usage.get("completion_tokens", usage.get("output_tokens"))
    log.info(
        "llm usage",
        extra={
            "mode": mode,
            "purpose": purpose,
            "model": model,
            "input_tokens": inp,
            "output_tokens": out,
        },
    )


class OpenRouterClient:
    def __init__(
        self,
        api_key: str,
        base_url: str,
        bucket: TokenBucket,
        *,
        temperature: float,
        max_retries: int,
        timeout: float,
        backoff_min: float,
        backoff_max: float,
        backoff_factor: float,
    ) -> None:
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._bucket = bucket
        self._temperature = temperature
        self._max_retries = max_retries
        self._backoff = (backoff_min, backoff_max, backoff_factor)
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))

    async def aclose(self) -> None:
        await self._client.aclose()

    # -- transport ----------------------------------------------------------

    async def _chat(self, model: str, system: str, user: str, *, purpose: str) -> str:
        """One chat completion. Returns the content string. Raises LLMError.

        Logs the response's usage block (tagged by ``purpose`` + ``model``) for
        cost attribution — the block OpenRouter returns and we otherwise discard."""
        url = f"{self._base}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            # OpenRouter attribution headers (optional, but good citizenship).
            "X-Title": "simplex-ingest",
        }
        body = {
            "model": model,
            "temperature": self._temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        backoff = Backoff(*self._backoff)
        attempt = 0
        while True:
            await self._bucket.acquire()
            try:
                resp = await self._client.post(url, json=body, headers=headers)
            except httpx.TransportError as exc:
                attempt += 1
                if attempt > self._max_retries:
                    raise LLMError(f"transport error after {attempt} attempts: {exc}") from exc
                log.warning("llm transport error, retrying", extra={"model": model, "err": str(exc)})
                await backoff.sleep()
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                attempt += 1
                if attempt > self._max_retries:
                    raise LLMError(f"llm throttled/5xx ({resp.status_code}) after {attempt} attempts")
                log.warning(
                    "llm throttled/5xx, retrying",
                    extra={"model": model, "status": resp.status_code, "attempt": attempt},
                )
                await backoff.sleep()
                continue

            if resp.status_code >= 400:
                raise LLMError(f"llm request failed: {resp.status_code} {resp.text[:200]}")

            try:
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
            except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
                raise LLMError(f"malformed completion envelope: {exc}") from exc
            log_usage(mode="sync", purpose=purpose, model=model, usage=data.get("usage"))
            return content

    # -- Stage A ------------------------------------------------------------

    async def extract_market(
        self, model: str, *, title: str | None, description: str | None,
        resolution_criteria: str | None, purpose: str = PURPOSE_SEMANTICS,
    ) -> MarketSemantics:
        system, user = build_extract_messages(
            title=title, description=description, resolution_criteria=resolution_criteria
        )
        content = await self._chat(model, system, user, purpose=purpose)
        return parse_semantics(content)

    # -- Stage B ------------------------------------------------------------

    async def classify_pair(
        self, model: str, *, market_a: dict, market_b: dict,
        purpose: str = PURPOSE_PAIR_PRIMARY,
    ) -> PairClassification:
        system, user = build_classify_messages(market_a=market_a, market_b=market_b)
        content = await self._chat(model, system, user, purpose=purpose)
        return parse_classification(content)
