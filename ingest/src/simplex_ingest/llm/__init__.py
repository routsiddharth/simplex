"""LLM extraction layer client (Stage 3).

Two transports for one set of prompts/shapes: the synchronous OpenRouter client
(:mod:`.client`) and the asynchronous, discounted Anthropic Message Batches client
(:mod:`.batch`). The message builders + result parsers are shared so a batched
request is byte-identical to its synchronous twin. The shapes
(`MarketSemantics`, `PairClassification`) and the relationship taxonomy are
venue/provider-neutral, owned here so the loop stays free of LLM specifics.
"""

from .batch import AnthropicBatchClient, BatchResult
from .client import (
    DIRECTIONS,
    PURPOSE_PAIR_PRIMARY,
    PURPOSE_PAIR_VERIFY,
    PURPOSE_SEMANTICS,
    RELATIONSHIP_TYPES,
    LLMError,
    MarketSemantics,
    OpenRouterClient,
    PairClassification,
    build_classify_messages,
    build_extract_messages,
    log_usage,
    parse_classification,
    parse_semantics,
)

__all__ = [
    "DIRECTIONS",
    "RELATIONSHIP_TYPES",
    "PURPOSE_SEMANTICS",
    "PURPOSE_PAIR_PRIMARY",
    "PURPOSE_PAIR_VERIFY",
    "LLMError",
    "MarketSemantics",
    "OpenRouterClient",
    "PairClassification",
    "AnthropicBatchClient",
    "BatchResult",
    "build_extract_messages",
    "build_classify_messages",
    "parse_semantics",
    "parse_classification",
    "log_usage",
]
