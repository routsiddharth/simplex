"""LLM extraction layer client (Stage 3).

The one module that talks to a hosted model. The transport is OpenRouter's
OpenAI-compatible chat surface (so the model id is a swappable constant); the
shapes (`MarketSemantics`, `PairClassification`) and the relationship taxonomy
are venue/provider-neutral, owned here so the loop stays free of LLM specifics.
"""

from .client import (
    DIRECTIONS,
    RELATIONSHIP_TYPES,
    LLMError,
    MarketSemantics,
    OpenRouterClient,
    PairClassification,
)

__all__ = [
    "DIRECTIONS",
    "RELATIONSHIP_TYPES",
    "LLMError",
    "MarketSemantics",
    "OpenRouterClient",
    "PairClassification",
]
