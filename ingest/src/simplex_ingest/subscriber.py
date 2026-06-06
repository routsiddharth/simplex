"""Exchange-agnostic subscriber contract.

Adding a new venue (Polymarket, ...) is a single new file implementing this
ABC. The WS loop drives subscribers through these hooks; only the parsing and
message shapes differ per exchange.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from .events import NormalizedEvent


class BaseSubscriber(ABC):
    #: platform tag stamped on emitted events
    platform: str

    @abstractmethod
    def ws_url(self) -> str:
        """WebSocket URL to connect to."""

    @abstractmethod
    def connect_headers(self) -> dict[str, str]:
        """Headers for the WS handshake (auth)."""

    @abstractmethod
    def subscribe_message(
        self, msg_id: int, channels: list[str], market_tickers: list[str]
    ) -> dict:
        """Build a subscribe command."""

    @abstractmethod
    def is_control(self, message: dict) -> bool:
        """True if ``message`` is a subscription-management reply, not market data."""

    @abstractmethod
    def parse(self, message: dict) -> list[NormalizedEvent]:
        """Turn one decoded inbound data message into normalized events.

        Returns ``[]`` for control or unrecognized messages. Must not raise on
        malformed input — log and drop instead.
        """
