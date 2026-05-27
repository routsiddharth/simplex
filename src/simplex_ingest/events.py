"""Normalized event shape shared across exchanges.

A subscriber turns one raw inbound WS message into zero or more
:class:`NormalizedEvent` objects. The ingest writes them verbatim to
``raw_events``; the normalized ``payload`` carries already-parsed numeric
fields (dollars, contract counts) so downstream loops never re-parse exchange
fixed-point strings.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class EventType(str, Enum):
    ORDERBOOK_SNAPSHOT = "orderbook_snapshot"
    ORDERBOOK_DELTA = "orderbook_delta"
    TRADE = "trade"
    LIFECYCLE = "lifecycle"


class Side(str, Enum):
    YES = "yes"
    NO = "no"


@dataclass(slots=True)
class NormalizedEvent:
    """One row destined for ``raw_events``."""

    received_ts: datetime          # our receive clock (UTC)
    platform: str
    market_id: str
    event_type: EventType
    payload: dict[str, Any]        # parsed, numeric where applicable
    ts: datetime | None = None     # exchange event time (UTC), if provided
    sequence: int | None = None    # exchange per-subscription seq

    def as_row(self) -> tuple[Any, ...]:
        """Column order matches the INSERT in :mod:`simplex_ingest.db`."""
        import json

        return (
            self.ts,
            self.received_ts,
            self.platform,
            self.market_id,
            self.event_type.value,
            self.sequence,
            json.dumps(self.payload, default=str),
        )
