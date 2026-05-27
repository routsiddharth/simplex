"""Small shared helpers (time, async backoff)."""

from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timedelta, timezone


def now_utc() -> datetime:
    """Timezone-aware current time in UTC."""
    return datetime.now(timezone.utc)


def naive_utc(dt: datetime) -> datetime:
    """Drop tzinfo after normalizing to UTC (DuckDB TIMESTAMP is naive).

    Naive datetimes are assumed already-UTC and returned unchanged.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None)


def parse_dt(value: object) -> datetime | None:
    """Parse a Kalshi timestamp into aware UTC.

    Accepts ISO-8601 strings (``...Z`` or offset), Unix seconds, or Unix
    milliseconds (heuristic: > 1e12). Returns None for empty/unparseable input.
    """
    if value is None or value == "" or value == 0:
        return None
    if isinstance(value, (int, float)):
        secs = value / 1000.0 if value > 1e12 else float(value)
        return datetime.fromtimestamp(secs, timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                return parse_dt(float(s))
            except ValueError:
                return None
    return None


def floor_to_interval(dt: datetime, seconds: int) -> datetime:
    """Floor ``dt`` down to the nearest ``seconds`` grid boundary (naive UTC)."""
    dt = naive_utc(dt)
    epoch = datetime(1970, 1, 1)
    bucket = int((dt - epoch).total_seconds()) // seconds * seconds
    return epoch + timedelta(seconds=bucket)


class Backoff:
    """Exponential backoff with full jitter, capped, never gives up.

    ``delay()`` returns the next sleep (and advances); ``reset()`` returns to
    the floor after a sustained success. Full jitter: actual sleep is uniform
    in [0, computed].
    """

    def __init__(self, minimum: float, maximum: float, factor: float) -> None:
        self._min = minimum
        self._max = maximum
        self._factor = factor
        self._attempt = 0

    def reset(self) -> None:
        self._attempt = 0

    def next_delay(self) -> float:
        ceiling = min(self._max, self._min * (self._factor ** self._attempt))
        self._attempt += 1
        return random.uniform(0.0, max(self._min, ceiling))

    async def sleep(self) -> float:
        d = self.next_delay()
        await asyncio.sleep(d)
        return d


class TokenBucket:
    """Simple async token bucket for client-side REST rate limiting.

    Refills at ``rate`` tokens/sec up to ``capacity``. ``acquire()`` waits until
    a token is available. Idle time builds headroom up to ``capacity`` (bursts).
    """

    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = float(capacity)
        self._tokens = float(capacity)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / self._rate)
