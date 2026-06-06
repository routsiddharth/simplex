"""Shared helpers: time parsing/normalization, the grid floor, backoff, bucket."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from simplex_ingest.util import (
    Backoff,
    TokenBucket,
    floor_to_interval,
    naive_utc,
    now_utc,
    parse_dt,
)


# -- naive_utc / now_utc ----------------------------------------------------

def test_naive_utc_strips_tz_after_converting():
    aware = datetime(2026, 6, 5, 12, 0, tzinfo=timezone(timedelta(hours=5)))
    out = naive_utc(aware)
    assert out.tzinfo is None
    assert out == datetime(2026, 6, 5, 7, 0)       # 12:00+05:00 -> 07:00 UTC


def test_naive_utc_passes_naive_through():
    naive = datetime(2026, 6, 5, 7, 0)
    assert naive_utc(naive) == naive


def test_now_utc_is_aware():
    assert now_utc().tzinfo is not None


# -- parse_dt ---------------------------------------------------------------

@pytest.mark.parametrize("value", [None, "", 0, "   ", "not-a-date"])
def test_parse_dt_returns_none_for_empty_or_garbage(value):
    assert parse_dt(value) is None


def test_parse_dt_iso_z_and_offset():
    assert parse_dt("2026-06-05T00:00:00Z") == datetime(2026, 6, 5, tzinfo=timezone.utc)
    assert parse_dt("2026-06-05T00:00:00+00:00") == datetime(2026, 6, 5, tzinfo=timezone.utc)


def test_parse_dt_naive_iso_assumed_utc():
    assert parse_dt("2026-06-05T00:00:00").tzinfo == timezone.utc


def test_parse_dt_unix_seconds_vs_millis():
    secs = parse_dt(1_700_000_000)
    millis = parse_dt(1_700_000_000_000)           # > 1e12 -> treated as ms
    assert secs == millis                           # same instant, different units
    assert secs.tzinfo == timezone.utc


# -- floor_to_interval ------------------------------------------------------

def test_floor_to_interval_snaps_down():
    dt = datetime(2026, 6, 5, 0, 0, 17, tzinfo=timezone.utc)
    assert floor_to_interval(dt, 10) == datetime(2026, 6, 5, 0, 0, 10)
    assert floor_to_interval(datetime(2026, 6, 5, 0, 0, 10), 10) == datetime(2026, 6, 5, 0, 0, 10)


# -- Backoff ----------------------------------------------------------------

def test_backoff_stays_within_bounds_and_resets():
    b = Backoff(1.0, 60.0, 2.0)
    delays = [b.next_delay() for _ in range(20)]
    assert all(0.0 <= d <= 60.0 for d in delays)    # full jitter, capped
    assert b._attempt == 20
    b.reset()
    assert b._attempt == 0
    assert b.next_delay() <= 1.0                      # back to the floor's ceiling


# -- TokenBucket ------------------------------------------------------------

async def test_token_bucket_allows_burst_up_to_capacity():
    bucket = TokenBucket(rate=1000.0, capacity=3)
    for _ in range(3):                                # full burst, no waiting needed
        await bucket.acquire()
    # A 4th acquire refills fast at 1000/s; just assert it completes.
    await bucket.acquire()
