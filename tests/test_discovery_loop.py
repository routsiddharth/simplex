"""Async tests for the discovery loop, over fake_rest + a throwaway DuckDB."""

from __future__ import annotations

import logging
from datetime import datetime

import pytest

from simplex_ingest import constants as C
from simplex_ingest.loops.discovery import DiscoveryLoop

T0 = datetime(2026, 1, 1)


def _seed_row(ticker, rank):
    return (ticker, T0, T0, True, False, True, 1, 0, 9999.0, rank)


async def test_happy_path_ranks_by_volume(tmp_db, make_fake_rest, make_runtime,
                                          make_admissible_event):
    rest = make_fake_rest(events=[
        make_admissible_event("AAA", volume=5000.0),
        make_admissible_event("BBB", volume=2000.0),
    ])
    loop = DiscoveryLoop(make_runtime(tmp_db, rest))
    await loop.discover()
    # Both admissible; AAA outranks BBB on volume (the tie-break after P1/struct).
    assert tmp_db.get_tracked_series() == ["AAA", "BBB"]


async def test_cap_enforced(tmp_db, make_fake_rest, make_runtime, make_admissible_event):
    # 40 admissible series, descending volume -> only the top MAX_TRACKED_SERIES kept.
    events = [
        make_admissible_event(f"S{i:03d}", volume=1000.0 + i * 100)
        for i in range(40)
    ]
    loop = DiscoveryLoop(make_runtime(tmp_db, make_fake_rest(events=events)))
    await loop.discover()

    tracked = tmp_db.get_tracked_series()
    assert len(tracked) == C.MAX_TRACKED_SERIES
    # Highest-volume series is S039; lowest kept is S010 (40 - 30).
    assert tracked[0] == "S039"
    assert "S009" not in tracked and "S010" in tracked


async def test_eviction_on_degradation(tmp_db, make_fake_rest, make_runtime,
                                       make_admissible_event):
    rt = make_runtime(tmp_db, make_fake_rest(events=[
        make_admissible_event("KEEP", volume=5000.0),
        make_admissible_event("DROP", volume=3000.0),
    ]))
    loop = DiscoveryLoop(rt)
    await loop.discover()
    assert set(tmp_db.get_tracked_series()) == {"KEEP", "DROP"}

    # DROP's volume collapses below the P3 floor; a fresh candidate appears.
    rt.rest.events = [
        make_admissible_event("KEEP", volume=5000.0),
        make_admissible_event("DROP", volume=10.0),   # now fails P3
        make_admissible_event("NEW", volume=4000.0),
    ]
    await loop.discover()
    tracked = set(tmp_db.get_tracked_series())
    assert "DROP" not in tracked
    assert tracked == {"KEEP", "NEW"}


async def test_rest_failure_preserves_table(tmp_db, make_fake_rest, make_runtime, caplog):
    tmp_db.replace_tracked_series([_seed_row("OLD", 1)])
    rt = make_runtime(tmp_db, make_fake_rest(raise_exc=RuntimeError("kalshi 500")))
    rt.shutdown.set()  # one eager cycle, then exit

    with caplog.at_level(logging.ERROR):
        await DiscoveryLoop(rt).run()

    assert tmp_db.get_tracked_series() == ["OLD"]          # untouched
    assert rt.heartbeats.alive("discovery", 60.0)          # heartbeat still fired
    assert any(r.levelno >= logging.ERROR for r in caplog.records)


async def test_empty_sweep_preserves_table(tmp_db, make_fake_rest, make_runtime):
    tmp_db.replace_tracked_series([_seed_row("OLD", 1)])
    loop = DiscoveryLoop(make_runtime(tmp_db, make_fake_rest(events=[])))
    await loop.discover()
    assert tmp_db.get_tracked_series() == ["OLD"]  # don't wipe on transient empty


async def test_eager_populate_before_sleep(tmp_db, make_fake_rest, make_runtime,
                                          make_admissible_event):
    rt = make_runtime(tmp_db, make_fake_rest(events=[make_admissible_event("AAA")]))
    rt.shutdown.set()  # if discover() only ran after the sleep, the table stays empty
    await DiscoveryLoop(rt).run()
    assert tmp_db.get_tracked_series() == ["AAA"]
