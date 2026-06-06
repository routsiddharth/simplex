"""Catalog poller internals beyond the tracked_series read path: the _market_row
field mapping (title/subtitle composition, rules -> description/criteria, ticker
sourcing) and that a market dropping out of REST keeps its row but loses its
subscription.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace

from simplex_ingest.loops.catalog import CatalogPoller, _market_row

T0 = datetime(2026, 1, 1)


def _tracked_row(ticker, rank=1):
    return (ticker, T0, T0, True, False, True, 1, 0, 9999.0, rank)


def _catalog_runtime(db, rest):
    return SimpleNamespace(
        db=db,
        rest=rest,
        subscriber=SimpleNamespace(platform="kalshi"),
        resubscribe_event=asyncio.Event(),
    )


def test_market_row_maps_fields_and_composes_title():
    market = {
        "ticker": "KXM-A",
        "title": "Will X win",
        "yes_sub_title": "Yes branch",
        "rules_primary": "primary rules",
        "rules_secondary": "secondary rules",
        "status": "active",
        "event_ticker": "E1",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": "2026-02-01T00:00:00Z",
    }
    event = {"series_ticker": "KXS", "title": "Series title", "event_ticker": "E1"}

    row = _market_row(market, event, "kalshi")
    assert row[0] == "KXM-A"                     # ticker
    assert row[1] == "kalshi"                    # platform
    assert "Will X win" in row[2] and "Yes branch" in row[2]  # subtitle appended
    assert row[3] == "primary rules"             # description <- rules_primary
    assert row[4] == "secondary rules"           # resolution_criteria <- rules_secondary
    assert row[5] == "KXS"                        # series from event
    assert row[6] == "E1"                         # event ticker
    assert row[10] == "active"                    # status


def test_market_row_falls_back_to_event_title_and_primary_rules():
    market = {"ticker": "KXM-B", "rules_primary": "only primary", "status": "active"}
    event = {"series_ticker": "KXS", "title": "Event title"}
    row = _market_row(market, event, "kalshi")
    assert row[2] == "Event title"               # title falls back to event
    assert row[4] == "only primary"              # criteria falls back to rules_primary


async def test_refresh_persists_metadata_and_retires_dropped_market(
    tmp_db, make_fake_rest, make_event, make_market
):
    tmp_db.replace_tracked_series([_tracked_row("KXCAT")])
    rest = make_fake_rest(events=[make_event(
        series="KXCAT", mutex=True,
        markets=[
            make_market("KXCAT-A", status="active", subtitle="A"),
            make_market("KXCAT-B", status="active", subtitle="B"),
        ],
    )])
    rt = _catalog_runtime(tmp_db, rest)

    await CatalogPoller(rt).refresh()
    assert tmp_db.get_active_market_ids() == {"KXCAT-A", "KXCAT-B"}
    assert rt.resubscribe_event.is_set()

    # Next sweep: KXCAT-B has gone (closed upstream). Its row is retained, but it
    # is no longer in the active subscription set.
    rest.events = [make_event(
        series="KXCAT", mutex=True,
        markets=[make_market("KXCAT-A", status="active", subtitle="A")],
    )]
    await CatalogPoller(rt).refresh()

    assert tmp_db.get_active_market_ids() == {"KXCAT-A"}
    with tmp_db._lock:
        ids = {r[0] for r in tmp_db._con.execute("SELECT market_id FROM markets").fetchall()}
    assert ids == {"KXCAT-A", "KXCAT-B"}  # B's history is kept
