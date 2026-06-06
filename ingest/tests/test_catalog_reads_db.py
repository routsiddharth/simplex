"""Phase B regression: the catalog poller reads tracked_series, not the YAML."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from types import SimpleNamespace

from simplex_ingest.loops.catalog import CatalogPoller

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


async def test_refresh_expands_tracked_series(tmp_db, make_fake_rest, make_event, make_market):
    tmp_db.replace_tracked_series([_tracked_row("KXCAT")])
    rest = make_fake_rest(events=[
        make_event(
            series="KXCAT",
            mutex=True,
            markets=[
                make_market("KXCAT-A", status="active", subtitle="A", volume=1000),
                make_market("KXCAT-B", status="active", subtitle="B", volume=1000),
                make_market("KXCAT-C", status="settled", subtitle="C"),  # not tradeable
            ],
        )
    ])
    rt = _catalog_runtime(tmp_db, rest)

    await CatalogPoller(rt).refresh()

    assert tmp_db.get_active_market_ids() == {"KXCAT-A", "KXCAT-B"}
    assert rt.resubscribe_event.is_set()


async def test_refresh_soft_fails_on_empty_tracked_set(tmp_db, make_fake_rest, caplog):
    rest = make_fake_rest(events=[])  # would be unused
    rt = _catalog_runtime(tmp_db, rest)

    with caplog.at_level(logging.WARNING):
        await CatalogPoller(rt).refresh()  # no exception

    assert rest.calls == 0                       # never hit REST
    assert tmp_db.get_active_market_ids() == set()
    assert not rt.resubscribe_event.is_set()
    assert any("no tracked series" in r.getMessage() for r in caplog.records)
