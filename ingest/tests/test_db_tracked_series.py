"""DuckDB integration tests for the tracked_series table operations."""

from __future__ import annotations

from datetime import datetime

import pytest

T0 = datetime(2026, 1, 1, 0, 0, 0)
T1 = datetime(2026, 2, 1, 0, 0, 0)


def _row(ticker, rank, admitted=T0, volume=1000.0):
    return (ticker, admitted, admitted, True, False, True, 1, 0, volume, rank)


def _admitted_at(db) -> dict[str, datetime]:
    with db._lock:
        rows = db._con.execute(
            "SELECT series_ticker, admitted_at FROM tracked_series"
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def test_get_tracked_series_empty(tmp_db):
    assert tmp_db.get_tracked_series() == []


def test_get_tracked_series_in_rank_order(tmp_db):
    # Insert deliberately out of rank order.
    tmp_db.replace_tracked_series([_row("C", 3), _row("A", 1), _row("B", 2)])
    assert tmp_db.get_tracked_series() == ["A", "B", "C"]


def test_replace_preserves_admitted_at(tmp_db):
    tmp_db.replace_tracked_series([_row("A", 1, T0), _row("B", 2, T0), _row("C", 3, T0)])
    # Re-admit B and C, drop A, add D — all stamped with the newer T1.
    tmp_db.replace_tracked_series([_row("B", 1, T1), _row("C", 2, T1), _row("D", 3, T1)])

    admitted = _admitted_at(tmp_db)
    assert set(admitted) == {"B", "C", "D"}
    assert admitted["B"] == T0  # preserved across re-admit
    assert admitted["C"] == T0
    assert admitted["D"] == T1  # newly admitted


def test_replace_is_atomic_on_bad_row(tmp_db):
    tmp_db.replace_tracked_series([_row("A", 1), _row("B", 2), _row("C", 3)])

    bad = (None, T0, T0, True, False, True, 1, 0, 1000.0, 2)  # NULL primary key
    with pytest.raises(Exception):
        tmp_db.replace_tracked_series([_row("X", 1), bad])

    # Table is exactly its prior contents — no partial write of "X".
    assert tmp_db.get_tracked_series() == ["A", "B", "C"]


def test_replace_empty_is_noop(tmp_db):
    tmp_db.replace_tracked_series([_row("A", 1)])
    tmp_db.replace_tracked_series([])  # guarded no-op: must not wipe
    assert tmp_db.get_tracked_series() == ["A"]
