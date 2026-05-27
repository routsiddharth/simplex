"""DuckDB access layer.

One shared connection guarded by a single write lock. DuckDB connections are
not safe under *concurrent* use, so every statement goes through ``self._lock``;
because the lock is a ``threading.Lock`` it also serializes calls dispatched via
``asyncio.to_thread`` from the loops. Methods here are synchronous — callers wrap
the blocking ones in ``asyncio.to_thread``.

raw_events writes are buffered and flushed in batches (the WS firehose would
otherwise hammer DuckDB with single-row inserts).
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .events import NormalizedEvent
from .util import naive_utc

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")

_RAW_INSERT = (
    "INSERT INTO raw_events (ts, received_ts, platform, market_id, event_type, "
    "sequence, payload) VALUES (?, ?, ?, ?, ?, ?, ?)"
)

_MARKET_UPSERT = """
INSERT INTO markets (market_id, platform, title, description, resolution_criteria,
    series_ticker, event_ticker, created_at, closes_at, resolved_at, status,
    raw_metadata, last_seen_ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (market_id) DO UPDATE SET
    platform = EXCLUDED.platform,
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    resolution_criteria = EXCLUDED.resolution_criteria,
    series_ticker = EXCLUDED.series_ticker,
    event_ticker = EXCLUDED.event_ticker,
    created_at = EXCLUDED.created_at,
    closes_at = EXCLUDED.closes_at,
    resolved_at = EXCLUDED.resolved_at,
    status = EXCLUDED.status,
    raw_metadata = EXCLUDED.raw_metadata,
    last_seen_ts = EXCLUDED.last_seen_ts
"""  # NB: deliberately does not touch `subscribed` (managed separately).

_SNAPSHOT_UPSERT = """
INSERT INTO snapshots (ts, platform, market_id, yes_bid, yes_ask, yes_mid,
    bid_depth_3c_usd, ask_depth_3c_usd, bid_levels_in_3c, ask_levels_in_3c,
    volume_10s, last_trade_price, status, built_ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (ts, platform, market_id) DO UPDATE SET
    yes_bid = EXCLUDED.yes_bid,
    yes_ask = EXCLUDED.yes_ask,
    yes_mid = EXCLUDED.yes_mid,
    bid_depth_3c_usd = EXCLUDED.bid_depth_3c_usd,
    ask_depth_3c_usd = EXCLUDED.ask_depth_3c_usd,
    bid_levels_in_3c = EXCLUDED.bid_levels_in_3c,
    ask_levels_in_3c = EXCLUDED.ask_levels_in_3c,
    volume_10s = EXCLUDED.volume_10s,
    last_trade_price = EXCLUDED.last_trade_price,
    status = EXCLUDED.status,
    built_ts = EXCLUDED.built_ts
"""

_BOOK_UPSERT = """
INSERT INTO book_state (market_id, last_sequence, last_ts, serialized_book)
VALUES (?, ?, ?, ?)
ON CONFLICT (market_id) DO UPDATE SET
    last_sequence = EXCLUDED.last_sequence,
    last_ts = EXCLUDED.last_ts,
    serialized_book = EXCLUDED.serialized_book
"""

_AUDIT_INSERT = (
    "INSERT INTO audit_results (ts, market_id, status, levels_diff_count, "
    "max_size_delta_pct, action_taken, details_json) VALUES (?, ?, ?, ?, ?, ?, ?)"
)


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(path))
        self._lock = threading.Lock()
        self._buffer: list[tuple[Any, ...]] = []
        self._buffer_lock = threading.Lock()
        self._init_schema()

    # -- schema -------------------------------------------------------------

    def _init_schema(self) -> None:
        raw = _SCHEMA_PATH.read_text()
        # Strip `-- ...` line comments (which may contain semicolons) before
        # splitting statements on ';'.
        lines = [line.split("--", 1)[0] for line in raw.splitlines()]
        sql = "\n".join(lines)
        with self._lock:
            for stmt in sql.split(";"):
                if stmt.strip():
                    self._con.execute(stmt)

    # -- raw_events (buffered) ---------------------------------------------

    def buffer_event(self, ev: NormalizedEvent) -> int:
        row = ev.as_row()
        # Normalize timestamps to naive UTC for DuckDB.
        row = (
            naive_utc(row[0]) if row[0] else None,
            naive_utc(row[1]),
            *row[2:],
        )
        with self._buffer_lock:
            self._buffer.append(row)
            return len(self._buffer)

    def flush_raw_events(self) -> int:
        with self._buffer_lock:
            if not self._buffer:
                return 0
            rows, self._buffer = self._buffer, []
        with self._lock:
            self._con.executemany(_RAW_INSERT, rows)
        return len(rows)

    # -- markets ------------------------------------------------------------

    def upsert_markets(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        with self._lock:
            self._con.executemany(_MARKET_UPSERT, rows)

    def set_active_set(self, active_ids: set[str]) -> None:
        """Mark exactly ``active_ids`` subscribed=true, everything else false.

        Closed/removed markets keep their row (and history); only the flag flips.
        """
        with self._lock:
            self._con.execute("UPDATE markets SET subscribed = FALSE")
            if active_ids:
                ids = list(active_ids)
                ph = ", ".join("?" * len(ids))
                self._con.execute(
                    f"UPDATE markets SET subscribed = TRUE WHERE market_id IN ({ph})",
                    ids,
                )

    def get_active_market_ids(self) -> set[str]:
        with self._lock:
            rows = self._con.execute(
                "SELECT market_id FROM markets WHERE subscribed = TRUE"
            ).fetchall()
        return {r[0] for r in rows}

    def get_active_markets(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._con.execute(
                "SELECT market_id, status FROM markets WHERE subscribed = TRUE"
            ).fetchall()
        return [{"market_id": r[0], "status": r[1]} for r in rows]

    # -- snapshots ----------------------------------------------------------

    def upsert_snapshots(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        with self._lock:
            self._con.executemany(_SNAPSHOT_UPSERT, rows)

    # -- book_state ---------------------------------------------------------

    def save_book_states(self, rows: list[tuple[str, int | None, datetime | None, str]]) -> None:
        if not rows:
            return
        norm = [(m, seq, naive_utc(ts) if ts else None, blob) for (m, seq, ts, blob) in rows]
        with self._lock:
            self._con.executemany(_BOOK_UPSERT, norm)

    def load_book_states(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            rows = self._con.execute(
                "SELECT market_id, last_sequence, last_ts, serialized_book FROM book_state"
            ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for market_id, last_seq, last_ts, blob in rows:
            out[market_id] = {
                "last_sequence": last_seq,
                "last_ts": last_ts,
                "book": json.loads(blob) if blob else {},
            }
        return out

    # -- audit --------------------------------------------------------------

    def insert_audit_results(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        norm = [(naive_utc(r[0]), *r[1:]) for r in rows]
        with self._lock:
            self._con.executemany(_AUDIT_INSERT, norm)

    # -- snapshot-builder queries ------------------------------------------

    def fetch_events_after(
        self, cursor_ts: datetime, cursor_rowid: int, limit: int
    ) -> list[dict[str, Any]]:
        """Events strictly after (received_ts, rowid), in ingest order."""
        cts = naive_utc(cursor_ts)
        with self._lock:
            rows = self._con.execute(
                """
                SELECT rowid, ts, received_ts, market_id, event_type, sequence, payload
                FROM raw_events
                WHERE received_ts > ? OR (received_ts = ? AND rowid > ?)
                ORDER BY received_ts, rowid
                LIMIT ?
                """,
                [cts, cts, cursor_rowid, limit],
            ).fetchall()
        return [
            {
                "rowid": r[0],
                "ts": r[1],
                "received_ts": r[2],
                "market_id": r[3],
                "event_type": r[4],
                "sequence": r[5],
                "payload": json.loads(r[6]) if r[6] else {},
            }
            for r in rows
        ]

    def window_trade_volume(
        self, window_start: datetime, window_end: datetime
    ) -> dict[str, float]:
        """Sum of trade contract counts per market in [start, end) by received_ts."""
        ws, we = naive_utc(window_start), naive_utc(window_end)
        with self._lock:
            rows = self._con.execute(
                """
                SELECT market_id,
                       SUM(CAST(json_extract_string(payload, '$.count') AS DOUBLE))
                FROM raw_events
                WHERE event_type = 'trade' AND received_ts >= ? AND received_ts < ?
                GROUP BY market_id
                """,
                [ws, we],
            ).fetchall()
        return {r[0]: float(r[1] or 0.0) for r in rows}

    def latest_trade_prices(self, market_ids: Iterable[str]) -> dict[str, float]:
        ids = list(market_ids)
        if not ids:
            return {}
        ph = ", ".join("?" * len(ids))
        with self._lock:
            rows = self._con.execute(
                f"""
                SELECT market_id,
                       arg_max(CAST(json_extract_string(payload, '$.price') AS DOUBLE),
                               received_ts)
                FROM raw_events
                WHERE event_type = 'trade' AND market_id IN ({ph})
                GROUP BY market_id
                """,
                ids,
            ).fetchall()
        return {r[0]: float(r[1]) for r in rows if r[1] is not None}

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        self.flush_raw_events()
        with self._lock:
            self._con.close()
