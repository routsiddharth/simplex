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

_TRACKED_UPSERT = """
INSERT INTO tracked_series (series_ticker, admitted_at, last_check_at, passes_p1,
    passes_p2, passes_p3, n_partition_events, n_hierarchy_events, volume_24h, rank_position)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (series_ticker) DO UPDATE SET
    last_check_at = EXCLUDED.last_check_at,
    passes_p1 = EXCLUDED.passes_p1,
    passes_p2 = EXCLUDED.passes_p2,
    passes_p3 = EXCLUDED.passes_p3,
    n_partition_events = EXCLUDED.n_partition_events,
    n_hierarchy_events = EXCLUDED.n_hierarchy_events,
    volume_24h = EXCLUDED.volume_24h,
    rank_position = EXCLUDED.rank_position
"""  # NB: admitted_at deliberately untouched on conflict (preserved from first admit).

_SEMANTICS_UPSERT = """
INSERT INTO market_semantics (market_id, platform, underlying_event, resolves_yes_when,
    resolves_no_when, resolution_timing, entities, dependencies, model,
    extraction_version, extracted_at, raw_response)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (market_id) DO UPDATE SET
    platform = EXCLUDED.platform,
    underlying_event = EXCLUDED.underlying_event,
    resolves_yes_when = EXCLUDED.resolves_yes_when,
    resolves_no_when = EXCLUDED.resolves_no_when,
    resolution_timing = EXCLUDED.resolution_timing,
    entities = EXCLUDED.entities,
    dependencies = EXCLUDED.dependencies,
    model = EXCLUDED.model,
    extraction_version = EXCLUDED.extraction_version,
    extracted_at = EXCLUDED.extracted_at,
    raw_response = EXCLUDED.raw_response
"""

_EDGE_UPSERT = """
INSERT INTO market_edges (platform, market_id_a, market_id_b, relationship_type,
    direction, confidence, trust_tier, agreement_status, rationale, model,
    verify_model, extraction_version, classified_at, raw_response)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (platform, market_id_a, market_id_b) DO UPDATE SET
    relationship_type = EXCLUDED.relationship_type,
    direction = EXCLUDED.direction,
    confidence = EXCLUDED.confidence,
    trust_tier = EXCLUDED.trust_tier,
    agreement_status = EXCLUDED.agreement_status,
    rationale = EXCLUDED.rationale,
    model = EXCLUDED.model,
    verify_model = EXCLUDED.verify_model,
    extraction_version = EXCLUDED.extraction_version,
    classified_at = EXCLUDED.classified_at,
    raw_response = EXCLUDED.raw_response
"""  # NB: review_status/reviewed_* deliberately untouched — a re-classification
     # must not silently clobber a human review decision on the same pair.


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
        # This module owns the raw_events column order: build the row from the
        # event's named fields right next to _RAW_INSERT, so the two can't drift
        # (timestamps normalized to naive UTC for DuckDB).
        row = (
            naive_utc(ev.ts) if ev.ts else None,
            naive_utc(ev.received_ts),
            ev.platform,
            ev.market_id,
            ev.event_type.value,
            ev.sequence,
            json.dumps(ev.payload, default=str),
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

    # -- tracked_series -----------------------------------------------------

    def get_tracked_series(self) -> list[str]:
        """Tracked series tickers in rank order (best first). [] if empty."""
        with self._lock:
            rows = self._con.execute(
                "SELECT series_ticker FROM tracked_series ORDER BY rank_position"
            ).fetchall()
        return [r[0] for r in rows]

    def replace_tracked_series(self, rows: list[tuple[Any, ...]]) -> None:
        """Atomically swap the tracked set to exactly ``rows``.

        Each row is
        ``(series_ticker, admitted_at, last_check_at, p1, p2, p3,
        n_partition_events, n_hierarchy_events, volume_24h, rank_position)``;
        ``admitted_at`` is the discovery cycle's "now" and is used only for
        rows new to the table — existing rows keep their original
        ``admitted_at`` (the upsert leaves it untouched). The whole swap runs in
        one transaction: any bad row rolls the table back to its prior contents.

        An empty ``rows`` would wipe the table, so callers must guard against
        replacing on a transient empty sweep (see the discovery loop).
        """
        if not rows:
            return
        norm = [(r[0], naive_utc(r[1]), naive_utc(r[2]), *r[3:]) for r in rows]
        keep = [r[0] for r in norm]
        placeholders = ", ".join("?" * len(keep))
        with self._lock:
            self._con.execute("BEGIN TRANSACTION")
            try:
                self._con.executemany(_TRACKED_UPSERT, norm)
                self._con.execute(
                    f"DELETE FROM tracked_series WHERE series_ticker NOT IN ({placeholders})",
                    keep,
                )
                self._con.execute("COMMIT")
            except Exception:
                self._con.execute("ROLLBACK")
                raise

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

    # -- extraction layer (market_semantics / market_edges) ----------------

    def upsert_market_semantics(self, rows: list[tuple[Any, ...]]) -> None:
        """Idempotent upsert of per-market semantic records.

        Each row is ``(market_id, platform, underlying_event, resolves_yes_when,
        resolves_no_when, resolution_timing, entities, dependencies, model,
        extraction_version, extracted_at, raw_response)``; entities/dependencies/
        raw_response are JSON strings, extracted_at a datetime."""
        if not rows:
            return
        norm = [(*r[:10], naive_utc(r[10]) if r[10] else None, r[11]) for r in rows]
        with self._lock:
            self._con.executemany(_SEMANTICS_UPSERT, norm)

    def get_markets_missing_semantics(self, version: int) -> list[dict[str, Any]]:
        """Active (subscribed) markets with no current-version semantics row.

        Returns the text fields the extractor needs. A market whose semantics were
        written at an older ``extraction_version`` is re-listed (the prompt/schema
        moved on); same-version rows are skipped (cached forever otherwise)."""
        with self._lock:
            rows = self._con.execute(
                """
                SELECT m.market_id, m.title, m.description, m.resolution_criteria
                FROM markets m
                LEFT JOIN market_semantics s ON s.market_id = m.market_id
                WHERE m.subscribed = TRUE
                  AND (s.market_id IS NULL OR s.extraction_version IS DISTINCT FROM ?)
                """,
                [version],
            ).fetchall()
        return [
            {"market_id": r[0], "title": r[1], "description": r[2], "resolution_criteria": r[3]}
            for r in rows
        ]

    def get_active_markets_with_semantics(self) -> list[dict[str, Any]]:
        """Active markets that already have a semantics row, joined with the
        hierarchy keys + entities the pair stage needs (candidate gen + prompt)."""
        with self._lock:
            rows = self._con.execute(
                """
                SELECT m.market_id, m.title, m.series_ticker, m.event_ticker,
                       s.underlying_event, s.resolves_yes_when, s.resolves_no_when,
                       s.resolution_timing, s.entities, s.dependencies
                FROM markets m
                JOIN market_semantics s ON s.market_id = m.market_id
                WHERE m.subscribed = TRUE
                """
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "market_id": r[0],
                    "title": r[1],
                    "series_ticker": r[2],
                    "event_ticker": r[3],
                    "underlying_event": r[4],
                    "resolves_yes_when": r[5],
                    "resolves_no_when": r[6],
                    "resolution_timing": r[7],
                    "entities": json.loads(r[8]) if r[8] else [],
                    "dependencies": json.loads(r[9]) if r[9] else [],
                }
            )
        return out

    def get_classified_pairs(self, version: int) -> set[tuple[str, str]]:
        """Canonical (a, b) pairs already classified at ``version`` — skip these."""
        with self._lock:
            rows = self._con.execute(
                "SELECT market_id_a, market_id_b FROM market_edges WHERE extraction_version = ?",
                [version],
            ).fetchall()
        return {(r[0], r[1]) for r in rows}

    def upsert_edges(self, rows: list[tuple[Any, ...]]) -> None:
        """Idempotent upsert of typed edges, keyed on the canonical pair.

        Each row is ``(platform, market_id_a, market_id_b, relationship_type,
        direction, confidence, trust_tier, agreement_status, rationale, model,
        verify_model, extraction_version, classified_at, raw_response)`` with
        ``market_id_a < market_id_b``. Human review columns are never touched."""
        if not rows:
            return
        norm = [(*r[:12], naive_utc(r[12]) if r[12] else None, r[13]) for r in rows]
        with self._lock:
            self._con.executemany(_EDGE_UPSERT, norm)

    _EDGE_COLS = (
        "platform, market_id_a, market_id_b, relationship_type, direction, "
        "confidence, trust_tier, agreement_status, rationale, model, verify_model, "
        "extraction_version, classified_at, review_status"
    )

    def _edge_dicts(self, rows: list[tuple]) -> list[dict[str, Any]]:
        keys = [c.strip() for c in self._EDGE_COLS.split(",")]
        return [dict(zip(keys, r)) for r in rows]

    def get_edges_for_pairs(self, pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
        """Edge rows for the given canonical (a, b) pairs, keyed by the pair."""
        pl = list(pairs)
        if not pl:
            return {}
        out: dict[tuple[str, str], dict[str, Any]] = {}
        with self._lock:
            for a, b in pl:
                row = self._con.execute(
                    f"SELECT {self._EDGE_COLS} FROM market_edges "
                    "WHERE market_id_a = ? AND market_id_b = ?",
                    [a, b],
                ).fetchone()
                if row is not None:
                    out[(a, b)] = self._edge_dicts([row])[0]
        return out

    def pending_review_edges(self) -> list[dict[str, Any]]:
        """The manual-review queue: review-tier edges still awaiting a decision."""
        with self._lock:
            rows = self._con.execute(
                f"SELECT {self._EDGE_COLS} FROM market_edges "
                "WHERE trust_tier = 'review' AND review_status = 'pending' "
                "ORDER BY confidence DESC"
            ).fetchall()
        return self._edge_dicts(rows)

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

    # -- retention ----------------------------------------------------------

    # The regenerable/append-only time-series tables and the timestamp column
    # each is pruned on. The durable LLM-derived graph (market_semantics,
    # market_edges) and the self-managed catalog (markets, tracked_series) are
    # deliberately absent — they are not time-series and are not pruned here.
    _RETENTION_TABLES = (
        ("raw_events", "received_ts"),
        ("snapshots", "ts"),
        ("audit_results", "ts"),
        ("book_state", "last_ts"),
    )

    def prune_time_series(self, cutoff: datetime) -> dict[str, int]:
        """Delete time-series rows older than ``cutoff`` (naive UTC). Returns the
        per-table deleted-row counts.

        This is the retention seam: it bounds the volume to a rolling window
        instead of letting the append-only log grow without limit. ``raw_events``
        stops being a permanent source of truth — it is the source of truth only
        within the retention window (see ARCHITECTURE §9). Flushes the raw_events
        buffer first so freshly-received-but-unwritten events can't be missed and
        then immediately resurrected past the cutoff."""
        self.flush_raw_events()
        co = naive_utc(cutoff)
        deleted: dict[str, int] = {}
        with self._lock:
            for table, ts_col in self._RETENTION_TABLES:
                before = self._con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                # last_ts can be NULL (a checkpoint never anchored); keep those.
                self._con.execute(f"DELETE FROM {table} WHERE {ts_col} < ?", [co])
                after = self._con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                deleted[table] = before - after
        return deleted

    # -- resolution (graph retention) --------------------------------------

    def graph_markets_pending_resolution(self, limit: int) -> list[str]:
        """Markets that are in the LLM graph, no longer subscribed, and whose
        resolution time is not yet known — the set to reconcile against Kalshi.

        A still-subscribed market is live (not resolved); one we already have a
        ``resolved_at`` for needs no re-check. Capped at ``limit`` so a backlog
        bounds the REST calls per cycle."""
        with self._lock:
            rows = self._con.execute(
                """
                SELECT m.market_id
                FROM markets m
                WHERE m.subscribed = FALSE
                  AND m.resolved_at IS NULL
                  AND (m.market_id IN (SELECT market_id FROM market_semantics)
                       OR m.market_id IN (SELECT market_id_a FROM market_edges)
                       OR m.market_id IN (SELECT market_id_b FROM market_edges))
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        return [r[0] for r in rows]

    def mark_resolved(self, rows: list[tuple[str, datetime]]) -> int:
        """Persist ``resolved_at`` for the given (market_id, ts) pairs, but never
        overwrite an existing value (first known resolution time wins)."""
        if not rows:
            return 0
        norm = [(naive_utc(ts), mid) for (mid, ts) in rows]
        with self._lock:
            self._con.executemany(
                "UPDATE markets SET resolved_at = ? WHERE market_id = ? AND resolved_at IS NULL",
                norm,
            )
            n = self._con.execute(
                "SELECT count(*) FROM markets WHERE resolved_at IS NOT NULL"
            ).fetchone()[0]
        return n

    def prune_resolved_graph(self, cutoff: datetime) -> dict[str, int]:
        """Delete the LLM graph (semantics + edges) for markets that resolved
        before ``cutoff``. An edge goes if *either* endpoint has resolved — a
        resolved leg can't be a live coherence constraint. Returns deleted counts.

        This is the one exception to the graph's keep-forever durability: a
        resolved market is terminal (it never reopens), so its semantics/edges
        are dead weight for the live engine — see ARCHITECTURE §5/§9."""
        co = naive_utc(cutoff)
        with self._lock:
            resolved = [
                r[0] for r in self._con.execute(
                    "SELECT market_id FROM markets WHERE resolved_at IS NOT NULL AND resolved_at < ?",
                    [co],
                ).fetchall()
            ]
            if not resolved:
                return {"market_semantics": 0, "market_edges": 0}
            ph = ", ".join("?" * len(resolved))
            sem_before = self._con.execute("SELECT count(*) FROM market_semantics").fetchone()[0]
            edge_before = self._con.execute("SELECT count(*) FROM market_edges").fetchone()[0]
            self._con.execute(
                f"DELETE FROM market_semantics WHERE market_id IN ({ph})", resolved
            )
            self._con.execute(
                f"DELETE FROM market_edges WHERE market_id_a IN ({ph}) OR market_id_b IN ({ph})",
                resolved + resolved,
            )
            sem_after = self._con.execute("SELECT count(*) FROM market_semantics").fetchone()[0]
            edge_after = self._con.execute("SELECT count(*) FROM market_edges").fetchone()[0]
        return {"market_semantics": sem_before - sem_after, "market_edges": edge_before - edge_after}

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        self.flush_raw_events()
        with self._lock:
            self._con.close()
