"""DB-layer tests for the extraction tables (idempotency + review preservation)."""

from __future__ import annotations

import json
from datetime import datetime

from simplex_ingest import constants as C

T0 = datetime(2026, 1, 1)
V = C.EXTRACTION_PROMPT_VERSION


def _market(mid, *, series=None, event=None):
    return (mid, C.PLATFORM, f"{mid} t", "d", "r", series, event, T0, None, None, "active", "{}", T0)


def _sem_row(mid, *, entities=(), version=V):
    return (mid, C.PLATFORM, "ev", "yes", "no", "soon",
            json.dumps(list(entities)), json.dumps([]), C.EXTRACTION_MODEL, version, T0, "{}")


def _edge_row(a, b, *, tier="soft", agreement="single", rel="correlated", version=V):
    return (C.PLATFORM, a, b, rel, "none", 0.7, tier, agreement, "why",
            C.PAIR_MODEL, None, version, T0, "{}")


def test_semantics_upsert_is_idempotent(tmp_db):
    tmp_db.upsert_markets([_market("M1")])
    tmp_db.set_active_set({"M1"})
    tmp_db.upsert_market_semantics([_sem_row("M1", entities=["a"])])
    tmp_db.upsert_market_semantics([_sem_row("M1", entities=["a", "b"])])  # re-extract

    markets = tmp_db.get_active_markets_with_semantics()
    assert len(markets) == 1
    assert markets[0]["entities"] == ["a", "b"]  # latest wins, one row


def test_missing_semantics_respects_version_and_subscription(tmp_db):
    tmp_db.upsert_markets([_market("A"), _market("B")])
    tmp_db.set_active_set({"A"})  # B not subscribed
    tmp_db.upsert_market_semantics([_sem_row("A")])

    # A is current-version & subscribed -> not missing; B unsubscribed -> not listed.
    assert tmp_db.get_markets_missing_semantics(V) == []
    # Bump version -> A becomes stale (re-extract), B still unsubscribed.
    assert [m["market_id"] for m in tmp_db.get_markets_missing_semantics(V + 1)] == ["A"]


def test_edge_upsert_idempotent_on_canonical_pair(tmp_db):
    tmp_db.upsert_edges([_edge_row("A", "B", tier="soft")])
    tmp_db.upsert_edges([_edge_row("A", "B", tier="trusted", agreement="agreed")])

    edges = tmp_db.get_edges_for_pairs([("A", "B")])
    assert len(edges) == 1
    assert edges[("A", "B")]["trust_tier"] == "trusted"


def test_get_classified_pairs_filters_by_version(tmp_db):
    tmp_db.upsert_edges([_edge_row("A", "B", version=V), _edge_row("A", "C", version=V + 9)])
    assert tmp_db.get_classified_pairs(V) == {("A", "B")}


def test_reclassification_preserves_human_review_decision(tmp_db):
    # An edge lands in the review queue and a human approves it.
    tmp_db.upsert_edges([_edge_row("A", "B", tier="review", rel="implies")])
    with tmp_db._lock:
        tmp_db._con.execute(
            "UPDATE market_edges SET review_status = 'approved', reviewed_by = 'sid' "
            "WHERE market_id_a = 'A' AND market_id_b = 'B'"
        )
    assert tmp_db.pending_review_edges() == []  # no longer pending

    # A later cycle re-classifies the same pair; the review decision must survive.
    tmp_db.upsert_edges([_edge_row("A", "B", tier="soft", rel="correlated")])
    edge = tmp_db.get_edges_for_pairs([("A", "B")])[("A", "B")]
    assert edge["trust_tier"] == "soft"          # classification updated
    assert edge["review_status"] == "approved"   # human decision preserved
