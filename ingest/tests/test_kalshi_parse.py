"""KalshiSubscriber.parse: normalization of each WS message type, plus the
hard invariant that a malformed message never raises (log-and-drop)."""

from __future__ import annotations

import pytest

from simplex_ingest.events import EventType
from simplex_ingest.kalshi.subscriber import KalshiSubscriber


@pytest.fixture
def sub() -> KalshiSubscriber:
    # parse() never touches the signer; a placeholder is fine.
    return KalshiSubscriber(signer=object(), ws_url="wss://example/trade-api/ws/v2")


def test_parse_orderbook_snapshot(sub):
    msg = {"type": "orderbook_snapshot", "seq": 1, "msg": {
        "market_ticker": "M", "yes_dollars_fp": [["0.40", "100"]], "no_dollars_fp": [["0.55", "80"]]}}
    (ev,) = sub.parse(msg)
    assert ev.event_type is EventType.ORDERBOOK_SNAPSHOT
    assert ev.market_id == "M"
    assert ev.sequence == 1
    assert ev.payload["yes"] == [(0.40, 100.0)]
    assert ev.payload["no"] == [(0.55, 80.0)]


def test_parse_orderbook_delta(sub):
    msg = {"type": "orderbook_delta", "seq": 2, "msg": {
        "market_ticker": "M", "side": "yes", "price_dollars": "0.41", "delta_fp": "5"}}
    (ev,) = sub.parse(msg)
    assert ev.event_type is EventType.ORDERBOOK_DELTA
    assert ev.payload == {"side": "yes", "price": 0.41, "delta": 5.0}


def test_parse_trade(sub):
    msg = {"type": "trade", "seq": 3, "msg": {
        "market_ticker": "M", "yes_price_dollars": "0.42", "count_fp": "10", "taker_side": "yes"}}
    (ev,) = sub.parse(msg)
    assert ev.event_type is EventType.TRADE
    assert ev.payload["price"] == 0.42
    assert ev.payload["count"] == 10.0


def test_parse_lifecycle(sub):
    msg = {"type": "market_lifecycle_v2", "msg": {"market_ticker": "M", "event_type": "activated"}}
    (ev,) = sub.parse(msg)
    assert ev.event_type is EventType.LIFECYCLE
    assert ev.payload["event_type"] == "activated"


def test_parse_drops_control_and_missing_ticker(sub):
    assert sub.parse({"type": "subscribed", "msg": {"sid": 1}}) == []
    assert sub.parse({"type": "orderbook_snapshot", "msg": {"yes_dollars_fp": []}}) == []
    assert sub.parse({"type": "unknown_channel", "msg": {"market_ticker": "M"}}) == []


@pytest.mark.parametrize("bad", [
    {},
    {"type": "trade"},                              # no msg
    {"type": "trade", "msg": [1, 2, 3]},            # msg not a dict -> .get raises internally
    {"type": "orderbook_delta", "msg": {"market_ticker": "M", "price_dollars": object()}},
    {"type": "orderbook_snapshot", "msg": {"market_ticker": "M", "yes_dollars_fp": "garbage"}},
    {"type": None},
])
def test_parse_never_raises_on_malformed(sub, bad):
    # Invariant: subscribers log-and-drop; they must never crash the WS reader.
    assert isinstance(sub.parse(bad), list)
