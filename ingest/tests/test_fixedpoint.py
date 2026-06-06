"""Kalshi fixed-point decoding — the one module that knows the exchange's
numeric surface. A units question lands here, so it gets exhaustive coverage."""

from __future__ import annotations

from simplex_ingest.kalshi.fixedpoint import (
    in_tradeable_band, level_map, levels, to_float, volume,
)


# -- to_float ---------------------------------------------------------------

def test_to_float_parses_and_guards():
    assert to_float("0.40") == 0.40
    assert to_float(5) == 5.0
    assert to_float(None) is None
    assert to_float("nope") is None
    assert to_float(object()) is None


# -- levels (WS list) -------------------------------------------------------

def test_levels_parses_pairs_and_skips_garbage():
    out = levels([["0.40", "100"], ["0.55", "80"], ["bad"], "nope", [1], None])
    assert out == [(0.40, 100.0), (0.55, 80.0)]
    assert levels(None) == []


# -- level_map (REST dict, quantized keys) ----------------------------------

def test_level_map_quantizes_keys():
    out = level_map([["0.400000001", "100"], ["0.55", "80"]])
    assert out == {0.4: 100.0, 0.55: 80.0}      # price rounded to 6dp
    assert level_map(None) == {}


def test_level_map_drops_out_of_band_levels():
    # 0c/100c extremes are dropped so the REST audit book matches the in-memory
    # book (which excludes them at apply time) — no phantom structural diff.
    out = level_map([["0.00", "99"], ["0.40", "100"], ["1.00", "5"]])
    assert out == {0.40: 100.0}


# -- in_tradeable_band ------------------------------------------------------

def test_in_tradeable_band_bounds():
    assert in_tradeable_band(0.01) and in_tradeable_band(0.99)   # inclusive bounds
    assert in_tradeable_band(0.50)
    assert not in_tradeable_band(0.0) and not in_tradeable_band(1.0)
    assert not in_tradeable_band(5.6)                            # decode-glitch territory


# -- volume -----------------------------------------------------------------

def test_volume_prefers_fp_then_falls_back():
    assert volume({"volume_fp": 5, "volume": 9}) == 5.0
    assert volume({"volume": 9}) == 9.0
    assert volume({"volume_24h_fp": 3}) == 3.0
    assert volume({}) == 0.0
    assert volume({"volume_fp": "bad", "volume": 2}) == 2.0   # bad key falls through
