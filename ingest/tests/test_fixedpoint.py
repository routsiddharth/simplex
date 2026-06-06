"""Kalshi fixed-point decoding — the one module that knows the exchange's
numeric surface. A units question lands here, so it gets exhaustive coverage."""

from __future__ import annotations

from simplex_ingest.kalshi.fixedpoint import level_map, levels, to_float, volume


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


# -- volume -----------------------------------------------------------------

def test_volume_prefers_fp_then_falls_back():
    assert volume({"volume_fp": 5, "volume": 9}) == 5.0
    assert volume({"volume": 9}) == 9.0
    assert volume({"volume_24h_fp": 3}) == 3.0
    assert volume({}) == 0.0
    assert volume({"volume_fp": "bad", "volume": 2}) == 2.0   # bad key falls through
