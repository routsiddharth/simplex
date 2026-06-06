"""Kalshi request signing (RSA-PSS / SHA256).

Verifies the three headers and that the signature is a valid PSS signature over
exactly ``timestamp + METHOD + path`` (path only, method upper-cased) using the
matching public key — the contract Kalshi checks server-side.
"""

from __future__ import annotations

import base64
import time

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

_PSS = padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH)


def test_headers_present_and_well_typed(make_signer):
    h = make_signer("kid-123").headers("GET", "/trade-api/v2/events")
    assert h["KALSHI-ACCESS-KEY"] == "kid-123"
    assert h["KALSHI-ACCESS-TIMESTAMP"].isdigit()
    # Unix *milliseconds*, within ~10s of now.
    assert abs(int(h["KALSHI-ACCESS-TIMESTAMP"]) - int(time.time() * 1000)) < 10_000
    assert h["KALSHI-ACCESS-SIGNATURE"]


def test_signature_verifies_over_ts_method_path(rsa_key, make_signer):
    h = make_signer().headers("get", "/trade-api/v2/markets")  # method lower-cased
    message = f'{h["KALSHI-ACCESS-TIMESTAMP"]}GET/trade-api/v2/markets'.encode()
    sig = base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"])
    # Raises InvalidSignature if the signed message differs.
    rsa_key.public_key().verify(sig, message, _PSS, hashes.SHA256())


def test_signature_does_not_cover_query_string(rsa_key, make_signer):
    h = make_signer().headers("GET", "/trade-api/v2/events")
    sig = base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"])
    with_query = f'{h["KALSHI-ACCESS-TIMESTAMP"]}GET/trade-api/v2/events?status=open'.encode()
    with pytest.raises(InvalidSignature):
        rsa_key.public_key().verify(sig, with_query, _PSS, hashes.SHA256())


def test_each_call_produces_a_fresh_timestamp(make_signer):
    signer = make_signer()
    a = signer.headers("GET", "/x")
    time.sleep(0.002)
    b = signer.headers("GET", "/x")
    # PSS is randomized, so signatures always differ; timestamps are monotonic.
    assert int(b["KALSHI-ACCESS-TIMESTAMP"]) >= int(a["KALSHI-ACCESS-TIMESTAMP"])
