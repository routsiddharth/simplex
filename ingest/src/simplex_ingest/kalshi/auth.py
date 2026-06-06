"""Kalshi request signing (RSA-PSS / SHA256).

Confirmed against docs.kalshi.com: three headers — KALSHI-ACCESS-KEY (key id),
KALSHI-ACCESS-TIMESTAMP (Unix ms), KALSHI-ACCESS-SIGNATURE (base64). The signed
message is ``timestamp + METHOD + path`` where path is the URL path with no
query string. Signature is RSA-PSS, MGF1(SHA256), salt length = digest length.
"""

from __future__ import annotations

import base64
import time

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


class KalshiSigner:
    def __init__(self, key_id: str, private_key: RSAPrivateKey) -> None:
        self._key_id = key_id
        self._key = private_key

    def _sign(self, message: str) -> str:
        sig = self._key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("ascii")

    def headers(self, method: str, path: str) -> dict[str, str]:
        """Auth headers for an HTTP request or WS handshake.

        ``path`` must be the path component only (no query string), e.g.
        ``/trade-api/v2/events`` or ``/trade-api/ws/v2``.
        """
        ts_ms = str(int(time.time() * 1000))
        signature = self._sign(f"{ts_ms}{method.upper()}{path}")
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": signature,
        }
