"""Environment-driven configuration (the secrets/deploy surface).

Only secrets and deployment-specific values come from the environment / .env.
Everything tunable lives in :mod:`simplex_ingest.constants`.

The four ingest values are required. Two *optional* secrets follow (secrets can't
be constants): ``OPENROUTER_API_KEY`` unlocks the Stage-3 extraction layer
(synchronous path) — absent it, the extraction loop soft-fails (idles) and plain
ingest runs unchanged; ``ANTHROPIC_API_KEY`` additionally unlocks the discounted
Anthropic Message Batches spend path — absent it, batch-routed work degrades to the
synchronous OpenRouter path.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Kalshi hosts per environment. REST base includes the /trade-api/v2 prefix;
# the signed REST path is everything from /trade-api/v2 onward. The WS signed
# path is always WS_SIGNING_PATH regardless of host.
_HOSTS = {
    "prod": {
        "rest": "https://api.elections.kalshi.com/trade-api/v2",
        "ws": "wss://api.elections.kalshi.com/trade-api/ws/v2",
    },
    "demo": {
        "rest": "https://demo-api.kalshi.co/trade-api/v2",
        "ws": "wss://demo-api.kalshi.co/trade-api/ws/v2",
    },
}

WS_SIGNING_PATH = "/trade-api/ws/v2"
"""Path component signed for the WS handshake: signature over ts + 'GET' + this."""


def normalize_pem(raw: str) -> str:
    """Coerce a private key value into canonical PEM text.

    Accepts: real multi-line PEM, a single line with ``\\n`` escapes, a fully
    glued single line (header touching the base64 touching the footer), or a
    filesystem path to a .pem file. Returns standard 64-column PEM so
    ``cryptography`` can always parse it.
    """
    s = raw.strip().strip('"').strip("'").strip()

    # A path to a key file rather than inline material.
    if "-----BEGIN" not in s:
        p = Path(s).expanduser()
        if p.is_file():
            s = p.read_text()
        else:
            raise ValueError(
                "KALSHI_API_SECRET has no PEM header and is not a readable file path"
            )

    # Unescape literal escape sequences that survive single-line transit.
    s = s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")

    m = re.search(
        r"-----BEGIN ([A-Z0-9 ]+?)-----(.*?)-----END \1-----", s, re.DOTALL
    )
    if not m:
        # Header/footer present but malformed; hand the raw text to cryptography.
        return s if s.endswith("\n") else s + "\n"

    label = m.group(1).strip()
    body_b64 = re.sub(r"\s+", "", m.group(2))
    wrapped = "\n".join(body_b64[i : i + 64] for i in range(0, len(body_b64), 64))
    return f"-----BEGIN {label}-----\n{wrapped}\n-----END {label}-----\n"


class Settings(BaseSettings):
    """The five environment inputs, plus derived helpers."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    kalshi_api_key_id: str = Field(..., alias="KALSHI_API_KEY_ID")
    kalshi_api_secret: str = Field(..., alias="KALSHI_API_SECRET")
    kalshi_env: str = Field("demo", alias="KALSHI_ENV")
    simplex_data_dir: Path = Field(Path("./data"), alias="SIMPLEX_DATA_DIR")
    # Optional: enables the Stage-3 extraction layer (synchronous OpenRouter path).
    # Empty => loop idles.
    openrouter_api_key: str = Field("", alias="OPENROUTER_API_KEY")
    # Optional: enables the Anthropic-direct Message Batches spend path (the 50%-off
    # async route for non-latency-sensitive extraction). Empty => batch-routed work
    # degrades to the synchronous OpenRouter path. Independent of the OpenRouter key.
    anthropic_api_key: str = Field("", alias="ANTHROPIC_API_KEY")

    # -- derived ------------------------------------------------------------

    @property
    def env(self) -> str:
        e = self.kalshi_env.strip().lower()
        if e not in _HOSTS:
            raise ValueError(f"KALSHI_ENV must be one of {sorted(_HOSTS)}, got {e!r}")
        return e

    @property
    def rest_base_url(self) -> str:
        return _HOSTS[self.env]["rest"]

    @property
    def ws_url(self) -> str:
        return _HOSTS[self.env]["ws"]

    @property
    def data_dir(self) -> Path:
        return self.simplex_data_dir.expanduser()

    @property
    def db_path(self) -> Path:
        return self.data_dir / "simplex.duckdb"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def load_private_key(self) -> RSAPrivateKey:
        pem = normalize_pem(self.kalshi_api_secret)
        key = load_pem_private_key(pem.encode("utf-8"), password=None)
        if not isinstance(key, RSAPrivateKey):
            raise ValueError("KALSHI_API_SECRET is not an RSA private key")
        return key


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
