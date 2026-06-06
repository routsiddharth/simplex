"""Process wiring (app.build_runtime).

The Stage-3 optionality is the load-bearing behavior: with no OPENROUTER_API_KEY
the LLM client is not constructed (rt.llm is None -> extraction idles); with one,
it is. Also checks the runtime is fully wired and the DuckDB schema is initialized
on the configured data dir. No network is touched (clients are built, not used).
"""

from __future__ import annotations

from cryptography.hazmat.primitives import serialization

from simplex_ingest.app import build_runtime
from simplex_ingest.config import Settings
from simplex_ingest.llm import OpenRouterClient


def _settings(tmp_path, rsa_key, openrouter=""):
    pem = rsa_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return Settings(
        _env_file=None,
        KALSHI_API_KEY_ID="kid",
        KALSHI_API_SECRET=pem,
        KALSHI_ENV="demo",
        SIMPLEX_DATA_DIR=str(tmp_path / "data"),
        OPENROUTER_API_KEY=openrouter,
    )


async def _aclose(rt):
    await rt.rest.aclose()
    await rt.audit_rest.aclose()
    if rt.llm is not None:
        await rt.llm.aclose()
    rt.db.close()


async def test_build_runtime_without_key_disables_extraction(tmp_path, rsa_key):
    rt = build_runtime(_settings(tmp_path, rsa_key))
    try:
        assert rt.llm is None  # extraction soft-fails / idles
        assert rt.subscriber.platform == "kalshi"
        assert rt.rest is not None and rt.audit_rest is not None
        assert rt.settings.db_path.exists()  # schema initialized on disk
        # Schema is live: a known table is queryable.
        with rt.db._lock:
            rt.db._con.execute("SELECT count(*) FROM raw_events").fetchone()
    finally:
        await _aclose(rt)


async def test_build_runtime_with_key_enables_extraction(tmp_path, rsa_key):
    rt = build_runtime(_settings(tmp_path, rsa_key, openrouter="sk-or-test"))
    try:
        assert isinstance(rt.llm, OpenRouterClient)
    finally:
        await _aclose(rt)


async def test_demo_env_uses_demo_hosts(tmp_path, rsa_key):
    rt = build_runtime(_settings(tmp_path, rsa_key))
    try:
        assert "demo" in rt.settings.rest_base_url
        assert rt.settings.ws_url.startswith("wss://")
    finally:
        await _aclose(rt)
