"""config.normalize_pem must recover the key from every documented transit shape,
and Settings.load_private_key must enforce RSA. None of this is network-bound.
"""

from __future__ import annotations

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from simplex_ingest.config import Settings, normalize_pem


@pytest.fixture(scope="module")
def pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _loads(pem_text: str) -> bool:
    load_pem_private_key(normalize_pem(pem_text).encode(), password=None)
    return True


def test_normalize_multiline_pem(pem):
    assert _loads(pem)


def test_normalize_backslash_n_escaped(pem):
    # As it arrives when newlines get \n-escaped in a single-line env var.
    assert _loads(pem.replace("\n", "\\n"))


def test_normalize_glued_single_line(pem):
    # Header/body/footer all collapsed onto one line (whitespace stripped).
    assert _loads(pem.replace("\n", ""))


def test_normalize_quoted_and_padded(pem):
    assert _loads(f'  "{pem}"  ')


def test_normalize_reads_from_file_path(pem, tmp_path):
    p = tmp_path / "key.pem"
    p.write_text(pem)
    assert _loads(str(p))


def test_normalize_rejects_non_pem_non_path():
    with pytest.raises(ValueError, match="no PEM header"):
        normalize_pem("definitely-not-a-key-or-a-file")


def test_load_private_key_happy_path(pem, monkeypatch, tmp_path):
    monkeypatch.setenv("KALSHI_API_KEY_ID", "kid")
    monkeypatch.setenv("KALSHI_API_SECRET", pem)
    monkeypatch.setenv("KALSHI_ENV", "demo")
    monkeypatch.setenv("SIMPLEX_DATA_DIR", str(tmp_path))
    settings = Settings(_env_file=None)
    assert isinstance(settings.load_private_key(), rsa.RSAPrivateKey)


def test_load_private_key_rejects_garbage_secret(monkeypatch, tmp_path):
    monkeypatch.setenv("KALSHI_API_KEY_ID", "kid")
    monkeypatch.setenv("KALSHI_API_SECRET", "not-a-key")
    monkeypatch.setenv("KALSHI_ENV", "demo")
    monkeypatch.setenv("SIMPLEX_DATA_DIR", str(tmp_path))
    with pytest.raises(ValueError):
        Settings(_env_file=None).load_private_key()
