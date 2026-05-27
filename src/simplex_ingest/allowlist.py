"""simplex_allowlist.yaml — the single source of truth for which series we ingest.

Shape:

    series:
      - ticker: KXFED
        notes: "Fed rate decisions — partition over rate buckets"
      - ticker: KXNBACHAMP
        notes: "NBA championship — hierarchical structure"
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from . import constants as C


class AllowlistError(Exception):
    """Raised when the allowlist is missing or empty — a fatal startup condition."""


@dataclass(slots=True)
class AllowlistEntry:
    ticker: str
    notes: str | None = None


def allowlist_path() -> Path:
    """Resolve the allowlist relative to the current working directory (repo root)."""
    return Path(C.ALLOWLIST_FILENAME)


def load_allowlist(path: Path | None = None) -> list[AllowlistEntry]:
    """Parse the allowlist. Returns [] if the file is absent or has no series."""
    p = path or allowlist_path()
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text()) or {}
    entries: list[AllowlistEntry] = []
    for item in data.get("series") or []:
        if isinstance(item, str):
            entries.append(AllowlistEntry(ticker=item.strip()))
        elif isinstance(item, dict) and item.get("ticker"):
            entries.append(
                AllowlistEntry(ticker=str(item["ticker"]).strip(), notes=item.get("notes"))
            )
    return entries


def require_allowlist(path: Path | None = None) -> list[AllowlistEntry]:
    """Startup gate: fail loudly if the allowlist is missing or empty."""
    p = path or allowlist_path()
    entries = load_allowlist(p)
    if not entries:
        raise AllowlistError(
            f"Allowlist {p} is missing or empty. Run the discovery script first:\n"
            f"    python -m scripts.discover_series\n"
            f"then review and commit {p}."
        )
    return entries
