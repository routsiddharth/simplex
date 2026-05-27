"""Structured JSON logging to stdout.

Every line is one JSON object: timestamp, level, the originating loop, the
message, and any contextual fields passed via ``extra=`` (market_id, sid,
count, ...). Get a logger bound to a loop with :func:`get_logger`.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

# Attributes present on every LogRecord; anything else is treated as a
# caller-supplied contextual field and folded into the JSON line.
_STD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "loop",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "loop": getattr(record, "loop", "-"),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(level: str) -> None:
    """Install the JSON handler on the root logger. Idempotent."""
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)


class _LoopAdapter(logging.LoggerAdapter):
    """Stamps every record with its loop name; merges per-call ``extra``."""

    def process(self, msg, kwargs):
        extra = kwargs.get("extra") or {}
        extra.setdefault("loop", self.extra["loop"])
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(loop: str, name: str | None = None) -> _LoopAdapter:
    return _LoopAdapter(logging.getLogger(name or f"simplex.{loop}"), {"loop": loop})
