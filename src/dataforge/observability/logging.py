"""JSON logging with run context.

Each line is JSON so logs can be queried. Run context (run_id, dataset, ...) is bound
once and added to every line. Keys that look like secrets are redacted as a safety net.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

_SENSITIVE_KEY = re.compile(
    r"(pass|pwd|secret|token|api[_-]?key|authorization|credential|private[_-]?key)", re.I
)
_REDACTED = "***redacted***"


def _scrub(value: Any, key: str = "") -> Any:
    if _SENSITIVE_KEY.search(key):
        return _REDACTED
    if isinstance(value, dict):
        return {k: _scrub(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub(v) for v in value]
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if context:
            payload |= _scrub(context)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class RunLogger:
    """A logger with a bound run context."""

    def __init__(self, name: str = "dataforge", **context: Any) -> None:
        self._logger = logging.getLogger(name)
        self._context = context
        if not self._logger.handlers:
            # Logs go to stderr; stdout is kept for the JSON result.
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(JsonFormatter())
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
            self._logger.propagate = False

    def bind(self, **context: Any) -> RunLogger:
        child = RunLogger(self._logger.name, **{**self._context, **context})
        return child

    def _emit(self, level: int, message: str, **fields: Any) -> None:
        self._logger.log(level, message, extra={"context": {**self._context, **fields}})

    def info(self, message: str, **fields: Any) -> None:
        self._emit(logging.INFO, message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self._emit(logging.WARNING, message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit(logging.ERROR, message, **fields)
