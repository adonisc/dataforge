"""Quality rule implementations.

* Row rules check one record and can quarantine it.
* Dataset rules check the whole batch (unique, volume, freshness) and block or warn.
  ``unique`` also quarantines the losing duplicates.

Register a new rule type with ``@row_rule`` or ``@dataset_rule``; nothing else changes.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

Record = dict[str, Any]


@dataclass(frozen=True, slots=True)
class Violation:
    rule_id: str
    rule_type: str
    column: str | None
    message: str


RowRule = Callable[[Record], Violation | None]
DatasetRule = Callable[[Sequence[Record]], tuple[list[int], list[Violation]]]
"""Returns (indices of offending rows, dataset-level violations)."""

_ROW_RULES: dict[str, Callable[..., RowRule]] = {}
_DATASET_RULES: dict[str, Callable[..., DatasetRule]] = {}


def row_rule(name: str) -> Callable[[Callable[..., RowRule]], Callable[..., RowRule]]:
    def deco(fn: Callable[..., RowRule]) -> Callable[..., RowRule]:
        _ROW_RULES[name] = fn
        return fn

    return deco


def dataset_rule(name: str) -> Callable[[Callable[..., DatasetRule]], Callable[..., DatasetRule]]:
    def deco(fn: Callable[..., DatasetRule]) -> Callable[..., DatasetRule]:
        _DATASET_RULES[name] = fn
        return fn

    return deco


def known_rule_types() -> set[str]:
    return set(_ROW_RULES) | set(_DATASET_RULES)


def is_dataset_rule(rule_type: str) -> bool:
    return rule_type in _DATASET_RULES


def build_row_rule(
    rule_id: str, rule_type: str, columns: list[str], params: dict[str, Any]
) -> RowRule:
    if rule_type not in _ROW_RULES:
        raise KeyError(f"unknown row rule type '{rule_type}'")
    return _ROW_RULES[rule_type](rule_id, columns, params)


def build_dataset_rule(
    rule_id: str, rule_type: str, columns: list[str], params: dict[str, Any]
) -> DatasetRule:
    if rule_type not in _DATASET_RULES:
        raise KeyError(f"unknown dataset rule type '{rule_type}'")
    return _DATASET_RULES[rule_type](rule_id, columns, params)


# --------------------------------------------------------------------------------------
# Row-level rules
# --------------------------------------------------------------------------------------


def _missing(value: Any) -> bool:
    """None or a blank string. CSV and APIs often send "" for no value."""
    return value is None or (isinstance(value, str) and not value.strip())


@row_rule("not_null")
def _not_null(rule_id: str, columns: list[str], params: dict[str, Any]) -> RowRule:  # noqa: ARG001
    def check(record: Record) -> Violation | None:
        for col in columns:
            if _missing(record.get(col)):
                return Violation(rule_id, "not_null", col, f"'{col}' is null or empty")
        return None

    return check


@row_rule("accepted_values")
def _accepted_values(rule_id: str, columns: list[str], params: dict[str, Any]) -> RowRule:
    allowed = set(params["values"])
    case_insensitive = bool(params.get("case_insensitive", False))
    if case_insensitive:
        allowed = {str(v).lower() for v in allowed}

    def check(record: Record) -> Violation | None:
        for col in columns:
            value = record.get(col)
            if _missing(value):
                continue  # nulls are not_null's job
            probe = str(value).lower() if case_insensitive else value
            if probe not in allowed:
                return Violation(
                    rule_id, "accepted_values", col, f"'{col}'={value!r} not in accepted set"
                )
        return None

    return check


@row_rule("range")
def _range(rule_id: str, columns: list[str], params: dict[str, Any]) -> RowRule:
    low = params.get("min")
    high = params.get("max")

    def check(record: Record) -> Violation | None:
        for col in columns:
            value = record.get(col)
            if _missing(value):
                continue
            try:
                number = Decimal(str(value))
            except (InvalidOperation, ValueError):
                return Violation(rule_id, "range", col, f"'{col}'={value!r} is not numeric")
            if low is not None and number < Decimal(str(low)):
                return Violation(rule_id, "range", col, f"'{col}'={value} < {low}")
            if high is not None and number > Decimal(str(high)):
                return Violation(rule_id, "range", col, f"'{col}'={value} > {high}")
        return None

    return check


@row_rule("regex")
def _regex(rule_id: str, columns: list[str], params: dict[str, Any]) -> RowRule:
    pattern = re.compile(params["pattern"])

    def check(record: Record) -> Violation | None:
        for col in columns:
            value = record.get(col)
            if _missing(value):
                continue
            if not pattern.fullmatch(str(value)):
                return Violation(rule_id, "regex", col, f"'{col}' does not match {pattern.pattern}")
        return None

    return check


@row_rule("timestamp_valid")
def _timestamp_valid(rule_id: str, columns: list[str], params: dict[str, Any]) -> RowRule:
    """Timestamps must parse, not be in the future (beyond a skew) and not predate a floor.

    Future dates would push the watermark ahead and make later runs skip real data.
    """
    max_skew = timedelta(minutes=int(params.get("max_future_skew_minutes", 60)))
    floor_raw = params.get("not_before")
    floor = datetime.fromisoformat(floor_raw).replace(tzinfo=UTC) if floor_raw else None

    def check(record: Record) -> Violation | None:
        now = datetime.now(UTC)
        for col in columns:
            value = record.get(col)
            if _missing(value):
                continue
            parsed = coerce_timestamp(value)
            if parsed is None:
                return Violation(rule_id, "timestamp_valid", col, f"'{col}'={value!r} unparseable")
            if parsed > now + max_skew:
                return Violation(
                    rule_id, "timestamp_valid", col, f"'{col}'={value} is in the future"
                )
            if floor and parsed < floor:
                return Violation(rule_id, "timestamp_valid", col, f"'{col}'={value} predates floor")
        return None

    return check


@row_rule("relationship")
def _relationship(rule_id: str, columns: list[str], params: dict[str, Any]) -> RowRule:
    """Value must be in ``params['known_keys']``, which the caller loads once per run."""
    known = set(params.get("known_keys") or [])
    to_column = params.get("to", "<dimension>")

    def check(record: Record) -> Violation | None:
        if not known:
            return None  # no keys loaded, so skip
        for col in columns:
            value = record.get(col)
            if _missing(value):
                continue
            if value not in known:
                return Violation(
                    rule_id, "relationship", col, f"'{col}'={value!r} not found in {to_column}"
                )
        return None

    return check


# --------------------------------------------------------------------------------------
# Dataset-level rules
# --------------------------------------------------------------------------------------


@dataset_rule("unique")
def _unique(rule_id: str, columns: list[str], params: dict[str, Any]) -> DatasetRule:
    """Flag every duplicate after the first, ordered by ``order_by`` (not arrival order)."""
    order_by: list[str] = list(params.get("order_by") or [])

    def check(records: Sequence[Record]) -> tuple[list[int], list[Violation]]:
        buckets: dict[tuple[Any, ...], list[int]] = {}
        for idx, record in enumerate(records):
            buckets.setdefault(tuple(record.get(c) for c in columns), []).append(idx)

        offenders: list[int] = []
        for indices in buckets.values():
            if len(indices) == 1:
                continue
            if order_by:
                indices = sorted(
                    indices, key=lambda i: _sort_key(records[i], order_by), reverse=True
                )
            offenders.extend(indices[1:])
        violations = (
            [
                Violation(
                    rule_id,
                    "unique",
                    ",".join(columns),
                    f"duplicate key, {len(offenders)} row(s) superseded",
                )
            ]
            if offenders
            else []
        )
        return sorted(offenders), violations

    return check


@dataset_rule("row_count_anomaly")
def _row_count_anomaly(rule_id: str, columns: list[str], params: dict[str, Any]) -> DatasetRule:  # noqa: ARG001
    """Volume check against ``min_rows`` and, if provided, the runtime ``baseline``."""
    min_rows = int(params.get("min_rows", 0))
    baseline = params.get("baseline")
    drop_threshold = float(params.get("drop_threshold", 0.5))

    def check(records: Sequence[Record]) -> tuple[list[int], list[Violation]]:
        n = len(records)
        violations: list[Violation] = []
        if n < min_rows:
            violations.append(
                Violation(rule_id, "row_count_anomaly", None, f"{n} rows < minimum {min_rows}")
            )
        if baseline and n < float(baseline) * drop_threshold:
            violations.append(
                Violation(
                    rule_id,
                    "row_count_anomaly",
                    None,
                    f"{n} rows is below {drop_threshold:.0%} of baseline {baseline}",
                )
            )
        return [], violations

    return check


@dataset_rule("freshness")
def _freshness(rule_id: str, columns: list[str], params: dict[str, Any]) -> DatasetRule:
    """Newest event timestamp must be within the SLO. Checks the data, not the DAG status."""
    max_age = timedelta(minutes=int(params["max_age_minutes"]))
    column = columns[0]

    def check(records: Sequence[Record]) -> tuple[list[int], list[Violation]]:
        stamps = [t for t in (coerce_timestamp(r.get(column)) for r in records) if t]
        if not stamps:
            return [], [Violation(rule_id, "freshness", column, "no parseable timestamps in batch")]
        age = datetime.now(UTC) - max(stamps)
        if age > max_age:
            return [], [
                Violation(
                    rule_id,
                    "freshness",
                    column,
                    f"newest record is {age.total_seconds() / 60:.0f}m old, SLO is {max_age.total_seconds() / 60:.0f}m",
                )
            ]
        return [], []

    return check


# --------------------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------------------


def coerce_timestamp(value: Any) -> datetime | None:
    """Parse common timestamp formats to UTC. Naive values are assumed to be UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000.0 if float(value) > 1e11 else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC)
    text = str(value).strip()
    if not text:
        return None
    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _sort_key(record: Record, columns: list[str]) -> tuple[Any, ...]:
    """Sort key that handles None and mixed types without raising."""
    key: list[Any] = []
    for col in columns:
        value = record.get(col)
        if value is None:
            key.append((0, ""))
        else:
            parsed = coerce_timestamp(value) if isinstance(value, str) and "-" in value else None
            key.append((1, parsed.isoformat() if parsed else str(value)))
    return tuple(key)
