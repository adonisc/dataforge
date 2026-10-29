"""Turn raw records into typed silver records.

Only structural work: types, trimming, UTC, decimal precision, metadata columns.
Business logic belongs in dbt (see docs/adr/ADR-0004).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from dataforge.contracts.models import DataContract, FieldSpec, FieldType
from dataforge.quality.rules import coerce_timestamp

Record = dict[str, Any]


class CoercionError(ValueError):
    """Raised when a value cannot be represented in its declared type."""


def coerce_value(value: Any, spec: FieldSpec) -> Any:
    """Coerce a value to its declared type.

    Raises instead of returning NULL, so the row gets quarantined rather than silently emptied.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None

    try:
        match spec.type:
            case FieldType.STRING | FieldType.JSON:
                return value if isinstance(value, str) else json.dumps(value, default=str)
            case FieldType.INTEGER | FieldType.LONG:
                # Go via Decimal so "1.0" works.
                return int(Decimal(str(value)))
            case FieldType.FLOAT:
                return float(value)
            case FieldType.DECIMAL:
                quant = Decimal(1).scaleb(-(spec.scale or 0))
                return Decimal(str(value)).quantize(quant)
            case FieldType.BOOLEAN:
                if isinstance(value, bool):
                    return value
                text = str(value).strip().lower()
                if text in {"true", "t", "1", "yes", "y"}:
                    return True
                if text in {"false", "f", "0", "no", "n"}:
                    return False
                raise CoercionError(f"'{value}' is not a boolean")
            case FieldType.DATE:
                stamp = coerce_timestamp(value)
                if stamp is None:
                    raise CoercionError(f"'{value}' is not a date")
                return stamp.date()
            case FieldType.TIMESTAMP:
                stamp = coerce_timestamp(value)
                if stamp is None:
                    raise CoercionError(f"'{value}' is not a timestamp")
                return stamp.astimezone(UTC)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise CoercionError(f"cannot coerce {value!r} to {spec.type}: {exc}") from exc
    raise CoercionError(f"unsupported type {spec.type}")  # pragma: no cover


def record_hash(record: Record, columns: list[str]) -> str:
    """Stable hash of the business columns, used for dedupe and SCD2 change detection."""
    payload = "|".join(f"{c}={record.get(c)!r}" for c in sorted(columns))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_batch(
    contract: DataContract,
    records: list[Record],
    *,
    run_id: str,
    source_uri: str,
    ingested_at: datetime | None = None,
) -> tuple[list[Record], list[tuple[Record, str]]]:
    """Returns (conformed records, [(bad record, reason)]).

    Columns not in the contract are dropped here; they're still in bronze.
    """
    ingested_at = ingested_at or datetime.now(UTC)
    schema = contract.spec.schema_
    business_columns = [f.name for f in schema.fields]

    good: list[Record] = []
    bad: list[tuple[Record, str]] = []

    for raw in records:
        out: Record = {}
        try:
            for spec in schema.fields:
                value = raw.get(spec.name)
                if isinstance(value, str):
                    value = value.strip()
                out[spec.name] = coerce_value(value, spec)
        except CoercionError as exc:
            bad.append((raw, str(exc)))
            continue

        # Pipeline metadata columns, prefixed with '_'.
        out["_ingested_at"] = ingested_at
        out["_run_id"] = run_id
        out["_source_uri"] = source_uri
        out["_contract_version"] = contract.metadata.version
        out["_record_hash"] = record_hash(out, business_columns)
        good.append(out)

    return good, bad
