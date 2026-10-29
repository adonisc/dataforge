"""SCD type 2 merge, in Python so it works for CDC sources and is quick to test.

* Changes are detected by row hash, so new columns are included automatically.
* Unchanged rows don't open a new version.
* Versions start at the event time, not the ingest time.
* Deletes close the current version; history is kept.
* Out-of-order records are returned to the caller, not applied.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

Record = dict[str, Any]

#: End date for open versions. Using a date instead of NULL keeps BETWEEN queries simple.
FAR_FUTURE = datetime(9999, 12, 31, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class ScdResult:
    rows: list[Record]
    closed: int
    opened: int
    unchanged: int
    out_of_order: list[Record]


def apply_scd2(
    current: Sequence[Record],
    incoming: Sequence[Record],
    *,
    key_columns: list[str],
    effective_column: str,
    hash_column: str = "_record_hash",
    deleted_column: str = "_is_deleted",
) -> ScdResult:
    """Merge ``incoming`` into ``current`` and return the new dimension. Writes nothing.

    ``current`` rows need ``_valid_from``, ``_valid_to`` and ``_is_current``.
    """
    history: dict[tuple[Any, ...], list[Record]] = {}
    for row in current:
        history.setdefault(tuple(row.get(c) for c in key_columns), []).append(dict(row))

    opened = closed = unchanged = 0
    out_of_order: list[Record] = []

    for record in sorted(incoming, key=lambda r: str(r.get(effective_column) or "")):
        key = tuple(record.get(c) for c in key_columns)
        effective_at = _as_utc(record.get(effective_column))
        versions = history.setdefault(key, [])
        active = next((v for v in versions if v.get("_is_current")), None)

        if active is not None and effective_at < _as_utc(active["_valid_from"]):
            # Older than the current version; fixing this needs a deliberate backfill.
            out_of_order.append(record)
            continue

        is_delete = bool(record.get(deleted_column))

        if (
            active is not None
            and not is_delete
            and active.get(hash_column) == record.get(hash_column)
        ):
            unchanged += 1
            continue

        if active is not None:
            active["_valid_to"] = effective_at
            active["_is_current"] = False
            closed += 1

        if is_delete:
            continue  # history preserved, no new open version

        new_version = dict(record)
        new_version["_valid_from"] = effective_at
        new_version["_valid_to"] = FAR_FUTURE
        new_version["_is_current"] = True
        new_version["_version"] = len(versions) + 1
        versions.append(new_version)
        opened += 1

    rows = [row for versions in history.values() for row in versions]
    rows.sort(key=lambda r: (tuple(str(r.get(c)) for c in key_columns), _as_utc(r["_valid_from"])))
    return ScdResult(
        rows=rows, closed=closed, opened=opened, unchanged=unchanged, out_of_order=out_of_order
    )


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    from dataforge.quality.rules import coerce_timestamp

    parsed = coerce_timestamp(value)
    if parsed is None:
        raise ValueError(f"cannot interpret {value!r} as a timestamp for SCD2 effective dating")
    return parsed
