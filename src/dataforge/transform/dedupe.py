"""Deduplication, for three different cases:

* Exact redeliveries: dropped by ``_record_hash``.
* Several versions of one key: keep the latest by ``dedupe_order_by``.
* CDC: keep the latest op per key. Deletes become tombstones so the merge removes them.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dataforge.quality.rules import _sort_key  # shared so ordering is defined once

Record = dict[str, Any]

#: Debezium-style operation codes. 'r' is a snapshot read.
_DELETE_OPS = {"d", "delete", "DELETE"}
_SNAPSHOT_OPS = {"r", "read", "READ"}


def drop_exact_duplicates(
    records: Sequence[Record], hash_column: str = "_record_hash"
) -> list[Record]:
    """Remove byte-identical redeliveries, preserving first-seen order."""
    seen: set[str] = set()
    out: list[Record] = []
    for record in records:
        digest = record.get(hash_column)
        if digest is None:
            out.append(record)
            continue
        if digest in seen:
            continue
        seen.add(digest)
        out.append(record)
    return out


def latest_per_key(
    records: Sequence[Record], key_columns: list[str], order_by: list[str]
) -> list[Record]:
    """Keep the greatest record per key by ``order_by``.

    Without ``order_by`` the last one in the batch wins, which isn't stable across replays.
    """
    winners: dict[tuple[Any, ...], Record] = {}
    for record in records:
        key = tuple(record.get(c) for c in key_columns)
        incumbent = winners.get(key)
        if incumbent is None:
            winners[key] = record
            continue
        if not order_by:
            winners[key] = record
            continue
        if _sort_key(record, order_by) >= _sort_key(incumbent, order_by):
            winners[key] = record
    return list(winners.values())


def collapse_cdc(
    records: Sequence[Record],
    key_columns: list[str],
    op_field: str,
    sequence_field: str,
    *,
    emit_tombstones: bool = True,
) -> list[Record]:
    """Collapse CDC to one row per key, with an ``_is_deleted`` flag.

    At the same sequence, a snapshot row loses to a streamed change, so an overlapping
    snapshot never overwrites a newer update.
    """

    def ordering(record: Record) -> tuple[Any, ...]:
        is_snapshot = 0 if str(record.get(op_field)) in _SNAPSHOT_OPS else 1
        return (*_sort_key(record, [sequence_field]), is_snapshot)

    winners: dict[tuple[Any, ...], Record] = {}
    for record in records:
        key = tuple(record.get(c) for c in key_columns)
        incumbent = winners.get(key)
        if incumbent is None or ordering(record) >= ordering(incumbent):
            winners[key] = record

    out: list[Record] = []
    for record in winners.values():
        deleted = str(record.get(op_field)) in _DELETE_OPS
        if deleted and not emit_tombstones:
            continue
        enriched = dict(record)
        enriched["_is_deleted"] = deleted
        out.append(enriched)
    return out
