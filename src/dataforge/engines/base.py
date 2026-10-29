"""Storage interface. Pipeline logic sits above it; the backend (Parquet, Iceberg, ...) is swappable."""

from __future__ import annotations

from typing import Any, Protocol

Record = dict[str, Any]


class Engine(Protocol):
    def write_append(self, table: str, records: list[Record], *, run_id: str) -> int:
        """Append rows. Used for bronze and quarantine, which are append-only."""
        ...

    def overwrite_partition(
        self,
        table: str,
        records: list[Record],
        *,
        partition_column: str,
        partition_value: str,
        run_id: str,
    ) -> int:
        """Replace one partition atomically, so re-running a date gives the same result.

        Snapshot engines do this in one commit; the local engine writes to temp, then swaps.
        """
        ...

    def read_table(self, table: str) -> list[Record]:
        """Read a table back. Used by SCD2 merges and by tests."""
        ...

    def table_exists(self, table: str) -> bool: ...
