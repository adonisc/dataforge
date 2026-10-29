"""Parquet lake engine: Hive-partitioned Parquet on local disk or S3.

Mirrors the production layout but has no catalog or snapshot isolation (see ``iceberg.py``).
"""

from __future__ import annotations

import contextlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from dataforge.contracts.models import DataContract, FieldType, IngestionMode
from dataforge.engines.storage import (
    delete_dir_contents,
    ensure_reachable,
    list_files,
    resolve,
)

Record = dict[str, Any]

_ARROW_TYPES: dict[FieldType, Any] = {
    FieldType.STRING: pa.string(),
    FieldType.JSON: pa.string(),
    FieldType.INTEGER: pa.int32(),
    FieldType.LONG: pa.int64(),
    FieldType.FLOAT: pa.float64(),
    FieldType.BOOLEAN: pa.bool_(),
    FieldType.DATE: pa.date32(),
    FieldType.TIMESTAMP: pa.timestamp("us", tz="UTC"),
}


def arrow_schema(
    contract: DataContract, *, masked_names: dict[str, str] | None = None
) -> pa.Schema:
    """Build the Arrow schema from the contract.

    Classifications go into the Parquet metadata so scanners can find sensitive columns.
    """
    masked_names = masked_names or {}
    fields: list[pa.Field] = []
    classifications: dict[str, str] = {}

    for spec in contract.spec.schema_.fields:
        name = masked_names.get(spec.name, spec.name)
        if spec.name in masked_names:
            arrow_type = pa.string()  # tokens and domains are always strings
        elif spec.type is FieldType.DECIMAL:
            arrow_type = pa.decimal128(spec.precision or 18, spec.scale or 2)
        else:
            arrow_type = _ARROW_TYPES[spec.type]
        fields.append(pa.field(name, arrow_type, nullable=not spec.required))
        classifications[name] = str(spec.classification)

    fields.extend(
        [
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("_run_id", pa.string(), nullable=False),
            pa.field("_source_uri", pa.string(), nullable=True),
            pa.field("_contract_version", pa.string(), nullable=False),
            pa.field("_record_hash", pa.string(), nullable=False),
        ]
    )

    # CDC carries a soft-delete flag into silver; without it deletes would be lost.
    if contract.spec.source.mode is IngestionMode.CDC:
        fields.append(pa.field("_is_deleted", pa.bool_(), nullable=False))

    metadata = {
        b"dataforge.contract": contract.name.encode(),
        b"dataforge.contract_version": contract.metadata.version.encode(),
        b"dataforge.owner": contract.metadata.owner.encode(),
        b"dataforge.classifications": json.dumps(classifications).encode(),
    }
    return pa.schema(fields, metadata=metadata)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


class ParquetLakeEngine:
    """Parquet implementation of :class:`dataforge.engines.base.Engine`.

    ParquetLakeEngine("data/lake")                 # local, CI
    ParquetLakeEngine("s3://acme-lake/warehouse")  # AWS or an S3 emulator
    """

    def __init__(self, root: str | Path, *, target_file_size_mb: int = 256) -> None:
        self.location = resolve(root)
        self.root = root
        self.target_file_size_mb = target_file_size_mb

    # -- paths -------------------------------------------------------------------------

    def _table_dir(self, table: str) -> str:
        # "silver.orders" -> <root>/silver/orders
        return self.location.child(*table.split("."))

    def _partition_dir(self, table: str, column: str, value: str) -> str:
        return f"{self._table_dir(table)}/{column}={value}"

    def table_exists(self, table: str) -> bool:
        return bool(list_files(self.location, self._table_dir(table)))

    # -- writes ------------------------------------------------------------------------

    def write_append(
        self, table: str, records: list[Record], *, run_id: str, schema: pa.Schema | None = None
    ) -> int:
        """Append rows. Used for bronze and quarantine, which are append-only."""
        if not records:
            return 0
        ensure_reachable(self.location)
        self._write_parquet(f"{self._table_dir(table)}/part-{run_id}.parquet", records, schema)
        return len(records)

    def overwrite_partition(
        self,
        table: str,
        records: list[Record],
        *,
        partition_column: str,
        partition_value: str,
        run_id: str,
        schema: pa.Schema | None = None,
    ) -> int:
        """Replace one partition so re-runs are idempotent.

        Locally this is write-to-staging then rename. S3 has no atomic rename, so new
        files are written and old ones deleted; a reader in that short window can see
        duplicates. ``max_active_runs=1`` per dataset keeps this rare.
        """
        ensure_reachable(self.location)
        target = self._partition_dir(table, partition_column, partition_value)
        chunks = self._chunk(records) if records else []

        if self.location.is_object_store:
            previous = set(list_files(self.location, target))
            written: set[str] = set()
            for index, chunk in enumerate(chunks):
                path = f"{target}/part-{run_id}-{index:04d}.parquet"
                self._write_parquet(path, chunk, schema)
                written.add(path)

            # Remove old files, but never the ones this run just wrote.
            for path in previous - written:
                with contextlib.suppress(FileNotFoundError, OSError):
                    self.location.fs.delete_file(path)
            return len(records)

        staging = f"{target}.staging"
        delete_dir_contents(self.location, staging)
        self.location.fs.create_dir(staging, recursive=True)
        for index, chunk in enumerate(chunks):
            self._write_parquet(f"{staging}/part-{run_id}-{index:04d}.parquet", chunk, schema)

        previous_dir = f"{target}.previous"
        if list_files(self.location, target):
            self.location.fs.move(target, previous_dir)
        self.location.fs.move(staging, target)
        with contextlib.suppress(FileNotFoundError, OSError):
            self.location.fs.delete_dir(previous_dir)
        return len(records)

    def _chunk(self, records: list[Record]) -> list[list[Record]]:
        """Split a partition into files near the target size."""
        approx_bytes_per_row = 512
        rows_per_file = max(1, (self.target_file_size_mb * 1024 * 1024) // approx_bytes_per_row)
        return [records[i : i + rows_per_file] for i in range(0, len(records), rows_per_file)]

    def _write_parquet(self, path: str, records: list[Record], schema: pa.Schema | None) -> None:
        if schema is not None:
            columns = {name: [r.get(name) for r in records] for name in schema.names}
            table = pa.Table.from_pydict(columns, schema=schema)
        else:
            table = pa.Table.from_pylist([{k: _jsonable(v) for k, v in r.items()} for r in records])

        parent = path.rsplit("/", 1)[0]
        # Needed locally, a no-op on S3.
        with contextlib.suppress(OSError):
            self.location.fs.create_dir(parent, recursive=True)

        with self.location.fs.open_output_stream(path) as sink:
            pq.write_table(
                table,
                sink,
                compression="zstd",  # ~20-30% smaller than snappy at comparable read speed
                use_dictionary=True,
                write_statistics=True,  # required for predicate pushdown / row-group pruning
            )

    # -- reads -------------------------------------------------------------------------

    def read_table(self, table: str) -> list[Record]:
        return self._read_dir(self._table_dir(table))

    def read_partition(
        self, table: str, partition_column: str, partition_value: str
    ) -> list[Record]:
        """Read one partition."""
        return self._read_dir(self._partition_dir(table, partition_column, partition_value))

    def _read_dir(self, directory: str) -> list[Record]:
        # Check on read too, so a bad endpoint can't look like an empty table.
        ensure_reachable(self.location)
        files = list_files(self.location, directory)
        if not files:
            return []
        # The partition column is already in the files; don't let Arrow add a second
        # one from the directory name.
        rows = pq.read_table(files, filesystem=self.location.fs, partitioning=None).to_pylist()
        return [dict(row) for row in rows]
