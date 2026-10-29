"""Iceberg engine for production.

A thin, partly illustrative adapter behind the ``iceberg`` extra. Not covered by unit
tests since it needs a real catalog. It shows how the storage operations map onto
Iceberg. For why Iceberg, see docs/adr/ADR-0001.
"""

from __future__ import annotations

from typing import Any

from dataforge.contracts.models import DataContract

Record = dict[str, Any]


class IcebergEngine:
    """pyiceberg-backed engine. Requires the ``iceberg`` extra."""

    def __init__(self, catalog_name: str, namespace: str, **catalog_properties: Any) -> None:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:  # pragma: no cover - extra not installed in CI
            raise ImportError(
                "IcebergEngine requires the 'iceberg' extra: pip install 'dataforge[iceberg]'"
            ) from exc
        self.catalog = load_catalog(catalog_name, **catalog_properties)
        self.namespace = namespace

    def ensure_table(self, contract: DataContract) -> Any:  # pragma: no cover - integration path
        """Create the table, or evolve it to match the contract's schema."""
        from pyiceberg.exceptions import NoSuchTableError

        from dataforge.engines.parquet_lake import arrow_schema

        identifier = f"{self.namespace}.{contract.name}"
        schema = arrow_schema(contract)
        try:
            table = self.catalog.load_table(identifier)
        except NoSuchTableError:
            partition = contract.spec.partitioning
            return self.catalog.create_table(
                identifier,
                schema=schema,
                properties={
                    "write.target-file-size-bytes": str(
                        partition.target_file_size_mb * 1024 * 1024
                    ),
                    "write.parquet.compression-codec": "zstd",
                    # Keep a week of snapshots; old ones pin data files and grow the bill.
                    "history.expire.max-snapshot-age-ms": str(7 * 24 * 3600 * 1000),
                    "write.metadata.delete-after-commit.enabled": "true",
                    "write.metadata.previous-versions-max": "20",
                },
            )
        with table.update_schema() as update:
            update.union_by_name(schema)  # additive-only; drops must go through CI's gate
        return table
