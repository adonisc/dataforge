#!/usr/bin/env python3
"""Register the Parquet lake as DuckDB views so dbt can read it.

Views read the files in place, so there's no copy to go stale and new pipeline runs
show up in dbt right away. Same idea as external tables in Trino, Athena or Snowflake.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

#: Layers exposed to dbt. Not gold, since dbt builds that itself.
LAYERS = ("bronze", "silver", "quarantine")


def register(lake_root: Path, db_path: Path, *, layers: tuple[str, ...] = LAYERS) -> int:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    registered = 0

    try:
        for layer in layers:
            layer_dir = lake_root / layer
            if not layer_dir.is_dir():
                continue
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{layer}"')

            for table_dir in sorted(p for p in layer_dir.iterdir() if p.is_dir()):
                if not any(table_dir.rglob("*.parquet")):
                    continue
                # Absolute path, since dbt queries from a different working directory.
                pattern = str((table_dir / "**" / "*.parquet").resolve())
                con.execute(
                    f'CREATE OR REPLACE VIEW "{layer}"."{table_dir.name}" AS '
                    f"SELECT * FROM read_parquet('{pattern}', "
                    "union_by_name = true, hive_partitioning = false)"
                )
                rows = con.execute(f'SELECT count(*) FROM "{layer}"."{table_dir.name}"').fetchone()
                print(f"  {layer}.{table_dir.name:<22} {rows[0] if rows else 0:>8} rows")
                registered += 1
    finally:
        con.close()

    return registered


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", default="data/lake")
    parser.add_argument("--database", default="data/warehouse/dev.duckdb")
    args = parser.parse_args()

    lake_root = Path(args.lake_root)
    if not lake_root.is_dir():
        print(
            f"lake root {lake_root} does not exist — run `make demo` first",
            file=sys.stderr,
        )
        return 1

    print(f"\nRegistering {lake_root} into {args.database}\n")
    count = register(lake_root, Path(args.database))

    if count == 0:
        print("no tables found — run `make demo` first", file=sys.stderr)
        return 1
    print(f"\n{count} view(s) registered. dbt can now read them as sources.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
