# ADR-0001 — Apache Iceberg as the table format

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

The platform serves analytics, reporting and ML from one copy of the data, read today
with SQL and later maybe with Spark or Python dataframes. The table format is hard to
change later: once every consumer, retention policy and maintenance job depends on it,
switching is a migration.

## Decision

Apache Iceberg on object storage with a REST catalog in production. Locally, a Parquet
lake behind the same `Engine` interface.

## Alternatives considered

**Delta Lake.** Technically comparable and open. Not chosen because its tooling and best
support still centre on Databricks, and engine independence is the main reason to pick
a table format here. **In a Databricks-only organisation, Delta would be the better
choice.**

**Plain partitioned Parquet (Hive style).** No atomic commits, snapshot isolation, schema
evolution by field ID or time travel, and readers can see half-written partitions. The
idempotency and audit design would have to rebuild all of that by hand.

**Warehouse-native tables (Snowflake/BigQuery).** Great to work with, but the data is
locked into one engine. ML and Python consumers pay export costs, and changing vendor
becomes a migration.

**Apache Hudi.** Strong for upsert-heavy CDC, which is part of this workload. Not chosen
because it's more complex to run and fewer engines support it. Iceberg's merge-on-read
handles the CDC cases here well enough.

## Consequences

**Costs.** The catalog becomes a critical service to run and back up. Maintenance
(compaction, snapshot expiry, orphan-file cleanup) is ongoing and must run on its own
schedule, not inside the data DAG. Engines support newer Iceberg features unevenly, so
in practice you use the common subset.

**Benefits.** Atomic partition overwrites in one commit, which makes merge-on-write
safe. Time travel, so you can see what a table said on the day a report ran. Schema
evolution by field ID, which keeps expand/contract cheap. Hidden partitioning, so
consumers get pruning without knowing the partition column.

## Revisit when

The organisation commits to one engine, a consumer needs a feature only another format
has, or running the catalog costs more than expected.
