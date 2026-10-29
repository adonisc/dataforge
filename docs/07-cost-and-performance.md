# 07 — Cost and performance

## Ordered by impact on the bill

Teams often spend weeks tuning queries while a nightly full refresh costs ten times
more. This list goes roughly from biggest impact to smallest at the assumed volumes.

### 1. Incremental over full refresh

This is the biggest avoidable cost. `fct_orders` keeps growing and old orders rarely
change, so rewriting all of history every night is waste.

At 5M orders/day over 3 years (~5.5B rows), a daily full refresh scans ~2 TB. An
incremental run with a 3-day late window scans ~6 GB, roughly 350 times less, and the
gap grows every day.

`on_schema_change='append_new_columns'` helps too: without it, adding a column forces a
full rebuild.

**A full refresh still makes sense** when the table is small (under ~10M rows), the
logic changed, or you no longer trust the incremental state. Use it to recover, not as
the schedule.

### 2. Partition pruning

Partition on what most queries filter by, which is usually the business date, not the
ingestion date. A query for last month should read 30 partitions, not 1,100.

A common mistake is partitioning by ingestion date because it's easy to write, so every
query on business date scans the whole table. Partition for readers.

Use daily partitions by default. Hourly partitions on a 200M-rows/day stream add up to
8,760 a year, which slows planning; monthly ones prune too little. Only change this based
on the query log.

### 3. Clustering on two or three columns at most

Clustering sorts rows within partitions so engines can skip row groups using min/max
stats. It works on **selective filters**: `channel` and `order_status` for orders,
`event_name` and `platform` for events.

Past two or three columns it costs more on write than it saves on read. Pick the columns
from the query log.

### 4. Size files on write instead of compacting later

Target 256 MB (512 MB for the event stream, 16 MB for small reference data).

Small files are the main hidden cost in object-store lakes. Each one costs a LIST and a
GET on read and a manifest entry on write, so 10,000 small files cost more than their
bytes. Compaction fixes it later with a full rewrite; sizing correctly up front is free.

`ParquetLakeEngine._chunk` uses a rough estimate (512 bytes per row). A real deployment
should use bytes per row from the previous run's stats.

### 5. Compression and encoding

`zstd` instead of `snappy`: 20–30% smaller at similar read speed. That also means fewer
bytes scanned, which is what serverless engines bill for.

Dictionary encoding and column statistics are on for every file. Without stats,
predicate pushdown can't skip anything.

### 6. Retention by layer

| Layer | Orders | Events | Why |
|---|---|---|---|
| Bronze | 90d | **14d** | Raw events are by far the biggest data and the least valuable per byte |
| Silver | 7y | 2y | Orders are financial records |
| Gold | indefinite | indefinite | Aggregates hold history in a fraction of the space |
| Quarantine | 30d | 14d | If it hasn't been replayed in a month, it won't be |

A governance test checks bronze retention is never longer than silver.

Lifecycle rules move old objects to infrequent access and then archive before they
expire. That's only worth it above a size threshold: transitions are billed per object,
so moving millions of tiny files to Glacier can cost more than leaving them. Another
reason to size files well (see 4).

### 7. Snapshot expiry

Old Iceberg snapshots **keep data files alive**. If storage costs don't drop after a
cleanup, an old snapshot is usually still referencing the files.

Keep seven days of snapshots. Run `expire_snapshots` and `remove_orphan_files` weekly
**on their own schedule, not in the data DAG**, or they'll get disabled the first time
they delay a nightly run.

### 8. Aggregates only where they pay off

Pre-aggregate when **queries per day × rows scanned** is large and the grain is stable.
`agg_revenue_daily` turns millions of rows into thousands and is queried hundreds of
times a day, so it's worth it.

Each aggregate is another table to build, monitor and reconcile, which is why
`aggregate_reconciles_to_fact` exists. If a candidate doesn't clear the bar, query the
fact table directly.

---

## Compute

| Workload | Shape | Why |
|---|---|---|
| Ingestion (orders, customers, fx) | Small single-node container | A few GB per run. Spark would spend longer starting up than working |
| Ingestion (events) | Spark or partitioned fan-out | 60 GB/day needs it |
| dbt marts | In the warehouse | Set-based work belongs next to the data |
| Ad hoc / BI | Serverless (Athena/Trino) | Spiky and unpredictable, so pay per query |
| Backfills | Separate pool, off-hours, lower concurrency | A backfill that maxes out the warehouse during the day is an incident |

**Serverless first, reserved capacity for the steady baseline.** The most common sizing
mistake is committing for peak load; the next most common is never committing for an
obvious baseline.

Set warehouse auto-suspend short (60s). Idle warehouses cost money for no work, and a
warm cache is rarely worth it.

---

## Latency, cost and correctness

You can't max out all three. The positions taken here:

| Choice | Gives up | Gets |
|---|---|---|
| Daily batch for orders | Intraday freshness | ~1/24 the compute of hourly |
| Micro-batch for events | Sub-minute latency | No always-on cluster or 24/7 streaming ops |
| Merge on write | Write throughput | Readers never see a half-written partition |
| Blocking quality rules | Availability on bad days | Never publishing a wrong number |
| Bronze retention | Storage | Replay, the cheapest insurance in the design |

---

## Measure before optimising

Monthly, per dataset: bytes scanned, bytes stored per layer, compute seconds, cost per
1,000 rows published, and the top ten queries by bytes scanned.

Watch **cost per 1,000 rows published** over time. Total cost grows with the business and
doesn't say much. Rising unit cost means something is degrading, usually small files or a
clustering key that no longer matches how people query.

Alert on any single query scanning more than 1 TB, and on more than 20% month-over-month
storage growth in any layer.
