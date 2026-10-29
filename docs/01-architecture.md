# 01 — Data architecture

## Layers and what each one is for

Bronze/silver/gold (or raw/cleaned/curated) is a common pattern. Each layer should have
a clear job; a layer that only exists because the diagram has three boxes is just cost.

| Layer | Contains | Readers | Retention | Its job |
|---|---|---|---|---|
| **Landing** | Source files as delivered | Machines only | 7–30 days | Separate delivery from processing |
| **Bronze** | Records as received + ingestion metadata, unknown columns kept | Platform engineers, forensics, replay | 14–90 days by tier | **Replay.** The only layer that can rebuild the others |
| **Silver** | Typed, deduped, conformed, masked, partitioned | Analytics engineers, dbt, analysts, ML | 2–7 years | **Trust.** One shape, one timezone, one definition of a duplicate |
| **Quarantine** | Rejected rows + full payload + every rule broken | Data engineers, source owners | 14–90 days | **Visibility and replay** of what was rejected |
| **Gold** | Dimensional models, aggregates, data products | BI, executives, reverse-ETL, data science | Indefinite | **Meaning.** Agreed business definitions |

### Why bronze is written before validation

If validation runs first and rejects a row, that data is gone unless the source can send
it again, which may be a day, a week or only hours away. Rules are wrong now and then,
and fixing one shouldn't depend on data you no longer have.

Writing bronze first costs storage, the cheapest resource here, and lets you fix a rule,
redeploy and reprocess.

Bronze also keeps **unknown columns**. If a source adds a field before the contract knows
about it, the history is already there when a later contract version picks it up.

### Why masking happens in silver

Silver is the first layer most people can read. Masking there means raw values exist
only in bronze, which can have short retention, strict access and its own audit trail.
See [06-security-governance](06-security-governance.md).

---

## Ingestion patterns

There are four source types behind one `Source` interface. Each returns a `Batch` with
records plus provenance (URIs, offsets, high-water mark), which is what makes a run
replayable and feeds lineage.

### Batch / file drops
The URI template contains `{logical_date}`, so each run reads exactly its own
partition. **There's no `now()` token.** Inputs that depend on the clock can't be
backfilled, replayed or tested.

### Incremental (watermark)
Each dataset has a high-water mark in `_control`, **saved only after a successful
publish**. The filter is inclusive (`>=`, minus a second), so records with the exact
watermark timestamp aren't skipped. The small overlap is removed by dedupe.

The watermark only moves forward: backfilling January in March won't rewind it.

> **Don't combine increments.** Watermark filtering applies *only* to sources that can't
> be addressed by partition (REST, direct DB extracts). If you filter a partition-based
> read by watermark too and then overwrite the partition, you'll silently drop rows.

### CDC
Debezium envelopes are landed in object storage by the sink connector. They're collapsed
to one row per key per run, ordered by **LSN, not wall-clock time**, since source clocks
aren't reliable.

Two details matter most:
- **Deletes become tombstones** (`_is_deleted`) instead of disappearing. A lost delete
  leaves the row alive in the target forever.
- **Snapshot rows (`op='r'`) lose to streamed changes at the same sequence**, so an
  initial snapshot can't overwrite a newer update.

### Streaming
The pipeline doesn't run a consumer. A managed sink (Kafka Connect S3, Firehose, Flink)
handles delivery and file rotation; the batch pipeline handles duplicates with
idempotent partition writes keyed on offsets.

So an Airflow outage only **delays** data, it doesn't lose it. Real streaming is worth it
below about a one-minute SLO. Details in
[ADR-0007](adr/ADR-0007-microbatch-over-streaming.md).

---

## Schema evolution

Each contract sets a policy (`BACKWARD` by default, `FULL` for master and reference
data), and **a CI check enforces it**.

```
BACKWARD : readers of the OLD schema still work on NEW data
FORWARD  : readers of the NEW schema still work on OLD data
FULL     : both
```

`BACKWARD` is the default because dashboards, models and syncs are built against the
table as it was, and they stick around.

**Breaking changes are fine with a major version bump.** To ship one:

1. **Expand:** add the new column next to the old one and populate both. Minor bump.
2. **Migrate:** mark the old column `deprecated: true` and tell the owners, with a date.
3. **Contract:** remove the old column. Major bump.

Use `renamed_from:` for renames, so the check doesn't see a drop plus an add.
**Relaxing a classification** (say `pii` to `internal`) is breaking under every policy,
including `none`, because it removes masking.

On Iceberg, schema evolution is a metadata change (field IDs, not positions), which keeps
expand/contract cheap.

---

## Late-arriving data

Each contract sets `late_arrival_window_days`. Three mechanisms:

1. **Partition by business date, not run date.** The batch is grouped by its own
   partition values and each partition is merged separately.
2. **Merge, never blind overwrite.** Read the partition, union, dedupe by primary key,
   write it back in one commit.
3. **Data outside the window is logged, not dropped**, so downstream knows an old
   partition changed. dbt's incremental models use a window variable that must be at
   least as wide as the contract's, or they'll miss those rows.

For SCD2, out-of-order records are **returned to the caller** rather than inserted into
a closed history. Repairing history is a deliberate backfill, not something a nightly
job should do on its own.

---

## Duplicates

Three different problems, handled separately:

| Problem | Looks like | Handled by |
|---|---|---|
| Redelivery | Identical copies | `_record_hash` |
| Several versions in one batch | Same key, different values | Explicit `dedupe_order_by`, latest wins |
| CDC operations | insert/update/delete for one key | `collapse_cdc`, using op type and LSN |

"Whichever came last in the file" isn't an ordering. It gives different results on replay.

---

## Auditing, tracing and replay

Every silver row carries `_run_id`, `_ingested_at`, `_source_uri`, `_contract_version`
and `_record_hash`. Every run writes a manifest with inputs, fingerprint, row counts,
quality metrics, partitions written, contract and code version, and outcome.

**Tracing a number:** gold row → `_run_id` → manifest → source URIs + fingerprint →
exact bronze files.

**Replay:** re-run the date. Same inputs give `skipped_duplicate`. Changed inputs (say a
corrected file) give a new fingerprint and reprocess the partition. Retries are safe and
reprocessing is deliberate.

**Time travel** on Iceberg answers *what did this table say on the day that report ran?*

---

## Storage and compute

The `Engine` interface has four methods. Code above it doesn't know whether storage is
Parquet on a laptop, Iceberg on S3 or Snowflake.

This is mostly about testing: a pipeline you can run end to end on a laptop in under a
minute gets its edge cases tested. The interface deliberately isn't `execute(sql)`;
passing arbitrary SQL through would defeat the point.

Target setup: **Iceberg on S3 with a REST catalog**, queried by Trino (ad hoc and BI),
Spark (large rewrites, ML features) and DuckDB (local, small marts). See
[ADR-0001](adr/ADR-0001-iceberg-table-format.md).

The engine resolves storage from the URI (`dataforge.engines.storage`), so the same code
serves a local folder and any S3-compatible endpoint (AWS, Floci, MinIO). The test suite
runs it against a live S3 API.

One real difference: **S3 has no atomic directory rename**. Locally, a partition is
written to a staging folder and swapped in. On S3, new files are written and old ones
deleted, so a reader can briefly see both. Fixing that is the main reason Iceberg is the
production target. See [docs/11](11-local-setup-and-testing.md).

---

## Managed vs self-managed

| Component | Choice | Why |
|---|---|---|
| Orchestrator | Managed Airflow (MWAA/Astronomer/Composer) | Running Airflow is real work and doesn't set the platform apart |
| Catalog | Managed REST catalog (Glue, Polaris, Nessie) | Critical to get right, nothing to gain from building it |
| Compute | Serverless first (Athena/Trino), reserved for steady load | Pay for peaks, commit for the baseline |
| Streaming sink | Managed (MSK Connect, Firehose) | See ADR-0007 |
| **The framework** | **Built in-house** | It encodes how this organisation defines good data |

Buy what's generic and build what's specific to you.
