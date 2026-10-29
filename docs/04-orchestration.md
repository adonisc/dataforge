# 04 — Orchestration and operations

## The workflow

```
 ingest__commerce__orders        (cron 02:00)      ─┐
   preflight → ingest_validate_publish → metrics    │  produces
                                                    │  Asset lake://silver.orders
 ingest__commerce__customers     (cron 02:00)      ─┤  Asset lake://silver.customers
 ingest__finance__fx_rates       (cron 02:00)      ─┤  Asset lake://silver.fx_rates
 ingest__product__app_events     (cron 05:00)      ─┘
                                                    │
                                                    ▼  scheduled on Assets, not cron
                               build__commerce__gold
                                 register_lake → dbt_build → dbt_docs → freshness_audit
```

Where each stage lives: **ingestion** and **validation** are both in
`ingest_validate_publish` (see below for why it's one task), **transformation** is
`dbt_build`, **publication** is the Asset update that triggers gold, and **monitoring**
is `publish_metrics` plus `freshness_audit`.

---

## Why a DAG factory

Hand-written DAGs drift apart. One team adds backoff, another doesn't; someone forgets
`max_active_runs=1` and a backfill corrupts a table. The real problem is not having one
place that defines how pipelines run.

With a factory, retries, concurrency, alerting, lineage and docs are defined once and
apply to every DAG.

**The costs:**

| Cost | Mitigation |
|---|---|
| Generated DAGs are harder to read in the UI | Shallow graphs, explicit task ids, and the contract rendered into `doc_md` |
| A factory error breaks *every* DAG | CI should import all DAGs and fail on errors or slow imports (not set up yet) |
| One-off needs have no obvious home | A documented way for a contract to opt out and ship its own DAG (not built yet) |
| Import-time work runs on every parse | The factory only parses YAML at import; config and secrets load *inside* tasks |

Loading secrets at import time would keep them in the DAG processor's memory and break
every DAG whenever the secret backend hiccups. Inside a task, the same problem is one
retryable failure.

---

## Airflow 3 features in use

**Assets.** Each ingestion DAG declares its silver table as an `Asset`, and gold runs on
those Assets. Gold starts when the data is ready, not when the clock says it probably is.

**DAG bundles and versioning.** With a Git bundle, each run records the bundle version it
ran. When re-runs are set to use their original version, a cleared or backfilled run uses
the same code it ran the first time, so you can tell whether a changed number came from
new data or new logic. That's why production should use a Git bundle rather than a bind
mount.

**Task SDK.** Tasks talk to Airflow through the Task Execution API and can't reach the
metadata DB directly. Nothing here reads Variables or sessions at module level.

**Scheduler-managed backfills.** Backfills run from the UI with the same concurrency
limits as normal runs, instead of from someone's laptop.

**Freshness on the data, not the task.** Freshness is checked with a `freshness` rule in
the contract and an external monitor, not by task duration.

> The external monitor matters most. A check inside the pipeline can't notice that the
> pipeline didn't run at all.

---

## Idempotency

Running a date N times should leave the same result as running it once. Three pieces
make that work.

**1. Input fingerprint.** A run is keyed by `(dataset, logical_date, fingerprint)`; the
fingerprint hashes the sorted source URIs, row count and high-water mark.

- Same inputs, already succeeded → `skipped_duplicate`. **Safe retries.**
- Different inputs (e.g. a corrected file) → new fingerprint → reprocesses. **Deliberate
  reprocessing.**

`--force` overrides this explicitly.

**2. Merge, then overwrite the partition.** Never append. Read the partition, union with
the new rows, dedupe by primary key, write it back in one commit. On Iceberg that's a
single `MERGE INTO`; locally it's write-to-staging then swap, so a crash leaves the old
partition intact.

**3. The watermark moves last, and only on success.** Moving it before the data is
written can silently lose data.

---

## Retries and failures

```python
retries=3
retry_delay=2min, retry_exponential_backoff=True, max_retry_delay=30min
execution_timeout=2h
max_active_runs=1
```

**Exponential backoff with a cap.** Most transient failures (503s, S3 throttling,
dropped connections) clear within minutes, and fast retries just add load.

**`max_active_runs=1` matters most.** Two runs of the same dataset race on the same
partition, and the loser's rows disappear while both show green. Running one at a time
costs a little latency and keeps the data correct.

**Failure types:**

| Failure | Exit | Airflow does | What to do |
|---|---|---|---|
| Transient infrastructure | 1 | Retries with backoff | Usually fixes itself |
| Source unavailable | 1 | Retries, then fails | Tell the source owner; the late window usually covers it |
| Blocking quality rule | 2 | Retries (harmless), then fails | **Investigate before clearing.** A retry will fail the same way |
| Quarantine-rate limit | 2 | Same | Usually an upstream schema change |
| Missing masking key | 1 | Fails in `preflight` | Environment issue; no data was touched |
| Code bug | 1 | Retries, then fails | Roll back the DAG bundle |

`preflight` fails *before* touching data. `publish_metrics` uses
`trigger_rule="all_done"` so failed runs still report their metrics.

---

## Why ingestion is one task

It's tempting to split `extract → validate → transform → load` into four tasks. It
stays as one because:

- The stages share one large in-memory batch. Splitting means passing it through XCom
  (a metadata-DB row, not a data channel) or re-reading it from storage each step.
- It doesn't add visibility. Per-stage metrics are already in the logs and manifest.
- It adds failure modes: four retry states and partial completion.

Airflow handles **dependencies between datasets**, not the steps inside one job.

---

## Backfills

```bash
# Airflow UI: trigger with {"force_reprocess": true} for the dates you need,
# or use the backfill UI for a range.
dataforge ingest orders --logical-date 2026-03-01 --force
```

Why it's safe:

- Writes are partition-level merges, so you can repeat them.
- The watermark only moves forward, so a backfill can't rewind it.
- `max_active_runs=1`, so a backfill can't race the scheduled run.
- DAG bundle versioning, so a backfill can use the original code.

**For dependent backfills:** bronze is already there, so start at silver and work up to
gold. Run ingestion for the dates, then dbt for the overlapping window. dbt's
`late_arrival_window_days` must be at least the contract's, or the mart will skip rows
written into older partitions. It's a single `vars` entry so it lives in one place.

**Large backfills** should run with lower concurrency on a separate compute pool,
outside business hours.

---

## Freshness and SLO monitoring

Two independent layers, because they catch different failures:

| Layer | Catches | Runs |
|---|---|---|
| `freshness` rule in the pipeline | This batch is stale | Inside the run |
| External freshness monitor | The pipeline didn't run, or ran and produced nothing | Outside Airflow |

The external monitor checks `max(event_ts)` per gold table on its own schedule and pages
when it's past `slo.freshness_minutes`. It's the only thing that notices a dead
scheduler. (It's on the roadmap, not built yet.)

Alert routing:

| Signal | Threshold | Route |
|---|---|---|
| Tier-1 freshness breach | > SLO | Page |
| Tier-1 run failure | 2 in a row | Page |
| Quarantine rate | > 2× trailing mean | Ticket to the dataset owner |
| Volume anomaly | > 50% drop | Page (tier-1) / dashboard (tier-2) |
| Tier-2 freshness | > SLO | Dashboard |
| dbt `warn` test | any | Weekly digest |

Alerts go to `contract.metadata.owner`, which is why a governance test checks it's set.

---

## Lineage and metadata

Two emitters work together:

- **Airflow's OpenLineage provider:** task level (which DAG run, which task, how long).
- **The framework:** dataset level (which partition, contract version, quality metrics,
  source URIs). It emits `START` / `COMPLETE` / `FAIL` with a `dataContract` run facet
  and a `dataQualityMetrics` output facet.

Both go to one collector: Marquez locally, DataHub or a managed catalog in production. A
lineage outage should never fail a data run.

Field classifications are included in the schema facet, so a catalog can show which
downstream tables PII reaches. Column-level lineage from the dbt manifest is on the
roadmap.
