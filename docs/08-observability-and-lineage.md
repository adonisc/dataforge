# 08 — Observability, monitoring and lineage

## What to measure

Four metric families, per run and per dataset. Together they cover the ways a data
product fails in a way consumers notice.

| Family | Metric | Catches |
|---|---|---|
| **Freshness** | `max(event_ts)`, run completion time, age vs SLO | Stale data; a pipeline that didn't run |
| **Volume** | Rows in and published, vs trailing baseline | Truncated extracts; upstream filters; unexpected growth |
| **Error rate** | Quarantine rate, blocking violations, hits per rule | Source quality slipping; upstream schema changes |
| **Completeness** | Valid/total, null rate on required fields | Partial loads; a producer dropping a field |

Plus the operational ones: duration, bytes scanned, partitions written, retries.

---

## A system can't report its own absence

A freshness check inside the pipeline only catches stale data in a run that happened. It
can't catch the run that never happened (a dead scheduler, a paused DAG, an expired
credential), and that's the more common and more harmful failure.

So freshness is checked twice, by two things that fail independently:

| Layer | Catches | Runs |
|---|---|---|
| `freshness` rule in the pipeline | This batch is stale | Inside the run |
| External freshness monitor | The pipeline didn't run, or produced nothing | Outside Airflow |

The external monitor checks `max(event_ts)` per gold table on its own schedule and pages
when it's past `slo.freshness_minutes`. It's #3 on the roadmap.

---

## Metrics are emitted on failure too

```python
@task(task_id="publish_metrics", trigger_rule="all_done")
```

Failed runs are the ones you most need metrics for. A metrics task that only runs on
success makes the dashboard look healthy during an outage.

---

## Structured logging

Every line is JSON with a stable set of keys. Run context (`run_id`, `dataset`,
`logical_date`, `env`, `contract_version`) is set once and added to every line, so one
filter shows a whole run.

That makes questions like *"every run where quarantine_rate > 2% in the last 7 days"* a
one-line query instead of a grep through prose.

Logs go to **stderr** and the run manifest goes to **stdout**, so callers can pipe the
result without the log noise.

Keys that look like secrets are redacted in the formatter as a safety net.

---

## Alert routing

Picking which signals wake someone up is the hard part. Every alert that fires and gets
ignored makes the others easier to ignore too.

| Signal | Threshold | Route | Why |
|---|---|---|---|
| Tier-1 freshness breach | > SLO | **Page** | Consumers are looking at stale numbers now |
| Tier-1 run failure | 2 in a row | **Page** | One failure is usually transient; two is a pattern |
| Blocking quality violation | any, tier-1 | **Page** | The data product isn't updating |
| Volume anomaly | > 50% drop | Page (t1) / dashboard (t2) | Usually a truncated extract |
| Quarantine rate | > 2× trailing mean | Ticket to the owner | Degradation, not an outage |
| dbt `error` test | any | Ticket | `dbt build` stops it before publishing |
| dbt `warn` test | any | Weekly digest | Expected now and then |
| Cost anomaly | > 50% week over week | Ticket | Rarely urgent, always worth knowing |

Alerts go to `contract.metadata.owner`, which is why a governance test checks it's set.

Deliberately **not** alerted: a single retry (that's what retries are for), a single
quarantined row (that's what quarantine is for), and `warn` rules.

---

## Lineage

### Emitted, not scraped

Lineage scraped from SQL or `information_schema` lags behind, misses things, and can't
tell you which **run** produced which **partition**, with what quality metrics, under
which contract version. The pipeline knows all of that, so it emits it directly. Column
info comes from the contract.

### OpenLineage

It's an open spec that many tools read (Marquez, DataHub, Atlan, Unity, Snowflake
Horizon partners), so lineage data stays usable if the catalog changes.

Two emitters: Airflow's OpenLineage provider (task level) and the framework (dataset
level, with `dataContract` and `dataQualityMetrics` facets). Both send to one collector.

A lineage outage should never fail a data run. Lineage matters, but less than the data.

### Classifications travel with lineage

The schema facet includes each field's classification, so a catalog can show which
downstream tables PII reaches. Table-level lineage tells you *what breaks*; column-level
lineage (from the dbt manifest, #4 on the roadmap) tells you *who to notify*.

---

## Tracing a number to its source

Every silver and gold row carries `_run_id`, `_ingested_at`, `_source_uri`,
`_contract_version` and `_record_hash`.

```
"This revenue figure looks wrong."
  → gold row._run_id
  → run manifest: inputs, fingerprint, contract version, code version, metrics, outcome
  → source URIs
  → the exact bronze files
  → (Iceberg) the snapshot from when the report was produced
```

Each step is **stored**, not reconstructed. In an audit, "we think it came from Tuesday's
file" and "here's the file, its hash and the manifest row showing this run read it" are
very different answers.

---

## Dashboards worth having

**Data product health** (one row per dataset): freshness vs SLO, last run status,
quarantine trend, volume vs baseline, owner. The first page on-call opens.

**Quality trend:** rule hits per rule over 30 days. Catches slow decline that never
crosses a threshold on any single day.

**Cost:** bytes scanned and stored per dataset and layer, plus **cost per 1,000 rows
published**. Rising unit cost means the design is degrading.

**Lineage graph:** for impact analysis before a change and root cause after an incident.

---

## Not built yet

- Metrics go to logs and are scraped, not pushed to a time-series backend (Prometheus or
  CloudWatch). The emission points exist; the sink doesn't.
- No external freshness monitor (#3 on the roadmap). This is the biggest gap.
- Volume anomalies use a fixed threshold against a trailing mean. That's blunt on
  purpose: a naive baseline would fire every weekend and holiday.
- No column-level lineage.
- No status page for consumers. Today they hear about incidents from a person.
