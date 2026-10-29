# 09 — Roadmap

Ordered by **what it costs not to have each item**, not by how interesting or hard it is.

Each item says what it is, why it's in that position, roughly how long it takes, and how
you know it's done.

---

## Tier 1: before anyone depends on this

### 1. Transactional state store (1 week)

**What.** Replace `JsonStateStore` with a `_control` schema in the warehouse
(`watermarks`, `run_manifests`, `quality_results`) with transactions and optimistic
concurrency.

**Why first.** It's the one part that's actually wrong for production, not just
incomplete. The JSON store isn't safe across processes: two workers writing at once can
lose a manifest, and that breaks the idempotency check everything else relies on. It
doesn't show up in a demo, which is why it needs to be called out.

**Effort.** About a week. The `StateStore` interface exists, so it's an adapter,
migrations and a concurrency test.

**Done when** a test that really runs two runs concurrently shows the state stays
consistent.

### 2. Tested Iceberg integration (1 week)

**What.** Add MinIO and an Iceberg REST catalog to Docker Compose, implement
`IcebergEngine` against them, and run the integration suite on both engines.

**Why second.** The storage interface is the architecture's main bet, and so far it has
only been proven with Parquet. A second implementation is what shows whether the
interface quietly assumed something only the first one can do. Better to find out now,
with the test suite in place, than during a migration.

**Effort.** About a week, mostly Compose setup and schema-mapping edge cases (decimal
precision, timestamp units, nested types).

**Done when** the same suite passes on both engines, including the merge and idempotency
tests, and a concurrent-reader test shows readers never see two generations of a
partition.

### 3. External freshness monitor (3 days)

**What.** A small service, scheduled outside Airflow, that checks `max(event_ts)` per
gold table against each contract's `slo.freshness_minutes`.

**Why third.** Everything else assumes the pipeline ran. If it doesn't (paused DAG, dead
scheduler, expired credential), nothing notices yet. It's the cheapest item in Tier 1
and closes the biggest blind spot.

**Effort.** About 3 days. It has to stay outside the orchestrator; as an Airflow DAG it
would have the same blind spot.

**Done when** pausing a DAG in a test environment triggers a page within the SLO window.

---

## Tier 2: before a second team uses this

### 4. Column-level lineage from the dbt manifest (1 week)

Parse `manifest.json` and emit `columnLineage` facets. Table-level lineage shows *what
breaks*; column-level shows *who to tell*. It also lets the compatibility check say
"this drops a column three dashboards use" instead of something generic.

### 5. Quarantine replay command (1 week)

`dataforge replay --dataset orders --rule orders_status_known --since 2026-05-01`. Rows
can be replayed already, but only by hand, and manual steps tend not to happen.

### 6. Metrics in a real time-series backend (3 days)

The emission points exist but the sink doesn't. Scraping logs works, but not for precise
alerting.

### 7. Backfill orchestration (2 weeks)

A planner that takes a date range and a dataset, works out which marts are affected,
runs them in dependency order with limited concurrency on a separate pool, and shows
progress. Today a long backfill is a loop in someone's shell.

### 8. Generate dbt sources from contracts (3 days)

`_sources.yml` is written by hand and repeats the contract. Generate it, or the two will
drift.

---

## Tier 3: scale and maturity

### 9. A schema registry (2–3 weeks)

Put contracts behind an API with a UI for non-engineers, keeping git as the source of
truth. **Only after Tiers 1 and 2.** Files in git work fine up to roughly 50 contracts;
a registry added too early is a system with no users. `ContractRegistry` is already
the seam for this.

### 10. Data product catalog and status page (2 weeks)

Built from contracts and run manifests: what exists, who owns it, its grain, its SLO,
and whether it's healthy right now.

### 11. Cost per data product (1 week)

Query tags plus storage accounting, rolled up per contract, tracking **cost per 1,000
rows published**. A single warehouse bill isn't something anyone can act on.

### 12. Seasonality-aware volume checks (1 week)

The current rule is a simple drop threshold against a trailing mean. Day-of-week
patterns plus a holiday calendar would cut false alarms. Full forecasting would be
overkill at this scale.

### 13. Great Expectations or Soda (1 week, only if already used)

**Only if** the organisation already runs one of them. The built-in rule engine covers
these contracts and is much lighter. If analysts already write GX suites, though, it's
better to meet them there.

---

## Not planned

| Not doing | Why |
|---|---|
| True streaming (Flink/Structured Streaming) | No consumer needs sub-minute latency (ADR-0007). Revisit when one does |
| Data mesh / per-domain platform teams | 3–6 engineers. Mesh solves an org problem this team doesn't have yet |
| A UI for writing contracts | Contracts need review, versioning and rollback, which git already gives |
| ML anomaly detection | Not enough history, and current failures are step changes a threshold catches. Revisit after a year of metrics |
| Support for a second orchestrator | The CLI already runs without Airflow, which is the portability that matters |

---

## Sequencing with a team

Tier 1 is about two and a half weeks of focused work and is what it takes before this
should run in production. #1 and #3 suit a senior engineer: #1 is subtle and fails
silently, #3 is small but important.

#2 makes a good onboarding task. It means reading the whole pipeline, and the existing
tests give clear feedback.

Tier 2 items can run in parallel. Don't commit to Tier 3 in advance; real usage will
reorder it.
