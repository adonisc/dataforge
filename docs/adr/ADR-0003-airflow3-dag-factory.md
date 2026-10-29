# ADR-0003 — Airflow 3 with a DAG factory

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

Pipelines need scheduling, dependencies, retries, backfills and an operations UI. The
team already knows Airflow, and Airflow 3 (currently 3.3) adds several features that
matter for a data platform.

## Decision

Managed Airflow 3, with DAGs generated from contracts by a factory: one ingestion DAG
per contract, plus a gold DAG scheduled on **Assets** instead of cron.

## Why Airflow 3

- **DAG bundles and versioning.** A cleared or backfilled run can use the code it first
  ran with, so you can tell whether a changed number came from new data or new logic.
- **Assets.** Gold runs when its inputs are ready, not at a guessed time.
- **Task SDK.** Tasks can't reach the metadata DB directly.
- **Scheduler-managed backfills.** Visible, auditable and subject to the same concurrency
  limits as normal runs.
- **Deadline Alerts instead of SLAs.** Freshness is checked on the data rather than on
  task duration, which is the better approach anyway.

## Alternatives considered

**Hand-written DAGs.** Easier to read one at a time, but they drift apart as a group.

**Dagster.** Its asset-based model fits data products well, arguably better than
Airflow. Not chosen because of team familiarity and migration cost. A fair challenge to
this decision.

**Step Functions / cloud-native.** Cheaper to run but weak for data pipelines: you'd end
up building backfills, catchup and an operations UI yourself.

**Prefect / Temporal.** Well engineered, but smaller data ecosystems and one more system
to run.

## Consequences

**Costs.** An error in the factory breaks every DAG; a CI job that imports all DAGs and
limits import time should catch this (not set up yet). Generated DAGs are harder to read
in the UI, which shallow graphs, explicit task ids and the contract rendered into
`doc_md` help with. One-off needs don't fit the factory naturally; a documented way for
a contract to ship its own DAG is planned.

**Benefits.** Operational behaviour changes in one file. Reproducible backfills.
Scheduling based on data readiness. A runbook in the UI next to each failure.
