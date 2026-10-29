# ADR-0008 — Pipeline state lives outside the orchestrator

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

Incremental pipelines need durable state: high-water marks and a record of what's
already been processed. Airflow's XComs and Variables are right there and need no extra
infrastructure.

## Decision

State lives in a `_control` schema in the warehouse (`watermarks`, `run_manifests`,
`quality_results`), behind a `StateStore` interface. The JSON implementation in this repo
is for local use and tests.

## Alternatives considered

**Airflow XComs.** Scoped to a DAG run, meant for small values, stored in the metadata DB
and removed by `db clean`. Clearing a task can reset or skip a watermark. And the
pipeline can't run anywhere without Airflow.

**Airflow Variables.** Global mutable state with no history, no transactions and no
record of who changed what. Two DAGs writing the same Variable can corrupt it without
either noticing.

**A file in object storage.** Close to what the local JSON store does. No transactions,
so concurrent writers lose updates. That's the known limitation of the JSON store and
why a transactional backend is first on the roadmap.

**Derive state from the target table** (`select max(updated_at) from silver.x`).
Appealing, but the watermark tracks what was *read* and the table holds what was
*written*, and those differ whenever rows are filtered, quarantined or late. It also
can't tell "no new data" from "the run failed before writing".

## Consequences

**Costs.** One more schema to create, back up and grant access to, plus some plumbing
XComs would have provided.

**Benefits.** The pipeline runs from a laptop, CI or Kubernetes with no orchestrator,
which is what makes the CI smoke test possible. History is queryable: "why did yesterday
only pick up 12 rows?" is a SQL query against the manifests. State survives deleted DAGs,
metadata cleanup and orchestrator migrations. Watermarks only move forward, so backfills
can't rewind them. And the manifest doubles as an audit record.
