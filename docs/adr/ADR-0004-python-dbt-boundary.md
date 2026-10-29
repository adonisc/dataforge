# ADR-0004 — Python up to silver, dbt from silver to gold

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

Transformations could live entirely in dbt, entirely in Python/Spark, or be split. If
they're split, the line needs a clear rule, or it moves with whoever wrote the last
pipeline.

## Decision

**Python** handles source → bronze → silver. **dbt** handles silver → gold.

The rule: **Python for procedural work that needs privileged access; SQL for set-based
work that benefits from dbt's tests, docs and lineage.**

| Concern | Where | Why |
|---|---|---|
| API paging, CDC envelopes, retries | Python | Control flow SQL can't express |
| Type coercion, timezones, decimal precision | Python | Per-field rules from the contract |
| Tokenisation | Python | Needs secrets |
| Quarantine routing | Python | Splits a batch into two outputs |
| Business definitions, joins, aggregates | dbt | Set-based, with tests, docs and lineage built in |
| Point-in-time dimension joins | dbt | Easier to express in SQL |

## Alternatives considered

**All dbt (land raw, transform in the warehouse).** Simplest, and the right default for
sources already in the warehouse. Not chosen because CDC collapsing, API paging and keyed
tokenisation fit SQL poorly, and quarantine needs to split a batch into two outputs,
which dbt doesn't do. Forcing it leads to messy macros and secrets in the warehouse.

**All Python/Spark.** One toolchain, and needed at very high volumes. Not chosen for gold
because it loses dbt's tests, docs and lineage, and analysts couldn't own business logic.
Definitions buried in Python can't be reviewed by analysts.

## Consequences

**Costs.** Two languages, two toolchains, two sets of tests. Contributors need to know
which side a change belongs on (the table above helps), and reviews have to keep logic
from leaking across.

**Benefits.** Each tool does what it's good at. Analytics engineers own business logic
without Python; data engineers own ingestion without SQL macros. Quarantine is possible
at all.

**Note:** this is a team decision as much as a technical one. On a team without Python
experience, all dbt plus a managed ingestion tool is the better answer.
