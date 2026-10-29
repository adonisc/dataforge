# ADR-0002 — Declarative contracts over imperative pipelines

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

The goal is a reusable pipeline pattern. The organisation expects 20–50 data products
within two years, with a team of 3–6 engineers. The usual approach (one Python module or
dbt project per source) is quick to start and gets harder to maintain with every dataset.

## Decision

One contract-driven runtime. Each data product has a YAML contract that drives
ingestion, schema, quality rules, privacy, physical layout, DAG generation, CI checks and
lineage. Adding a data product means adding a YAML file and getting it reviewed.

## Alternatives considered

**One pipeline per source.** Fastest for the first dataset and easy to review on its
own. Not chosen because by twenty pipelines, changing retry or masking behaviour means
twenty merge requests, and some never get done. Behaviour drifts and nobody can say what
the platform guarantees.

**Logic inside config** (SQL snippets or expressions in YAML). Not chosen: that's code
without type checks, tests or a debugger. Here, config says *what* and *how severe*; code
says *how*.

**A low-code ingestion tool** (Fivetran/Airbyte style). The right choice for standard
SaaS sources. Not chosen as the pattern because it can't express per-rule severity,
classification-driven masking or quarantine with replay.

## Consequences

**Costs.** More indirection: you need to understand the framework before any single
dataset, which makes onboarding harder. A framework bug hits every data product at once,
so the test suite matters a lot. The contract model is itself an API, so changing it is
a migration.

**Benefits.** Fix a bug once for every data product. Review operational behaviour in one
place. Onboarding a dataset is mechanical. Consistency comes from the design, not from
reviewers catching drift. CI can check every dataset at once; the governance tests over
the shipped contracts only work because contracts are data.

## Revisit when

Fewer than three data products are expected overall, or two products need fundamentally
different runtimes. Then use two named patterns, not one with branches.
