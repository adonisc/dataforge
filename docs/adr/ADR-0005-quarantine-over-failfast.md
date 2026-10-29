# ADR-0005 — Quarantine by default, blocking where needed

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

When a record fails a quality rule, the pipeline can fail the batch, drop the row, or set
it aside. That choice decides whether people trust the platform, and it's often made by
accident in the first pipeline someone writes.

## Decision

Three severities, set **per rule in the contract**: `blocking`, `quarantine` (default)
and `warn`. Plus a **quarantine-rate limit** that blocks a batch when too much of it is
rejected.

## Alternatives considered

**Fail fast on everything.** Sounds safe, but one bad row in ten million stops a tier-1
pipeline, someone disables the rule to get the run through, and it never comes back.
Failing fast is right for some rules, so it's a severity rather than the policy.

**Silently drop bad rows.** The worst option. It leaves unexplained gaps, destroys the
evidence needed to fix the source, and consumers just see numbers that look a bit low.

**Always publish and flag downstream.** Every consumer then decides separately, so they
decide differently, and some don't decide at all.

**One global severity.** Doesn't work: `row_count_anomaly` should be **blocking** on
`orders` (a half-empty file is probably a truncated upload) and **warn** on `customers`
(CDC feeds have quiet days).

## The quarantine-rate limit

Quarantine can hide a big problem: say a schema change quarantines most rows every
night, each rule is "only" quarantine, and nothing alerts. `max_quarantine_rate` turns
that into a blocked batch.

## Consequences

**Costs.** Someone has to monitor and drain quarantine, which is why a replay command is
on the roadmap. Consumers need to know a published batch may be missing rows; the
manifest and completeness metric show this. Each contract owner has to choose
severities instead of inheriting a default.

**Benefits.** Pipelines stay fresh despite ordinary bad rows. Problems are visible and
traceable. Rows can be **replayed**, since quarantine keeps the full payload and every
rule broken. Severity choices are reviewable in a diff by the person who knows the
dataset.
