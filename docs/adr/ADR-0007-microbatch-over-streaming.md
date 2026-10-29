# ADR-0007 — Micro-batch from a landed sink, not true streaming

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform

## Context

Event data arrives on Kafka, and the obvious answer is a streaming processor. The first
question, though, is what latency consumers actually need. Here the tightest is a 2-hour
freshness SLO on product analytics.

## Decision

A managed sink (Kafka Connect S3 or Firehose) writes micro-batches to object storage,
rotating hourly. The batch pipeline reads those files the same way it reads every other
source. The pipeline runs **no long-lived consumer**.

## Alternatives considered

**Flink or Spark Structured Streaming.** The right choice below about a one-minute SLO.
Not chosen because it means an always-on cluster, a second processing model with its own
failure modes (checkpoint corruption, growing state, watermark semantics), 24/7
operations and separate quality checks, all for latency nobody needs.

**A Kafka consumer inside the pipeline.** Ties offset management to the orchestrator, so
an Airflow outage could lose data, not just delay it.

**Lambda architecture** (streaming plus batch, reconciled). Every transformation written
twice, with reconciliation forever. Only worth it when there are hard latency *and*
correctness requirements.

## What this gets you

The **managed sink handles delivery** (at least once), and the **batch pipeline handles
duplicates** with idempotent partition writes keyed on offsets.

So **an Airflow outage delays data but never loses it.** Events keep landing, the next
run picks them up, and the merge is idempotent.

## Consequences

**Costs.** Latency of about an hour (sink rotation plus run schedule). Managing small
files becomes a sink configuration task. Event-time windowing, if needed, has to happen
in SQL.

**Benefits.** One processing model, one quality framework, one set of runbooks. No
always-on cluster. The same replay, idempotency and quarantine guarantees as every other
source.

## Revisit when

A consumer genuinely needs sub-minute data (fraud detection, live operations dashboards,
real-time personalisation). Then add a *separate* streaming path for that use case
rather than converting this one.
