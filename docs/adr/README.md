# Architecture Decision Records

One file per decision that would be expensive to reverse. Each covers the context, the
decision, the alternatives considered and the trade-offs accepted.

They answer "why is it like this?" months later, and the rejected alternatives show
what new evidence would justify revisiting a decision.

| ADR | Decision | Status |
|---|---|---|
| [0001](ADR-0001-iceberg-table-format.md) | Apache Iceberg as the table format | Accepted |
| [0002](ADR-0002-declarative-contracts.md) | Declarative contracts over imperative pipelines | Accepted |
| [0003](ADR-0003-airflow3-dag-factory.md) | Airflow 3 with a DAG factory | Accepted |
| [0004](ADR-0004-python-dbt-boundary.md) | Python up to silver, dbt silver→gold | Accepted |
| [0005](ADR-0005-quarantine-over-failfast.md) | Quarantine by default, blocking where needed | Accepted |
| [0006](ADR-0006-masking-at-ingestion.md) | Mask on write into silver with keyed HMAC | Accepted |
| [0007](ADR-0007-microbatch-over-streaming.md) | Micro-batch from a landed sink over true streaming | Accepted |
| [0008](ADR-0008-state-outside-orchestrator.md) | Pipeline state outside the orchestrator | Accepted |
