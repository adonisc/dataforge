# 03 — Data quality, contracts and testing

## The idea

Bad data that quietly reaches a consumer is worse than a pipeline that fails loudly. A
failed run has an owner and a fix. Wrong data gets used in decisions and found weeks
later, and it costs trust.

So the guiding rule is: **late can be fixed, wrong can't.**

---

## Severities

Each rule has one of three severities, set **in the contract**, not in the engine.

| Severity | What happens | Use when |
|---|---|---|
| `blocking` | Nothing is published. Run exits 2; the manifest records why | The data can't be trusted |
| `quarantine` | Bad rows are set aside with their payload and every rule they broke; the rest is published | Occasional row-level defects |
| `warn` | A metric is emitted and the row passes | Worth tracking, not worth paging |

The same rule type can need different severities in different contracts:

```yaml
# orders.yml: a half-empty extract is probably a truncated upload,
# and publishing it would under-report revenue.
- id: orders_volume
  type: row_count_anomaly
  severity: blocking

# customers.yml: CDC feeds have quiet days.
- id: customers_volume
  type: row_count_anomaly
  severity: warn
```

Neither choice is right everywhere. The contract owner knows which one fits.

### Why not fail fast on everything

Failing on every rule means one bad row in ten million stops a tier-1 pipeline. Then
someone disables the rule to get the run through, and it never comes back.

Quarantine keeps data fresh, isolates the bad rows and makes them visible, with a limit
so it can't be abused.

### The quarantine-rate limit

```yaml
quality:
  max_quarantine_rate: 0.02
```

If more than this share of a batch is quarantined, the batch **blocks**, even if no
single rule is blocking.

This catches the case where, say, a schema change starts quarantining 90% of rows each
night and nothing alerts because each rule is "only" quarantine. Past the threshold the
batch is treated as broken, not just dirty.

Each contract sets its own value: `0.02` for orders, `0.01` for customers, `0.05` for
tier-2 events, `0.0` for FX rates.

---

## Tiers

Not every dataset needs the same guarantees.

| | Tier 1 (orders, customers, fx_rates) | Tier 2 (app_events) |
|---|---|---|
| Quarantine limit | 0–2% | 5% |
| Volume anomaly | Blocking | Warn |
| Timestamp validity | Blocking | Quarantine |
| Freshness SLO | 6–24h, paged | 2h, dashboard |
| Bronze retention | 90–365 days | 14 days |
| Review | Named owner approves contract changes | Standard review |

Losing a few pageviews is fine; losing orders is a finance problem. The tier is set in
`metadata.tags`, and a governance test checks that tier-1 datasets have a volume rule.

`fx_rates` is the strictest: `max_quarantine_rate: 0.0` and nearly every rule blocking.
All gold revenue is converted through it, and a bad rate doesn't show up in row counts.

---

## Rule catalogue

Row rules (can quarantine single rows):

| Type | Checks | Notes |
|---|---|---|
| `not_null` | Required fields present | Treats `""` as missing, since CSV and APIs often send that for "no value" |
| `accepted_values` | Value is in a set | Skips nulls; that's `not_null`'s job |
| `range` | Numeric bounds | Uses Decimal; non-numeric input is a violation |
| `regex` | Format | Full match, not search |
| `timestamp_valid` | Parses, not in the future, not before a floor | **Blocking** on tier-1, see below |
| `relationship` | Referential integrity | Known keys are passed in by the caller, never queried from inside the rule |

Dataset rules (whole batch):

| Type | Checks |
|---|---|
| `unique` | Grain; marks the losing duplicates using a declared order |
| `row_count_anomaly` | Minimum rows and drop against a trailing baseline |
| `freshness` | Newest event timestamp within the SLO |

### Why `timestamp_valid` blocks on tier-1

One future-dated `updated_at` pushes the watermark past every real record, and the next
incremental runs pick up nothing while staying green. The watermark code also caps at
`now()`, so there are two defences.

### Why `unique` quarantines on orders but blocks on customers

Redeliveries are normal, so on orders the rule keeps the latest by
`[updated_at, _ingested_at]` and carries on. On `customers`, CDC is collapsed to one row
per key *before* validation, so a duplicate there means the collapse or LSN ordering is
broken.

---

## Quarantine can be replayed

```python
{
  "_run_id": "...", "_dataset": "orders",
  "_rule_ids": ["orders_amount_sane", "orders_currency_iso"],
  "_columns": ["gross_amount", "currency_code"],
  "_reasons": ["'gross_amount'=-42.00 < 0", "..."],
  "payload": { ...the full original record... }
}
```

**Every reason, not just the first.** All rules run on every record, so one look at
quarantine tells you everything wrong with a row.

**The full payload.** Once the source is fixed, rows can go back through the same
pipeline. A quarantine table that only stores error messages can't do that.

Useful standing queries: quarantine rate per dataset over 7 days; top rules by hits;
rows quarantined by rules that no longer exist; age of the oldest unreplayed row. If that
last one keeps growing, nobody is draining quarantine.

---

## Where checks run

### In CI, on every merge request

1. **Contract validity:** every file parses and every rule type exists. With
   `extra="forbid"`, a typo in a key is an error instead of a setting that silently
   does nothing.
2. **Schema compatibility check:** catches changes that would break someone else's
   dashboard, before merge.
3. **Engine unit tests:** the severity behaviour itself is tested.
4. **The real pipeline over synthetic data with known defects**, to show the wiring
   still quarantines and blocks what it should.
5. **`dbt build`:** runs models and their tests together, so nothing builds on a model
   that failed its tests.

### On every scheduled run

Same engine, same rules, no special production mode. Results go to the run manifest, the
logs and the OpenLineage `dataQualityMetrics` facet.

### Source freshness, before models build

`dbt source freshness` runs first, so a green run over stale data doesn't slip through.

---

## dbt tests

Tests default to `severity: error` with `store_failures: true` (failing rows are saved
to a table). Two are deliberately `warn`:

- `fct_orders.customer_key → dim_customer`: a new customer can show up in orders before
  the CDC feed catches up.
- `silver.orders.customer_id → silver.customers`, for the same reason.

And one is a threshold instead of an absolute rule:

```yaml
- expression_is_true:
    expression: "count(case when has_missing_fx_rate then 1 end) * 1.0 / nullif(count(*),0) < 0.01"
```

It checks that missing FX rates stay rare, not that they never happen.

### Three custom generic tests

Generic tests can be reused on any model with a few lines of YAML.

- `scd2_no_overlapping_versions`: no overlapping validity intervals in a type 2 dimension.
- `aggregate_reconciles_to_fact`: an aggregate still sums to its fact table.
- `expression_is_true`: threshold checks.

There's also one singular test, `assert_no_revenue_from_deleted_customers`. New revenue
for an erased customer means the erasure didn't propagate or an id was reused.

---

## The contract as an API

A data contract is an agreement between a producer and its consumers, so it's treated
like an API:

- **Versioned**, with the version stored on every row (`_contract_version`).
- **Reviewed.** Changes are diffs with an author and an approver.
- **Tested** before deployment.
- **Owned.** A governance test checks there's an owner to route alerts to.
- **Shipped with the code**, so test and prod run the same rules.
- **Rolled back** with `git revert`.

To move to a networked registry later (Schema Registry, DataHub, Collibra), keep the
`ContractRegistry` interface and change how it finds contracts. Callers already get a
registry object rather than a file glob.
