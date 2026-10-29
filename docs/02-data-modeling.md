# 02 — Data modelling

## Grain first

Every model states its grain at the top of its description. The grain is the one thing
a consumer has to know to use a table correctly, and most double-counting comes from an
assumed grain.

| Model | Grain | Type |
|---|---|---|
| `stg_orders` | One row per order | View over silver |
| `stg_customers` | One row per customer per CDC change | View over silver |
| `dim_customer` | One row per customer **per attribute version** | SCD2 dimension |
| `fct_orders` | One row per order | Transaction fact |
| `agg_revenue_daily` | One row per (date, channel, currency) | Aggregate |

Changing a primary key is **breaking under every compatibility policy**, because a grain
change quietly breaks every aggregate built on the table.

---

## Why SCD2 for customers

Type 1 (overwrite with the current state) is simpler and cheaper, but wrong for
attributes used in reporting.

Questions like *"revenue by customer segment last quarter"* need the segment each
customer was in *then*. With type 1, every re-tiering silently changes past numbers.

Type 2 costs more rows, a point-in-time join and a merge that has to be right. Here's
what keeps it manageable:

- **Changes are detected by row hash**, so new contract columns are included
  automatically.
- **Unchanged rows don't create a version.** A source that re-sends everything nightly
  won't multiply the dimension by 365.
- **Versions start at the event time**, not ingest time, so late records land in the
  right period.
- **Surrogate keys are reproducible**: `hash(natural_key, valid_from)`, not a sequence.
  A rebuild gives the same keys and facts stay valid.
- **`valid_to` uses a far-future date instead of NULL**, which keeps `between` queries
  simple and lets engines prune on the range.
- **Deletes close the current version** and keep history, since existing facts still
  need something to join to.

Overlapping validity intervals are the first thing to break when a merge is wrong, so
there's a generic test for it (`scd2_no_overlapping_versions`). An overlap makes
point-in-time joins fan out and double count.

---

## Fact design

`fct_orders` has one row per order.

**1. Incremental merge, not full refresh.** The table keeps growing and old orders
rarely change, so rewriting all of history nightly is wasteful.
`on_schema_change='append_new_columns'` means adding a column doesn't force a rebuild
either.

**2. The customer key is looked up as of the order time**, not today:

```sql
left join customer
    on  orders.customer_id = customer.customer_id
    and orders.order_ts   >= customer.valid_from
    and orders.order_ts    < customer.valid_to
```

Joining on `is_current` would throw away the history that SCD2 exists to keep.

**3. Currency is converted here, once, and stored.** Converting in the BI layer means two
dashboards can report different revenue for the same quarter. The original amount is
kept too, so the conversion can be checked.

**Quality flags live on the fact.** `has_missing_fx_rate` and `has_unmatched_customer`
let consumers filter to trusted rows without knowing how the pipeline works, and let a
test check these stay rare. A test that bans something that legitimately happens
sometimes would stay red, and people would learn to ignore it.

---

## Star vs wide table vs aggregate

Each has its place. Don't pick one as a house style.

| Shape | Use when | Cost |
|---|---|---|
| **Star (fact + dims)** | Several facts share dimensions; history matters; a semantic layer sits on top | Joins; consumers need to understand the model |
| **Wide / OBT** | One dominant query pattern, self-service users, a BI tool that joins poorly | Duplication; a dimension change rewrites the whole table |
| **Aggregate** | Many queries over many rows, **and** a stable grain | Another table to keep in sync |

This repo has a star schema and one aggregate, and no wide table: with four data
products there's no dominant query pattern yet. When one shows up, start with a view
over the star and only materialise it if it's too slow.

`agg_revenue_daily` has a **reconciliation test against its fact table**. That catches
the aggregate drifting from its source (e.g. after a late backfill), which no other test
would notice, since the aggregate is still unique, non-null and in range.

---

## Physical design follows access patterns

| Consumer | Pattern | Physical design |
|---|---|---|
| Executive BI | Last 13 months by date/channel | `agg_revenue_daily`, partitioned by date |
| Ad-hoc analysts | Arbitrary slices, mostly recent | `fct_orders` partitioned by `order_date`, clustered on `channel`, `order_status` |
| ML features | Full history, few columns, point-in-time correct | Columnar storage + SCD2 |
| Reverse-ETL | Incremental pulls on `_built_at` | Keep `_built_at` on marts |
| Finance reconciliation | One period, with quality flags | Flags on the fact; original amount kept |

Cluster on the **two most selective filters from the query log**. Beyond two or three
keys, clustering costs more on write than it saves on read.

---

## Keeping consumers working

- **dbt model contracts (`contract: enforced`)** fix the output schema at build time.
  With the dataforge contract on the input side, an upstream change fails the build
  instead of silently changing what consumers see.
- **Additive by default.** New columns are safe; removals go through expand/contract.
- **Views help with deprecation.** When a mart changes shape, the old name becomes a view
  over the new table for a while, so consumers can migrate at their own pace.
- **Masked columns are renamed** (`customer_email` → `customer_email_token`), so nobody
  mistakes a hash for a real email.

---

## The Python/SQL boundary

| Concern | Where | Why |
|---|---|---|
| API paging, CDC envelopes, retries | Python | Procedural logic |
| Type coercion, timezones, decimal precision | Python | Per-field rules from the contract |
| Tokenisation | Python | Needs secrets |
| Quarantine routing | Python | Splits a batch into two outputs |
| Business definitions, joins, aggregates | dbt | Set-based, with tests, docs and lineage built in |
| Point-in-time dimension joins | dbt | Easier to express in SQL |

Mixing them gets you the worst of both: business rules hidden in Python where analysts
can't review them, and type handling in SQL that quietly casts to NULL.
