# 11 — Running and testing locally

Three tracks, each covering more than the last. Start with A and only go further if you
need what the next one adds.

| Track | Needs | Time | Covers |
|---|---|---|---|
| **A — Pure local** | Python 3.11+ | ~2 min | Framework, quality engine, masking, dbt models, full pipeline |
| **B — Floci (emulated AWS)** | Docker | ~5 min | The S3 and Secrets Manager code paths |
| **C — Airflow 3** | Docker, ~4 GB RAM | ~10 min | DAG factory, Assets, the real scheduler |

> **Does this need a cloud account?** No. Track A uses no network or credentials. The
> cloud code paths (object storage, secrets backend) are real, and Track B runs them
> against an emulator.

---

## Track A — pure local (start here)

```bash
# 1. Python 3.11 or newer
python3 --version

# 2. A virtual environment, so the dev tools (pytest, ruff, mypy, moto, boto3)
#    stay out of your system Python
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 3. Install the package and dev tooling
make install            # == pip install -e ".[dev,duckdb]"

# 4. Everything CI runs: lint, strict typing, contract checks, tests
make ci

# 5. Generate synthetic data and run the pipeline end to end
make demo

# 6. Watch the quality gate block a bad batch (exits 2 on purpose)
make demo-fail
```

`make ci` takes about fifteen seconds and should print `116 passed, 2 skipped`. The two
skips are governance tests that only apply to tier-1 datasets and to non-finance
domains.

### What `make demo` does

```bash
make demo
#   generates synthetic data with seven injected defect types
#   ingests orders, customers (CDC) and app_events for 3 dates
#   writes bronze -> silver and sends bad rows to quarantine
#   writes a run manifest and OpenLineage events
```

Look at the results:

```bash
# Run manifests: inputs, fingerprint, row counts, quality metrics, outcome
python -m json.tool data/_control/state.json | head -40

# Lineage events, one JSON object per line
head -2 data/_control/lineage.jsonl | python -m json.tool

# What's in the lake
find data/lake -name '*.parquet' | head
```

```bash
# Read a table back, check the grain, and confirm no raw PII reached silver
python - <<'PY'
from dataforge.engines.parquet_lake import ParquetLakeEngine
e = ParquetLakeEngine("data/lake")
rows = e.read_table("silver.orders")
print("rows:", len(rows), "unique order_id:", len({r["order_id"] for r in rows}))
print("clear email present:", "customer_email" in rows[0])
print("token:", rows[0]["customer_email_token"])
q = e.read_table("quarantine.orders")
print("quarantined:", len(q), "| first reason:", q[0]["_reasons"])
PY
```

### Things to try

```bash
export DATAFORGE_MASKING_KEY="local-dev-key-not-a-secret"

# Idempotency: the second run is skipped, the third reprocesses on purpose
dataforge ingest orders --logical-date 2026-05-01            # succeeded
dataforge ingest orders --logical-date 2026-05-01            # skipped_duplicate
dataforge ingest orders --logical-date 2026-05-01 --force    # succeeded, same row count

# Validate without writing anything
dataforge ingest orders --logical-date 2026-05-01 --dry-run

# Contract checks
dataforge validate-contracts
dataforge show orders | head -30

# Schema compatibility check: delete a field from contracts/orders.yml,
# leave the version at 1.2.0, then:
make compat        # fails: breaking change without a major version bump
```

Logs are JSON on **stderr** and the run manifest is JSON on **stdout**, so this works:

```bash
dataforge ingest orders --logical-date 2026-05-01 2>/dev/null | jq .metrics
```

### dbt (optional)

```bash
pip install dbt-core dbt-duckdb
make dbt     # dbt deps && dbt build --target dev --fail-fast
make docs    # generate dbt docs
```

The Docker path (`make verify`, see [docs/12](12-docker-desktop-runbook.md)) runs dbt
for you with everything preinstalled.

---

## Track B — Floci: emulated AWS

[Floci](https://floci.io) is an AWS emulator, similar to LocalStack: one container
serving AWS APIs on `:4566`. It supports the three services used here: **S3** (lake and
landing), **Secrets Manager** (masking key and FX API token) and **Glue** (an Iceberg
catalog, if you want it).

### Why bother

Track A never touches object storage or a secrets backend, and those are where the
tricky problems are: addressing style, credentials, listing behaviour, and the fact that
**S3 has no atomic directory rename**, which changes how partition overwrites work. An
emulator lets you find those on a laptop.

### Setup

```bash
# 1. Install the AWS extras (boto3; pyarrow is already installed)
pip install -e ".[aws,dev,duckdb]"

# 2. Start Floci and wait for its health check
make floci-up

# 3. Create buckets, secrets and a Glue database, and upload the landing data.
#    Safe to re-run.
make floci-bootstrap

# 4. Check the cloud code paths work before running the pipeline
make floci-verify

# 5. Run the whole pipeline against emulated AWS
make floci-demo
```

Don't skip `make floci-verify`. It runs six checks, ordered so the first failure tells
you the most:

```
PASS  endpoint reachable: http://localhost:4566
PASS  bucket exists and is reachable: s3://dataforge-lake
PASS  Parquet round-trip on object storage: 50 rows
PASS  partition overwrite idempotent across 3 runs: 20 rows
PASS  secret resolved from the backend: secret://dataforge/masking-key
PASS  masking key resolves via secret://… and tokens are deterministic
6/6 checks passed
```

### What changes between A and B

**Only configuration.** The AWS SDKs and pyarrow both read `AWS_ENDPOINT_URL`:

| | Track A | Track B |
|---|---|---|
| Lake | `data/lake` | `s3://dataforge-lake/warehouse` |
| Landing | `data/landing` | `s3://dataforge-landing` |
| Masking key | `DATAFORGE_MASKING_KEY` (literal) | `secret://dataforge/masking-key` |
| FX API token | `DATAFORGE_API_TOKEN` | the contract's `secret_ref` |
| Endpoint | — | `AWS_ENDPOINT_URL=http://localhost:4566` |

There's no emulator-specific branch anywhere in `src/`.

### Running it by hand

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export DATAFORGE_MASKING_KEY_SECRET_REF=secret://dataforge/masking-key
unset DATAFORGE_MASKING_KEY          # make sure the secret path is used

dataforge ingest orders --logical-date 2026-05-01 \
  --landing-root s3://dataforge-landing \
  --lake-root    s3://dataforge-lake/warehouse \
  --state-path   data/_control/cloud-state.json
```

See what landed:

```bash
# With the AWS CLI, if you have it
aws --endpoint-url http://localhost:4566 s3 ls --recursive s3://dataforge-lake/ | head

# Or with the engine, no extra tools needed
python - <<'PY'
from dataforge.engines.parquet_lake import ParquetLakeEngine
e = ParquetLakeEngine("s3://dataforge-lake/warehouse")
print("silver.orders:", len(e.read_table("silver.orders")), "rows")
print("tombstones:", sum(1 for r in e.read_table("silver.customers") if r["_is_deleted"]))
PY
```

### Managing Floci

```bash
make floci-logs     # follow the container logs
make floci-down     # stop, keep state (buckets and secrets survive)
make floci-reset    # stop and delete state
curl -s http://localhost:4566/_localstack/health | python -m json.tool
```

Or use Floci's own CLI:

```bash
curl -fsSL https://floci.io/install.sh | sh
floci start && eval "$(floci env)"      # exports AWS_ENDPOINT_URL and friends
floci status ; floci doctor             # when something's wrong
```

### One real difference between local disk and S3

`overwrite_partition`, which keeps re-runs idempotent, works differently on each:

- **Local:** write to a `.staging` folder, then swap it in with a directory rename. The
  rename is atomic, so readers see either the old partition or the new one.
- **S3:** there's no atomic rename and no multi-object transaction. The engine writes the
  new files, then deletes the old ones, so for a few hundred milliseconds a reader can
  see both and get duplicate rows.

That gap is the clearest reason to use a table format
([ADR-0001](adr/ADR-0001-iceberg-table-format.md)). Iceberg, Delta and Hudi all solve it
with a manifest of the table's files, swapped in one atomic commit, so readers get the
file list from the catalog instead of listing a prefix.

Until Iceberg is in place, `max_active_runs=1` per dataset keeps the current run as the
only writer, and publishing outside busy query hours helps where the SLO allows.

---

## Track C — Airflow 3

```bash
make up            # Airflow 3 on local disk
# or
make airflow-floci # Airflow 3 writing to emulated AWS
```

Open <http://localhost:8080>. There's no login on the local stack (see
[docs/12](12-docker-desktop-runbook.md)).

You should see five DAGs: four `ingest__<domain>__<dataset>` from the factory, and
`build__commerce__gold`, which runs on **Assets** instead of a cron schedule, so it
starts when its silver inputs are refreshed.

Things to look at:

- Any ingestion DAG → **Docs**. The runbook there comes from the contract: owner, grain,
  each rule with its severity, the freshness SLO and what to do on failure.
- Trigger `ingest__commerce__orders` with `{"force_reprocess": true}` to reprocess a date.
- The **Assets** view shows how the ingestion DAGs feed the gold DAG.

```bash
docker compose logs -f scheduler        # or dag-processor, api-server
make down                               # stop and remove volumes
```

---

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `dataforge: command not found` | Not installed or venv not active: `source .venv/bin/activate && make install`. All `make` targets work without installing, since they run `python -m dataforge.cli` with `src` on the path |
| `ModuleNotFoundError: pydantic` in `make test` | A `pytest` from another Python is on your `PATH`. Use `python -m pytest` (the Makefile already does) |
| `MaskingKeyMissingError` | A contract declares PII and no key is set. `export DATAFORGE_MASKING_KEY=local-dev-key-not-a-secret`, or set `DATAFORGE_MASKING_KEY_SECRET_REF`. This failure is intentional |
| `s3 URI is missing a bucket: 's3:/…'` | Only one slash: something passed the URI through `pathlib.Path`, which turns `s3://` into `s3:/`. Pass it as a string |
| `endpoint unreachable: http://localhost:4566` | Floci isn't running. `make floci-up`, then `docker compose -f docker-compose.floci.yml ps` |
| S3 calls fail inside an Airflow container | Containers can't reach `localhost:4566`; they use `http://floci:4566` (the overlay sets it). `localhost` only works from the host |
| `bucket … not usable` | Bootstrap hasn't run. `make floci-bootstrap` |
| `StorageUnreachableError: could not reach s3://… via aws` | `AWS_ENDPOINT_URL` isn't exported in this shell, so the call went to real AWS. Make targets set it; manual commands don't |
| `could not read secret … from Secrets Manager` | The secret isn't seeded (run the bootstrap) or the endpoint is wrong. The error shows which endpoint was used |
| `orders_volume` fails after a partial run | The volume rule compares against earlier runs. `make clean` resets state, or use `--force` |
| Pipeline exits 2 | Not a crash: a blocking rule fired and nothing was published. Exit 1 is a crash |
| Port 4566 or 8080 in use | Something else has it. `lsof -i :4566`. Floci's port is set by `QUARKUS_HTTP_PORT` |

---

## What each command checks

| Command | Checks |
|---|---|
| `make lint` | Formatting and lint rules |
| `make typecheck` | Strict mypy |
| `make contracts` | All contracts parse; every rule type exists |
| `make test` | Unit, integration, governance and object-store tests |
| `make demo` | Full pipeline on local disk, 3 dates, 3 datasets |
| `make demo-fail` | A blocked batch publishes nothing and exits 2 |
| `make floci-verify` | S3 round-trip, partition idempotency, secret resolution |
| `make floci-demo` | Full pipeline with landing and lake on object storage |
| `make dbt` | dbt models and their tests |
| `make up` / `make airflow-floci` | The Airflow 3 stack |
