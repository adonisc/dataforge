# 12 — Running everything on Docker Desktop

**You only need Docker Desktop.** No Python, virtualenv, dbt or AWS CLI. Every command
below runs in a container built from one image.

The `tools` container uses the *same image* as the Airflow services, so what you run by
hand is exactly what a DAG runs.

---

## 0. Preflight (one minute)

```bash
docker --version           # 24+ is fine; Compose v2 required
docker compose version     # must be v2.x or later
docker info | grep -i 'total memory'
```

| Check | Needs | Why |
|---|---|---|
| Memory | **≥ 4 GB** for Docker | Airflow 3 runs four processes plus Postgres. Below 4 GB the scheduler gets killed and DAGs look stuck |
| Disk | ~3 GB | The image is ~1.5 GB (Airflow + dbt + AWS libraries) |
| File sharing | The repo folder must be shared | Docker Desktop → Settings → Resources → File sharing. `/Users` is shared by default |
| Free ports | 8080, plus 4566 for Floci | `lsof -i :8080` |

Raise memory in **Docker Desktop → Settings → Resources → Memory**.

### Apple Silicon

`apache/airflow` and `postgres` both publish `linux/arm64`, so the stack builds natively
on M-series Macs.

`floci/floci:latest` is a native image, which is architecture-specific. If it won't
start or runs slowly under emulation, switch to the JVM build:

```yaml
# docker-compose.floci.yml
image: floci/floci:latest-jvm
```

---

## 1. Build the image

```bash
git clone <your-repo-url> dataforge && cd dataforge
make build
```

or without `make`:

```bash
docker compose --profile tools build
```

The first build takes 3–6 minutes. It installs Airflow providers using the official
constraints file, the `dataforge` package, and dbt with its packages. Later builds are
cached.

> **Why dbt packages are installed at build time:** running `dbt deps` on every DAG run
> needs network access from workers, fails when the package hub is down, and can pick up
> a different version than the one you tested. Installing them in the image avoids all
> three.

---

## 2. Run it end to end

```bash
make verify
```

This takes about a minute and:

1. generates synthetic source data (with seven kinds of injected defects),
2. ingests **all four source types**: files, Debezium CDC, a landed stream and a
   paginated REST API (against a local stub),
3. registers the Parquet lake as DuckDB views,
4. runs `dbt build`: 3 marts, 4 staging models, **50 tests**.

It should end with:

```
Done. PASS=50 WARN=2 ERROR=0 SKIP=0

  End to end OK: 4 source types ingested, marts built, dbt tests passed.
```

**The 2 warnings are expected.** They're the two referential-integrity tests set to
`warn` (`orders → customers`, `fct_orders → dim_customer`). A new customer can show up in
orders before the CDC feed catches up, which isn't worth paging anyone over. Green with
two warnings is a pass.

### Or step by step

```bash
make seed              # synthetic data only
make pipeline          # ingest all four source types
make warehouse-docker  # expose the lake to DuckDB
make dbt-docker        # build the marts and run the dbt tests
make test-docker       # the Python test suite
make gate              # everything CI runs: lint, mypy --strict, contracts, tests
```

The same thing without `make`:

```bash
docker compose --profile tools run --rm tools "python scripts/generate_mock_data.py --date 2026-05-01 --days 3"
docker compose --profile tools run --rm tools "dataforge ingest orders --logical-date 2026-05-01"
docker compose --profile tools run --rm tools "pytest -q"
docker compose --profile tools run --rm tools "cd transform/dbt && dbt build --target dev"
docker compose --profile tools run --rm tools bash     # interactive shell
```

---

## 3. Start Airflow 3

```bash
make up
make logs        # follow startup; ctrl-c stops following, not the stack
```

Open **http://localhost:8080**.

**There's no login.** Airflow 3 uses `SimpleAuthManager` by default, and the local stack
sets `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`, so everyone who opens the UI
is an admin. That's fine on localhost and **never okay in a shared environment**, which
should use a real auth manager connected to your identity provider.

### What you should see

Five DAGs: four `ingest__<domain>__<dataset>` generated from the four contracts, plus
`build__commerce__gold`.

| Look at | What it shows |
|---|---|
| Any ingestion DAG → **Docs** | A runbook built from the contract: owner, grain, each rule and its severity, the freshness SLO, and what to do on failure |
| **Assets** view | `build__commerce__gold` runs on Assets, not cron: it starts when its silver inputs are refreshed |
| The gold DAG | `register_lake → dbt_build → dbt_docs → freshness_audit`. The first task connects the Python and SQL sides |
| Trigger `ingest__commerce__orders` with `{"force_reprocess": true}` | Reprocesses a date. Safe to repeat, since writes are partition-level merges |

```bash
make ps       # container status
make down     # stop, keep data
make reset    # stop and delete everything (database, lake, warehouse)
```

---

## 4. Emulated AWS (Floci), optional

Runs the object-storage and Secrets Manager code paths without an AWS account.

```bash
docker compose -f docker-compose.floci.yml up -d floci
docker compose --profile tools run --rm tools "python scripts/floci_bootstrap.py"
docker compose --profile tools run --rm tools "python scripts/verify_cloud.py --bucket dataforge-lake"
```

Or run Airflow with its DAGs writing to emulated S3:

```bash
docker compose -f docker-compose.yml -f docker-compose.floci.yml up -d
```

**Most common mistake:** from your Mac the endpoint is `http://localhost:4566`, but from
inside a container it's `http://floci:4566`. The overlay sets the container value; only
override it when running something from the host.

---

## 5. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Scheduler keeps restarting or DAGs never run | Docker has less than 4 GB. Raise it in Settings → Resources |
| `bind source path does not exist` | The repo is outside Docker Desktop's shared paths. Move it under your home folder or add the path in Settings → Resources → File sharing |
| `port is already allocated` | Something is using 8080. `lsof -i :8080`, or change the port in `docker-compose.yml` |
| Permission errors writing to `data/` | Containers run as uid 50000. `airflow-init` sets up that folder on boot; if you created `data/` yourself, `make reset` and let it rebuild |
| `dbt: source not found` | The lake isn't registered. `make warehouse-docker`, or let the gold DAG's `register_lake` task do it |
| Duplicate rows in marts after editing a model | `on_schema_change='append_new_columns'` keeps rows built with the old logic. Run `make dbt-rebuild` (a `--full-refresh`) after changing model logic |
| `make verify` shows 2 warnings | That's a pass. See section 2 |
| Floci slow or failing on Apple Silicon | Switch to `floci/floci:latest-jvm` |
| Image build fails at `dbt deps` | No outbound network. The build needs it once; nothing does after that |
