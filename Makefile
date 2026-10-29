# Developer commands. CI runs these same targets.

SHELL := /bin/bash
PY ?= python3
DATE ?= 2026-05-01
DAYS ?= 3
export DATAFORGE_MASKING_KEY ?= local-dev-key-not-a-secret

# Run as a module with src on the path, so targets work on a fresh clone without `make install`.
export PYTHONPATH := src:$(PYTHONPATH)
DATAFORGE := $(PY) -m dataforge.cli
# `python -m` makes sure tools use the same interpreter as $(PY).

.DEFAULT_GOAL := help
.PHONY: help install fmt lint typecheck test test-fast contracts compat data demo demo-fail dbt docs up down clean ci \
        floci-up floci-bootstrap floci-verify floci-demo floci-logs floci-down floci-reset airflow-floci \
        demo-fx register warehouse


# ---------------------------------------------------------------------------------
# DOCKER-ONLY PATH (no Python, no dbt, no venv on the host — only Docker Desktop)
#
# `tools` is a one-shot container built from the Airflow image. The raw
# `docker compose` commands are in docs/12 if you'd rather skip make.
# ---------------------------------------------------------------------------------
DC        := docker compose
TOOLS     := $(DC) --profile tools run --rm -T tools
DATE      ?= 2026-05-01
DAYS      ?= 3

.PHONY: build up down logs ps shell verify seed pipeline warehouse-docker dbt-docker test-docker gate reset

build: ## Build the one image everything runs on
	$(DC) --profile tools build

up: build ## Start Airflow 3 (api-server, scheduler, dag-processor, triggerer)
	$(DC) up -d
	@echo ""
	@echo "  Airflow UI  http://localhost:8080   (no login: local-only dev setting)"
	@echo "  Follow boot: make logs"

down: ## Stop everything, keep volumes
	$(DC) --profile tools --profile lineage down

reset: ## Stop everything and destroy all state (database, lake, warehouse)
	$(DC) --profile tools --profile lineage down -v
	$(TOOLS) "rm -rf data/lake data/warehouse data/_control data/landing transform/dbt/target" || true

ps: ## Show container status
	$(DC) ps

logs: ## Follow Airflow logs
	$(DC) logs -f --tail=80 scheduler api-server dag-processor

shell: ## Interactive shell inside the image
	$(DC) --profile tools run --rm tools bash

seed: ## Generate synthetic source data (in Docker)
	$(TOOLS) "python scripts/generate_mock_data.py --date $(DATE) --days $(DAYS)"

pipeline: seed ## Run the ingestion pipeline for all four source types (in Docker)
	$(TOOLS) 'for d in $$(python -c "import datetime as t;s=t.date.fromisoformat(\"$(DATE)\");print(\" \".join(str(s+t.timedelta(days=i)) for i in range($(DAYS))))"); do for ds in orders customers app_events; do dataforge ingest $$ds --logical-date $$d || exit 1; done; done'
	$(TOOLS) "python scripts/ingest_fx_local.py --date $(DATE) --days $(DAYS)"

warehouse-docker: ## Expose the lake to DuckDB so dbt can read it (in Docker)
	$(TOOLS) "python scripts/register_lake.py --lake-root /work/data/lake --database /work/data/warehouse/dev.duckdb"

dbt-docker: warehouse-docker ## Build the curated layer and run its 50 tests (in Docker)
	$(TOOLS) "cd transform/dbt && dbt build --target dev"

test-docker: ## Run the 116-test suite (in Docker)
	$(TOOLS) "pytest -q"

gate: ## Everything CI runs, in Docker: lint, types, contracts, tests
	$(TOOLS) "ruff format --check src tests scripts airflow && ruff check src tests scripts airflow && mypy && dataforge validate-contracts && pytest -q"

verify: pipeline dbt-docker ## Full end-to-end proof: pipeline -> lake -> dbt marts
	@echo ""
	@echo "  End to end OK: 4 source types ingested, marts built, dbt tests passed."

help: ##help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## [optional, host] Install the package and dev dependencies
	$(PY) -m pip install -e ".[dev,duckdb]"

fmt: ## [optional, host] Auto-format
	ruff format src tests scripts airflow
	ruff check --fix src tests scripts airflow

lint: ## [optional, host] Lint without modifying
	ruff format --check src tests scripts airflow
	ruff check src tests scripts airflow

typecheck: ## [optional, host] Strict static typing
	$(PY) -m mypy

test: ## [optional, host] Full test suite with coverage
	$(PY) -m pytest --cov=dataforge --cov-report=term-missing

test-fast: ## [optional, host] Tests without coverage (the inner loop)
	$(PY) -m pytest -q

contracts: ## Validate every contract and compile its rules
	$(DATAFORGE) validate-contracts

compat: ## Check schema evolution against the default branch
	@git worktree add -f /tmp/dataforge-base origin/main 2>/dev/null || true
	$(DATAFORGE) check-compatibility --base /tmp/dataforge-base/contracts

data: ## Generate synthetic sample data
	$(PY) scripts/generate_mock_data.py --date $(DATE) --days $(DAYS)

demo: data ## Run the whole pipeline locally over synthetic data (all 4 source types)
	@for d in $$($(PY) -c "import datetime as t;s=t.date.fromisoformat('$(DATE)');print(' '.join(str(s+t.timedelta(days=i)) for i in range($(DAYS))))"); do \
	  for ds in orders customers app_events; do \
	    $(DATAFORGE) ingest $$ds --logical-date $$d || exit 1; \
	  done; \
	done
	@$(MAKE) --no-print-directory demo-fx
	@echo "done. lake: data/lake  manifests: data/_control/state.json  lineage: data/_control/lineage.jsonl"

demo-fx: ## Ingest fx_rates against a local stub of the Treasury API
	@$(PY) scripts/ingest_fx_local.py --date $(DATE) --days $(DAYS)

demo-fail: ## Demonstrate the blocking quality gate (expected to exit 2)
	@mkdir -p data/landing/orders/dt=2030-01-01
	@echo '{"order_id":"BAD","customer_id":"C1","order_status":"paid","channel":"web","currency_code":"USD","gross_amount":"10.00","order_ts":"2099-01-01T00:00:00Z","updated_at":"2099-01-01T00:00:00Z","order_date":"2030-01-01"}' \
	  > data/landing/orders/dt=2030-01-01/orders.jsonl
	@$(DATAFORGE) ingest orders --logical-date 2030-01-01; \
	  test $$? -eq 2 && echo "blocked as designed: nothing was published, exit code 2 distinguishes bad data from a crash"

register: ## Expose the Parquet lake to DuckDB as views, so dbt can read it
	$(PY) scripts/register_lake.py

warehouse: demo register ## Everything dbt needs: data in the lake, registered in DuckDB

dbt: register ## Build the curated layer and run its tests
	cd transform/dbt && dbt deps && dbt build --target dev

dbt-rebuild: register ## Rebuild the marts from scratch (use after changing model logic)
	cd transform/dbt && dbt deps && dbt build --target dev --full-refresh

docs: ## Generate dbt documentation
	cd transform/dbt && dbt docs generate --target dev

# ---------------------------------------------------------------------------------
# Floci: emulated AWS (S3 + Secrets Manager + Glue) on localhost:4566.
# These targets set the AWS env vars themselves, so your shell state doesn't matter.
# ---------------------------------------------------------------------------------
FLOCI_ENV := AWS_ENDPOINT_URL=http://localhost:4566 \
             AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
             AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 \
             DATAFORGE_MASKING_KEY_SECRET_REF=secret://dataforge/masking-key \
             DATAFORGE_MASKING_KEY=
LAKE_URI  ?= s3://dataforge-lake/warehouse
LANDING_URI ?= s3://dataforge-landing

floci-up: ## Start Floci (emulated AWS) and wait until it is healthy
	docker compose -f docker-compose.floci.yml up -d floci
	@printf "waiting for Floci"
	@for i in $$(seq 1 60); do \
	  if curl -fsS http://localhost:4566/_localstack/health >/dev/null 2>&1; then echo " ready"; exit 0; fi; \
	  printf "."; sleep 2; \
	done; echo " TIMEOUT"; docker compose -f docker-compose.floci.yml logs --tail=40 floci; exit 1

floci-bootstrap: data ## Create buckets, secrets and a Glue database; upload landing data
	$(FLOCI_ENV) $(PY) scripts/floci_bootstrap.py

floci-verify: ## Prove the S3 and secret code paths work before trusting a run
	$(FLOCI_ENV) $(PY) scripts/verify_cloud.py --bucket dataforge-lake

floci-demo: ## Run the pipeline end to end against emulated AWS
	@for d in $$($(PY) -c "import datetime as t;s=t.date.fromisoformat('$(DATE)');print(' '.join(str(s+t.timedelta(days=i)) for i in range($(DAYS))))"); do \
	  for ds in orders customers app_events; do \
	    $(FLOCI_ENV) $(DATAFORGE) ingest $$ds --logical-date $$d \
	      --landing-root $(LANDING_URI) --lake-root $(LAKE_URI) \
	      --state-path data/_control/cloud-state.json || exit 1; \
	  done; \
	done
	@echo "published to $(LAKE_URI)"

floci-logs: ## Tail Floci logs
	docker compose -f docker-compose.floci.yml logs -f --tail=100 floci

floci-down: ## Stop Floci, keeping its state volume
	docker compose -f docker-compose.floci.yml down

floci-reset: ## Stop Floci and destroy its state (buckets, secrets)
	docker compose -f docker-compose.floci.yml down -v

airflow-floci: ## Start Airflow 3 with its DAGs writing to emulated AWS
	docker compose -f docker-compose.yml -f docker-compose.floci.yml up -d --build
	@echo "Airflow UI: http://localhost:8080 (admin/admin)   Floci: http://localhost:4566"

clean: ## Remove generated data and caches
	rm -rf data/lake data/warehouse data/_control data/landing
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov coverage.xml
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

ci: lint typecheck contracts test ## Everything CI runs, locally
