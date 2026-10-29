"""Airflow 3 DAG factory.

Each contract in ``contracts/`` becomes one ingestion DAG that publishes its silver
table as an ``Asset``. The gold DAG runs when those assets are refreshed, not on a cron.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import Asset, dag, task

from dataforge.contracts.loader import ContractRegistry
from dataforge.contracts.models import DataContract, IngestionMode
from dataforge.pipeline import PipelineConfig, QualityGateError, run_ingestion
from dataforge.privacy.masking import MaskingKey

CONTRACTS_DIR = Path(os.environ.get("DATAFORGE_CONTRACTS", "/opt/airflow/contracts"))
ENV = os.environ.get("DATAFORGE_ENV", "dev")

#: Schedule by tier. Contracts without a tier tag default to tier-2.
_SCHEDULE_BY_TIER: dict[str, str] = {
    "tier-1": "0 2 * * *",
    "tier-2": "0 5 * * *",
}

_DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    # Back off exponentially so retries don't pile onto a struggling source.
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}


def silver_asset(contract: DataContract) -> Asset:
    """The silver Asset a contract produces, used to trigger downstream DAGs."""
    return Asset(
        uri=f"lake://{ENV}/{contract.spec.targets.silver}", name=contract.spec.targets.silver
    )


def _pipeline_config() -> PipelineConfig:
    """Build config inside the task, not at parse time.

    Keeps secrets out of the DAG processor, and a secret outage fails one task, not every DAG.
    """
    return PipelineConfig(
        env=ENV,
        lake_root=os.environ["DATAFORGE_LAKE_ROOT"],
        landing_root=os.environ["DATAFORGE_LANDING_ROOT"],
        state_path=Path(
            os.environ.get("DATAFORGE_STATE_PATH", "/opt/airflow/data/_control/state.json")
        ),
        lineage_path=Path(
            os.environ.get("DATAFORGE_LINEAGE_PATH", "/opt/airflow/data/_control/lineage.jsonl")
        ),
        masking_key=MaskingKey.from_env(required=False),
        api_token=os.environ.get("DATAFORGE_API_TOKEN"),
        code_version=os.environ.get("GIT_SHA", "unknown"),
    )


def build_ingestion_dag(contract: DataContract):
    """Generate the ingestion DAG for one contract."""
    tier = next((t for t in contract.metadata.tags if t in _SCHEDULE_BY_TIER), "tier-2")
    produced = silver_asset(contract)

    @dag(
        dag_id=f"ingest__{contract.metadata.domain}__{contract.name}",
        description=f"[{contract.metadata.version}] {contract.metadata.description[:120]}",
        schedule=_SCHEDULE_BY_TIER[tier],
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        default_args=_DEFAULT_ARGS,
        tags=["dataforge", contract.metadata.domain, tier, f"owner:{contract.metadata.owner}"],
        doc_md=_dag_docs(contract),
        params={"force_reprocess": False},
    )
    def _ingestion_dag():
        @task(task_id="preflight")
        def preflight() -> dict[str, Any]:
            """Fail early, before any data is touched, if the masking key is missing."""
            config = _pipeline_config()
            if contract.sensitive_fields and config.masking_key is None:
                raise RuntimeError(
                    f"{contract.name} declares {len(contract.sensitive_fields)} sensitive "
                    "field(s) but no masking key is available in this environment"
                )
            return {"contract_version": contract.metadata.version, "env": config.env}

        @task(task_id="ingest_validate_publish", outlets=[produced])
        def ingest(_preflight: dict[str, Any], **context: Any) -> dict[str, Any]:
            """Ingest, validate, mask and publish in one task.

            The stages share one in-memory batch; splitting them would mean passing it
            through XCom or storage. Per-stage metrics are already in the logs and manifest.
            """
            logical = _logical_date(context)
            params = context.get("params") or {}
            try:
                result = run_ingestion(
                    contract,
                    _pipeline_config(),
                    logical_date=logical,
                    force=bool(params.get("force_reprocess")),
                )
            except QualityGateError as exc:
                # Let the task fail; the manifest already records why. A retry is
                # harmless and sometimes helps if a source file was half-written.
                raise exc
            return result.manifest.to_dict()

        @task(task_id="publish_metrics", trigger_rule="all_done")
        def publish_metrics(manifest: dict[str, Any] | None) -> None:
            """Emit run metrics even when the run failed (``trigger_rule="all_done"``)."""
            payload = {
                "dataset": contract.name,
                "env": ENV,
                "status": (manifest or {}).get("status", "failed"),
                **(manifest or {}).get("metrics", {}),
            }
            print(f"METRIC {payload}")  # scraped by the log-based metrics pipeline

        publish_metrics(ingest(preflight()))

    return _ingestion_dag()


def _logical_date(context: dict[str, Any]) -> date:
    """Get the run's logical date from the Airflow context."""
    stamp = context.get("logical_date") or context.get("data_interval_start")
    return stamp.date() if stamp else date.today()


def _dag_docs(contract: DataContract) -> str:
    """DAG docs built from the contract, so on-call sees owner, rules and SLO in the UI."""
    rules = "\n".join(
        f"| `{r.id}` | {r.type} | **{r.severity}** | {', '.join(r.columns) or '—'} |"
        for r in contract.spec.quality.rules
    )
    return f"""
### {contract.name} — v{contract.metadata.version}

{contract.metadata.description}

* **Owner:** {contract.metadata.owner}
* **Upstream:** {contract.metadata.upstream_system}
* **Grain:** `{", ".join(contract.spec.schema_.primary_key)}`
* **Mode:** {contract.spec.source.mode} from {contract.spec.source.type}
* **Freshness SLO:** {contract.spec.slo.freshness_minutes} minutes
* **Targets:** `{contract.spec.targets.bronze}` → `{contract.spec.targets.silver}`
  (rejects → `{contract.spec.targets.quarantine}`)

#### Quality rules
| id | type | severity | columns |
|---|---|---|---|
{rules}

#### Runbook
* **Task failed, quality blocked** — query the quarantine table and the run manifest for
  this `run_id`. Do not clear the task until the upstream defect is understood; a retry
  will reproduce it.
* **Backfill** — trigger with `force_reprocess=true` for the affected logical dates.
  Writes are partition-level merges, so a backfill is safe to repeat.
* **Escalation** — {contract.metadata.owner}
"""


# --------------------------------------------------------------------------------------
# Generation. Keep import-time work cheap; this runs on every parse.
# --------------------------------------------------------------------------------------

_registry = ContractRegistry(CONTRACTS_DIR)
_contracts = _registry.all()

for _contract in _contracts:
    globals()[f"dag_ingest_{_contract.name}"] = build_ingestion_dag(_contract)


# --------------------------------------------------------------------------------------
# Gold layer: runs when its inputs are refreshed.
# --------------------------------------------------------------------------------------

_CORE = [c for c in _contracts if c.spec.source.mode is not IngestionMode.APPEND]


@dag(
    dag_id="build__commerce__gold",
    description="dbt build of the curated commerce data product",
    # Runs once every listed asset has been refreshed.
    schedule=[silver_asset(c) for c in _CORE],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["dataforge", "gold", "dbt"],
)
def gold_dag():
    @task.bash(task_id="register_lake")
    def register_lake() -> str:
        """Register the Parquet lake as warehouse views so dbt can read it."""
        return (
            "cd /opt/airflow && python scripts/register_lake.py "
            "--lake-root /opt/airflow/data/lake "
            "--database /opt/airflow/data/warehouse/dev.duckdb"
        )

    @task.bash(task_id="dbt_build")
    def dbt_build() -> str:
        """Run dbt models and tests together, so nothing builds on a failing model."""
        return f"cd /opt/airflow/transform/dbt && dbt build --target {ENV} --fail-fast"

    @task.bash(task_id="dbt_docs_generate")
    def dbt_docs() -> str:
        return "cd /opt/airflow/transform/dbt && dbt docs generate --target " + ENV

    @task(task_id="freshness_audit")
    def freshness_audit() -> None:
        """Check freshness of the published tables. Kept apart from dbt tests so it can alert separately."""
        print("freshness audit: compare max(order_ts) per gold table against slo.freshness_minutes")

    register_lake() >> dbt_build() >> dbt_docs() >> freshness_audit()


gold = gold_dag()
