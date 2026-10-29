"""OpenLineage events, emitted by the pipeline itself.

OpenLineage is an open spec most catalogs understand. Emitting from the pipeline means
each event knows the run, partition, quality metrics and contract version.
``FileLineageEmitter`` writes JSON lines for local runs and tests. A lineage outage
should never fail a data run.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from dataforge.contracts.models import DataContract

_PRODUCER = "https://github.com/acme/dataforge/tree/main"
_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"


class LineageEmitter(Protocol):
    def emit(self, event: dict[str, Any]) -> None: ...


class FileLineageEmitter:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")


class NullLineageEmitter:
    def emit(self, event: dict[str, Any]) -> None:  # noqa: ARG002
        return None


def _dataset(namespace: str, name: str, facets: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"namespace": namespace, "name": name, "facets": facets or {}}


def schema_facet(contract: DataContract) -> dict[str, Any]:
    return {
        "_producer": _PRODUCER,
        "_schemaURL": f"{_SCHEMA_URL}#/definitions/SchemaDatasetFacet",
        "fields": [
            {
                "name": f.name,
                "type": str(f.type),
                "description": f.description,
                # Lets a catalog trace where PII flows downstream.
                "tags": [str(f.classification)],
            }
            for f in contract.spec.schema_.fields
        ],
    }


def build_run_event(
    *,
    event_type: str,
    run_id: str,
    job_namespace: str,
    job_name: str,
    inputs: list[tuple[str, str]],
    outputs: list[tuple[str, str]],
    contract: DataContract | None = None,
    quality_metrics: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    run_facets: dict[str, Any] = {}
    if contract:
        run_facets["dataContract"] = {
            "_producer": _PRODUCER,
            "name": contract.name,
            "version": contract.metadata.version,
            "owner": contract.metadata.owner,
            "domain": contract.metadata.domain,
        }
    if error:
        run_facets["errorMessage"] = {"_producer": _PRODUCER, "message": error}

    output_facets: dict[str, Any] = {}
    if contract:
        output_facets["schema"] = schema_facet(contract)
    if quality_metrics:
        output_facets["dataQualityMetrics"] = {
            "_producer": _PRODUCER,
            "rowCount": quality_metrics.get("rows_valid"),
            "columnMetrics": {},
            "quarantineRate": quality_metrics.get("quarantine_rate"),
            "completeness": quality_metrics.get("completeness"),
        }

    return {
        "eventTime": datetime.now(UTC).isoformat(),
        "eventType": event_type,
        "producer": _PRODUCER,
        "schemaURL": _SCHEMA_URL,
        "run": {"runId": run_id, "facets": run_facets},
        "job": {"namespace": job_namespace, "name": job_name},
        "inputs": [_dataset(ns, name) for ns, name in inputs],
        "outputs": [_dataset(ns, name, output_facets) for ns, name in outputs],
    }


def new_run_id() -> str:
    return str(uuid.uuid4())
