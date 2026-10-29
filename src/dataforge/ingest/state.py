"""Run state: watermarks and run manifests.

State lives outside Airflow so pipelines run anywhere and keep an audit trail
(see docs/adr/ADR-0008). The JSON store is for local use and tests.

A run is keyed by ``(dataset, logical_date, input_fingerprint)``. Same inputs → the
run is skipped. Changed inputs (e.g. a corrected file) → new fingerprint, so it reprocesses.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED_BY_QUALITY = "blocked_by_quality"
    SKIPPED_DUPLICATE = "skipped_duplicate"


@dataclass(slots=True)
class RunManifest:
    run_id: str
    dataset: str
    logical_date: str
    input_fingerprint: str
    status: RunStatus = RunStatus.RUNNING
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at: str | None = None
    contract_version: str = ""
    code_version: str = ""
    rows_in: int = 0
    rows_published: int = 0
    rows_quarantined: int = 0
    target_partition: str | None = None
    source_uris: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = str(self.status)
        return payload


def fingerprint_inputs(source_uris: list[str], row_count: int, high_water_mark: str | None) -> str:
    payload = json.dumps(
        {"uris": sorted(source_uris), "rows": row_count, "hwm": high_water_mark}, sort_keys=True
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class StateStore(Protocol):
    def get_watermark(self, dataset: str) -> str | None: ...
    def set_watermark(self, dataset: str, value: str) -> None: ...
    def find_run(self, dataset: str, logical_date: str, fingerprint: str) -> RunManifest | None: ...
    def save_run(self, manifest: RunManifest) -> None: ...
    def recent_row_counts(self, dataset: str, limit: int = 14) -> list[int]: ...


class JsonStateStore:
    """File-backed store for local runs and tests. Not safe across processes."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text(json.dumps({"watermarks": {}, "runs": []}), encoding="utf-8")

    def _load(self) -> dict[str, Any]:
        loaded: dict[str, Any] = json.loads(self.path.read_text(encoding="utf-8"))
        return loaded

    def _save(self, state: dict[str, Any]) -> None:
        # Write then rename, so a crash can't leave a half-written file.
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.path)

    def get_watermark(self, dataset: str) -> str | None:
        value = self._load()["watermarks"].get(dataset)
        return str(value) if value is not None else None

    def set_watermark(self, dataset: str, value: str) -> None:
        state = self._load()
        current = state["watermarks"].get(dataset)
        # Only move forward; a backfill must not rewind the watermark.
        if current is None or value > current:
            state["watermarks"][dataset] = value
            self._save(state)

    def find_run(self, dataset: str, logical_date: str, fingerprint: str) -> RunManifest | None:
        for row in reversed(self._load()["runs"]):
            if (
                row["dataset"] == dataset
                and row["logical_date"] == logical_date
                and row["input_fingerprint"] == fingerprint
                and row["status"] == RunStatus.SUCCEEDED
            ):
                row = dict(row)
                row["status"] = RunStatus(row["status"])
                return RunManifest(**row)
        return None

    def save_run(self, manifest: RunManifest) -> None:
        state = self._load()
        state["runs"] = [r for r in state["runs"] if r["run_id"] != manifest.run_id]
        state["runs"].append(manifest.to_dict())
        self._save(state)

    def recent_row_counts(self, dataset: str, limit: int = 14) -> list[int]:
        runs = [
            r
            for r in self._load()["runs"]
            if r["dataset"] == dataset and r["status"] == RunStatus.SUCCEEDED
        ]
        return [int(r["rows_in"]) for r in runs[-limit:]]


def partition_value(logical_date: date, granularity: str = "day") -> str:
    match granularity:
        case "hour":
            return logical_date.strftime("%Y-%m-%dT%H")
        case "month":
            return logical_date.strftime("%Y-%m")
        case _:
            return logical_date.isoformat()
