"""The ingestion pipeline. One function, driven entirely by the contract.

    read → bronze → normalize → dedupe/CDC → validate → mask → silver → watermark

The order matters:

* Bronze first, so raw data can always be replayed.
* Dedupe before validate, so redeliveries don't count as uniqueness failures.
* Validate before mask, since rules check real values, not tokens.
* Mask before silver, the first widely readable layer.
* Watermark last, and only on success, so no data is skipped.

A blocking rule failure saves the manifest as ``BLOCKED_BY_QUALITY`` and raises
``QualityGateError``. Nothing partial is published.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dataforge.contracts.models import DataContract, IngestionMode
from dataforge.engines.parquet_lake import ParquetLakeEngine, arrow_schema
from dataforge.ingest.sources import Batch, build_source
from dataforge.ingest.state import (
    JsonStateStore,
    RunManifest,
    RunStatus,
    StateStore,
    fingerprint_inputs,
    partition_value,
)
from dataforge.observability.lineage import (
    FileLineageEmitter,
    LineageEmitter,
    NullLineageEmitter,
    build_run_event,
    new_run_id,
)
from dataforge.observability.logging import RunLogger
from dataforge.privacy.masking import Masker, MaskingKey
from dataforge.privacy.secrets import resolve_optional
from dataforge.quality.engine import ValidationEngine, ValidationReport
from dataforge.transform.dedupe import collapse_cdc, drop_exact_duplicates, latest_per_key
from dataforge.transform.normalize import normalize_batch

Record = dict[str, Any]


class QualityGateError(RuntimeError):
    """A blocking quality rule failed. Nothing was published."""

    def __init__(self, dataset: str, report: ValidationReport) -> None:
        self.dataset = dataset
        self.report = report
        super().__init__(f"{dataset}: quality gate failed — {report.failure_summary()}")


@dataclass(slots=True)
class PipelineConfig:
    """All environment-specific settings. Nothing else reads env vars or globals."""

    env: str
    # URIs, not Paths: Path turns "s3://bucket" into "s3:/bucket".
    lake_root: str | Path
    landing_root: str | Path
    state_path: Path
    lineage_path: Path | None = None
    masking_key: MaskingKey | None = None
    api_token: str | None = None
    code_version: str = "dev"
    dry_run: bool = False


@dataclass(slots=True)
class PipelineResult:
    manifest: RunManifest
    report: ValidationReport


def run_ingestion(
    contract: DataContract,
    config: PipelineConfig,
    *,
    logical_date: date,
    state: StateStore | None = None,
    lineage: LineageEmitter | None = None,
    force: bool = False,
) -> PipelineResult:
    run_id = new_run_id()
    log = RunLogger().bind(
        run_id=run_id,
        dataset=contract.name,
        logical_date=logical_date.isoformat(),
        env=config.env,
        contract_version=contract.metadata.version,
    )
    state = state or JsonStateStore(config.state_path)
    lineage = lineage or (
        FileLineageEmitter(config.lineage_path) if config.lineage_path else NullLineageEmitter()
    )
    engine = ParquetLakeEngine(
        config.lake_root, target_file_size_mb=contract.spec.partitioning.target_file_size_mb
    )
    spec = contract.spec
    part_value = partition_value(logical_date, spec.partitioning.granularity)

    # ---------------------------------------------------------------- 1. read ---------
    watermark = (
        state.get_watermark(contract.name)
        if spec.source.mode is IngestionMode.INCREMENTAL
        else None
    )
    # Resolve the contract's secret here so every caller gets it the same way.
    token = config.api_token
    if token is None and spec.source.secret_ref:
        token = resolve_optional(spec.source.secret_ref)
        log.info("resolved source credential", secret_ref=spec.source.secret_ref)
    source = build_source(contract, root=config.landing_root, token=token)
    batch: Batch = source.read(logical_date=logical_date, watermark=watermark)
    log.info("source read", rows=len(batch.records), watermark_in=watermark, **batch.provenance)

    fingerprint = fingerprint_inputs(batch.source_uris, len(batch.records), batch.high_water_mark)
    manifest = RunManifest(
        run_id=run_id,
        dataset=contract.name,
        logical_date=logical_date.isoformat(),
        input_fingerprint=fingerprint,
        contract_version=contract.metadata.version,
        code_version=config.code_version,
        rows_in=len(batch.records),
        target_partition=part_value,
        source_uris=batch.source_uris[:20],
    )

    # ------------------------------------------------- 2. idempotency short-circuit ---
    previous = state.find_run(contract.name, logical_date.isoformat(), fingerprint)
    if previous and not force:
        manifest.status = RunStatus.SKIPPED_DUPLICATE
        manifest.finished_at = _now()
        manifest.rows_published = previous.rows_published
        state.save_run(manifest)
        log.info(
            "identical inputs already processed successfully; skipping",
            previous_run_id=previous.run_id,
            rows_published=previous.rows_published,
        )
        return PipelineResult(manifest=manifest, report=ValidationReport(dataset=contract.name))

    lineage.emit(
        build_run_event(
            event_type="START",
            run_id=run_id,
            job_namespace=f"dataforge.{config.env}",
            job_name=f"ingest.{contract.name}",
            inputs=[("source", uri) for uri in batch.source_uris[:5]]
            or [("source", spec.source.uri)],
            outputs=[("lake", spec.targets.silver)],
            contract=contract,
        )
    )

    try:
        # ------------------------------------------------------------ 3. bronze -------
        # Raw records as received, unknown columns included.
        if not config.dry_run:
            engine.write_append(
                spec.targets.bronze,
                [{**r, "_run_id": run_id, "_ingested_at": _now()} for r in batch.records],
                run_id=run_id,
            )

        # ----------------------------------------------------------- 4. normalize -----
        normalized, coercion_failures = normalize_batch(
            contract,
            batch.records,
            run_id=run_id,
            source_uri=batch.source_uris[0] if batch.source_uris else spec.source.uri,
        )
        if coercion_failures:
            log.warning("type coercion failures routed to quarantine", count=len(coercion_failures))

        # ---------------------------------------------------- 5. dedupe / CDC ---------
        deduped = drop_exact_duplicates(normalized)
        if spec.source.mode is IngestionMode.CDC:
            deduped = collapse_cdc(
                deduped,
                spec.schema_.primary_key,
                op_field=spec.source.cdc_op_field or "op",
                sequence_field=spec.source.cdc_sequence_field or "_sequence",
            )
        elif spec.source.mode in (IngestionMode.INCREMENTAL, IngestionMode.FULL):
            deduped = latest_per_key(
                deduped, spec.schema_.primary_key, spec.schema_.dedupe_order_by
            )
        log.info("deduplicated", rows_before=len(normalized), rows_after=len(deduped))

        # ----------------------------------------------------------- 6. validate ------
        overrides = _runtime_rule_overrides(contract, state)
        report = ValidationEngine(contract, rule_overrides=overrides).run(deduped)
        manifest.rows_quarantined = report.quarantined_count + len(coercion_failures)
        manifest.metrics = report.metrics()
        log.info("validation complete", **report.metrics())

        if not config.dry_run:
            quarantine_rows = [q.to_row(run_id, contract.name) for q in report.quarantined] + [
                {
                    "_run_id": run_id,
                    "_dataset": contract.name,
                    "_rule_ids": ["type_coercion"],
                    "_columns": [],
                    "_reasons": [reason],
                    "payload": record,
                }
                for record, reason in coercion_failures
            ]
            if quarantine_rows:
                engine.write_append(spec.targets.quarantine, quarantine_rows, run_id=run_id)

        if not report.passed:
            manifest.status = RunStatus.BLOCKED_BY_QUALITY
            manifest.finished_at = _now()
            manifest.error = report.failure_summary()
            state.save_run(manifest)
            lineage.emit(
                build_run_event(
                    event_type="FAIL",
                    run_id=run_id,
                    job_namespace=f"dataforge.{config.env}",
                    job_name=f"ingest.{contract.name}",
                    inputs=[],
                    outputs=[("lake", spec.targets.silver)],
                    contract=contract,
                    quality_metrics=report.metrics(),
                    error=report.failure_summary(),
                )
            )
            log.error("quality gate blocked publication", reason=report.failure_summary())
            raise QualityGateError(contract.name, report)

        # --------------------------------------------------------------- 7. mask ------
        masker = Masker(contract, key=config.masking_key)
        published = masker.mask_batch(report.valid)

        # ------------------------------------------------------------- 8. silver ------
        if not config.dry_run:
            touched = _publish_silver(
                engine,
                contract,
                published,
                logical_date=logical_date,
                default_partition=part_value,
                run_id=run_id,
                log=log,
            )
            manifest.metrics["partitions_written"] = touched

        # ---------------------------------------------------------- 9. watermark ------
        # Only after silver is written.
        if (
            batch.high_water_mark
            and spec.source.mode is IngestionMode.INCREMENTAL
            and not config.dry_run
        ):
            state.set_watermark(contract.name, batch.high_water_mark)

        manifest.status = RunStatus.SUCCEEDED
        manifest.rows_published = len(published)
        manifest.finished_at = _now()
        state.save_run(manifest)

        lineage.emit(
            build_run_event(
                event_type="COMPLETE",
                run_id=run_id,
                job_namespace=f"dataforge.{config.env}",
                job_name=f"ingest.{contract.name}",
                inputs=[("source", uri) for uri in batch.source_uris[:5]],
                outputs=[("lake", spec.targets.silver)],
                contract=contract,
                quality_metrics=report.metrics(),
            )
        )
        log.info(
            "published",
            rows_published=manifest.rows_published,
            partition=part_value,
            watermark_out=batch.high_water_mark,
        )
        return PipelineResult(manifest=manifest, report=report)

    except QualityGateError:
        raise
    except Exception as exc:
        manifest.status = RunStatus.FAILED
        manifest.finished_at = _now()
        manifest.error = f"{type(exc).__name__}: {exc}"
        state.save_run(manifest)
        log.error("run failed", error=manifest.error)
        raise


def _publish_silver(
    engine: ParquetLakeEngine,
    contract: DataContract,
    records: list[Record],
    *,
    logical_date: date,
    default_partition: str,
    run_id: str,
    log: RunLogger,
) -> list[str]:
    """Merge records into silver, one partition at a time.

    Rows are grouped by their own partition value (so late data lands in the right
    place), merged with what's already there, then the partition is overwritten. This
    keeps re-runs idempotent without wiping earlier rows. Returns the partitions written.
    """
    spec = contract.spec
    part_col = spec.partitioning.column
    masked_names = {
        name: Masker.output_name(name, strategy)
        for name, strategy in contract.masked_fields().items()
    }
    schema = arrow_schema(contract, masked_names=masked_names)

    if part_col:
        groups: dict[str, list[Record]] = {}
        for record in records:
            value = record.get(part_col)
            groups.setdefault(str(value) if value is not None else default_partition, []).append(
                record
            )
    else:
        # Unpartitioned tables still get one directory per load date, for retention/replay.
        groups = {default_partition: records}
        part_col = "_partition_date"

    window = spec.source.late_arrival_window_days
    written: list[str] = []

    for value, rows in sorted(groups.items()):
        age_days = _partition_age_days(value, logical_date)
        if age_days is not None and age_days > window:
            # Expected occasionally; logged so downstream knows an old partition changed.
            log.warning(
                "late arrival outside the declared window; rewriting an older partition",
                partition=value,
                age_days=age_days,
                window_days=window,
                rows=len(rows),
            )

        existing = engine.read_partition(spec.targets.silver, part_col, value)
        combined = existing + rows
        if spec.source.mode is IngestionMode.APPEND:
            merged = drop_exact_duplicates(combined)
        else:
            merged = latest_per_key(
                combined, spec.schema_.primary_key, spec.schema_.dedupe_order_by
            )

        engine.overwrite_partition(
            spec.targets.silver,
            merged,
            partition_column=part_col,
            partition_value=value,
            run_id=run_id,
            schema=schema,
        )
        written.append(value)
        log.info(
            "partition merged",
            partition=value,
            rows_existing=len(existing),
            rows_incoming=len(rows),
            rows_after_merge=len(merged),
        )

    return written


def _partition_age_days(partition_value_: str, logical_date: date) -> int | None:
    try:
        return (logical_date - date.fromisoformat(partition_value_[:10])).days
    except ValueError:
        return None


def _runtime_rule_overrides(contract: DataContract, state: StateStore) -> dict[str, dict[str, Any]]:
    """Fill in rule params only known at runtime, e.g. the row-count baseline."""
    overrides: dict[str, dict[str, Any]] = {}
    history = state.recent_row_counts(contract.name)
    if len(history) >= 3:
        baseline = sum(history) / len(history)
        for rule in contract.spec.quality.rules:
            if rule.type == "row_count_anomaly":
                overrides[rule.id] = {
                    "baseline": baseline,
                    "drop_threshold": contract.spec.slo.volume_drop_threshold,
                }
    return overrides


def _now() -> str:
    return datetime.now(UTC).isoformat()
