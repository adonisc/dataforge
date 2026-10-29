"""Applies a contract's quality rules to a batch.

``ValidationEngine.run`` is pure: no I/O, no side effects. The caller decides what to
do with the report. Keep it that way so it stays easy to test and reuse.

Order: dataset rules, then every row rule on every row (no short-circuit, so all
reasons are recorded), then the quarantine-rate check, which can block the batch.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from dataforge.contracts.models import DataContract, Severity
from dataforge.quality.rules import (
    Record,
    Violation,
    build_dataset_rule,
    build_row_rule,
    is_dataset_rule,
    known_rule_types,
)


@dataclass(frozen=True, slots=True)
class QuarantinedRecord:
    """A rejected row with its reasons. The original payload is kept so it can be replayed."""

    record: Record
    violations: list[Violation]

    def to_row(self, run_id: str, dataset: str) -> dict[str, Any]:
        return {
            "_run_id": run_id,
            "_dataset": dataset,
            "_rule_ids": [v.rule_id for v in self.violations],
            "_columns": [v.column for v in self.violations if v.column],
            "_reasons": [v.message for v in self.violations],
            "payload": self.record,
        }


@dataclass(slots=True)
class ValidationReport:
    dataset: str
    total_rows: int = 0
    valid: list[Record] = field(default_factory=list)
    quarantined: list[QuarantinedRecord] = field(default_factory=list)
    blocking_violations: list[Violation] = field(default_factory=list)
    warnings: list[Violation] = field(default_factory=list)
    rule_hits: dict[str, int] = field(default_factory=dict)

    @property
    def valid_count(self) -> int:
        return len(self.valid)

    @property
    def quarantined_count(self) -> int:
        return len(self.quarantined)

    @property
    def quarantine_rate(self) -> float:
        return self.quarantined_count / self.total_rows if self.total_rows else 0.0

    @property
    def passed(self) -> bool:
        """Whether the batch may be published."""
        return not self.blocking_violations

    def metrics(self) -> dict[str, Any]:
        """Flat, low-cardinality metrics for monitoring, including per-rule hit counts."""
        return {
            "dataset": self.dataset,
            "rows_in": self.total_rows,
            "rows_valid": self.valid_count,
            "rows_quarantined": self.quarantined_count,
            "quarantine_rate": round(self.quarantine_rate, 6),
            "completeness": round(self.valid_count / self.total_rows, 6)
            if self.total_rows
            else 1.0,
            "blocking_violations": len(self.blocking_violations),
            "warnings": len(self.warnings),
            "rule_hits": dict(sorted(self.rule_hits.items())),
        }

    def failure_summary(self) -> str:
        return "; ".join(f"[{v.rule_id}] {v.message}" for v in self.blocking_violations)


class ValidationEngine:
    """Compiles a contract's rules once, then applies them to batches."""

    def __init__(
        self, contract: DataContract, *, rule_overrides: dict[str, dict[str, Any]] | None = None
    ):
        """``rule_overrides`` adds runtime params by rule id (e.g. known keys, volume baseline)."""
        self.contract = contract
        self._overrides = rule_overrides or {}
        self._validate_rule_types()

    def _validate_rule_types(self) -> None:
        """Fail early on unknown rule types so CI catches them."""
        unknown = {
            r.type for r in self.contract.spec.quality.rules if r.type not in known_rule_types()
        }
        if unknown:
            raise KeyError(
                f"contract '{self.contract.name}' references unknown rule types: {sorted(unknown)}. "
                f"Known types: {sorted(known_rule_types())}"
            )

    def run(self, records: Sequence[Record]) -> ValidationReport:
        report = ValidationReport(dataset=self.contract.name, total_rows=len(records))
        quality = self.contract.spec.quality
        flagged: dict[int, list[Violation]] = {}

        # --- 1. dataset-level rules ---------------------------------------------------
        for spec in quality.rules:
            if not is_dataset_rule(spec.type):
                continue
            params = {**spec.params, **self._overrides.get(spec.id, {})}
            dataset_rule_fn = build_dataset_rule(spec.id, spec.type, spec.columns, params)
            offending_indices, violations = dataset_rule_fn(records)

            for dataset_violation in violations:
                self._record_hit(report, dataset_violation)
                self._route(report, spec.severity, dataset_violation)

            if offending_indices and spec.severity is Severity.QUARANTINE:
                for idx in offending_indices:
                    detail = Violation(
                        spec.id, spec.type, ",".join(spec.columns), "superseded duplicate row"
                    )
                    flagged.setdefault(idx, []).append(detail)
                    self._record_hit(report, detail)

        # --- 2. row-level rules -------------------------------------------------------
        row_specs = [
            (
                spec,
                build_row_rule(
                    spec.id,
                    spec.type,
                    spec.columns,
                    {**spec.params, **self._overrides.get(spec.id, {})},
                ),
            )
            for spec in quality.rules
            if not is_dataset_rule(spec.type)
        ]

        for idx, record in enumerate(records):
            for spec, row_rule_fn in row_specs:
                row_violation = row_rule_fn(record)
                if row_violation is None:
                    continue
                self._record_hit(report, row_violation)
                if spec.severity is Severity.BLOCKING:
                    report.blocking_violations.append(row_violation)
                elif spec.severity is Severity.QUARANTINE:
                    flagged.setdefault(idx, []).append(row_violation)
                else:
                    report.warnings.append(row_violation)

        # --- 3. split the batch -------------------------------------------------------
        for idx, record in enumerate(records):
            if idx in flagged:
                report.quarantined.append(QuarantinedRecord(record=record, violations=flagged[idx]))
            else:
                report.valid.append(record)

        # --- 4. circuit breaker -------------------------------------------------------
        if report.total_rows and report.quarantine_rate > quality.max_quarantine_rate:
            report.blocking_violations.append(
                Violation(
                    "max_quarantine_rate",
                    "circuit_breaker",
                    None,
                    f"quarantine rate {report.quarantine_rate:.2%} exceeds the contract "
                    f"threshold of {quality.max_quarantine_rate:.2%}; the batch is treated as "
                    "structurally invalid rather than merely dirty",
                )
            )

        return report

    @staticmethod
    def _record_hit(report: ValidationReport, violation: Violation) -> None:
        report.rule_hits[violation.rule_id] = report.rule_hits.get(violation.rule_id, 0) + 1

    @staticmethod
    def _route(report: ValidationReport, severity: Severity, violation: Violation) -> None:
        if severity is Severity.BLOCKING:
            report.blocking_violations.append(violation)
        elif severity is Severity.WARN:
            report.warnings.append(violation)
        # QUARANTINE at dataset level is handled by the caller via offending_indices.
