"""Compare two contract versions and flag breaking schema changes. Runs in CI.

    BACKWARD : old readers still work on new data (adding an optional column is fine).
    FORWARD  : new readers still work on old data.

To ship a breaking change: add the new column, mark the old one ``deprecated``, then
remove it with a major version bump. Use ``renamed_from`` for renames.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from dataforge.contracts.models import Compatibility, DataContract, FieldSpec, FieldType

#: Type changes that only widen the value range, so existing readers are safe.
_SAFE_WIDENING: dict[FieldType, set[FieldType]] = {
    FieldType.INTEGER: {FieldType.LONG, FieldType.DECIMAL, FieldType.FLOAT, FieldType.STRING},
    FieldType.LONG: {FieldType.DECIMAL, FieldType.STRING},
    FieldType.FLOAT: {FieldType.STRING},
    FieldType.DECIMAL: {FieldType.STRING},
    FieldType.DATE: {FieldType.TIMESTAMP, FieldType.STRING},
    FieldType.BOOLEAN: {FieldType.STRING},
}


class ChangeKind(StrEnum):
    FIELD_ADDED_OPTIONAL = "field_added_optional"
    FIELD_ADDED_REQUIRED = "field_added_required"
    FIELD_REMOVED = "field_removed"
    FIELD_RENAMED = "field_renamed"
    TYPE_WIDENED = "type_widened"
    TYPE_NARROWED = "type_narrowed"
    NULLABILITY_TIGHTENED = "nullability_tightened"
    NULLABILITY_RELAXED = "nullability_relaxed"
    PRIMARY_KEY_CHANGED = "primary_key_changed"
    CLASSIFICATION_RELAXED = "classification_relaxed"
    DEPRECATION_MARKED = "deprecation_marked"


@dataclass(frozen=True, slots=True)
class SchemaChange:
    kind: ChangeKind
    field: str
    detail: str
    breaking: bool


@dataclass(frozen=True, slots=True)
class CompatibilityReport:
    contract: str
    old_version: str
    new_version: str
    policy: Compatibility
    changes: list[SchemaChange]

    @property
    def breaking_changes(self) -> list[SchemaChange]:
        return [c for c in self.changes if c.breaking]

    @property
    def is_compatible(self) -> bool:
        return not self.breaking_changes

    @property
    def requires_major_bump(self) -> bool:
        return bool(self.breaking_changes)

    def summary(self) -> str:
        if not self.changes:
            return f"{self.contract}: no schema changes"
        lines = [f"{self.contract}: {self.old_version} -> {self.new_version} ({self.policy})"]
        for change in self.changes:
            flag = "BREAKING" if change.breaking else "ok      "
            lines.append(f"  [{flag}] {change.kind}: {change.field} — {change.detail}")
        return "\n".join(lines)


def _is_breaking(kind: ChangeKind, policy: Compatibility) -> bool:
    """Whether a change breaks the policy. Kept in one place so it's easy to audit."""
    if policy is Compatibility.NONE:
        return False

    always_breaking = {ChangeKind.PRIMARY_KEY_CHANGED, ChangeKind.TYPE_NARROWED}
    backward_breaking = {
        ChangeKind.FIELD_REMOVED,  # old consumers select a column that vanished
        ChangeKind.FIELD_RENAMED,  # same, under a different name
        ChangeKind.NULLABILITY_RELAXED,  # old consumers assumed NOT NULL
    }
    forward_breaking = {
        ChangeKind.FIELD_ADDED_REQUIRED,  # old data has no value for it
        ChangeKind.NULLABILITY_TIGHTENED,  # old data may violate it
    }

    if kind in always_breaking:
        return True
    if policy in (Compatibility.BACKWARD, Compatibility.FULL) and kind in backward_breaking:
        return True
    return policy in (Compatibility.FORWARD, Compatibility.FULL) and kind in forward_breaking


def _compare_field(old: FieldSpec, new: FieldSpec, policy: Compatibility) -> list[SchemaChange]:
    changes: list[SchemaChange] = []

    if old.type is not new.type:
        widened = new.type in _SAFE_WIDENING.get(old.type, set())
        kind = ChangeKind.TYPE_WIDENED if widened else ChangeKind.TYPE_NARROWED
        changes.append(
            SchemaChange(kind, new.name, f"{old.type} -> {new.type}", _is_breaking(kind, policy))
        )

    if old.required != new.required:
        kind = ChangeKind.NULLABILITY_TIGHTENED if new.required else ChangeKind.NULLABILITY_RELAXED
        detail = f"required {old.required} -> {new.required}"
        changes.append(SchemaChange(kind, new.name, detail, _is_breaking(kind, policy)))

    # Downgrading a classification drops masking, so it's always breaking.
    sensitive = {"pii", "pci", "confidential"}
    if old.classification.value in sensitive and new.classification.value not in sensitive:
        changes.append(
            SchemaChange(
                ChangeKind.CLASSIFICATION_RELAXED,
                new.name,
                f"{old.classification} -> {new.classification} (privacy regression)",
                True,
            )
        )

    if new.deprecated and not old.deprecated:
        changes.append(
            SchemaChange(ChangeKind.DEPRECATION_MARKED, new.name, "marked deprecated", False)
        )

    return changes


def check_compatibility(old: DataContract, new: DataContract) -> CompatibilityReport:
    """Compare two versions of the same contract."""
    if old.name != new.name:
        raise ValueError(f"cannot compare different contracts: {old.name} vs {new.name}")

    policy = new.spec.evolution.compatibility
    old_fields = {f.name: f for f in old.spec.schema_.fields}
    new_fields = {f.name: f for f in new.spec.schema_.fields}
    renames = {f.renamed_from: f for f in new.spec.schema_.fields if f.renamed_from}

    changes: list[SchemaChange] = []

    for name, new_field in new_fields.items():
        if name in old_fields:
            changes.extend(_compare_field(old_fields[name], new_field, policy))
        elif new_field.renamed_from and new_field.renamed_from in old_fields:
            kind = ChangeKind.FIELD_RENAMED
            changes.append(
                SchemaChange(
                    kind,
                    name,
                    f"renamed from '{new_field.renamed_from}'",
                    _is_breaking(kind, policy),
                )
            )
            changes.extend(_compare_field(old_fields[new_field.renamed_from], new_field, policy))
        else:
            kind = (
                ChangeKind.FIELD_ADDED_REQUIRED
                if new_field.required
                else ChangeKind.FIELD_ADDED_OPTIONAL
            )
            changes.append(
                SchemaChange(kind, name, f"new {new_field.type}", _is_breaking(kind, policy))
            )

    for name in old_fields:
        if name in new_fields or name in renames:
            continue
        kind = ChangeKind.FIELD_REMOVED
        changes.append(
            SchemaChange(kind, name, "dropped from contract", _is_breaking(kind, policy))
        )

    if old.spec.schema_.primary_key != new.spec.schema_.primary_key:
        kind = ChangeKind.PRIMARY_KEY_CHANGED
        changes.append(
            SchemaChange(
                kind,
                ",".join(new.spec.schema_.primary_key),
                f"{old.spec.schema_.primary_key} -> {new.spec.schema_.primary_key}; "
                "grain change invalidates every downstream aggregate",
                True,
            )
        )

    return CompatibilityReport(
        contract=new.name,
        old_version=old.metadata.version,
        new_version=new.metadata.version,
        policy=policy,
        changes=changes,
    )


def enforce(report: CompatibilityReport) -> list[str]:
    """Return CI violations (empty means OK). Breaking changes are allowed with a major bump."""
    violations: list[str] = []
    if report.requires_major_bump:
        old_major = int(report.old_version.split(".")[0])
        new_major = int(report.new_version.split(".")[0])
        if new_major <= old_major:
            names = ", ".join(f"{c.field} ({c.kind})" for c in report.breaking_changes)
            violations.append(
                f"{report.contract}: breaking change(s) [{names}] require a major version "
                f"bump (currently {report.old_version} -> {report.new_version})"
            )
    return violations
