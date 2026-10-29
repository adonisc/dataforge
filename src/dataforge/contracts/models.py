"""Typed data contract model.

Validating in CI means a bad contract fails the merge request, not a prod run.
``extra="forbid"`` turns a typo'd YAML key into an error instead of a silently ignored
setting.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --------------------------------------------------------------------------------------
# Enumerations
# --------------------------------------------------------------------------------------


class Severity(StrEnum):
    """What happens when a rule fails. Chosen per rule by the contract owner."""

    BLOCKING = "blocking"
    """Stop the run and publish nothing. For data you can't trust (broken PK, volume collapse)."""

    QUARANTINE = "quarantine"
    """Send bad rows to quarantine and publish the rest. For low-volume row defects."""

    WARN = "warn"
    """Log a metric and continue. For trends worth watching but not worth paging on."""


class Classification(StrEnum):
    """Field sensitivity. Drives masking, access control and retention."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    PII = "pii"
    PCI = "pci"


class MaskingStrategy(StrEnum):
    HASH_HMAC = "hash_hmac"
    """Keyed HMAC-SHA256. Deterministic, so joins still work. Default for identifiers."""

    EMAIL_DOMAIN_ONLY = "email_domain_only"
    """Keep only the email domain."""

    LAST4 = "last4"
    REDACT = "redact"
    NONE = "none"


class IngestionMode(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"
    APPEND = "append"
    CDC = "cdc"


class SourceType(StrEnum):
    FILESYSTEM = "filesystem"
    REST_API = "rest_api"
    DATABASE = "database"
    STREAM = "stream"


class Compatibility(StrEnum):
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"


class FieldType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    DECIMAL = "decimal"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    JSON = "json"


# --------------------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------------------


class FieldSpec(_Base):
    name: str
    type: FieldType
    required: bool = False
    description: str = ""
    classification: Classification = Classification.INTERNAL
    masking: MaskingStrategy = MaskingStrategy.NONE
    # Decimal precision/scale. Money is never a float (see docs/02-data-modeling.md).
    precision: int | None = None
    scale: int | None = None
    # Marks a rename, so the compatibility check doesn't see a drop + add.
    renamed_from: str | None = None
    deprecated: bool = False

    @model_validator(mode="after")
    def _decimal_needs_precision(self) -> FieldSpec:
        if self.type is FieldType.DECIMAL and (self.precision is None or self.scale is None):
            raise ValueError(f"field '{self.name}': decimal type requires precision and scale")
        if self.masking is not MaskingStrategy.NONE and self.type is not FieldType.STRING:
            raise ValueError(f"field '{self.name}': masking is only supported on string fields")
        return self


class SchemaSpec(_Base):
    primary_key: list[str] = Field(min_length=1)
    fields: list[FieldSpec] = Field(min_length=1)
    # Tie-break order when a key appears more than once in a batch.
    dedupe_order_by: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _keys_exist(self) -> SchemaSpec:
        names = {f.name for f in self.fields}
        for col in self.primary_key:
            if col not in names:
                raise ValueError(f"primary key '{col}' is not declared as a field")
        for col in self.dedupe_order_by:
            # '_' columns are added by the pipeline, so they're allowed here.
            if col not in names and not col.startswith("_"):
                raise ValueError(f"'{col}' referenced in dedupe_order_by but not declared")
        for pk in self.primary_key:
            spec = next(f for f in self.fields if f.name == pk)
            if not spec.required:
                raise ValueError(f"primary key field '{pk}' must be required")
        return self

    def field(self, name: str) -> FieldSpec | None:
        return next((f for f in self.fields if f.name == name), None)


# --------------------------------------------------------------------------------------
# Quality rules
# --------------------------------------------------------------------------------------


class RuleSpec(_Base):
    """A quality rule, declared as data so it's reviewable and queryable."""

    id: str
    type: str
    severity: Severity = Severity.QUARANTINE
    columns: list[str] = Field(default_factory=list)
    # Validated by each rule, so new rule types don't touch this model.
    params: dict[str, Any] = Field(default_factory=dict)
    description: str = ""


class QualitySpec(_Base):
    rules: list[RuleSpec] = Field(default_factory=list)
    # If more than this share of rows is quarantined, fail the whole batch.
    max_quarantine_rate: Annotated[float, Field(ge=0.0, le=1.0)] = 0.02

    @model_validator(mode="after")
    def _unique_rule_ids(self) -> QualitySpec:
        seen: set[str] = set()
        for rule in self.rules:
            if rule.id in seen:
                raise ValueError(f"duplicate quality rule id '{rule.id}'")
            seen.add(rule.id)
        return self


# --------------------------------------------------------------------------------------
# Source, physical layout, SLOs
# --------------------------------------------------------------------------------------


class SourceSpec(_Base):
    type: SourceType
    mode: IngestionMode
    format: Literal["jsonl", "csv", "parquet", "json", "avro"] = "jsonl"
    # May contain {logical_date} / {logical_date_nodash}, so backfills read the right day.
    uri: str = ""
    # Name of a secret resolved at runtime; never the value itself.
    secret_ref: str | None = None
    watermark_field: str | None = None
    # CDC envelopes: where to find op type and ordering.
    cdc_op_field: str | None = None
    cdc_sequence_field: str | None = None
    # How late a record can arrive and still land in its original partition.
    late_arrival_window_days: int = 3
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _mode_requirements(self) -> SourceSpec:
        if self.mode is IngestionMode.INCREMENTAL and not self.watermark_field:
            raise ValueError("incremental ingestion requires a watermark_field")
        if self.mode is IngestionMode.CDC and not (self.cdc_op_field and self.cdc_sequence_field):
            raise ValueError("cdc ingestion requires cdc_op_field and cdc_sequence_field")
        return self


class PartitioningSpec(_Base):
    column: str | None = None
    granularity: Literal["hour", "day", "month"] = "day"
    # Columns consumers filter on most; used for file pruning.
    cluster_by: list[str] = Field(default_factory=list)
    target_file_size_mb: int = 256


class RetentionSpec(_Base):
    bronze_days: int = 90
    silver_days: int = 1825
    quarantine_days: int = 30


class SloSpec(_Base):
    """Service levels for this dataset, read by the freshness and volume monitors."""

    freshness_minutes: int = 1440
    expected_min_rows_per_run: int = 0
    # Flag the run if rows fall below this fraction of the historical mean.
    volume_drop_threshold: float = 0.5
    completeness_target: Annotated[float, Field(ge=0.0, le=1.0)] = 0.99


class EvolutionSpec(_Base):
    compatibility: Compatibility = Compatibility.BACKWARD
    # Keep unknown source columns in bronze for replay; they never reach silver.
    allow_unknown_fields_in_bronze: bool = True


class LayerTargets(_Base):
    bronze: str
    silver: str
    quarantine: str


class ContractMetadata(_Base):
    name: str
    domain: str
    owner: str
    description: str = ""
    # Contract semver. CI requires a major bump for breaking changes.
    version: str = "1.0.0"
    tags: list[str] = Field(default_factory=list)
    upstream_system: str = ""


class ContractSpec(_Base):
    source: SourceSpec
    targets: LayerTargets
    schema_: SchemaSpec = Field(alias="schema")
    quality: QualitySpec = QualitySpec()
    partitioning: PartitioningSpec = PartitioningSpec()
    retention: RetentionSpec = RetentionSpec()
    slo: SloSpec = SloSpec()
    evolution: EvolutionSpec = EvolutionSpec()

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class DataContract(_Base):
    api_version: Literal["dataforge/v1"] = Field(default="dataforge/v1", alias="apiVersion")
    kind: Literal["DataContract"] = "DataContract"
    metadata: ContractMetadata
    spec: ContractSpec

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    # -- convenience accessors ----------------------------------------------------------

    @property
    def name(self) -> str:
        return self.metadata.name

    @property
    def major_version(self) -> int:
        return int(self.metadata.version.split(".")[0])

    @property
    def sensitive_fields(self) -> list[FieldSpec]:
        return [
            f
            for f in self.spec.schema_.fields
            if f.classification in (Classification.PII, Classification.PCI)
        ]

    def masked_fields(self) -> dict[str, MaskingStrategy]:
        return {
            f.name: f.masking
            for f in self.spec.schema_.fields
            if f.masking is not MaskingStrategy.NONE
        }
