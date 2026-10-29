"""Source readers for files, REST APIs, CDC drops and landed stream batches.

Each returns a ``Batch`` with provenance for replay and lineage. Streams are read as
micro-batches a sink already landed in storage (see ADR-0007).
"""

from __future__ import annotations

import csv
import json
import os
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from dataforge.contracts.models import DataContract, IngestionMode, SourceType
from dataforge.engines.storage import list_matching, read_text, resolve
from dataforge.quality.rules import coerce_timestamp

Record = dict[str, Any]


@dataclass(slots=True)
class Batch:
    records: list[Record]
    source_uris: list[str] = field(default_factory=list)
    #: Max watermark value seen. Saved only after a successful publish.
    high_water_mark: str | None = None
    #: Extra details (pages, offsets, ...) for the manifest and lineage.
    provenance: dict[str, Any] = field(default_factory=dict)


class Source(Protocol):
    def read(self, *, logical_date: date, watermark: str | None) -> Batch: ...


# --------------------------------------------------------------------------------------


def source_uri(contract: DataContract) -> str:
    """Source URI for this environment: the contract's default, or an env override.

        DATAFORGE_SOURCE_URI__FX_RATES=http://localhost:8099/v1/fx/rates?date={logical_date}

    Keeps the contract free of per-environment addresses.
    """
    override = os.environ.get(f"DATAFORGE_SOURCE_URI__{contract.name.upper()}")
    return override or contract.spec.source.uri


def render_uri(template: str, logical_date: date) -> str:
    """Fill date tokens into the URI. No ``now()`` token, so runs stay replayable."""
    return template.format(
        logical_date=logical_date.isoformat(),
        logical_date_nodash=logical_date.strftime("%Y%m%d"),
        year=logical_date.year,
        month=f"{logical_date.month:02d}",
        day=f"{logical_date.day:02d}",
    )


class FilesystemSource:
    """Reads jsonl / csv / json files from a local path or S3."""

    def __init__(self, contract: DataContract, root: str | Path) -> None:
        self.contract = contract
        self.location = resolve(root)

    def read(self, *, logical_date: date, watermark: str | None) -> Batch:
        spec = self.contract.spec.source
        pattern = render_uri(source_uri(self.contract), logical_date)
        paths = list_matching(self.location, pattern)

        records: list[Record] = []
        uris: list[str] = []
        for path in paths:
            uris.append(self.location.uri(path))
            records.extend(self._read_file(path, spec.format))

        # If the URI has {logical_date}, the partition is already the increment.
        # Filtering by watermark too would drop rows and truncate the partition on overwrite.
        partition_addressable = "{logical_date" in source_uri(self.contract)
        if not partition_addressable:
            records = _apply_watermark(records, spec.watermark_field, watermark, spec.mode)
        return Batch(
            records=records,
            source_uris=uris,
            high_water_mark=_max_watermark(records, spec.watermark_field),
            provenance={"file_count": len(uris), "pattern": pattern},
        )

    def _read_file(self, path: str, fmt: str) -> Iterator[Record]:
        text = read_text(self.location, path)
        if fmt == "jsonl":
            for line_no, line in enumerate(text.splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    # Pass bad lines on as records so they get quarantined, not crash the run.
                    yield {"_malformed": True, "_raw": line, "_error": str(exc), "_line": line_no}
        elif fmt == "csv":
            yield from csv.DictReader(text.splitlines())
        elif fmt == "json":
            payload = json.loads(text)
            yield from (payload if isinstance(payload, list) else [payload])
        else:
            raise ValueError(f"unsupported file format for filesystem source: {fmt}")


class RestApiSource:
    """Paginated REST reader (GET only)."""

    def __init__(
        self, contract: DataContract, *, token: str | None = None, opener: Any = None
    ) -> None:
        self.contract = contract
        self.token = token
        self._opener = opener or urllib.request.urlopen

    def read(self, *, logical_date: date, watermark: str | None) -> Batch:
        spec = self.contract.spec.source
        options = spec.options
        page_param = options.get("page_param", "page")
        records_path = options.get("records_path", "data")
        max_pages = int(options.get("max_pages", 50))

        base = render_uri(source_uri(self.contract), logical_date)
        if spec.mode is IngestionMode.INCREMENTAL and watermark:
            sep = "&" if "?" in base else "?"
            base = f"{base}{sep}{options.get('since_param', 'updated_since')}={watermark}"

        records: list[Record] = []
        uris: list[str] = []
        for page in range(1, max_pages + 1):
            sep = "&" if "?" in base else "?"
            url = f"{base}{sep}{page_param}={page}"
            uris.append(url)
            payload = self._get_json(url)
            page_records = payload.get(records_path, []) if isinstance(payload, dict) else payload
            if not page_records:
                break
            records.extend(page_records)
            if len(page_records) < int(options.get("page_size", len(page_records) or 1)):
                break

        return Batch(
            records=records,
            source_uris=uris[:1],  # one representative URI; the rest is in provenance
            high_water_mark=_max_watermark(records, spec.watermark_field),
            provenance={"pages_fetched": len(uris), "endpoint": base},
        )

    def _get_json(self, url: str) -> Any:
        request = urllib.request.Request(url, method="GET")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        with self._opener(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


class StreamSource:
    """Reads micro-batches a streaming sink (Kafka Connect, Firehose, ...) has landed.

    The sink handles delivery; idempotent partition writes handle duplicates. An Airflow
    outage only delays data.
    """

    def __init__(self, contract: DataContract, root: str | Path) -> None:
        self._inner = FilesystemSource(contract, root)
        self.contract = contract

    def read(self, *, logical_date: date, watermark: str | None) -> Batch:
        batch = self._inner.read(logical_date=logical_date, watermark=watermark)
        offsets = {
            str(r.get("_partition")): r.get("_offset")
            for r in batch.records
            if r.get("_partition") is not None
        }
        batch.provenance["kafka_offsets"] = offsets
        return batch


def build_source(contract: DataContract, *, root: str | Path, token: str | None = None) -> Source:
    """Pick the source class for the contract's source type."""
    match contract.spec.source.type:
        case SourceType.FILESYSTEM:
            return FilesystemSource(contract, root)
        case SourceType.STREAM:
            return StreamSource(contract, root)
        case SourceType.REST_API:
            return RestApiSource(contract, token=token)
        case SourceType.DATABASE:
            # CDC lands as Debezium files, so the filesystem reader handles it.
            return FilesystemSource(contract, root)
    raise ValueError(f"unsupported source type: {contract.spec.source.type}")


# --------------------------------------------------------------------------------------


def _apply_watermark(
    records: list[Record], watermark_field: str | None, watermark: str | None, mode: IngestionMode
) -> list[Record]:
    """Keep records at or after the watermark.

    Inclusive so rows sharing the watermark timestamp aren't lost; dedupe handles the overlap.
    """
    if mode is not IngestionMode.INCREMENTAL or not watermark or not watermark_field:
        return records
    floor = coerce_timestamp(watermark)
    if floor is None:
        return records
    floor -= timedelta(seconds=1)
    kept: list[Record] = []
    for record in records:
        stamp = coerce_timestamp(record.get(watermark_field))
        if stamp is None or stamp >= floor:
            kept.append(record)
    return kept


def _max_watermark(records: list[Record], watermark_field: str | None) -> str | None:
    if not watermark_field:
        return None
    stamps = [t for t in (coerce_timestamp(r.get(watermark_field)) for r in records) if t]
    if not stamps:
        return None
    # Cap at now() so one future-dated record can't push the watermark ahead.
    ceiling = datetime.now(UTC)
    return min(max(stamps), ceiling).isoformat()
