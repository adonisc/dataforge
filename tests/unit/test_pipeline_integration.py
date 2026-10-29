"""End-to-end pipeline tests against a temporary lake.

These cover bugs that only show up across components, like a partition getting
truncated on overwrite or CDC deletes being lost on write.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from dataforge.contracts.loader import load_contract
from dataforge.engines.parquet_lake import ParquetLakeEngine
from dataforge.ingest.state import JsonStateStore, RunStatus
from dataforge.pipeline import PipelineConfig, QualityGateError, run_ingestion
from dataforge.privacy.masking import MaskingKey

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"


@pytest.fixture
def workspace(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        env="test",
        lake_root=tmp_path / "lake",
        landing_root=tmp_path / "landing",
        state_path=tmp_path / "state.json",
        lineage_path=tmp_path / "lineage.jsonl",
        masking_key=MaskingKey(secret=b"test-key", version="v1"),
        code_version="test",
    )


def _order(
    order_id: str,
    *,
    day: str,
    status: str = "paid",
    amount: str = "10.00",
    updated: str | None = None,
) -> dict:
    return {
        "order_id": order_id,
        "customer_id": "CUST-1",
        "customer_email": f"{order_id}@example.invalid",
        "order_status": status,
        "channel": "web",
        "currency_code": "USD",
        "gross_amount": amount,
        "discount_amount": "0.00",
        "order_ts": f"{day}T10:00:00+00:00",
        "updated_at": updated or f"{day}T10:05:00+00:00",
        "order_date": day,
    }


def _land(config: PipelineConfig, day: str, rows: list[dict]) -> None:
    path = Path(config.landing_root) / f"orders/dt={day}/orders.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")


def test_happy_path_publishes_masked_rows_and_records_a_manifest(workspace: PipelineConfig) -> None:
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    _land(workspace, day, [_order(f"O{i}", day=day) for i in range(10)])

    result = run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    assert result.manifest.status is RunStatus.SUCCEEDED
    assert result.manifest.rows_published == 10

    rows = ParquetLakeEngine(workspace.lake_root).read_table("silver.orders")
    assert len(rows) == 10
    assert "customer_email" not in rows[0], "the clear value must not reach silver"
    assert rows[0]["customer_email_token"].startswith("v1:")
    assert rows[0]["_contract_version"] == contract.metadata.version


def test_rerunning_identical_inputs_is_a_no_op(workspace: PipelineConfig) -> None:
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    _land(workspace, day, [_order(f"O{i}", day=day) for i in range(10)])
    logical = date.fromisoformat(day)

    run_ingestion(contract, workspace, logical_date=logical)
    second = run_ingestion(contract, workspace, logical_date=logical)

    assert second.manifest.status is RunStatus.SKIPPED_DUPLICATE
    assert len(ParquetLakeEngine(workspace.lake_root).read_table("silver.orders")) == 10


def test_forced_reprocess_is_idempotent_rather_than_duplicating(workspace: PipelineConfig) -> None:
    """Running the same date several times gives the same result as running it once."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    _land(workspace, day, [_order(f"O{i}", day=day) for i in range(10)])
    logical = date.fromisoformat(day)

    run_ingestion(contract, workspace, logical_date=logical)
    run_ingestion(contract, workspace, logical_date=logical, force=True)
    run_ingestion(contract, workspace, logical_date=logical, force=True)

    rows = ParquetLakeEngine(workspace.lake_root).read_table("silver.orders")
    assert len(rows) == 10
    assert len({r["order_id"] for r in rows}) == 10


def test_an_incremental_run_merges_into_the_partition_instead_of_replacing_it(
    workspace: PipelineConfig,
) -> None:
    """A second run with three corrected rows must keep the other seven in the partition."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    logical = date.fromisoformat(day)

    _land(workspace, day, [_order(f"O{i}", day=day) for i in range(10)])
    run_ingestion(contract, workspace, logical_date=logical)

    _land(
        workspace,
        day,
        [_order("O0", day=day, status="refunded", updated=f"{day}T23:00:00+00:00")],
    )
    run_ingestion(contract, workspace, logical_date=logical)

    rows = {
        r["order_id"]: r for r in ParquetLakeEngine(workspace.lake_root).read_table("silver.orders")
    }
    assert len(rows) == 10, "the other nine orders must survive the merge"
    assert rows["O0"]["order_status"] == "refunded", "and the corrected row must win"


def test_a_blocking_rule_publishes_nothing_at_all(workspace: PipelineConfig) -> None:
    """A blocked run publishes nothing, not a partial batch."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    bad = _order("O-BAD", day=day)
    bad["order_ts"] = "2099-01-01T00:00:00+00:00"  # future-dated -> blocking
    _land(workspace, day, [*(_order(f"O{i}", day=day) for i in range(5)), bad])

    with pytest.raises(QualityGateError):
        run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    assert ParquetLakeEngine(workspace.lake_root).read_table("silver.orders") == []
    manifest = json.loads(workspace.state_path.read_text())["runs"][-1]
    assert manifest["status"] == RunStatus.BLOCKED_BY_QUALITY


def test_quarantined_rows_are_queryable_and_replayable(workspace: PipelineConfig) -> None:
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    rows = [_order(f"O{i}", day=day) for i in range(100)]
    rows.append(_order("O-NEG", day=day, amount="-5.00"))
    _land(workspace, day, rows)

    run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    quarantine = ParquetLakeEngine(workspace.lake_root).read_table("quarantine.orders")
    assert len(quarantine) == 1
    assert "orders_amount_sane" in quarantine[0]["_rule_ids"]
    # The original payload is kept so the row can be replayed.
    assert quarantine[0]["payload"]["order_id"] == "O-NEG"


def test_bronze_retains_everything_including_rows_silver_rejected(
    workspace: PipelineConfig,
) -> None:
    """Bronze keeps every raw row, including rejected ones, for replay."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    rows = [_order(f"O{i}", day=day) for i in range(100)]
    rows.append(_order("O-NEG", day=day, amount="-5.00"))
    _land(workspace, day, rows)

    run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    bronze = ParquetLakeEngine(workspace.lake_root).read_table("bronze.orders_raw")
    assert len(bronze) == 101
    assert any(r["order_id"] == "O-NEG" for r in bronze)


def test_late_arriving_rows_land_in_their_own_business_partition(workspace: PipelineConfig) -> None:
    """Late records go to the partition of their own date, not the run date."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-03"
    late = _order("O-LATE", day="2026-05-01", updated=f"{day}T09:00:00+00:00")
    _land(workspace, day, [_order("O-TODAY", day=day), late])

    result = run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    assert sorted(result.manifest.metrics["partitions_written"]) == ["2026-05-01", "2026-05-03"]
    engine = ParquetLakeEngine(workspace.lake_root)
    assert len(engine.read_partition("silver.orders", "order_date", "2026-05-01")) == 1


def test_lineage_events_are_emitted_for_every_run(workspace: PipelineConfig) -> None:
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    _land(workspace, day, [_order("O1", day=day)])

    run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    events = [json.loads(line) for line in workspace.lineage_path.read_text().splitlines()]
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    complete = events[-1]
    assert complete["outputs"][0]["name"] == "silver.orders"
    assert complete["run"]["facets"]["dataContract"]["version"] == contract.metadata.version
    assert complete["outputs"][0]["facets"]["dataQualityMetrics"]["rowCount"] == 1


def test_watermark_only_advances_after_a_successful_publish(workspace: PipelineConfig) -> None:
    """The watermark only moves after a successful write."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    bad = _order("O-BAD", day=day)
    bad["order_ts"] = "2099-01-01T00:00:00+00:00"
    _land(workspace, day, [bad])

    with pytest.raises(QualityGateError):
        run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    assert JsonStateStore(workspace.state_path).get_watermark("orders") is None


def test_dry_run_validates_without_writing_anything(workspace: PipelineConfig) -> None:
    """This is how CI checks a data sample in a merge request."""
    contract = load_contract(CONTRACTS / "orders.yml")
    day = "2026-05-01"
    _land(workspace, day, [_order(f"O{i}", day=day) for i in range(5)])
    workspace.dry_run = True

    result = run_ingestion(contract, workspace, logical_date=date.fromisoformat(day))

    assert result.report.passed
    assert not (workspace.lake_root / "silver").exists()
