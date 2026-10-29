"""Tests for normalisation, deduplication, CDC collapse and SCD2."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from dataforge.transform.dedupe import collapse_cdc, drop_exact_duplicates, latest_per_key
from dataforge.transform.normalize import CoercionError, coerce_value, normalize_batch, record_hash
from dataforge.transform.scd import FAR_FUTURE, apply_scd2
from tests.conftest import build_contract

# ---------------------------------------------------------------- normalisation -------


def test_money_is_decimal_not_float() -> None:
    """Money uses Decimal, because 0.1 + 0.2 != 0.3 in floating point."""
    contract = build_contract()
    spec = next(f for f in contract.spec.schema_.fields if f.name == "amount")

    assert coerce_value("0.1", spec) + coerce_value("0.2", spec) == Decimal("0.30")
    assert coerce_value("19.999", spec) == Decimal("20.00"), "quantised to the declared scale"


def test_coercion_failure_raises_rather_than_silently_nulling() -> None:
    contract = build_contract()
    spec = next(f for f in contract.spec.schema_.fields if f.name == "amount")
    with pytest.raises(CoercionError):
        coerce_value("not-a-number", spec)


def test_normalize_routes_bad_rows_out_instead_of_failing_the_batch() -> None:
    contract = build_contract()
    records = [
        {
            "widget_id": "W1",
            "status": "ok",
            "amount": "10.50",
            "updated_at": "2026-01-01T00:00:00Z",
            "event_date": "2026-01-01",
        },
        {
            "widget_id": "W2",
            "status": "ok",
            "amount": "banana",
            "updated_at": "2026-01-01T00:00:00Z",
            "event_date": "2026-01-01",
        },
    ]

    good, bad = normalize_batch(contract, records, run_id="r1", source_uri="s3://x")

    assert len(good) == 1
    assert len(bad) == 1
    assert "banana" in bad[0][1]


def test_normalize_adds_technical_lineage_columns() -> None:
    contract = build_contract()
    good, _ = normalize_batch(
        contract,
        [
            {
                "widget_id": "W1",
                "status": "ok",
                "updated_at": "2026-01-01T00:00:00Z",
                "event_date": "2026-01-01",
            }
        ],
        run_id="run-42",
        source_uri="s3://bucket/key",
    )
    row = good[0]
    assert row["_run_id"] == "run-42"
    assert row["_source_uri"] == "s3://bucket/key"
    assert row["_contract_version"] == "1.0.0"
    assert len(row["_record_hash"]) == 64


def test_naive_timestamps_are_assumed_utc() -> None:
    contract = build_contract()
    good, _ = normalize_batch(
        contract,
        [
            {
                "widget_id": "W1",
                "status": "ok",
                "updated_at": "2026-01-01 12:00:00",
                "event_date": "2026-01-01",
            }
        ],
        run_id="r",
        source_uri="s",
    )
    assert good[0]["updated_at"].tzinfo is UTC


def test_record_hash_is_order_independent_and_change_sensitive() -> None:
    a = {"x": 1, "y": 2}
    b = {"y": 2, "x": 1}
    c = {"x": 1, "y": 3}
    assert record_hash(a, ["x", "y"]) == record_hash(b, ["x", "y"])
    assert record_hash(a, ["x", "y"]) != record_hash(c, ["x", "y"])


# ---------------------------------------------------------------- deduplication -------


def test_exact_redelivery_is_dropped() -> None:
    rows = [{"_record_hash": "h1", "v": 1}, {"_record_hash": "h1", "v": 1}, {"_record_hash": "h2"}]
    assert len(drop_exact_duplicates(rows)) == 2


def test_latest_per_key_is_deterministic_regardless_of_input_order() -> None:
    newer = {"id": "A", "updated_at": "2026-02-01T00:00:00Z", "v": "new"}
    older = {"id": "A", "updated_at": "2026-01-01T00:00:00Z", "v": "old"}

    forward = latest_per_key([older, newer], ["id"], ["updated_at"])
    reverse = latest_per_key([newer, older], ["id"], ["updated_at"])

    assert forward[0]["v"] == "new"
    assert reverse[0]["v"] == "new"


def test_latest_per_key_tolerates_null_ordering_values() -> None:
    """None values in the sort key must not crash dedupe."""
    rows = [{"id": "A", "updated_at": None}, {"id": "A", "updated_at": "2026-01-01T00:00:00Z"}]
    result = latest_per_key(rows, ["id"], ["updated_at"])
    assert len(result) == 1
    assert result[0]["updated_at"] is not None


# ------------------------------------------------------------------------ CDC ---------


def test_cdc_collapse_keeps_the_highest_lsn_per_key() -> None:
    rows = [
        {"id": "A", "op": "c", "lsn": 1, "v": "first"},
        {"id": "A", "op": "u", "lsn": 3, "v": "latest"},
        {"id": "A", "op": "u", "lsn": 2, "v": "middle"},
    ]
    out = collapse_cdc(rows, ["id"], op_field="op", sequence_field="lsn")
    assert len(out) == 1
    assert out[0]["v"] == "latest"


def test_cdc_delete_becomes_a_tombstone_not_a_disappearance() -> None:
    """Deletes become tombstones; otherwise the row would live on in the target."""
    rows = [{"id": "A", "op": "u", "lsn": 1}, {"id": "A", "op": "d", "lsn": 2}]
    out = collapse_cdc(rows, ["id"], op_field="op", sequence_field="lsn")
    assert len(out) == 1
    assert out[0]["_is_deleted"] is True


def test_snapshot_rows_never_overwrite_a_newer_streamed_change() -> None:
    """A snapshot row never overwrites a newer streamed change."""
    rows = [
        {"id": "A", "op": "u", "lsn": 10, "v": "streamed"},
        {"id": "A", "op": "r", "lsn": 10, "v": "snapshot"},
    ]
    out = collapse_cdc(rows, ["id"], op_field="op", sequence_field="lsn")
    assert out[0]["v"] == "streamed"


# ----------------------------------------------------------------------- SCD2 ---------


def _dim_row(key: str, value: str, hash_: str, valid_from: str) -> dict:
    return {
        "customer_id": key,
        "segment": value,
        "_record_hash": hash_,
        "_valid_from": datetime.fromisoformat(valid_from),
        "_valid_to": FAR_FUTURE,
        "_is_current": True,
        "_version": 1,
    }


def test_scd2_opens_a_new_version_and_closes_the_old_one() -> None:
    current = [_dim_row("C1", "smb", "h1", "2026-01-01T00:00:00+00:00")]
    incoming = [
        {
            "customer_id": "C1",
            "segment": "enterprise",
            "_record_hash": "h2",
            "source_ts": "2026-06-01T00:00:00Z",
        }
    ]

    result = apply_scd2(
        current, incoming, key_columns=["customer_id"], effective_column="source_ts"
    )

    assert result.opened == 1
    assert result.closed == 1
    closed = next(r for r in result.rows if not r["_is_current"])
    opened = next(r for r in result.rows if r["_is_current"])
    assert closed["_valid_to"] == opened["_valid_from"], "no gap, no overlap in the chain"
    assert opened["segment"] == "enterprise"


def test_scd2_ignores_a_rewrite_that_changes_nothing() -> None:
    """Re-sending an unchanged row doesn't create a new version."""
    current = [_dim_row("C1", "smb", "h1", "2026-01-01T00:00:00+00:00")]
    incoming = [
        {
            "customer_id": "C1",
            "segment": "smb",
            "_record_hash": "h1",
            "source_ts": "2026-06-01T00:00:00Z",
        }
    ]

    result = apply_scd2(
        current, incoming, key_columns=["customer_id"], effective_column="source_ts"
    )

    assert result.unchanged == 1
    assert result.opened == 0
    assert len(result.rows) == 1


def test_scd2_effective_dates_from_the_event_not_from_ingestion() -> None:
    current: list[dict] = []
    incoming = [
        {
            "customer_id": "C1",
            "segment": "smb",
            "_record_hash": "h1",
            "source_ts": "2026-03-15T08:00:00Z",
        }
    ]

    result = apply_scd2(
        current, incoming, key_columns=["customer_id"], effective_column="source_ts"
    )

    assert result.rows[0]["_valid_from"] == datetime(2026, 3, 15, 8, tzinfo=UTC)


def test_scd2_delete_closes_history_without_erasing_it() -> None:
    current = [_dim_row("C1", "smb", "h1", "2026-01-01T00:00:00+00:00")]
    incoming = [
        {
            "customer_id": "C1",
            "_record_hash": "h9",
            "_is_deleted": True,
            "source_ts": "2026-06-01T00:00:00Z",
        }
    ]

    result = apply_scd2(
        current, incoming, key_columns=["customer_id"], effective_column="source_ts"
    )

    assert result.closed == 1
    assert result.opened == 0
    assert len(result.rows) == 1, "facts already written still need something to join to"
    assert result.rows[0]["_is_current"] is False


def test_scd2_surfaces_out_of_order_arrivals_instead_of_mis_sequencing_them() -> None:
    current = [_dim_row("C1", "enterprise", "h2", "2026-06-01T00:00:00+00:00")]
    incoming = [
        {
            "customer_id": "C1",
            "segment": "smb",
            "_record_hash": "h3",
            "source_ts": "2026-02-01T00:00:00Z",
        }
    ]

    result = apply_scd2(
        current, incoming, key_columns=["customer_id"], effective_column="source_ts"
    )

    assert result.out_of_order and result.opened == 0
