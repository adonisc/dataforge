"""Tests for the validation engine.

Covers the core behaviour: blocking rules stop publishing, quarantine rules don't,
each row keeps every failure reason, and the quarantine-rate limit kicks in.
"""

from __future__ import annotations

import pytest

from dataforge.quality.engine import ValidationEngine
from tests.conftest import build_contract, iso


def _rule(
    rule_id: str, rule_type: str, severity: str, columns: list[str], **params: object
) -> dict:
    return {
        "id": rule_id,
        "type": rule_type,
        "severity": severity,
        "columns": columns,
        "params": params,
    }


def test_clean_batch_publishes_everything() -> None:
    contract = build_contract(
        spec={"quality": {"rules": [_rule("nn", "not_null", "blocking", ["widget_id"])]}}
    )
    records = [{"widget_id": f"W{i}", "status": "ok", "updated_at": iso()} for i in range(5)]

    report = ValidationEngine(contract).run(records)

    assert report.passed
    assert report.valid_count == 5
    assert report.quarantined_count == 0
    assert report.metrics()["completeness"] == 1.0


def test_blocking_rule_prevents_publication_and_names_the_reason() -> None:
    contract = build_contract(
        spec={"quality": {"rules": [_rule("nn", "not_null", "blocking", ["widget_id"])]}}
    )
    records = [{"widget_id": "W1"}, {"widget_id": None}]

    report = ValidationEngine(contract).run(records)

    assert not report.passed
    assert len(report.blocking_violations) == 1
    assert "widget_id" in report.failure_summary()


def test_quarantine_isolates_bad_rows_but_keeps_the_batch() -> None:
    contract = build_contract(
        spec={
            "quality": {
                "max_quarantine_rate": 0.5,
                "rules": [
                    _rule(
                        "status", "accepted_values", "quarantine", ["status"], values=["ok", "bad"]
                    )
                ],
            }
        }
    )
    records = [
        {"widget_id": "W1", "status": "ok"},
        {"widget_id": "W2", "status": "exploded"},
        {"widget_id": "W3", "status": "bad"},
    ]

    report = ValidationEngine(contract).run(records)

    assert report.passed
    assert report.valid_count == 2
    assert report.quarantined_count == 1
    assert report.quarantined[0].record["widget_id"] == "W2"


def test_quarantined_row_carries_every_reason_not_just_the_first() -> None:
    """A quarantined row lists every rule it failed, not just the first."""
    contract = build_contract(
        spec={
            "quality": {
                "max_quarantine_rate": 1.0,
                "rules": [
                    _rule("status", "accepted_values", "quarantine", ["status"], values=["ok"]),
                    _rule("amount", "range", "quarantine", ["amount"], min=0),
                    _rule("ccy", "regex", "quarantine", ["currency"], pattern="[A-Z]{3}"),
                ],
            }
        }
    )
    records = [{"widget_id": "W1", "status": "bogus", "amount": -5, "currency": "dollars"}]

    report = ValidationEngine(contract).run(records)

    rule_ids = {v.rule_id for v in report.quarantined[0].violations}
    assert rule_ids == {"status", "amount", "ccy"}


def test_warn_severity_never_blocks_and_never_quarantines() -> None:
    contract = build_contract(
        spec={
            "quality": {
                "rules": [_rule("chan", "accepted_values", "warn", ["channel"], values=["web"])]
            }
        }
    )
    records = [{"widget_id": "W1", "channel": "carrier_pigeon"}]

    report = ValidationEngine(contract).run(records)

    assert report.passed
    assert report.valid_count == 1
    assert len(report.warnings) == 1


def test_circuit_breaker_blocks_when_too_much_of_the_batch_quarantines() -> None:
    """If too many rows are quarantined, the whole batch is blocked."""
    contract = build_contract(
        spec={
            "quality": {
                "max_quarantine_rate": 0.1,
                "rules": [_rule("st", "accepted_values", "quarantine", ["status"], values=["ok"])],
            }
        }
    )
    records = [{"widget_id": f"W{i}", "status": "wrong"} for i in range(10)]

    report = ValidationEngine(contract).run(records)

    assert not report.passed
    assert report.quarantine_rate == 1.0
    assert any(v.rule_type == "circuit_breaker" for v in report.blocking_violations)


def test_unique_rule_supersedes_by_declared_order_not_arrival_order() -> None:
    contract = build_contract(
        spec={
            "quality": {
                "max_quarantine_rate": 0.9,
                "rules": [
                    _rule("pk", "unique", "quarantine", ["widget_id"], order_by=["updated_at"])
                ],
            }
        }
    )
    records = [
        {"widget_id": "W1", "updated_at": "2026-01-02T00:00:00Z", "v": "newer"},
        {"widget_id": "W1", "updated_at": "2026-01-01T00:00:00Z", "v": "older"},
    ]

    report = ValidationEngine(contract).run(records)

    assert report.valid_count == 1
    assert report.valid[0]["v"] == "newer", "the survivor must be chosen by updated_at"


def test_future_timestamp_is_rejected() -> None:
    contract = build_contract(
        spec={
            "quality": {
                "rules": [
                    _rule(
                        "ts",
                        "timestamp_valid",
                        "blocking",
                        ["updated_at"],
                        max_future_skew_minutes=10,
                    )
                ]
            }
        }
    )

    assert not ValidationEngine(contract).run([{"updated_at": iso(60 * 24 * 365)}]).passed
    assert ValidationEngine(contract).run([{"updated_at": iso(5)}]).passed, "skew tolerance applies"


def test_unknown_rule_type_fails_at_construction_not_at_runtime() -> None:
    contract = build_contract(
        spec={"quality": {"rules": [_rule("x", "telepathy", "blocking", ["widget_id"])]}}
    )
    with pytest.raises(KeyError, match="telepathy"):
        ValidationEngine(contract)


def test_empty_batch_is_valid_and_does_not_divide_by_zero() -> None:
    contract = build_contract(
        spec={"quality": {"rules": [_rule("nn", "not_null", "blocking", ["widget_id"])]}}
    )
    report = ValidationEngine(contract).run([])
    assert report.passed
    assert report.metrics()["completeness"] == 1.0
