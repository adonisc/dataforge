"""Tests for the schema compatibility check."""

from __future__ import annotations

from dataforge.contracts.compatibility import ChangeKind, check_compatibility, enforce
from tests.conftest import build_contract

_BASE_FIELDS = [
    {"name": "widget_id", "type": "string", "required": True},
    {"name": "status", "type": "string", "required": True},
    {"name": "amount", "type": "decimal", "precision": 18, "scale": 2},
    {"name": "updated_at", "type": "timestamp", "required": True},
    {"name": "event_date", "type": "date", "required": True},
]


def _with_fields(fields: list[dict], version: str = "1.0.0", compatibility: str = "backward"):
    return build_contract(
        metadata={
            "name": "widgets",
            "domain": "test",
            "owner": "t@acme.example",
            "version": version,
        },
        spec={
            "schema": {
                "primary_key": ["widget_id"],
                "dedupe_order_by": ["updated_at"],
                "fields": fields,
            },
            "evolution": {"compatibility": compatibility},
        },
    )


def test_adding_an_optional_field_is_backward_compatible() -> None:
    old = _with_fields(_BASE_FIELDS)
    new = _with_fields([*_BASE_FIELDS, {"name": "promo_code", "type": "string"}], version="1.1.0")

    report = check_compatibility(old, new)

    assert report.is_compatible
    assert enforce(report) == []
    assert report.changes[0].kind is ChangeKind.FIELD_ADDED_OPTIONAL


def test_dropping_a_field_is_breaking_and_requires_a_major_bump() -> None:
    old = _with_fields(_BASE_FIELDS)
    new = _with_fields([f for f in _BASE_FIELDS if f["name"] != "amount"], version="1.1.0")

    report = check_compatibility(old, new)

    assert not report.is_compatible
    violations = enforce(report)
    assert violations and "major version bump" in violations[0]


def test_dropping_a_field_passes_the_gate_when_the_major_version_is_bumped() -> None:
    """Breaking changes are fine with a major version bump, not without."""
    old = _with_fields(_BASE_FIELDS)
    new = _with_fields([f for f in _BASE_FIELDS if f["name"] != "amount"], version="2.0.0")

    report = check_compatibility(old, new)

    assert not report.is_compatible
    assert enforce(report) == []


def test_widening_a_type_is_allowed_narrowing_is_not() -> None:
    old = _with_fields([*_BASE_FIELDS, {"name": "qty", "type": "integer"}])
    widened = _with_fields([*_BASE_FIELDS, {"name": "qty", "type": "long"}], version="1.1.0")
    narrowed = _with_fields([*_BASE_FIELDS, {"name": "qty", "type": "boolean"}], version="1.1.0")

    assert check_compatibility(old, widened).is_compatible
    assert not check_compatibility(old, narrowed).is_compatible


def test_changing_the_primary_key_is_always_breaking() -> None:
    """Changing the primary key is always breaking, even with policy 'none'."""
    old = _with_fields(_BASE_FIELDS, compatibility="none")
    new = build_contract(
        metadata={
            "name": "widgets",
            "domain": "test",
            "owner": "t@acme.example",
            "version": "1.1.0",
        },
        spec={
            "schema": {
                "primary_key": ["widget_id", "event_date"],
                "dedupe_order_by": ["updated_at"],
                "fields": _BASE_FIELDS,
            },
            "evolution": {"compatibility": "none"},
        },
    )

    report = check_compatibility(old, new)

    assert not report.is_compatible
    assert report.breaking_changes[0].kind is ChangeKind.PRIMARY_KEY_CHANGED


def test_a_declared_rename_is_recognised_rather_than_read_as_drop_plus_add() -> None:
    old = _with_fields(_BASE_FIELDS)
    renamed = [
        {"name": "widget_status", "type": "string", "required": True, "renamed_from": "status"}
        if f["name"] == "status"
        else f
        for f in _BASE_FIELDS
    ]
    new = _with_fields(renamed, version="2.0.0")

    report = check_compatibility(old, new)
    kinds = {c.kind for c in report.changes}

    assert ChangeKind.FIELD_RENAMED in kinds
    assert ChangeKind.FIELD_REMOVED not in kinds
    assert enforce(report) == [], "signalled by the major bump"


def test_relaxing_a_classification_is_breaking_regardless_of_policy() -> None:
    """Downgrading pii to internal drops masking, so it's breaking even with policy 'none'."""
    old = _with_fields(
        [
            *_BASE_FIELDS,
            {"name": "email", "type": "string", "classification": "pii", "masking": "hash_hmac"},
        ],
        compatibility="none",
    )
    new = _with_fields(
        [*_BASE_FIELDS, {"name": "email", "type": "string", "classification": "internal"}],
        version="1.0.1",
        compatibility="none",
    )

    report = check_compatibility(old, new)

    assert not report.is_compatible
    assert report.breaking_changes[0].kind is ChangeKind.CLASSIFICATION_RELAXED
    assert enforce(report)


def test_making_an_optional_field_required_is_forward_breaking_only() -> None:
    old_fields = [*_BASE_FIELDS, {"name": "note", "type": "string"}]
    new_fields = [*_BASE_FIELDS, {"name": "note", "type": "string", "required": True}]

    backward = check_compatibility(
        _with_fields(old_fields),
        _with_fields(new_fields, version="1.1.0", compatibility="backward"),
    )
    full = check_compatibility(
        _with_fields(old_fields, compatibility="full"),
        _with_fields(new_fields, version="1.1.0", compatibility="full"),
    )

    assert backward.is_compatible, "old readers are unaffected by a tightened constraint"
    assert not full.is_compatible, "old *data* may violate it, so FULL rejects it"
