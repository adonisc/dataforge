"""Shared fixtures.

Contracts are built in Python, not loaded from the shipped YAML, so tests only break
when the code does.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dataforge.contracts.models import DataContract
from dataforge.privacy.masking import MaskingKey


@pytest.fixture
def masking_key() -> MaskingKey:
    return MaskingKey(secret=b"unit-test-key-not-a-secret", version="v1")


@pytest.fixture
def now() -> datetime:
    return datetime.now(UTC)


def build_contract(**overrides: Any) -> DataContract:
    """A small, realistic contract that each test can tweak."""
    payload: dict[str, Any] = {
        "apiVersion": "dataforge/v1",
        "kind": "DataContract",
        "metadata": {
            "name": "widgets",
            "domain": "test",
            "owner": "test@acme.example",
            "version": "1.0.0",
        },
        "spec": {
            "source": {
                "type": "filesystem",
                "mode": "incremental",
                "format": "jsonl",
                "uri": "widgets/dt={logical_date}/*.jsonl",
                "watermark_field": "updated_at",
            },
            "targets": {
                "bronze": "bronze.widgets_raw",
                "silver": "silver.widgets",
                "quarantine": "quarantine.widgets",
            },
            "schema": {
                "primary_key": ["widget_id"],
                "dedupe_order_by": ["updated_at"],
                "fields": [
                    {"name": "widget_id", "type": "string", "required": True},
                    {"name": "status", "type": "string", "required": True},
                    {"name": "amount", "type": "decimal", "precision": 18, "scale": 2},
                    {"name": "updated_at", "type": "timestamp", "required": True},
                    {"name": "event_date", "type": "date", "required": True},
                ],
            },
            "quality": {"rules": []},
            "partitioning": {"column": "event_date", "granularity": "day"},
        },
    }
    _deep_merge(payload, overrides)
    return DataContract.model_validate(payload)


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> None:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def iso(offset_minutes: int = 0) -> str:
    return (datetime.now(UTC) + timedelta(minutes=offset_minutes)).isoformat()
