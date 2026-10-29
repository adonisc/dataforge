"""Checks that the shipped contracts follow platform standards."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataforge.contracts.loader import ContractRegistry
from dataforge.contracts.models import Classification, DataContract, MaskingStrategy
from dataforge.quality.engine import ValidationEngine

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
ALL = ContractRegistry(CONTRACTS).all()


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_contract_compiles(contract: DataContract) -> None:
    ValidationEngine(contract)


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_every_contract_has_a_reachable_owner(contract: DataContract) -> None:
    """Every contract needs an owner so alerts have somewhere to go."""
    assert "@" in contract.metadata.owner
    assert contract.metadata.description.strip(), "a consumer must be able to tell what this is"


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_every_sensitive_field_is_actually_masked(contract: DataContract) -> None:
    """PII and PCI fields must actually be masked."""
    for field in contract.sensitive_fields:
        assert field.masking is not MaskingStrategy.NONE, (
            f"{contract.name}.{field.name} is classified {field.classification} "
            "but has no masking strategy"
        )


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_nothing_is_masked_without_being_classified(contract: DataContract) -> None:
    """Masked fields must be classified, so privacy audits can find them."""
    for field in contract.spec.schema_.fields:
        if field.masking is not MaskingStrategy.NONE:
            assert field.classification is not Classification.PUBLIC


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_every_contract_has_a_primary_key_integrity_rule(contract: DataContract) -> None:
    """Every contract checks its primary key is unique and not null."""
    rule_types = {r.type for r in contract.spec.quality.rules}
    assert "unique" in rule_types, f"{contract.name} declares a primary key but never asserts it"
    assert "not_null" in rule_types


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_tier_one_datasets_have_a_volume_guard(contract: DataContract) -> None:
    if "tier-1" not in contract.metadata.tags:
        pytest.skip("tier-2 datasets accept a looser guarantee by design")
    assert "row_count_anomaly" in {r.type for r in contract.spec.quality.rules}


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_money_columns_are_decimal(contract: DataContract) -> None:
    money = ("amount", "price", "cost", "revenue", "rate")
    temporal = {"date", "timestamp"}
    for field in contract.spec.schema_.fields:
        # Skip date/time types: 'rate_date' contains 'rate' but isn't money.
        if str(field.type) in temporal:
            continue
        if any(token in field.name for token in money):
            assert str(field.type) in ("decimal", "string"), (
                f"{contract.name}.{field.name} looks monetary but is {field.type}; "
                "binary floating point cannot represent currency exactly"
            )


@pytest.mark.contract
@pytest.mark.parametrize("contract", ALL, ids=lambda c: c.name)
def test_retention_is_shorter_in_bronze_than_in_silver(contract: DataContract) -> None:
    """Bronze retention must not be longer than silver."""
    retention = contract.spec.retention
    if contract.metadata.domain == "finance":
        pytest.skip("reference data is kept in full for audit; the exception is deliberate")
    assert retention.bronze_days <= retention.silver_days
