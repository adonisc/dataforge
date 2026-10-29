"""Load and discover contract files.

Contracts live in git, not a database, so changes get reviewed, ship with the code,
and roll back with ``git revert``.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

from dataforge.contracts.models import DataContract


class ContractError(Exception):
    """Raised when a contract file is missing, malformed, or fails validation."""


def load_contract(path: str | Path) -> DataContract:
    path = Path(path)
    if not path.exists():
        raise ContractError(f"contract not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:  # pragma: no cover - defensive
        raise ContractError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ContractError(f"{path}: contract must be a mapping")
    try:
        return DataContract.model_validate(raw)
    except Exception as exc:
        # Include the file name; pydantic errors don't.
        raise ContractError(f"{path}: {exc}") from exc


class ContractRegistry:
    """Discovers and caches contracts for a directory tree."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @lru_cache(maxsize=1)  # noqa: B019 - registry instances are process-scoped
    def _index(self) -> dict[str, Path]:
        index: dict[str, Path] = {}
        for path in sorted(self.root.rglob("*.y*ml")):
            contract = load_contract(path)
            if contract.name in index:
                raise ContractError(
                    f"duplicate contract name '{contract.name}' in {path} and {index[contract.name]}"
                )
            index[contract.name] = path
        return index

    def names(self) -> list[str]:
        return sorted(self._index())

    def get(self, name: str) -> DataContract:
        index = self._index()
        if name not in index:
            raise ContractError(f"unknown contract '{name}'. Known: {', '.join(index)}")
        return load_contract(index[name])

    def all(self) -> list[DataContract]:
        return [load_contract(p) for _, p in sorted(self._index().items())]
