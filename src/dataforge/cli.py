"""dataforge command line: run pipelines and check contracts.

The same commands run locally, in CI and inside Airflow tasks.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

from dataforge.contracts import ContractError, ContractRegistry, check_compatibility, enforce
from dataforge.contracts.loader import load_contract
from dataforge.pipeline import PipelineConfig, QualityGateError, run_ingestion
from dataforge.privacy.masking import MaskingKey
from dataforge.quality.engine import ValidationEngine

_DEFAULT_CONTRACTS = Path(os.environ.get("DATAFORGE_CONTRACTS", "contracts"))
# Roots are URIs: a bare path, file:// or s3://(any S3-compatible endpoint).
_DEFAULT_LAKE = os.environ.get("DATAFORGE_LAKE_ROOT", "data/lake")
_DEFAULT_LANDING = os.environ.get("DATAFORGE_LANDING_ROOT", "data/landing")


def _config(args: argparse.Namespace) -> PipelineConfig:
    return PipelineConfig(
        env=args.env,
        lake_root=args.lake_root,
        landing_root=args.landing_root,
        state_path=Path(args.state_path),
        lineage_path=Path(args.lineage_path) if args.lineage_path else None,
        masking_key=MaskingKey.from_env(required=False),
        api_token=os.environ.get("DATAFORGE_API_TOKEN"),
        code_version=os.environ.get("GIT_SHA", "dev"),
        dry_run=args.dry_run,
    )


def cmd_ingest(args: argparse.Namespace) -> int:
    registry = ContractRegistry(args.contracts)
    contract = registry.get(args.contract)
    logical_date = (
        date.fromisoformat(args.logical_date) if args.logical_date else datetime.now().date()
    )
    try:
        result = run_ingestion(contract, _config(args), logical_date=logical_date, force=args.force)
    except QualityGateError as exc:
        print(json.dumps({"status": "blocked_by_quality", "detail": str(exc)}, indent=2))
        # 2 = bad data, 1 = crash, so the orchestrator can treat them differently.
        return 2
    print(json.dumps(result.manifest.to_dict(), indent=2, default=str))
    return 0


def cmd_validate_contracts(args: argparse.Namespace) -> int:
    """CI check: every contract parses and every rule type exists."""
    registry = ContractRegistry(args.contracts)
    failures: list[str] = []
    for name in registry.names():
        try:
            contract = registry.get(name)
            ValidationEngine(contract)  # compiles the rules; raises on unknown types
            print(
                f"  ok       {name:<24} v{contract.metadata.version}  owner={contract.metadata.owner}"
            )
        except (ContractError, KeyError, ValueError) as exc:
            failures.append(f"{name}: {exc}")
            print(f"  FAILED   {name}: {exc}")
    if failures:
        print(f"\n{len(failures)} contract(s) invalid", file=sys.stderr)
        return 1
    print(f"\n{len(registry.names())} contract(s) valid")
    return 0


def cmd_check_compatibility(args: argparse.Namespace) -> int:
    """CI check: compare contracts with the base branch. Breaking changes need a major bump."""
    base = ContractRegistry(args.base)
    head = ContractRegistry(args.contracts)
    violations: list[str] = []

    for name in head.names():
        if name not in base.names():
            print(f"  new      {name} (no baseline to compare)")
            continue
        report = check_compatibility(base.get(name), head.get(name))
        print(report.summary())
        violations.extend(enforce(report))

    for name in base.names():
        if name not in head.names():
            violations.append(f"{name}: contract deleted — deprecate it before removal")

    if violations:
        print("\nCOMPATIBILITY GATE FAILED", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1
    print("\ncompatibility gate passed")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    contract = (
        load_contract(args.path)
        if args.path
        else ContractRegistry(args.contracts).get(args.contract)
    )
    print(json.dumps(contract.model_dump(mode="json", by_alias=True), indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dataforge", description=__doc__)
    parser.add_argument("--contracts", default=str(_DEFAULT_CONTRACTS))
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("ingest", help="run the ingestion pipeline for one contract")
    run.add_argument("contract")
    run.add_argument("--logical-date", help="YYYY-MM-DD; defaults to today")
    run.add_argument("--env", default=os.environ.get("DATAFORGE_ENV", "dev"))
    run.add_argument(
        "--lake-root", default=_DEFAULT_LAKE, help="path, file:// or s3://bucket/prefix"
    )
    run.add_argument("--landing-root", default=_DEFAULT_LANDING, help="path, file:// or s3://...")
    run.add_argument("--state-path", default="data/_control/state.json")
    run.add_argument("--lineage-path", default="data/_control/lineage.jsonl")
    run.add_argument("--dry-run", action="store_true", help="validate without writing")
    run.add_argument(
        "--force", action="store_true", help="reprocess even if identical inputs already succeeded"
    )
    run.set_defaults(func=cmd_ingest)

    validate = sub.add_parser("validate-contracts", help="parse and compile every contract")
    validate.set_defaults(func=cmd_validate_contracts)

    compat = sub.add_parser("check-compatibility", help="compare contracts against a baseline")
    compat.add_argument("--base", required=True, help="path to the baseline contracts directory")
    compat.set_defaults(func=cmd_check_compatibility)

    show = sub.add_parser("show", help="print a resolved contract as JSON")
    show.add_argument("contract", nargs="?")
    show.add_argument("--path")
    show.set_defaults(func=cmd_show)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
