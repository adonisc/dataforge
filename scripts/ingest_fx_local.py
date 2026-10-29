#!/usr/bin/env python3
"""Ingest fx_rates locally against the stub API.

Starts the stub, points the source URI at it via an env override, and runs the normal
pipeline for each date. Only the address differs from production.

    python -m scripts.ingest_fx_local --date 2026-05-01 --days 3
"""

from __future__ import annotations

import argparse
import os
from datetime import date, timedelta
from pathlib import Path

from scripts.fx_api_stub import serve

from dataforge.contracts.loader import ContractRegistry
from dataforge.pipeline import PipelineConfig, QualityGateError, run_ingestion
from dataforge.privacy.masking import MaskingKey

STUB_TOKEN = "local-stub-token-not-a-secret"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--contracts", default="contracts")
    parser.add_argument("--landing-root", default="data/landing")
    parser.add_argument("--lake-root", default="data/lake")
    parser.add_argument("--state-path", default="data/_control/state.json")
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()

    contract = ContractRegistry(args.contracts).get("fx_rates")
    landing = Path(args.landing_root)
    start = date.fromisoformat(args.date)

    with serve(landing, port=args.port, token=STUB_TOKEN) as base_url:
        # Point the contract at the stub.
        os.environ["DATAFORGE_SOURCE_URI__FX_RATES"] = (
            f"{base_url}/v1/fx/rates?date={{logical_date}}"
        )
        config = PipelineConfig(
            env="dev",
            lake_root=args.lake_root,
            landing_root=args.landing_root,
            state_path=Path(args.state_path),
            lineage_path=Path("data/_control/lineage.jsonl"),
            masking_key=MaskingKey.from_env(required=False),
            api_token=STUB_TOKEN,
            code_version="local",
        )

        failures = 0
        for offset in range(args.days):
            logical = start + timedelta(days=offset)
            try:
                result = run_ingestion(contract, config, logical_date=logical)
                print(
                    f"  fx_rates {logical}  {result.manifest.status}  rows={result.manifest.rows_published}"
                )
            except QualityGateError as exc:
                print(f"  fx_rates {logical}  BLOCKED  {exc}")
                failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
