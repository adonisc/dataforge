#!/usr/bin/env python3
"""Check that S3 storage and secret resolution work against an emulator or real AWS.

Kept apart from the unit tests, which need no cloud at all.

    export AWS_ENDPOINT_URL=http://localhost:4566
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
    python scripts/verify_cloud.py --bucket dataforge-lake

Exit codes: 0 all good, 1 a check failed, 2 the endpoint was unreachable.
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import UTC, datetime

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


def ok(msg: str) -> None:
    print(f"  {GREEN}PASS{RESET}  {msg}")


def fail(msg: str, hint: str = "") -> None:
    print(f"  {RED}FAIL{RESET}  {msg}")
    if hint:
        print(f"        {YELLOW}{hint}{RESET}")


def info(msg: str) -> None:
    print(f"  {DIM}{msg}{RESET}")


def check_endpoint() -> bool:
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        info("AWS_ENDPOINT_URL is not set — verifying against real AWS")
        return True
    import urllib.error
    import urllib.request

    try:
        # Any HTTP response (even 403/404) means the endpoint is up.
        urllib.request.urlopen(endpoint, timeout=5)
        ok(f"endpoint reachable: {endpoint}")
    except urllib.error.HTTPError:
        ok(f"endpoint reachable: {endpoint}")
    except Exception as exc:
        fail(
            f"endpoint unreachable: {endpoint} ({exc})",
            "Is Floci running?  docker compose -f docker-compose.floci.yml ps",
        )
        return False
    return True


def check_bucket(bucket: str) -> bool:
    try:
        import boto3
    except ImportError:
        fail("boto3 not installed", "pip install -e '.[aws,dev]'")
        return False
    try:
        boto3.client("s3").head_bucket(Bucket=bucket)
        ok(f"bucket exists and is reachable: s3://{bucket}")
        return True
    except Exception as exc:
        fail(f"bucket s3://{bucket} not usable ({type(exc).__name__})", "Run: make floci-bootstrap")
        return False


def check_parquet_roundtrip(bucket: str) -> bool:
    from dataforge.engines.parquet_lake import ParquetLakeEngine

    prefix = f"s3://{bucket}/_verify/{uuid.uuid4().hex[:8]}"
    engine = ParquetLakeEngine(prefix)
    rows = [{"id": f"R{i}", "n": i, "ts": datetime.now(UTC).isoformat()} for i in range(50)]

    try:
        engine.write_append("bronze.probe", rows, run_id="verify")
        back = engine.read_table("bronze.probe")
    except Exception as exc:
        fail(
            f"Parquet write/read on S3 failed: {type(exc).__name__}: {exc}",
            "Usually credentials or addressing style. Path-style is required for "
            "emulators and is the default in dataforge.engines.storage.",
        )
        return False

    if len(back) != len(rows):
        fail(f"wrote {len(rows)} rows, read back {len(back)}")
        return False
    ok(f"Parquet round-trip on object storage: {len(back)} rows")
    return True


def check_partition_idempotency(bucket: str) -> bool:
    """Overwriting a partition several times must leave one copy of the data on S3."""
    from dataforge.engines.parquet_lake import ParquetLakeEngine

    engine = ParquetLakeEngine(f"s3://{bucket}/_verify/{uuid.uuid4().hex[:8]}")
    rows = [{"id": f"R{i}", "d": "2026-05-01"} for i in range(20)]

    try:
        for run in ("run-a", "run-b", "run-c"):
            engine.overwrite_partition(
                "silver.probe", rows, partition_column="d", partition_value="2026-05-01", run_id=run
            )
        final = engine.read_partition("silver.probe", "d", "2026-05-01")
    except Exception as exc:
        fail(f"partition overwrite failed: {type(exc).__name__}: {exc}")
        return False

    if len(final) != len(rows):
        fail(
            f"partition overwrite is not idempotent: expected {len(rows)} rows, got {len(final)}",
            "Stale objects from a previous run were not removed.",
        )
        return False
    ok(f"partition overwrite idempotent across 3 runs: {len(final)} rows")
    return True


def check_secret(ref: str) -> bool:
    from dataforge.privacy.secrets import SecretResolutionError, clear_cache, resolve_secret

    clear_cache()
    try:
        value = resolve_secret(ref)
    except SecretResolutionError as exc:
        fail(f"could not resolve {ref}: {exc}", "Run: make floci-bootstrap")
        return False
    if not value:
        fail(f"{ref} resolved to an empty value")
        return False
    ok(f"secret resolved from the backend: {ref} ({len(value)} chars, value not printed)")
    return True


def check_masking_determinism(ref: str) -> bool:
    """Two keys loaded from the same secret must produce the same tokens, or joins break."""
    from dataforge.privacy.masking import MaskingKey, hmac_token
    from dataforge.privacy.secrets import clear_cache

    os.environ["DATAFORGE_MASKING_KEY_SECRET_REF"] = ref
    os.environ.pop("DATAFORGE_MASKING_KEY", None)

    clear_cache()
    first = MaskingKey.from_env(required=True)
    clear_cache()
    second = MaskingKey.from_env(required=True)

    if first is None or second is None:
        fail("masking key did not resolve from the secret reference")
        return False

    a, b = hmac_token("alice@example.invalid", first), hmac_token("alice@example.invalid", second)
    if a != b:
        fail("tokens are not deterministic across key resolutions")
        return False
    ok(f"masking key resolves via {ref} and tokens are deterministic ({a[:12]}…)")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.environ.get("DATAFORGE_S3_BUCKET", "dataforge-lake"))
    parser.add_argument(
        "--secret-ref",
        default=os.environ.get(
            "DATAFORGE_MASKING_KEY_SECRET_REF", "secret://dataforge/masking-key"
        ),
    )
    parser.add_argument("--skip-secrets", action="store_true")
    args = parser.parse_args()

    print(f"\nVerifying cloud integration  endpoint={os.environ.get('AWS_ENDPOINT_URL', 'AWS')}\n")

    if not check_endpoint():
        return 2

    # Count skipped checks as failures so the summary can't look like a pass.
    checks: list[bool] = [True]  # endpoint, already verified above
    bucket_ok = check_bucket(args.bucket)
    checks.append(bucket_ok)
    if bucket_ok:
        checks += [check_parquet_roundtrip(args.bucket), check_partition_idempotency(args.bucket)]
    else:
        info("skipping storage checks: no usable bucket")
        checks += [False, False]

    if not args.skip_secrets:
        secret_ok = check_secret(args.secret_ref)
        checks.append(secret_ok)
        checks.append(check_masking_determinism(args.secret_ref) if secret_ok else False)

    passed, total = sum(1 for c in checks if c), len(checks)
    print(f"\n{passed}/{total} checks passed\n")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
