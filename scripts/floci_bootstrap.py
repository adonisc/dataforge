#!/usr/bin/env python3
"""Set up an emulated AWS account for the pipeline.

Creates the S3 buckets, secrets and (optionally) a Glue database, then uploads the
landing files. Safe to re-run; existing resources are left alone.

    export AWS_ENDPOINT_URL=http://localhost:4566
    export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
    python scripts/floci_bootstrap.py

    # or just:
    make floci-bootstrap

Won't run against real AWS unless you pass --i-know-this-is-real.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from pathlib import Path

LAKE_BUCKET = os.environ.get("DATAFORGE_S3_BUCKET", "dataforge-lake")
LANDING_BUCKET = os.environ.get("DATAFORGE_LANDING_BUCKET", "dataforge-landing")
MASKING_SECRET = "dataforge/masking-key"
FX_SECRET = "platform/fx-api/readonly"
GLUE_DATABASE = "dataforge"

GREEN, YELLOW, RED, DIM, RESET = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[0m"


def created(what: str) -> None:
    print(f"  {GREEN}created{RESET}  {what}")


def existing(what: str) -> None:
    print(f"  {DIM}exists {RESET}  {what}")


def skipped(what: str, why: str) -> None:
    print(f"  {YELLOW}skip   {RESET}  {what} {DIM}({why}){RESET}")


def ensure_bucket(s3, name: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=name)
        existing(f"s3://{name}")
        return
    except ClientError:
        pass
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    kwargs = {"Bucket": name}
    if region != "us-east-1":
        # S3 quirk: every region except us-east-1 needs a LocationConstraint.
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**kwargs)
    created(f"s3://{name}")


def ensure_secret(sm, name: str, value: str, *, description: str) -> None:
    from botocore.exceptions import ClientError

    try:
        sm.describe_secret(SecretId=name)
        existing(f"secret://{name}")
        return
    except ClientError:
        pass
    sm.create_secret(Name=name, SecretString=value, Description=description)
    created(f"secret://{name}")


def ensure_glue_database(glue, name: str) -> None:
    from botocore.exceptions import ClientError

    try:
        glue.get_database(Name=name)
        existing(f"glue database {name}")
        return
    except ClientError:
        pass
    try:
        glue.create_database(DatabaseInput={"Name": name, "Description": "dataforge lakehouse"})
        created(f"glue database {name}")
    except Exception as exc:  # pragma: no cover - emulator coverage varies
        skipped(f"glue database {name}", f"{type(exc).__name__}")


def upload_landing(s3, landing_dir: Path, bucket: str) -> None:
    files = sorted(landing_dir.rglob("*.jsonl"))
    if not files:
        skipped("landing upload", f"no files under {landing_dir} — run `make data` first")
        return
    for path in files:
        s3.upload_file(str(path), bucket, str(path.relative_to(landing_dir)))
    created(f"{len(files)} landing file(s) -> s3://{bucket}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-dir", default="data/landing")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-glue", action="store_true")
    parser.add_argument(
        "--i-know-this-is-real",
        action="store_true",
        help="allow running without AWS_ENDPOINT_URL set, i.e. against real AWS",
    )
    args = parser.parse_args()

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint and not args.i_know_this_is_real:
        print(
            f"{RED}refusing to run: AWS_ENDPOINT_URL is not set, so this would create "
            f"buckets and secrets in a real AWS account.{RESET}\n"
            "Start Floci and export it:\n"
            "    docker compose -f docker-compose.floci.yml up -d floci\n"
            "    export AWS_ENDPOINT_URL=http://localhost:4566\n"
            "Or pass --i-know-this-is-real if that is genuinely what you want.",
            file=sys.stderr,
        )
        return 2

    try:
        import boto3
    except ImportError:
        print(f"{RED}boto3 is not installed:{RESET} pip install -e '.[aws,dev]'", file=sys.stderr)
        return 2

    print(f"\nBootstrapping  endpoint={endpoint or 'AWS (real)'}\n")

    s3 = boto3.client("s3")
    ensure_bucket(s3, LAKE_BUCKET)
    ensure_bucket(s3, LANDING_BUCKET)

    sm = boto3.client("secretsmanager")
    # Random key; never hard-code one.
    ensure_secret(
        sm,
        MASKING_SECRET,
        secrets.token_urlsafe(32),
        description="HMAC pepper for PII tokenisation (local emulator only)",
    )
    ensure_secret(
        sm,
        FX_SECRET,
        "local-emulator-fx-token",
        description="Treasury FX API bearer token (local emulator only)",
    )

    if not args.skip_glue:
        ensure_glue_database(boto3.client("glue"), GLUE_DATABASE)

    if not args.skip_upload:
        upload_landing(s3, Path(args.landing_dir), LANDING_BUCKET)

    print(
        f"\n{GREEN}ready{RESET}. Next:\n"
        "    make floci-verify     # prove the storage and secret paths work\n"
        "    make floci-demo       # run the pipeline against emulated AWS\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
