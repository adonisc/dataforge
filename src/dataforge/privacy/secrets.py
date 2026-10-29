"""Resolve secret references like ``secret://platform/fx-api/readonly``.

    secret://<name>   AWS Secrets Manager (prod)
    ssm://<path>      SSM Parameter Store
    env://<VAR>       Environment variable (local, CI)

Values are cached per process, so a secret can't change mid-run. boto3 is optional and
imported lazily. Set ``AWS_ENDPOINT_URL`` to use a local emulator.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse


class SecretResolutionError(RuntimeError):
    """A secret could not be resolved. Always fatal, with no fallback."""


def _client(service: str) -> Any:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - exercised by the absence of boto3
        raise SecretResolutionError(
            "resolving a secret:// or ssm:// reference requires boto3. "
            "Install it with: pip install 'dataforge[aws]'"
        ) from exc

    # botocore reads AWS_ENDPOINT_URL itself.
    return boto3.client(
        service,
        region_name=os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1",
    )


@lru_cache(maxsize=64)
def _fetch_secretsmanager(name: str) -> str:
    client = _client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=name)
    except Exception as exc:
        raise SecretResolutionError(
            f"could not read secret {name!r} from Secrets Manager "
            f"(endpoint={os.environ.get('AWS_ENDPOINT_URL', 'aws')}): {exc}"
        ) from exc
    if "SecretString" in response:
        return str(response["SecretString"])
    raise SecretResolutionError(f"secret {name!r} holds binary data; expected a string")


@lru_cache(maxsize=64)
def _fetch_ssm(path: str) -> str:
    client = _client("ssm")
    try:
        response = client.get_parameter(Name=path, WithDecryption=True)
    except Exception as exc:
        raise SecretResolutionError(f"could not read SSM parameter {path!r}: {exc}") from exc
    return str(response["Parameter"]["Value"])


def resolve_secret(ref: str) -> str:
    """Resolve one secret reference. Unknown schemes raise instead of being used as literals."""
    parsed = urlparse(ref)
    target = f"{parsed.netloc}{parsed.path}".strip("/")

    match parsed.scheme:
        case "secret" | "secretsmanager":
            return _fetch_secretsmanager(target)
        case "ssm":
            return _fetch_ssm(f"/{target}")
        case "env":
            value = os.environ.get(target)
            if value is None:
                raise SecretResolutionError(f"environment variable {target!r} is not set")
            return value
        case _:
            raise SecretResolutionError(
                f"unsupported secret scheme {parsed.scheme!r} in {ref!r}. "
                "Supported: secret:// (Secrets Manager), ssm://, env://"
            )


def resolve_optional(ref: str | None) -> str | None:
    """Like ``resolve_secret``, but returns None when no reference is given."""
    return resolve_secret(ref) if ref else None


def clear_cache() -> None:
    """Clear cached secrets (tests, or after a rotation)."""
    _fetch_secretsmanager.cache_clear()
    _fetch_ssm.cache_clear()
