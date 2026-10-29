"""Masks sensitive fields before they reach silver.

Raw values then exist only in bronze, which has short retention and tight access.
Uses keyed HMAC, not a plain hash, because plain hashes of emails are easy to reverse
with a dictionary. Tokens are deterministic so joins still work, and they're prefixed
with the key version so rotations are traceable.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass
from typing import Any

from dataforge.contracts.models import DataContract, MaskingStrategy

Record = dict[str, Any]

_DEFAULT_KEY_ENV = "DATAFORGE_MASKING_KEY"
_DEFAULT_KEY_VERSION_ENV = "DATAFORGE_MASKING_KEY_VERSION"
_DEFAULT_KEY_SECRET_REF_ENV = "DATAFORGE_MASKING_KEY_SECRET_REF"


class MaskingKeyMissingError(RuntimeError):
    """Masked fields are declared but no key is set. Always fatal, with no fallback."""


@dataclass(frozen=True, slots=True)
class MaskingKey:
    secret: bytes
    version: str = "v1"

    @classmethod
    def from_env(cls, *, required: bool) -> MaskingKey | None:
        """Load the masking key.

        Checks ``DATAFORGE_MASKING_KEY_SECRET_REF`` (a secret reference, used in prod)
        first, then ``DATAFORGE_MASKING_KEY`` (a literal, local/CI only). The reference
        wins so a stale local env var can't override the real key.
        """
        from dataforge.privacy.secrets import SecretResolutionError, resolve_optional

        version = os.environ.get(_DEFAULT_KEY_VERSION_ENV, "v1")

        if ref := os.environ.get(_DEFAULT_KEY_SECRET_REF_ENV):
            try:
                resolved = resolve_optional(ref)
            except SecretResolutionError as exc:
                # Don't fall back to the env var if the secret backend fails.
                raise MaskingKeyMissingError(
                    f"{_DEFAULT_KEY_SECRET_REF_ENV}={ref!r} could not be resolved: {exc}"
                ) from exc
            if resolved:
                return cls(secret=resolved.encode("utf-8"), version=version)

        raw = os.environ.get(_DEFAULT_KEY_ENV)
        if not raw:
            if required:
                raise MaskingKeyMissingError(
                    f"no masking key available, but the contract declares masked fields. "
                    f"Set {_DEFAULT_KEY_SECRET_REF_ENV} to a secret reference "
                    f"(e.g. secret://dataforge/masking-key) or, for local development only, "
                    f"{_DEFAULT_KEY_ENV} to a literal value."
                )
            return None
        return cls(secret=raw.encode("utf-8"), version=version)


def hmac_token(value: str, key: MaskingKey) -> str:
    digest = hmac.new(key.secret, value.strip().lower().encode("utf-8"), hashlib.sha256).hexdigest()
    # 128 bits is plenty to avoid collisions and keeps tokens short.
    return f"{key.version}:{digest[:32]}"


def apply_strategy(value: Any, strategy: MaskingStrategy, key: MaskingKey | None) -> Any:
    if value is None or strategy is MaskingStrategy.NONE:
        return value
    text = str(value)

    if strategy is MaskingStrategy.REDACT:
        return "[REDACTED]"
    if strategy is MaskingStrategy.LAST4:
        return f"****{text[-4:]}" if len(text) >= 4 else "****"
    if strategy is MaskingStrategy.EMAIL_DOMAIN_ONLY:
        return text.rsplit("@", 1)[-1].lower() if "@" in text else None
    if strategy is MaskingStrategy.HASH_HMAC:
        if key is None:
            raise MaskingKeyMissingError("hash_hmac requested without a configured key")
        return hmac_token(text, key)
    raise ValueError(f"unsupported masking strategy: {strategy}")


class Masker:
    """Applies a contract's masking policy to records.

    Masked fields are renamed (``_token``, ``_domain`` or ``_masked``) and the original
    is dropped, so nobody mistakes a hash for a real email.
    """

    def __init__(self, contract: DataContract, key: MaskingKey | None = None) -> None:
        self.policy = contract.masked_fields()
        self.key = key
        if self.policy and any(s is MaskingStrategy.HASH_HMAC for s in self.policy.values()):
            self.key = key or MaskingKey.from_env(required=True)

    @staticmethod
    def output_name(field: str, strategy: MaskingStrategy) -> str:
        if strategy is MaskingStrategy.HASH_HMAC:
            return f"{field}_token"
        if strategy is MaskingStrategy.EMAIL_DOMAIN_ONLY:
            return f"{field}_domain"
        return f"{field}_masked"

    def mask_record(self, record: Record) -> Record:
        if not self.policy:
            return dict(record)
        out = dict(record)
        for fieldname, strategy in self.policy.items():
            if fieldname not in out:
                continue
            value = out.pop(fieldname)
            out[self.output_name(fieldname, strategy)] = apply_strategy(value, strategy, self.key)
        return out

    def mask_batch(self, records: list[Record]) -> list[Record]:
        return [self.mask_record(r) for r in records]
