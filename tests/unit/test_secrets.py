"""Tests for secret resolution and masking key precedence.

moto's ``mock_aws`` works here because this code uses boto3. Storage tests need a real
endpoint instead (see ``test_storage.py``).
"""

from __future__ import annotations

import pytest

from dataforge.privacy.masking import MaskingKey, MaskingKeyMissingError, hmac_token
from dataforge.privacy.secrets import (
    SecretResolutionError,
    clear_cache,
    resolve_optional,
    resolve_secret,
)


@pytest.fixture(autouse=True)
def _isolate_cache():
    """Clear the secret cache so tests don't affect each other."""
    clear_cache()
    yield
    clear_cache()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch):
    for var in (
        "DATAFORGE_MASKING_KEY",
        "DATAFORGE_MASKING_KEY_SECRET_REF",
        "DATAFORGE_MASKING_KEY_VERSION",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


# -------------------------------------------------------------------- env:// ---------


def test_env_scheme_reads_an_environment_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TOKEN", "abc123")
    assert resolve_secret("env://MY_TOKEN") == "abc123"


def test_a_missing_environment_variable_is_an_error_not_an_empty_string() -> None:
    with pytest.raises(SecretResolutionError, match="not set"):
        resolve_secret("env://DEFINITELY_NOT_SET_12345")


def test_an_unknown_scheme_is_rejected_rather_than_treated_as_a_literal() -> None:
    """An unknown scheme raises instead of returning the reference as the value."""
    with pytest.raises(SecretResolutionError, match="unsupported secret scheme"):
        resolve_secret("vault://platform/token")


def test_resolve_optional_treats_absence_as_absence_and_failure_as_failure() -> None:
    assert resolve_optional(None) is None
    assert resolve_optional("") is None
    with pytest.raises(SecretResolutionError):
        resolve_optional("env://DEFINITELY_NOT_SET_12345")


# --------------------------------------------------------- Secrets Manager ----------


@pytest.fixture
def secretsmanager():
    moto = pytest.importorskip("moto", reason="moto is required for Secrets Manager tests")
    boto3 = pytest.importorskip("boto3")
    with moto.mock_aws():
        yield boto3.client("secretsmanager", region_name="us-east-1")


def test_secret_scheme_reads_from_secrets_manager(secretsmanager) -> None:
    secretsmanager.create_secret(Name="dataforge/masking-key", SecretString="pepper-value")
    assert resolve_secret("secret://dataforge/masking-key") == "pepper-value"


def test_a_missing_secret_names_the_endpoint_in_the_error(secretsmanager) -> None:  # noqa: ARG001
    """The error names the endpoint, since "wrong endpoint" is a common cause."""
    with pytest.raises(SecretResolutionError, match="could not read secret"):
        resolve_secret("secret://nothing/here")


def test_resolution_is_cached_so_a_batch_cannot_span_two_key_generations(
    secretsmanager,
) -> None:
    """Secrets are cached so a key can't change mid-run."""
    secretsmanager.create_secret(Name="rotating", SecretString="first")
    assert resolve_secret("secret://rotating") == "first"

    secretsmanager.put_secret_value(SecretId="rotating", SecretString="second")
    assert resolve_secret("secret://rotating") == "first", "cached within the process"

    clear_cache()
    assert resolve_secret("secret://rotating") == "second", "and refreshable deliberately"


# ----------------------------------------------------------- MaskingKey -------------


def test_the_secret_reference_wins_over_a_literal_env_var(
    secretsmanager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The secret reference wins over a literal env var."""
    secretsmanager.create_secret(Name="dataforge/masking-key", SecretString="managed-key")
    monkeypatch.setenv("DATAFORGE_MASKING_KEY", "stale-local-key")
    monkeypatch.setenv("DATAFORGE_MASKING_KEY_SECRET_REF", "secret://dataforge/masking-key")

    key = MaskingKey.from_env(required=True)

    assert key is not None
    assert hmac_token("a@b.com", key) == hmac_token("a@b.com", MaskingKey(secret=b"managed-key"))


def test_a_backend_failure_does_not_silently_fall_back_to_the_env_var(
    secretsmanager,  # noqa: ARG001 - requested to activate mock_aws, not for its value
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the secret backend fails, don't fall back to the env var."""
    monkeypatch.setenv("DATAFORGE_MASKING_KEY", "would-be-fallback")
    monkeypatch.setenv("DATAFORGE_MASKING_KEY_SECRET_REF", "secret://not/seeded")

    with pytest.raises(MaskingKeyMissingError, match="could not be resolved"):
        MaskingKey.from_env(required=True)


def test_the_literal_env_var_still_works_for_local_development(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAFORGE_MASKING_KEY", "local-dev")
    monkeypatch.setenv("DATAFORGE_MASKING_KEY_VERSION", "v7")

    key = MaskingKey.from_env(required=True)

    assert key is not None
    assert key.version == "v7"
    assert hmac_token("a@b.com", key).startswith("v7:")


def test_no_key_at_all_is_a_hard_failure_naming_both_options() -> None:
    with pytest.raises(MaskingKeyMissingError) as exc:
        MaskingKey.from_env(required=True)
    message = str(exc.value)
    assert "DATAFORGE_MASKING_KEY_SECRET_REF" in message
    assert "DATAFORGE_MASKING_KEY" in message


def test_no_key_is_tolerated_when_the_contract_declares_no_pii() -> None:
    assert MaskingKey.from_env(required=False) is None
