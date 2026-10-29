"""Tests for masking policy and source readers."""

from __future__ import annotations

import io
import json
from datetime import date

import pytest

from dataforge.contracts.models import MaskingStrategy
from dataforge.ingest.sources import RestApiSource, render_uri
from dataforge.ingest.state import JsonStateStore, fingerprint_inputs
from dataforge.privacy.masking import Masker, MaskingKey, MaskingKeyMissingError, apply_strategy
from tests.conftest import build_contract

_PII_FIELDS = [
    {"name": "widget_id", "type": "string", "required": True},
    {"name": "status", "type": "string", "required": True},
    {"name": "updated_at", "type": "timestamp", "required": True},
    {"name": "event_date", "type": "date", "required": True},
    {"name": "email", "type": "string", "classification": "pii", "masking": "hash_hmac"},
    {"name": "phone", "type": "string", "classification": "pii", "masking": "last4"},
    {"name": "full_name", "type": "string", "classification": "pii", "masking": "redact"},
    {
        "name": "work_email",
        "type": "string",
        "classification": "pii",
        "masking": "email_domain_only",
    },
]


def _pii_contract():
    return build_contract(
        spec={
            "schema": {
                "primary_key": ["widget_id"],
                "dedupe_order_by": ["updated_at"],
                "fields": _PII_FIELDS,
            }
        }
    )


def test_tokens_are_deterministic_so_joins_survive_masking(masking_key: MaskingKey) -> None:
    """Unlike redaction, HMAC tokens can still be joined on."""
    a = apply_strategy("Alice@Example.com", MaskingStrategy.HASH_HMAC, masking_key)
    b = apply_strategy("alice@example.com ", MaskingStrategy.HASH_HMAC, masking_key)
    assert a == b, "case and whitespace are normalised before hashing"


def test_different_keys_produce_different_tokens() -> None:
    """Tokens from different keys don't match, which is why tokens carry a key version."""
    v1 = apply_strategy("a@b.com", MaskingStrategy.HASH_HMAC, MaskingKey(b"key-one", "v1"))
    v2 = apply_strategy("a@b.com", MaskingStrategy.HASH_HMAC, MaskingKey(b"key-two", "v2"))
    assert v1 != v2
    assert v1.startswith("v1:") and v2.startswith("v2:")


def test_token_does_not_contain_the_plaintext(masking_key: MaskingKey) -> None:
    token = apply_strategy("alice@example.com", MaskingStrategy.HASH_HMAC, masking_key)
    assert "alice" not in token and "example.com" not in token


def test_missing_key_is_a_hard_failure_not_a_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no key, masking fails instead of passing PII through."""
    monkeypatch.delenv("DATAFORGE_MASKING_KEY", raising=False)
    with pytest.raises(MaskingKeyMissingError):
        Masker(_pii_contract())


def test_masked_columns_are_renamed_so_nobody_mistakes_a_token_for_an_email(
    masking_key: MaskingKey,
) -> None:
    masker = Masker(_pii_contract(), key=masking_key)
    out = masker.mask_record(
        {
            "widget_id": "W1",
            "email": "a@b.com",
            "phone": "+15551234567",
            "full_name": "Real Person",
            "work_email": "someone@acme.co.uk",
        }
    )

    assert "email" not in out and "full_name" not in out
    assert out["email_token"].startswith("v1:")
    assert out["phone_masked"] == "****4567"
    assert out["full_name_masked"] == "[REDACTED]"
    assert out["work_email_domain"] == "acme.co.uk", "analytical value kept, identity dropped"


def test_no_declared_pii_means_no_key_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAFORGE_MASKING_KEY", raising=False)
    masker = Masker(build_contract())
    assert masker.mask_record({"widget_id": "W1"}) == {"widget_id": "W1"}


# ------------------------------------------------------------------- sources ---------


def test_uri_rendering_depends_only_on_the_logical_date() -> None:
    """URIs depend only on the logical date, so backfills work."""
    rendered = render_uri(
        "orders/y={year}/m={month}/d={day}/{logical_date_nodash}", date(2026, 3, 7)
    )
    assert rendered == "orders/y=2026/m=03/d=07/20260307"


def test_rest_source_paginates_and_stops_on_an_empty_page() -> None:
    """Uses a fake opener so pagination is tested without a network."""
    pages = {
        1: {"data": [{"currency_code": "EUR", "rate_date": "2026-03-07"}] * 2},
        2: {"data": [{"currency_code": "GBP", "rate_date": "2026-03-07"}]},
        3: {"data": []},
    }
    calls: list[str] = []

    class _Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_opener(request, timeout=30):  # noqa: ARG001
        url = request.full_url
        calls.append(url)
        page = int(url.rsplit("page=", 1)[1])
        return _Response(json.dumps(pages.get(page, {"data": []})).encode())

    contract = build_contract(
        spec={
            "source": {
                "type": "rest_api",
                "mode": "incremental",
                "uri": "https://api.example/v1/fx?date={logical_date}",
                "watermark_field": "rate_date",
                "options": {"records_path": "data", "page_size": 2, "max_pages": 10},
            }
        }
    )

    batch = RestApiSource(contract, token="t", opener=fake_opener).read(
        logical_date=date(2026, 3, 7), watermark=None
    )

    assert len(batch.records) == 3
    # Two requests, not three: a short page means it's the last one.
    assert batch.provenance["pages_fetched"] == 2
    assert "date=2026-03-07" in calls[0]


def test_rest_source_appends_the_watermark_for_incremental_reads() -> None:
    captured: list[str] = []

    class _Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_opener(request, timeout=30):  # noqa: ARG001
        captured.append(request.full_url)
        return _Response(b'{"data": []}')

    contract = build_contract(
        spec={
            "source": {
                "type": "rest_api",
                "mode": "incremental",
                "uri": "https://api.example/v1/items",
                "watermark_field": "updated_at",
                "options": {"records_path": "data", "since_param": "updated_since"},
            }
        }
    )

    RestApiSource(contract, opener=fake_opener).read(
        logical_date=date(2026, 3, 7), watermark="2026-03-06T00:00:00+00:00"
    )

    assert "updated_since=2026-03-06" in captured[0]


# --------------------------------------------------------------------- state ---------


def test_watermark_is_monotonic_so_a_backfill_cannot_rewind_it(tmp_path) -> None:
    """A backfill of an old date must not move the watermark backwards."""
    store = JsonStateStore(tmp_path / "state.json")
    store.set_watermark("widgets", "2026-06-01T00:00:00+00:00")
    store.set_watermark("widgets", "2026-01-01T00:00:00+00:00")
    assert store.get_watermark("widgets") == "2026-06-01T00:00:00+00:00"


def test_fingerprint_is_stable_under_uri_ordering_but_changes_with_content() -> None:
    a = fingerprint_inputs(["s3://b/2", "s3://b/1"], 100, "wm")
    b = fingerprint_inputs(["s3://b/1", "s3://b/2"], 100, "wm")
    c = fingerprint_inputs(["s3://b/1", "s3://b/2"], 101, "wm")
    assert a == b
    assert a != c, "a corrected file must be allowed to reprocess"
