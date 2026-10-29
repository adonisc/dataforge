"""Tests for storage resolution and S3 behaviour.

Heads up: moto's ``mock_aws`` patches botocore, but pyarrow's S3FileSystem uses its own
C++ client and makes real HTTP calls. So these tests run against ``moto.server`` on a
real port, the same way Floci or LocalStack are used.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
import uuid

import pytest

from dataforge.engines.storage import Location, list_matching, read_text, resolve

# --------------------------------------------------------------------- resolution ----


def test_a_bare_path_resolves_to_the_local_filesystem(tmp_path) -> None:
    location = resolve(tmp_path / "lake")
    assert location.scheme == "file"
    assert not location.is_object_store
    assert location.path.endswith("/lake")
    assert location.path.startswith("/"), "local paths are made absolute"


def test_file_scheme_resolves_the_same_way_as_a_bare_path(tmp_path) -> None:
    assert resolve(f"file://{tmp_path}/lake").path == resolve(tmp_path / "lake").path


def test_s3_uri_splits_into_bucket_and_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")

    location = resolve("s3://my-bucket/warehouse/lake")

    assert location.scheme == "s3"
    assert location.is_object_store
    assert location.path == "my-bucket/warehouse/lake", "no scheme, no leading slash"


def test_an_unknown_scheme_raises_rather_than_falling_back_to_local() -> None:
    """An unknown scheme raises instead of quietly falling back to local disk."""
    with pytest.raises(ValueError, match="unsupported storage scheme"):
        resolve("gs://some-bucket/lake")


def test_an_s3_uri_without_a_bucket_raises() -> None:
    with pytest.raises(ValueError, match="missing a bucket"):
        resolve("s3:///just-a-path")


def test_child_joins_without_doubling_or_dropping_separators() -> None:
    location = Location(fs=None, path="bucket/prefix", scheme="s3")  # type: ignore[arg-type]
    assert location.child("silver", "orders") == "bucket/prefix/silver/orders"
    assert location.child("/silver/", "/orders/") == "bucket/prefix/silver/orders"
    assert location.child() == "bucket/prefix"


def test_uri_renders_a_scheme_for_logs_and_lineage() -> None:
    assert Location(fs=None, path="b/p", scheme="s3").uri() == "s3://b/p"  # type: ignore[arg-type]
    assert Location(fs=None, path="/tmp/x", scheme="file").uri() == "file:///tmp/x"  # type: ignore[arg-type]


# ------------------------------------------------------------------ local listing ----


def test_list_matching_honours_a_glob_pattern(tmp_path) -> None:
    for day in ("2026-05-01", "2026-05-02"):
        directory = tmp_path / f"orders/dt={day}"
        directory.mkdir(parents=True)
        (directory / "part-0.jsonl").write_text('{"a":1}', encoding="utf-8")
        (directory / "_SUCCESS").write_text("", encoding="utf-8")

    location = resolve(tmp_path)
    matched = list_matching(location, "orders/dt=2026-05-01/*.jsonl")

    assert len(matched) == 1, "the other day and the _SUCCESS marker must not match"
    assert matched[0].endswith("dt=2026-05-01/part-0.jsonl")


def test_list_matching_returns_empty_for_a_missing_prefix(tmp_path) -> None:
    """A missing partition just means zero rows, not an error."""
    assert list_matching(resolve(tmp_path), "orders/dt=2099-01-01/*.jsonl") == []


def test_list_matching_finds_a_file_named_without_a_wildcard(tmp_path) -> None:
    (tmp_path / "ref").mkdir()
    (tmp_path / "ref/rates.json").write_text("[]", encoding="utf-8")
    assert len(list_matching(resolve(tmp_path), "ref/rates.json")) == 1


def test_read_text_round_trips_utf8(tmp_path) -> None:
    (tmp_path / "f.jsonl").write_text('{"city":"Ciudad de Guatemala"}', encoding="utf-8")
    location = resolve(tmp_path)
    assert "Guatemala" in read_text(location, f"{location.path}/f.jsonl")


# ------------------------------------------------------- object store (moto server) --


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def s3_endpoint() -> str:
    """A real S3 endpoint on a local port (see module docstring for why)."""
    pytest.importorskip("moto", reason="moto is required for object-store tests")
    port = _free_port()
    process = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    endpoint = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.1)
    else:  # pragma: no cover
        process.terminate()
        pytest.skip("moto server did not start")
    yield endpoint
    process.terminate()
    process.wait(timeout=10)


@pytest.fixture
def s3_bucket(s3_endpoint: str, monkeypatch: pytest.MonkeyPatch) -> str:
    boto3 = pytest.importorskip("boto3")
    monkeypatch.setenv("AWS_ENDPOINT_URL", s3_endpoint)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    name = f"df-test-{uuid.uuid4().hex[:10]}"
    boto3.client("s3").create_bucket(Bucket=name)
    return name


@pytest.mark.cloud
def test_parquet_round_trip_on_object_storage(s3_bucket: str) -> None:
    from dataforge.engines.parquet_lake import ParquetLakeEngine

    engine = ParquetLakeEngine(f"s3://{s3_bucket}/lake")
    rows = [{"id": f"R{i}", "n": i} for i in range(25)]

    written = engine.write_append("bronze.probe", rows, run_id="r1")
    back = engine.read_table("bronze.probe")

    assert written == 25
    assert len(back) == 25
    assert {r["id"] for r in back} == {r["id"] for r in rows}


@pytest.mark.cloud
def test_partition_overwrite_is_idempotent_on_object_storage(s3_bucket: str) -> None:
    """Repeated partition overwrites on S3 leave exactly one copy of the data.

    S3 has no atomic rename, so the engine lists old files, writes new ones, then
    deletes the old ones. Worth testing directly.
    """
    from dataforge.engines.parquet_lake import ParquetLakeEngine

    engine = ParquetLakeEngine(f"s3://{s3_bucket}/lake")
    rows = [{"id": f"R{i}", "d": "2026-05-01"} for i in range(10)]

    for run in ("run-1", "run-2", "run-3"):
        engine.overwrite_partition(
            "silver.probe", rows, partition_column="d", partition_value="2026-05-01", run_id=run
        )

    final = engine.read_partition("silver.probe", "d", "2026-05-01")
    assert len(final) == 10, "three overwrites must leave one copy of the data"


@pytest.mark.cloud
def test_retried_run_with_the_same_run_id_does_not_delete_its_own_output(s3_bucket: str) -> None:
    """A retry with the same run id must not delete the files it just wrote.

    Only files that existed before the write may be deleted.
    """
    from dataforge.engines.parquet_lake import ParquetLakeEngine

    engine = ParquetLakeEngine(f"s3://{s3_bucket}/lake")
    rows = [{"id": f"R{i}", "d": "2026-05-01"} for i in range(5)]

    engine.overwrite_partition(
        "silver.probe", rows, partition_column="d", partition_value="2026-05-01", run_id="same-id"
    )
    engine.overwrite_partition(
        "silver.probe", rows, partition_column="d", partition_value="2026-05-01", run_id="same-id"
    )

    assert len(engine.read_partition("silver.probe", "d", "2026-05-01")) == 5


@pytest.mark.cloud
def test_listing_and_reading_a_landing_zone_on_object_storage(s3_bucket: str) -> None:
    """Reading landing files from S3 works too, not just writing the lake."""
    boto3 = pytest.importorskip("boto3")
    client = boto3.client("s3")
    client.put_object(
        Bucket=s3_bucket, Key="orders/dt=2026-05-01/part-0.jsonl", Body=b'{"order_id":"O1"}\n'
    )
    client.put_object(Bucket=s3_bucket, Key="orders/dt=2026-05-02/part-0.jsonl", Body=b"{}\n")

    location = resolve(f"s3://{s3_bucket}")
    matched = list_matching(location, "orders/dt=2026-05-01/*.jsonl")

    assert len(matched) == 1
    assert "O1" in read_text(location, matched[0])


@pytest.mark.cloud
def test_a_missing_bucket_raises_instead_of_reporting_an_empty_table(s3_bucket: str) -> None:
    """A missing bucket raises instead of looking like an empty table.

    pyarrow returns an empty listing for both, so without this check a wrong endpoint
    would make every read silently return zero rows.
    """
    from dataforge.engines.parquet_lake import ParquetLakeEngine
    from dataforge.engines.storage import StorageUnreachableError, clear_reachability_cache

    clear_reachability_cache()
    engine = ParquetLakeEngine(f"s3://{s3_bucket}-does-not-exist/lake")

    with pytest.raises(StorageUnreachableError, match="does not exist"):
        engine.read_table("silver.orders")


@pytest.mark.cloud
def test_an_unwritten_partition_in_a_real_bucket_is_still_just_zero_rows(s3_bucket: str) -> None:
    """An existing bucket with nothing in it yet is fine, so first loads work."""
    from dataforge.engines.parquet_lake import ParquetLakeEngine
    from dataforge.engines.storage import clear_reachability_cache

    clear_reachability_cache()
    engine = ParquetLakeEngine(f"s3://{s3_bucket}/lake")
    assert engine.read_partition("silver.orders", "order_date", "2099-01-01") == []
