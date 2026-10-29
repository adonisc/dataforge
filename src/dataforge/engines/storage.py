"""Turns a lake URI into a pyarrow filesystem, so one engine works locally and on S3.

    data/lake, file:///abs/path  -> LocalFileSystem
    s3://bucket/lake             -> S3FileSystem (AWS or any S3-compatible API)

Uses pyarrow.fs rather than s3fs/boto3, since pyarrow is already a dependency.
"""

from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.fs as pafs


@dataclass(frozen=True, slots=True)
class Location:
    """A filesystem plus a root path in the form it expects.

    Local paths are absolute; S3 paths are ``bucket/prefix`` with no scheme or leading
    slash. Use ``child()`` to join paths; the slash rules differ between the two.
    """

    fs: pafs.FileSystem
    path: str
    scheme: str

    @property
    def is_object_store(self) -> bool:
        """True if there's no atomic directory rename (see ``overwrite_partition``)."""
        return self.scheme == "s3"

    def child(self, *parts: str) -> str:
        suffix = "/".join(p.strip("/") for p in parts if p)
        return f"{self.path.rstrip('/')}/{suffix}" if suffix else self.path

    def uri(self, path: str | None = None) -> str:
        """Full URI for logs, manifests and lineage."""
        target = path or self.path
        return f"s3://{target}" if self.scheme == "s3" else f"file://{target}"


def _s3_options() -> dict[str, object]:
    """Build S3FileSystem options from the standard AWS env vars.

    If no keys are set, pyarrow uses the default AWS credential chain (what prod uses).
    """
    options: dict[str, object] = {
        "region": os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1",
        # Emulators and MinIO only serve path-style URLs.
        "force_virtual_addressing": False,
    }

    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlparse(endpoint if "//" in endpoint else f"//{endpoint}")
        options["endpoint_override"] = parsed.netloc or parsed.path
        # Take http/https from the endpoint so there's only one setting.
        options["scheme"] = parsed.scheme or "http"

    if (key := os.environ.get("AWS_ACCESS_KEY_ID")) and (
        secret := os.environ.get("AWS_SECRET_ACCESS_KEY")
    ):
        options["access_key"] = key
        options["secret_key"] = secret
        if token := os.environ.get("AWS_SESSION_TOKEN"):
            options["session_token"] = token

    return options


def resolve(uri: str | Path) -> Location:
    """Resolve a bare path, ``file://`` or ``s3://`` URI to a :class:`Location`.

    Unknown schemes raise instead of falling back to local, so a typo can't quietly
    write to the container's disk.
    """
    text = str(uri)
    parsed = urlparse(text)

    if parsed.scheme in ("", "file"):
        local_path = parsed.path if parsed.scheme == "file" else text
        return Location(
            fs=pafs.LocalFileSystem(), path=str(Path(local_path).absolute()), scheme="file"
        )

    if parsed.scheme in ("s3", "s3a"):
        bucket_and_prefix = f"{parsed.netloc}{parsed.path}".rstrip("/")
        if not parsed.netloc:
            raise ValueError(f"s3 URI is missing a bucket: {text!r}")
        return Location(
            fs=pafs.S3FileSystem(**_s3_options()),
            path=bucket_and_prefix,
            scheme="s3",
        )

    raise ValueError(
        f"unsupported storage scheme {parsed.scheme!r} in {text!r}. "
        "Supported: a local path, file://, s3:// (including S3-compatible endpoints "
        "via AWS_ENDPOINT_URL)."
    )


class StorageUnreachableError(RuntimeError):
    """The storage root is missing or unreachable (not the same as "empty")."""


#: Cached bucket checks, keyed by (scheme, bucket, endpoint). A dict because FileSystem
#: isn't hashable; the endpoint is in the key because tests change it mid-run.
_REACHABLE: dict[tuple[str, str, str], bool] = {}


def clear_reachability_cache() -> None:
    """Forget cached bucket checks (tests, or after creating a bucket)."""
    _REACHABLE.clear()


def ensure_reachable(location: Location) -> None:
    """Raise if the S3 bucket doesn't exist.

    pyarrow returns an empty listing both for an unwritten partition and for a missing
    bucket or wrong endpoint. Without this check, a misconfigured endpoint makes every
    read silently return zero rows. Costs one cached HEAD per bucket; local is skipped.
    """
    if not location.is_object_store:
        return
    bucket = location.path.split("/", 1)[0]
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or "aws"
    key = (location.scheme, bucket, endpoint)
    if key not in _REACHABLE:
        try:
            _REACHABLE[key] = location.fs.get_file_info(bucket).type is not pafs.FileType.NotFound
        except Exception as exc:
            # Don't cache network errors; they're usually transient.
            raise StorageUnreachableError(
                f"could not reach s3://{bucket} via {endpoint}: {exc}"
            ) from exc
    if not _REACHABLE[key]:
        raise StorageUnreachableError(
            f"bucket s3://{bucket} does not exist or is not visible via endpoint "
            f"{endpoint!r}.\n"
            "  * Running against an emulator? Export AWS_ENDPOINT_URL "
            "(e.g. http://localhost:4566) and run: make floci-bootstrap\n"
            "  * Running against AWS? Check the bucket name, the region and the "
            "credentials in use."
        )


def list_files(location: Location, path: str, suffix: str = ".parquet") -> list[str]:
    """Recursively list files under ``path``, sorted. Missing paths return [] (no rows yet)."""
    try:
        selector = pafs.FileSelector(path, recursive=True, allow_not_found=True)
        infos = location.fs.get_file_info(selector)
    except (FileNotFoundError, OSError):
        return []
    return sorted(
        info.path
        for info in infos
        if info.type == pafs.FileType.File and info.path.endswith(suffix)
    )


def delete_dir_contents(location: Location, path: str) -> None:
    """Remove everything under ``path``, tolerating its absence."""
    with contextlib.suppress(FileNotFoundError, OSError):
        location.fs.delete_dir_contents(path, missing_dir_ok=True)


def list_matching(location: Location, pattern: str) -> list[str]:
    """List files matching a glob ``pattern`` relative to the root.

    S3 has no glob, so this lists the longest wildcard-free prefix and filter. Same code
    locally and on S3, and the listing stays narrow (bucket-wide LISTs are slow and billed).
    """
    from fnmatch import fnmatch

    segments = pattern.strip("/").split("/")
    static: list[str] = []
    for segment in segments:
        if any(ch in segment for ch in "*?["):
            break
        static.append(segment)

    prefix = location.child(*static)
    candidates = list_files(location, prefix, suffix="")
    if not candidates and not any(ch in pattern for ch in "*?["):
        # No wildcard: the pattern may name a single object, which listing won't find.
        info = location.fs.get_file_info(prefix)
        if info.type == pafs.FileType.File:
            return [prefix]
        return []

    base = location.path.rstrip("/") + "/"
    matched = [
        path
        for path in candidates
        if fnmatch(path[len(base) :] if path.startswith(base) else path, pattern)
    ]
    return sorted(matched)


def read_text(location: Location, path: str) -> str:
    """Read a whole object as UTF-8. Fine for tens of MB, not for bigger files."""
    with location.fs.open_input_stream(path) as handle:
        return str(handle.read().decode("utf-8"))
