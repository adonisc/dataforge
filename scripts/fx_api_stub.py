#!/usr/bin/env python3
"""Local stub of the Treasury FX API, so the REST source can run end to end.

``GET /v1/fx/rates?date=YYYY-MM-DD&page=N`` returns ``{"data": [...]}`` for that date,
paginated. An unknown date returns an empty page, like a real API on a non-trading day.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DEFAULT_PORT = 8099
DEFAULT_PAGE_SIZE = 500


def _load_rates(landing_root: Path, date_str: str) -> list[dict]:
    path = landing_root / f"fx_rates/dt={date_str}/rates.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _make_handler(landing_root: Path, page_size: int, token: str | None):
    class Handler(BaseHTTPRequestHandler):
        # Quiet the default request logging; the pipeline logs are enough.
        def log_message(self, *args: object) -> None:  # noqa: ARG002
            return

        def _send(self, status: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if not parsed.path.rstrip("/").endswith("/fx/rates"):
                self._send(404, {"error": "not found"})
                return

            # Check the token when one is set, so auth handling is actually tested.
            if token is not None:
                supplied = self.headers.get("Authorization", "")
                if supplied != f"Bearer {token}":
                    self._send(401, {"error": "missing or invalid bearer token"})
                    return

            query = parse_qs(parsed.query)
            date_str = (query.get("date") or [""])[0]
            page = int((query.get("page") or ["1"])[0])

            rows = _load_rates(landing_root, date_str)
            start = (page - 1) * page_size
            self._send(200, {"data": rows[start : start + page_size]})

    return Handler


@contextlib.contextmanager
def serve(
    landing_root: Path,
    *,
    port: int = DEFAULT_PORT,
    page_size: int = DEFAULT_PAGE_SIZE,
    token: str | None = None,
) -> Iterator[str]:
    """Run the stub inside a ``with`` block and yield its base URL. Stops on exit."""
    server = ThreadingHTTPServer(("127.0.0.1", port), _make_handler(landing_root, page_size, token))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-root", default="data/landing")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--token", default=None, help="require this bearer token")
    args = parser.parse_args()

    with serve(Path(args.landing_root), port=args.port, token=args.token) as base:
        print(f"FX stub on {base}/v1/fx/rates?date=YYYY-MM-DD   (ctrl-c to stop)")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            print("\nstopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
