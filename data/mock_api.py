#!/usr/bin/env python3
"""
SignalWatch — mock public-events API.

Zero dependencies. Python 3.9+.

    python mock_api.py                 # http://localhost:9000
    python mock_api.py --port 9100
    python mock_api.py --flaky         # ~15% of requests fail with 503 (test your retries)

Endpoints
    GET /health
    GET /api/v1/events?page=1&page_size=50
    GET /api/v1/events?simulate=error       -> 500
    GET /api/v1/events?simulate=timeout     -> hangs 30s
    GET /api/v1/events?simulate=malformed   -> truncated, unparseable JSON
    GET /api/v1/events?simulate=empty       -> valid envelope, zero results
"""
import argparse
import json
import os
import random
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(HERE, "events_api.json")

with open(DATA_FILE, encoding="utf-8") as fh:
    EVENTS = json.load(fh)

FLAKY = False
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    # ---------------------------------------------------------------- helpers
    def _send(self, status, payload, raw=False):
        body = payload.encode() if raw else json.dumps(payload, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"  {self.address_string()}  {fmt % args}")

    # ---------------------------------------------------------------- routing
    def do_GET(self):
        url = urlparse(self.path)
        qs = parse_qs(url.query)

        if url.path in ("/health", "/api/v1/health"):
            return self._send(200, {"status": "ok", "records": len(EVENTS)})

        if url.path not in ("/api/v1/events", "/events"):
            return self._send(404, {"error": "not_found", "path": url.path})

        simulate = qs.get("simulate", [None])[0]
        if simulate == "error":
            return self._send(500, {"error": "internal_server_error"})
        if simulate == "timeout":
            time.sleep(30)
            return self._send(200, {"results": []})
        if simulate == "malformed":
            return self._send(200, '{"total": 90, "results": [{"event_id": "EVT-', raw=True)
        if simulate == "empty":
            return self._send(200, {"page": 1, "page_size": DEFAULT_PAGE_SIZE, "total": 0,
                                    "total_pages": 0, "has_more": False, "results": []})

        if FLAKY and random.random() < 0.15:
            return self._send(503, {"error": "service_unavailable", "retry_after": 1})

        try:
            page = max(1, int(qs.get("page", ["1"])[0]))
            page_size = int(qs.get("page_size", [DEFAULT_PAGE_SIZE])[0])
        except ValueError:
            return self._send(400, {"error": "bad_request",
                                    "detail": "page and page_size must be integers"})
        page_size = max(1, min(page_size, MAX_PAGE_SIZE))

        total = len(EVENTS)
        total_pages = (total + page_size - 1) // page_size
        start = (page - 1) * page_size
        results = EVENTS[start:start + page_size]

        self._send(200, {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_more": page < total_pages,
            "results": results,
        })


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--flaky", action="store_true",
                    help="randomly return 503 on ~15%% of requests")
    args = ap.parse_args()
    FLAKY = args.flaky

    print(f"SignalWatch mock API  ->  http://{args.host}:{args.port}")
    print(f"  {len(EVENTS)} records loaded from {os.path.basename(DATA_FILE)}")
    print(f"  GET /health")
    print(f"  GET /api/v1/events?page=1&page_size=50")
    if FLAKY:
        print("  flaky mode ON")
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
