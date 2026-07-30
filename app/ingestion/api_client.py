"""Client for the SignalWatch mock REST API (see data/mock_api.py).

Handles: full pagination (loops until has_more is false), request timeouts,
and retries with backoff on transient failures (connection errors, 5xx,
including the --flaky mode's random 503s).
"""
from __future__ import annotations

import time
from typing import Optional

import requests

from app.utils.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT_SECONDS = 5
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.0


class ApiIngestionError(Exception):
    """Raised when the API cannot be read after exhausting retries."""


def _get_with_retries(url: str, params: dict, timeout: float) -> dict:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"server error {response.status_code}", response=response
                )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            logger.warning(
                "API request failed (attempt %d/%d) params=%s: %s",
                attempt,
                MAX_RETRIES,
                params,
                exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise ApiIngestionError(
        f"Giving up on {url} after {MAX_RETRIES} attempts: {last_exc}"
    )


def fetch_all_events(
    base_url: str,
    page_size: int = 50,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict]:
    """Fetch every page of /api/v1/events and return the merged results.

    Loops on `has_more` rather than assuming a fixed page count, so it keeps
    working even if the total record count or page size changes.
    """
    url = f"{base_url.rstrip('/')}/api/v1/events"
    all_results: list[dict] = []
    page = 1

    while True:
        try:
            payload = _get_with_retries(
                url, {"page": page, "page_size": page_size}, timeout
            )
        except ApiIngestionError as exc:
            logger.error("Aborting API ingestion at page %d: %s", page, exc)
            break

        results = payload.get("results", [])
        all_results.extend(results)
        logger.info(
            "Fetched page %d/%s (%d records, running total %d)",
            payload.get("page", page),
            payload.get("total_pages", "?"),
            len(results),
            len(all_results),
        )

        if not payload.get("has_more"):
            break
        page += 1

    logger.info("API ingestion complete: %d records", len(all_results))
    return all_results
