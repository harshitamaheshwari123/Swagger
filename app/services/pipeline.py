"""Orchestrates the full six-stage pipeline and caches the latest result
in memory so the read endpoints (/companies, /events) don't need to
re-run ingestion + Spark on every request.

A real deployment would persist this in SQLite/Postgres (see README ->
"what you'd improve"); an in-memory cache is enough for a take-home whose
API is exercised interactively / by a test suite in a single process.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from app.ingestion.api_client import ApiIngestionError, fetch_all_events
from app.ingestion.csv_reader import read_csv_events
from app.processing.dedup import deduplicate
from app.processing.spark_processing import score_events, write_outputs
from app.processing.validation import canonicalize_company_names, validate_batch
from app.services.analytics import compute_score_stats, plot_top_companies
from app.utils.logging_config import get_logger

logger = get_logger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "data"))
OUTPUT_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "outputs"))

API_BASE_URL = os.environ.get("SIGNALWATCH_API_URL", "http://127.0.0.1:9000")
CSV_PATH = os.environ.get("SIGNALWATCH_CSV_PATH", os.path.join(DATA_DIR, "events.csv"))
USE_LOCAL_JSON_FALLBACK = os.environ.get("SIGNALWATCH_USE_JSON_FALLBACK", "0") == "1"
LOCAL_JSON_PATH = os.path.join(DATA_DIR, "events_api.json")


class PipelineCache:
    """Holds the most recent /ingest result in memory."""

    def __init__(self) -> None:
        self.summary: Optional[dict] = None
        self.event_rows: list[dict] = []
        self.company_rows: list[dict] = []
        self.rejects: list[dict] = []
        self.stats: Optional[dict] = None
        self.chart_path: Optional[str] = None
        self.last_run_at: Optional[datetime] = None

    def is_ready(self) -> bool:
        return self.summary is not None


cache = PipelineCache()


def as_of_wallclock_id() -> str:
    """A filesystem-safe, sortable id for this run, based on wall-clock time
    (not the scoring `as_of` param, which may repeat across runs)."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _write_latest_pointer(run_id: str) -> None:
    """Overwrite (not delete-and-recreate) a small pointer file so callers
    always know where the most recent run's artifacts live, without us
    ever needing to delete a previous run's directory."""
    import json

    pointer_path = os.path.join(OUTPUT_DIR, "latest_run.json")
    with open(pointer_path, "w", encoding="utf-8") as fh:
        json.dump({"run_id": run_id, "run_dir": f"run_{run_id}"}, fh, indent=2)


def _load_raw_records() -> list[dict]:
    if USE_LOCAL_JSON_FALLBACK:
        import json

        with open(LOCAL_JSON_PATH, encoding="utf-8") as fh:
            api_records = json.load(fh)
        logger.info("Loaded %d records from local JSON fallback (no HTTP)", len(api_records))
    else:
        try:
            api_records = fetch_all_events(API_BASE_URL)
        except ApiIngestionError:
            logger.error(
                "API unreachable at %s; falling back to bundled events_api.json", API_BASE_URL
            )
            import json

            with open(LOCAL_JSON_PATH, encoding="utf-8") as fh:
                api_records = json.load(fh)

    csv_records = read_csv_events(CSV_PATH)
    merged = api_records + csv_records
    logger.info(
        "Merged %d API records + %d CSV records = %d total",
        len(api_records),
        len(csv_records),
        len(merged),
    )
    return merged


def run_pipeline(as_of: Optional[datetime] = None) -> dict:
    """Runs collect -> clean -> dedupe -> score, caches the result, returns
    the /ingest summary dict."""
    as_of = as_of or datetime.now(timezone.utc)

    raw_records = _load_raw_records()
    received_records = len(raw_records)

    clean_events, rejects = validate_batch(raw_records)
    clean_events = canonicalize_company_names(clean_events)
    kept_events, duplicates = deduplicate(clean_events)

    event_rows, company_rows = score_events(kept_events, as_of=as_of)

    run_id = as_of_wallclock_id()
    run_output_dir = os.path.join(OUTPUT_DIR, f"run_{run_id}")
    try:
        write_outputs(event_rows, company_rows, run_output_dir)
        _write_latest_pointer(run_id)
    except Exception as exc:  # noqa: BLE001
        # Output writing is best-effort -- don't fail the whole ingest run
        # over a disk/permissions issue.
        logger.warning("Could not write pipeline output files: %s", exc)

    stats = compute_score_stats(company_rows)
    chart_path = None
    try:
        chart_path = plot_top_companies(
            company_rows, os.path.join(OUTPUT_DIR, "top_company_risks.png")
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not generate chart: %s", exc)

    summary = {
        "status": "completed",
        "received_records": received_records,
        "valid_records": len(kept_events),
        "rejected_records": len(rejects),
        "duplicate_records": len(duplicates),
        "companies_processed": len(company_rows),
    }

    cache.summary = summary
    cache.event_rows = event_rows
    cache.company_rows = company_rows
    cache.rejects = rejects
    cache.stats = stats
    cache.chart_path = chart_path
    cache.last_run_at = as_of

    logger.info("Pipeline run complete: %s", summary)
    return summary
