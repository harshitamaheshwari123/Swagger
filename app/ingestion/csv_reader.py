"""Reads the second data source, events.csv, straight from disk."""
from __future__ import annotations

import csv

from app.utils.logging_config import get_logger

logger = get_logger(__name__)


def read_csv_events(path: str) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        records = [dict(row) for row in reader]
    logger.info("Read %d records from %s", len(records), path)
    return records
