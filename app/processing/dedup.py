"""Duplicate detection.

Strategy (see README for full reasoning): two clean events are the same
underlying incident if they share the same normalized company name,
canonical category, and normalized description -- NOT event_id, which the
data dictionary explicitly tells us is unreliable (some duplicates keep the
original event_id, most don't; some records have no event_id at all).

Within a duplicate group we keep the record with the most complete optional
fields (event_id, country, source), breaking ties by keeping the first-seen
record, so we don't arbitrarily discard the more informative copy.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Tuple

from app.models.schemas import CleanEvent
from app.utils.logging_config import get_logger

logger = get_logger(__name__)


def _completeness(event: CleanEvent) -> int:
    return sum(
        1
        for field in (event.event_id, event.country, event.source)
        if field
    )


def deduplicate(events: list[CleanEvent]) -> Tuple[list[CleanEvent], list[CleanEvent]]:
    """Returns (kept, dropped_duplicates)."""
    groups: dict[str, list[CleanEvent]] = defaultdict(list)
    for event in events:
        groups[event.dedup_key].append(event)

    kept: list[CleanEvent] = []
    dropped: list[CleanEvent] = []

    for key, group in groups.items():
        if len(group) == 1:
            kept.append(group[0])
            continue
        # most complete first, stable sort keeps first-seen order for ties
        ordered = sorted(group, key=_completeness, reverse=True)
        kept.append(ordered[0])
        dropped.extend(ordered[1:])
        logger.info(
            "Duplicate group (%d records) collapsed for key=%r", len(group), key
        )

    logger.info(
        "Dedup complete: %d kept, %d duplicates dropped out of %d",
        len(kept),
        len(dropped),
        len(events),
    )
    return kept, dropped
