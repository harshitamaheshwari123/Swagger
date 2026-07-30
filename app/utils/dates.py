"""Date parsing for the six published_at formats documented in DATA_DICTIONARY.md.

We deliberately do NOT lean on dateutil.parser.parse alone: it happily
misreads ambiguous strings like "2026/07/05 05:48" (month/day vs day/month)
depending on locale-ish heuristics. Instead we try a fixed, ordered list of
explicit formats first, and only fall back to dateutil for the two
free-text month-name formats where an explicit strptime pattern is simplest.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

# Ordered: most specific / least ambiguous first.
_EXPLICIT_FORMATS = [
    "%Y-%m-%dT%H:%M:%SZ",   # 2026-07-20T10:30:00Z
    "%Y-%m-%d %H:%M:%S",    # 2026-07-20 10:30:00
    "%Y/%m/%d %H:%M",       # 2026/07/20 10:30
    "%d-%m-%Y",             # 20-07-2026
]

_TEXT_FORMATS = [
    "%B %d, %Y",   # July 3, 2026
    "%d %b %Y",    # 03 Jul 2026
]


def parse_published_at(raw: Optional[str]) -> Optional[datetime]:
    """Return a timezone-aware UTC datetime, or None if unparseable.

    None is treated by the validation layer as a hard rejection reason.
    """
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None

    for fmt in _EXPLICIT_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    for fmt in _TEXT_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None
