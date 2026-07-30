"""Validation and normalization of raw event records.

Design goal: one bad record must never kill the run. `clean_record` never
raises -- it always returns either a CleanEvent or a rejection reason string.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

from app.models.schemas import CANONICAL_CATEGORIES, CleanEvent
from app.utils.dates import parse_published_at
from app.utils.logging_config import get_logger

logger = get_logger(__name__)

# Maps every observed variant (lowercased, whitespace-collapsed) to one of
# the six canonical categories. Extend this table if new variants show up.
_CATEGORY_MAP = {
    "cybersecurity": "cybersecurity",
    "cyber security": "cybersecurity",
    "cyber_security": "cybersecurity",
    "cyber-security": "cybersecurity",
    "cyber": "cybersecurity",
    "legal_regulatory": "legal_regulatory",
    "legal regulatory": "legal_regulatory",
    "legal": "legal_regulatory",
    "regulatory": "legal_regulatory",
    "financial": "financial",
    "financial_distress": "financial",
    "financial distress": "financial",
    "finance": "financial",
    "supply_chain": "supply_chain",
    "supply chain": "supply_chain",
    "supply-chain": "supply_chain",
    "leadership": "leadership",
    "leadership_change": "leadership",
    "leadership change": "leadership",
    "management": "leadership",
    "fraud_reputation": "fraud_reputation",
    "fraud reputation": "fraud_reputation",
    "fraud": "fraud_reputation",
    "reputation": "fraud_reputation",
}

_LEGAL_SUFFIXES = re.compile(
    r"\b(ltd\.?|pvt\.?\s*ltd\.?|limited|inc\.?|llc|llp|corp\.?|co\.?)\b\.?\s*$",
    re.IGNORECASE,
)


def normalize_company_name(raw: Optional[str]) -> Optional[str]:
    """Collapse whitespace/case noise. Keeps legal suffixes in the display
    name (they're informative) but the caller uses `dedup_key_company` for
    matching, which strips them."""
    if raw is None:
        return None
    value = re.sub(r"\s+", " ", str(raw)).strip().strip(",")
    return value or None


def dedup_key_company(display_name: str) -> str:
    """Aggressive normalization used only for matching/grouping, not display."""
    value = display_name.lower()
    value = _LEGAL_SUFFIXES.sub("", value).strip()
    value = re.sub(r"[^a-z0-9]+", " ", value).strip()
    return value


def normalize_country(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    value = re.sub(r"\s+", " ", str(raw)).strip()
    if not value or value.lower() in ("null", "none", "n/a"):
        return None
    key = value.lower().replace(".", "").replace(" ", "")
    aliases = {
        "usa": "United States",
        "us": "United States",
        "unitedstates": "United States",
        "uk": "United Kingdom",
        "unitedkingdom": "United Kingdom",
        "uae": "United Arab Emirates",
        "unitedarabemirates": "United Arab Emirates",
        "aus": "Australia",
        "australia": "Australia",
        "india": "India",
        "japan": "Japan",
        "germany": "Germany",
        "singapore": "Singapore",
    }
    return aliases.get(key, value.title())


def normalize_category(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    key = re.sub(r"\s+", " ", str(raw)).strip().lower()
    key = key.replace("-", " ").replace("_", " ")
    key = re.sub(r"\s+", " ", key).strip()
    # try both the raw-with-underscore and the space-normalized key
    return _CATEGORY_MAP.get(key) or _CATEGORY_MAP.get(str(raw).strip().lower())


def coerce_severity(raw) -> Optional[int]:
    try:
        value = int(str(raw).strip())
    except (ValueError, TypeError):
        return None
    if 1 <= value <= 5:
        return value
    return None


def coerce_confidence(raw) -> Optional[float]:
    try:
        value = float(str(raw).strip())
    except (ValueError, TypeError):
        return None
    if 0.0 <= value <= 1.0:
        return value
    return None


def normalize_optional_str(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    value = re.sub(r"\s+", " ", str(raw)).strip()
    if not value or value.lower() in ("null", "none", "n/a"):
        return None
    return value


def clean_record(raw: dict) -> Tuple[Optional[CleanEvent], Optional[str]]:
    """Validate + normalize one raw record.

    Returns (CleanEvent, None) on success, or (None, reason) on rejection.
    Never raises -- any unexpected exception is itself converted into a
    rejection reason so a single malformed record can't kill the pipeline.
    """
    try:
        company_name = normalize_company_name(raw.get("company_name"))
        if not company_name:
            return None, "missing or blank company_name"

        category = normalize_category(raw.get("category"))
        if not category:
            return None, f"unrecognized category: {raw.get('category')!r}"

        severity = coerce_severity(raw.get("severity"))
        if severity is None:
            return None, f"invalid severity (must be integer 1-5): {raw.get('severity')!r}"

        confidence = coerce_confidence(raw.get("confidence"))
        if confidence is None:
            return None, f"invalid confidence (must be float 0-1): {raw.get('confidence')!r}"

        published_at = parse_published_at(raw.get("published_at"))
        if published_at is None:
            return None, f"unparseable published_at: {raw.get('published_at')!r}"

        event_id = normalize_optional_str(raw.get("event_id"))
        country = normalize_country(raw.get("country"))
        source = normalize_optional_str(raw.get("source"))
        description = normalize_optional_str(raw.get("description"))

        desc_key = re.sub(r"\s+", " ", (description or "").lower()).strip()
        desc_key = re.sub(r"[^a-z0-9 ]+", "", desc_key)
        dedup_key = f"{dedup_key_company(company_name)}|{category}|{desc_key}"

        return (
            CleanEvent(
                event_id=event_id,
                company_name=company_name,
                category=category,
                severity=severity,
                confidence=confidence,
                published_at=published_at,
                country=country,
                source=source,
                description=description,
                dedup_key=dedup_key,
            ),
            None,
        )
    except Exception as exc:  # noqa: BLE001 - deliberate catch-all, see docstring
        return None, f"unexpected error during cleaning: {exc}"


def validate_batch(raw_records: list[dict]) -> Tuple[list[CleanEvent], list[dict]]:
    """Validate every record. Returns (clean_events, rejects).

    Each reject dict is {"record": <original>, "reason": <str>}.
    """
    clean: list[CleanEvent] = []
    rejects: list[dict] = []

    for raw in raw_records:
        event, reason = clean_record(raw)
        if event is not None:
            clean.append(event)
        else:
            rejects.append({"record": raw, "reason": reason})
            logger.warning("Rejected record %s: %s", raw.get("event_id"), reason)

    logger.info(
        "Validation complete: %d valid, %d rejected out of %d",
        len(clean),
        len(rejects),
        len(raw_records),
    )
    return clean, rejects


def canonicalize_company_names(events: list[CleanEvent]) -> list[CleanEvent]:
    """Collapse cosmetic company-name variants into one display name.

    Two events can refer to the same company without being duplicates
    (different category/description) -- e.g. "kestrel airlines" and
    "Kestrel Airlines" reporting two unrelated incidents. Left alone, these
    would be counted as two different companies downstream. This groups
    every event by the same aggressive key used for dedup matching
    (`dedup_key_company`) and rewrites `company_name` to the most common
    formatting seen for that key, so company-level aggregation counts them
    as one company.

    Does not touch `dedup_key`, which is already built from the normalized
    company key and is unaffected by which display variant wins here.
    """
    from collections import Counter, defaultdict

    variants: dict[str, Counter] = defaultdict(Counter)
    for event in events:
        variants[dedup_key_company(event.company_name)][event.company_name] += 1

    canonical_name = {
        key: counter.most_common(1)[0][0] for key, counter in variants.items()
    }

    for event in events:
        event.company_name = canonical_name[dedup_key_company(event.company_name)]

    return events
