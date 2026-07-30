"""Pydantic models used across the API and pipeline."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

CANONICAL_CATEGORIES = (
    "cybersecurity",
    "legal_regulatory",
    "financial",
    "supply_chain",
    "leadership",
    "fraud_reputation",
)

RISK_LEVELS = ("LOW", "MEDIUM", "HIGH")


class RawEvent(BaseModel):
    """Shape of a record as it arrives from either source, before cleaning.

    Every field is optional/loose here on purpose -- validation happens
    explicitly in app.processing.validation, not via pydantic coercion,
    so that we control exactly which values are accepted vs rejected and why.
    """

    event_id: Optional[str] = None
    company_name: Optional[str] = None
    category: Optional[str] = None
    severity: Optional[object] = None
    confidence: Optional[object] = None
    published_at: Optional[str] = None
    country: Optional[str] = None
    source: Optional[str] = None
    description: Optional[str] = None


class CleanEvent(BaseModel):
    """A record that has passed validation and normalization."""

    event_id: Optional[str]
    company_name: str
    category: str
    severity: int
    confidence: float
    published_at: datetime
    country: Optional[str]
    source: Optional[str]
    description: Optional[str]
    dedup_key: str


class RejectedEvent(BaseModel):
    record: dict
    reason: str


class EventScore(BaseModel):
    event_id: Optional[str]
    company_name: str
    category: str
    severity: int
    confidence: float
    published_at: datetime
    country: Optional[str]
    source: Optional[str]
    description: Optional[str]
    days_old: int
    recency_weight: float
    event_risk_score: float


class CompanyScore(BaseModel):
    company_name: str
    risk_score: float
    risk_level: str
    event_count: int
    top_categories: list[str]
    country: Optional[str] = None


class IngestSummary(BaseModel):
    status: str
    received_records: int
    valid_records: int
    rejected_records: int
    duplicate_records: int
    companies_processed: int
