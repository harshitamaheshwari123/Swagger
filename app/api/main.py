"""SignalWatch REST API (FastAPI).

    uvicorn app.api.main:app --reload --port 8000

/health and read endpoints work even before /ingest has been called, but
/companies, /companies/{name} and /events return an empty/placeholder
result until a pipeline run has populated the in-memory cache -- call
POST /ingest first.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from app.models.schemas import IngestSummary, RISK_LEVELS
from app.services import pipeline
from app.utils.logging_config import get_logger

logger = get_logger(__name__)

app = FastAPI(
    title="SignalWatch",
    description="Company risk intelligence from messy public-event data.",
    version="1.0.0",
)


@app.get("/health")
def health() -> dict:
    return {"status": "healthy"}


@app.post("/ingest", response_model=IngestSummary)
def ingest() -> dict:
    """Runs the full six-stage pipeline and caches the results in memory."""
    try:
        return pipeline.run_pipeline()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Pipeline run failed")
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {exc}") from exc


def _require_cache() -> None:
    if not pipeline.cache.is_ready():
        raise HTTPException(
            status_code=409,
            detail="No pipeline results yet. Call POST /ingest first.",
        )


@app.get("/companies")
def list_companies(
    risk_level: Optional[str] = Query(None, description="LOW, MEDIUM or HIGH"),
    country: Optional[str] = Query(None),
    minimum_score: Optional[float] = Query(None, ge=0, le=100),
    limit: Optional[int] = Query(None, gt=0),
) -> list[dict]:
    _require_cache()

    if risk_level is not None and risk_level.upper() not in RISK_LEVELS:
        raise HTTPException(
            status_code=400, detail=f"risk_level must be one of {RISK_LEVELS}"
        )

    rows = pipeline.cache.company_rows

    if risk_level is not None:
        rows = [r for r in rows if r["risk_level"] == risk_level.upper()]
    if country is not None:
        rows = [
            r for r in rows if (r.get("country") or "").lower() == country.lower()
        ]
    if minimum_score is not None:
        rows = [r for r in rows if r["risk_score"] >= minimum_score]

    rows = sorted(rows, key=lambda r: r["risk_score"], reverse=True)
    if limit is not None:
        rows = rows[:limit]

    return rows


@app.get("/companies/{company_name}")
def get_company(company_name: str) -> dict:
    _require_cache()

    for row in pipeline.cache.company_rows:
        if row["company_name"].lower() == company_name.lower():
            return row

    raise HTTPException(status_code=404, detail=f"Unknown company: {company_name!r}")


@app.get("/events")
def list_events(
    company: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    min_score: Optional[float] = Query(None, ge=0, le=100),
    country: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
) -> list[dict]:
    _require_cache()

    provided = [
        v is not None for v in (company, category, min_score, country, start_date or end_date)
    ]
    if sum(provided) < 2:
        raise HTTPException(
            status_code=400,
            detail=(
                "Provide at least two filters: company, category, min_score, "
                "country, and/or a date range (start_date/end_date)."
            ),
        )

    rows = pipeline.cache.event_rows

    if company is not None:
        rows = [r for r in rows if r["company_name"].lower() == company.lower()]
    if category is not None:
        rows = [r for r in rows if r["category"].lower() == category.lower()]
    if min_score is not None:
        rows = [r for r in rows if r["event_risk_score"] >= min_score]
    if country is not None:
        rows = [r for r in rows if (r.get("country") or "").lower() == country.lower()]
    if start_date is not None:
        rows = [r for r in rows if r["published_at"] >= start_date.replace(tzinfo=None)]
    if end_date is not None:
        rows = [r for r in rows if r["published_at"] <= end_date.replace(tzinfo=None)]

    return rows


@app.get("/stats")
def stats() -> dict:
    """Bonus endpoint: NumPy summary stats over the current company scores."""
    _require_cache()
    return pipeline.cache.stats
