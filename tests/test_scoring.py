from datetime import datetime, timedelta, timezone

from app.models.schemas import CleanEvent
from app.processing.spark_processing import score_events


def _event(company, severity, confidence, days_ago, category="financial", event_id=None):
    published = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return CleanEvent(
        event_id=event_id,
        company_name=company,
        category=category,
        severity=severity,
        confidence=confidence,
        published_at=published,
        country="India",
        source="Test",
        description=f"{company}-{category}-{days_ago}",
        dedup_key=f"{company.lower()}|{category}|{days_ago}",
    )


def test_event_risk_score_formula_recent():
    # severity 4, confidence 0.5, <=7 days old -> weight 1.0
    # 4 * 20 * 0.5 * 1.0 = 40.0
    as_of = datetime.now(timezone.utc)
    events = [_event("Acme Corp", 4, 0.5, days_ago=2)]
    event_rows, _ = score_events(events, as_of=as_of)
    assert event_rows[0]["event_risk_score"] == 40.0
    assert event_rows[0]["recency_weight"] == 1.0


def test_event_risk_score_recency_buckets():
    as_of = datetime.now(timezone.utc)
    events = [
        _event("Bucket Co", 5, 1.0, days_ago=5),   # <=7 -> 1.0
        _event("Bucket Co", 5, 1.0, days_ago=20),  # 8-30 -> 0.8
        _event("Bucket Co", 5, 1.0, days_ago=60),  # >30 -> 0.6
    ]
    event_rows, _ = score_events(events, as_of=as_of)
    weights = {row["days_old"]: row["recency_weight"] for row in event_rows}
    assert weights[5] == 1.0
    assert weights[20] == 0.8
    assert weights[60] == 0.6


def test_event_risk_score_capped_at_100():
    # severity 5, confidence 1.0, weight 1.0 -> raw 100, already at cap
    # use severity 5 * 20 * 1.0 * 1.0 = 100 exactly; formula can't exceed
    # bounds given inputs, so this confirms the min() cap doesn't under-cap.
    events = [_event("Cap Co", 5, 1.0, days_ago=1)]
    event_rows, _ = score_events(events, as_of=datetime.now(timezone.utc))
    assert event_rows[0]["event_risk_score"] == 100.0


def test_company_score_is_average_of_top_five_events():
    as_of = datetime.now(timezone.utc)
    # 7 events for one company; only the top 5 scores should count
    severities = [5, 5, 5, 5, 5, 1, 1]  # last two are low scorers
    events = [
        _event("Multi Event Co", sev, 1.0, days_ago=1, event_id=f"E{i}")
        for i, sev in enumerate(severities)
    ]
    event_rows, company_rows = score_events(events, as_of=as_of)
    company = next(r for r in company_rows if r["company_name"] == "Multi Event Co")

    top_five_scores = sorted((r["event_risk_score"] for r in event_rows), reverse=True)[:5]
    expected = round(sum(top_five_scores) / 5, 2)
    assert company["risk_score"] == expected
    assert company["event_count"] == 7


def test_fewer_than_five_events_averages_all_of_them():
    as_of = datetime.now(timezone.utc)
    events = [
        _event("Small Co", 3, 0.5, days_ago=1, event_id="A"),
        _event("Small Co", 4, 0.6, days_ago=1, event_id="B"),
    ]
    _, company_rows = score_events(events, as_of=as_of)
    company = next(r for r in company_rows if r["company_name"] == "Small Co")
    # 3*20*0.5=30.0, 4*20*0.6=48.0 -> avg 39.0
    assert company["risk_score"] == 39.0
    assert company["event_count"] == 2


def test_risk_level_classification_thresholds():
    as_of = datetime.now(timezone.utc)
    events = [
        _event("Low Co", 1, 0.5, days_ago=1),     # 1*20*0.5=10 -> LOW
        _event("Med Co", 3, 0.8, days_ago=1),     # 3*20*0.8=48 -> MEDIUM
        _event("High Co", 5, 0.9, days_ago=1),    # 5*20*0.9=90 -> HIGH
    ]
    _, company_rows = score_events(events, as_of=as_of)
    levels = {r["company_name"]: r["risk_level"] for r in company_rows}
    assert levels["Low Co"] == "LOW"
    assert levels["Med Co"] == "MEDIUM"
    assert levels["High Co"] == "HIGH"
