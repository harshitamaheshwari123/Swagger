from app.processing.dedup import deduplicate
from app.processing.validation import clean_record


def _clean(raw):
    event, reason = clean_record(raw)
    assert event is not None, reason
    return event


def test_duplicates_with_different_formatting_are_collapsed():
    a = _clean({
        "event_id": "EVT-1",
        "company_name": "Acme Corp",
        "category": "cybersecurity",
        "severity": 3,
        "confidence": 0.8,
        "published_at": "2026-07-20T10:30:00Z",
        "description": "A denial-of-service attack disrupted the payments reconciliation service.",
    })
    b = _clean({
        "event_id": "",  # duplicates may carry a blank event_id
        "company_name": " acme  corp, ",
        "category": "CYBER_SECURITY",
        "severity": 3,
        "confidence": 0.8,
        "published_at": "20-07-2026",
        "description": "A denial-of-service attack disrupted the payments reconciliation service.",
    })
    kept, dropped = deduplicate([a, b])
    assert len(kept) == 1
    assert len(dropped) == 1


def test_different_categories_are_not_duplicates():
    a = _clean({
        "company_name": "Acme Corp", "category": "cybersecurity", "severity": 3,
        "confidence": 0.8, "published_at": "2026-07-20T10:30:00Z",
        "description": "Same wording used for two different incident types.",
    })
    b = _clean({
        "company_name": "Acme Corp", "category": "financial", "severity": 3,
        "confidence": 0.8, "published_at": "2026-07-20T10:30:00Z",
        "description": "Same wording used for two different incident types.",
    })
    kept, dropped = deduplicate([a, b])
    assert len(kept) == 2
    assert len(dropped) == 0


def test_dedup_keeps_the_more_complete_record():
    sparse = _clean({
        "event_id": "",
        "company_name": "Acme Corp",
        "category": "leadership",
        "severity": 2,
        "confidence": 0.5,
        "published_at": "2026-07-01T00:00:00Z",
        "description": "The chief financial officer resigned.",
    })
    complete = _clean({
        "event_id": "EVT-42",
        "company_name": "Acme Corp",
        "category": "leadership",
        "severity": 2,
        "confidence": 0.5,
        "published_at": "2026-07-01T00:00:00Z",
        "country": "India",
        "source": "Test Wire",
        "description": "The chief financial officer resigned.",
    })
    kept, dropped = deduplicate([sparse, complete])
    assert len(kept) == 1
    assert kept[0].event_id == "EVT-42"
