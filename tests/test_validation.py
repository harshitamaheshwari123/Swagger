from app.processing.validation import clean_record, validate_batch


def test_accepts_valid_record_with_stringified_numbers():
    raw = {
        "event_id": "EVT-9001",
        "company_name": "  Acme  Corp ",
        "category": "CYBER_SECURITY",
        "severity": "4",
        "confidence": "0.85",
        "published_at": "2026-07-20T10:30:00Z",
        "country": "usa",
        "source": "Test Source",
        "description": "A test incident.",
    }
    event, reason = clean_record(raw)
    assert reason is None
    assert event is not None
    assert event.severity == 4
    assert event.confidence == 0.85
    assert event.category == "cybersecurity"
    assert event.country == "United States"
    assert event.company_name == "Acme Corp"


def test_rejects_blank_company_name():
    raw = {"company_name": "", "category": "financial", "severity": 3,
           "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None
    assert "company_name" in reason


def test_rejects_null_company_name():
    raw = {"company_name": None, "category": "financial", "severity": 3,
           "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None


def test_rejects_severity_zero():
    raw = {"company_name": "X", "category": "financial", "severity": 0,
           "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None
    assert "severity" in reason


def test_rejects_severity_above_five():
    raw = {"company_name": "X", "category": "financial", "severity": 7,
           "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None


def test_rejects_non_numeric_severity():
    raw = {"company_name": "X", "category": "financial", "severity": "high",
           "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None


def test_rejects_confidence_above_one():
    raw = {"company_name": "X", "category": "financial", "severity": 3,
           "confidence": 1.4, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None
    assert "confidence" in reason


def test_rejects_negative_confidence():
    raw = {"company_name": "X", "category": "financial", "severity": 3,
           "confidence": -0.2, "published_at": "2026-07-01T00:00:00Z"}
    event, reason = clean_record(raw)
    assert event is None


def test_rejects_unparseable_date():
    raw = {"company_name": "X", "category": "financial", "severity": 3,
           "confidence": 0.5, "published_at": "not-a-date"}
    event, reason = clean_record(raw)
    assert event is None
    assert "published_at" in reason


def test_all_six_date_formats_parse():
    formats = [
        "2026-07-20T10:30:00Z",
        "2026-07-20 10:30:00",
        "2026/07/20 10:30",
        "20-07-2026",
        "July 3, 2026",
        "03 Jul 2026",
    ]
    for value in formats:
        raw = {"company_name": "X", "category": "financial", "severity": 3,
               "confidence": 0.5, "published_at": value}
        event, reason = clean_record(raw)
        assert event is not None, f"format {value!r} failed to parse: {reason}"


def test_one_bad_record_does_not_break_the_batch():
    raw_records = [
        {"company_name": "Good Co", "category": "financial", "severity": 3,
         "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"},
        {"company_name": "", "category": "financial", "severity": 3,
         "confidence": 0.5, "published_at": "2026-07-01T00:00:00Z"},
    ]
    clean, rejects = validate_batch(raw_records)
    assert len(clean) == 1
    assert len(rejects) == 1
