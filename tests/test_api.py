import pytest
from fastapi.testclient import TestClient

from app.api.main import app

client = TestClient(app)


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


def test_reads_require_ingest_first():
    # /companies before any /ingest call should 409, not crash
    # (skipped if a previous test in this module already ran /ingest)
    from app.services import pipeline
    if pipeline.cache.is_ready():
        pytest.skip("cache already populated by an earlier test in this run")
    resp = client.get("/companies")
    assert resp.status_code == 409


@pytest.fixture(scope="module")
def ingested():
    resp = client.post("/ingest")
    assert resp.status_code == 200
    return resp.json()


def test_ingest_summary_shape(ingested):
    for key in (
        "status", "received_records", "valid_records",
        "rejected_records", "duplicate_records", "companies_processed",
    ):
        assert key in ingested
    assert ingested["status"] == "completed"
    assert ingested["received_records"] == 150
    # 8 deliberately-invalid + ~10 duplicate records per the data dictionary
    assert ingested["rejected_records"] >= 6
    assert ingested["duplicate_records"] >= 5


def test_list_companies(ingested):
    resp = client.get("/companies")
    assert resp.status_code == 200
    companies = resp.json()
    assert len(companies) == ingested["companies_processed"]
    assert all(c["risk_level"] in ("LOW", "MEDIUM", "HIGH") for c in companies)


def test_list_companies_with_filters(ingested):
    resp = client.get("/companies", params={"risk_level": "HIGH", "limit": 3})
    assert resp.status_code == 200
    companies = resp.json()
    assert len(companies) <= 3
    assert all(c["risk_level"] == "HIGH" for c in companies)


def test_get_single_company(ingested):
    all_companies = client.get("/companies").json()
    name = all_companies[0]["company_name"]
    resp = client.get(f"/companies/{name}")
    assert resp.status_code == 200
    assert resp.json()["company_name"] == name


def test_get_unknown_company_404(ingested):
    resp = client.get("/companies/Not A Real Company XYZ")
    assert resp.status_code == 404


def test_events_requires_two_filters(ingested):
    resp = client.get("/events", params={"company": "Acme"})
    assert resp.status_code == 400


def test_events_with_two_filters(ingested):
    all_companies = client.get("/companies").json()
    name = all_companies[0]["company_name"]
    resp = client.get("/events", params={"company": name, "min_score": 0})
    assert resp.status_code == 200
    events = resp.json()
    assert all(e["company_name"] == name for e in events)
