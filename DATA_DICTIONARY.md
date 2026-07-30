# SignalWatch — Data Dictionary

Everything in this pack is synthetic. No real company, person or incident is described.

| File | Records | What it is |
|---|---|---|
| `events_api.json` | 90 | Payload served by the mock REST API |
| `events.csv` | 60 | The second source, read straight from disk |
| `mock_api.py` | — | Zero-dependency server that exposes `events_api.json` over HTTP |

**Total supplied records: 150.**

---

## 1. Running the mock API

Python 3.9+, no packages needed.

```bash
python mock_api.py            # http://localhost:9000
python mock_api.py --port 9100
python mock_api.py --flaky    # ~15% of requests return 503 — use this to test retries
```

### `GET /health`

```json
{ "status": "ok", "records": 90 }
```

### `GET /api/v1/events`

| Param | Type | Default | Notes |
|---|---|---|---|
| `page` | int | `1` | 1-indexed |
| `page_size` | int | `50` | capped at 100 |

```json
{
  "page": 1,
  "page_size": 50,
  "total": 90,
  "total_pages": 2,
  "has_more": true,
  "results": [ { "event_id": "EVT-1024", "...": "..." } ]
}
```

**The response is paginated.** With the default page size there are 2 pages. If you only
read page 1 you will silently lose 40 records — loop until `has_more` is `false`.

### Failure simulation

Append `?simulate=...` to `/api/v1/events` to exercise your error handling:

| Value | Behaviour |
|---|---|
| `error` | HTTP 500 |
| `timeout` | Hangs for 30 seconds |
| `malformed` | HTTP 200 with truncated, unparseable JSON |
| `empty` | Valid envelope, zero results |

If you would rather not run a server, `events_api.json` is a plain array of the same 90
records — read it directly and note the choice in your README.

---

## 2. Field reference

Both sources use the same nine fields. The CSV header row is lowercase and stable; only
the *values* are messy.

| Field | Type | Required | Description |
|---|---|---|---|
| `event_id` | string | no | Identifier from the source. Not globally unique — see §3. |
| `company_name` | string | **yes** | Affected company. Formatting varies. |
| `category` | string | **yes** | Event type. Formatting varies. |
| `severity` | integer 1–5 | **yes** | Impact of the event. Sometimes arrives as a string. |
| `confidence` | float 0–1 | **yes** | Source reliability. Sometimes arrives as a string. |
| `published_at` | datetime | **yes** | Publication timestamp. Six different formats appear. |
| `country` | string | no | Country tied to the event. |
| `source` | string | no | Reporting source. |
| `description` | string | no | Free-text summary. Primary deduplication signal. |

### Canonical categories

`cybersecurity` · `legal_regulatory` · `financial` · `supply_chain` · `leadership` · `fraud_reputation`

Variants you will actually see include `Cyber Security`, `CYBER_SECURITY`, `cyber`,
`SUPPLY-CHAIN`, `supply chain`, `FINANCIAL_DISTRESS`, `finance`, `LEADERSHIP_CHANGE`,
`management`, `FRAUD`, `reputation`, `REGULATORY`, `legal`.

Map them to the six canonical values. State your mapping in the README.

### Date formats present

```text
2026-07-20T10:30:00Z      ISO 8601 with Z
2026-07-20 10:30:00       space separated
2026/07/20 10:30          slash separated
20-07-2026                day-month-year
July 3, 2026              long month name
03 Jul 2026               short month name
```

All timestamps represent UTC.

---

## 3. Known data-quality issues

These are deliberate. Handling them **is** the assignment.

| Issue | Roughly how many | Notes |
|---|---|---|
| Duplicate events | 10 | Same company + category + description, differently formatted |
| Unrecoverably invalid records | 8 | Should be rejected, not repaired |
| Inconsistent company names | ~15 | Case, double spaces, leading/trailing space, `Ltd.` / `Pvt Ltd`, trailing comma |
| Inconsistent categories | ~30 | See variant list above |
| Inconsistent countries | ~40 | `USA` / `U.S.A.` / `usa`, `UK`, `UAE`, ` India `, `AUS` |
| Mixed date formats | ~50 | See list above |
| Numbers as strings | ~15 | `"severity": "4"`, `"confidence": "0.85"` |
| Missing optional fields | ~15 | `country`, `source` or `event_id` blank or null |
| Low-confidence records | ~10 | `confidence` between 0.05 and 0.24 — valid, but weak signal |

### On the 8 invalid records

Each one demonstrates a different failure mode: blank `company_name`, null
`company_name`, `severity` of `0`, `severity` of `7`, `severity` of `"high"`,
`confidence` of `1.4`, `confidence` of `-0.2`, and a `published_at` of `"not-a-date"`.

Write them to a rejects file with the reason attached. Do not silently drop them, and do
not let one of them kill the run.

### On `event_id`

Three of the ten duplicates reuse the original's `event_id`. The other seven carry a
**new** `event_id`. Deduplicating on `event_id` alone will therefore catch 3 of 10 and
miss 7 — which is exactly why the brief defines duplicates by company, category and
description instead.

Some records have a blank or null `event_id`. It is not a reliable primary key.

### On low-confidence records

Keeping them, down-weighting them or excluding them below a threshold are all defensible.
The scoring formula already multiplies by `confidence`, so they mostly sink on their own.
Whatever you choose, say so in the README.

---

## 4. Reference dates

`published_at` values run from roughly 190 days before **2026-07-24** up to that date.
Recency weight is calculated against **the date you run the pipeline**, so results drift
as time passes. No event sits on a bucket boundary (no record is exactly 7, 8, 30 or 31
days old), so scores stay stable for several days after the data was issued.

If you want reproducible output, make the "as of" date configurable and default it to
`now()`. That is a nice touch, not a requirement.

---

## 5. What good output looks like

Your `POST /ingest` summary should land close to this:

```json
{
  "status": "completed",
  "received_records": 150,
  "valid_records": 132,
  "rejected_records": 8,
  "duplicate_records": 10,
  "companies_processed": 24
}
```

Small deviations are fine and expected — if your validator repairs a record that the
reference rejects, or your dedupe key is stricter, your numbers will shift by one or two.
Being able to explain *why* your numbers differ is worth more than matching them exactly.
