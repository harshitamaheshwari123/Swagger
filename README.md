# SignalWatch

Turns messy public-event data into company risk intelligence: ingest two inconsistent
sources, clean and deduplicate them, score risk with PySpark, and serve the results over
a REST API.

## 1. Overview

Two data sources describe the same 24 companies with a lot of formatting noise: a
paginated REST API (`data/mock_api.py`, 90 records) and a CSV (`data/events.csv`, 60
records). The pipeline merges them, rejects records that can't be trusted, collapses
duplicates, computes a per-event and per-company risk score, and exposes everything
through FastAPI.

Six stages, run in order on every `POST /ingest`:

1. **Collect** — fetch every page from the API, read the CSV, merge.
2. **Clean** — validate and normalize into one shape; reject what can't be repaired.
3. **Dedupe** — collapse repeated events regardless of formatting differences.
4. **Process** — PySpark, explicit schema, event- and company-level aggregation.
5. **Score** — the formula in §5 below.
6. **Serve** — REST API over the cached result.

## 2. Architecture

```
app/
  ingestion/
    api_client.py     fetch_all_events() -- loops pagination, retries on 5xx/timeouts
    csv_reader.py      read_csv_events()
  processing/
    validation.py      clean_record(), validate_batch(), canonicalize_company_names()
    dedup.py            deduplicate()
    spark_processing.py score_events(), write_outputs() -- all PySpark
  services/
    pipeline.py         orchestrates the six stages, caches the latest result in memory
    analytics.py        NumPy stats + Matplotlib chart
  api/
    main.py              FastAPI routes
  models/
    schemas.py           pydantic models (RawEvent, CleanEvent, EventScore, CompanyScore...)
  utils/
    dates.py             the six-date-format parser
    logging_config.py    shared logger

data/            the two source files + the mock API server
outputs/         sample_output/ (committed), top_company_risks.png, and run_<timestamp>/
                  directories written by each real ingest run (gitignored)
tests/           pytest suite (29 tests)
```

`POST /ingest` runs the pipeline and caches the result **in memory** (see
`app.services.pipeline.PipelineCache`). `/companies`, `/companies/{name}`, `/events` and
`/stats` all read from that cache rather than re-running the pipeline per request. This
is deliberately simple for a single-process take-home; see §8 for what a persistent
version would look like.

## 3. Setup

```bash
pip install -r requirements.txt

# Terminal 1 -- the mock data source
python data/mock_api.py                # http://localhost:9000

# Terminal 2 -- the app
uvicorn app.api.main:app --reload --port 8000
```

Then:

```bash
curl -X POST http://localhost:8000/ingest
curl http://localhost:8000/companies
curl "http://localhost:8000/companies?risk_level=HIGH&limit=5"
curl http://localhost:8000/companies/Cobalt%20Financial%20Group
curl "http://localhost:8000/events?company=Cobalt%20Financial%20Group&min_score=0"
```

Environment variables (all optional):

| Variable | Default | Purpose |
|---|---|---|
| `SIGNALWATCH_API_URL` | `http://127.0.0.1:9000` | where to fetch API records from |
| `SIGNALWATCH_CSV_PATH` | `data/events.csv` | CSV source path |
| `SIGNALWATCH_USE_JSON_FALLBACK` | `0` | set to `1` to read `data/events_api.json` directly instead of over HTTP (useful if you don't want to run the mock server — the tests use this) |

Run the tests:

```bash
pytest
```

29 tests, ~30s (most of that is the one-time Spark JVM startup, shared across the whole
run via a session-scoped fixture).

## 4. API docs

| Method | Path | Behaviour |
|---|---|---|
| GET | `/health` | `{"status": "healthy"}` |
| POST | `/ingest` | Runs the full pipeline, caches the result, returns the summary (see §5) |
| GET | `/companies` | Filters: `risk_level`, `country`, `minimum_score`, `limit` |
| GET | `/companies/{company_name}` | Case-insensitive exact match; 404 if unknown |
| GET | `/events` | Filters: `company`, `category`, `min_score`, `country`, `start_date`/`end_date` — at least two required, 400 otherwise |
| GET | `/stats` | Bonus: NumPy mean/median/std/p90 over the current company scores |

All read endpoints return `409` until `/ingest` has been called at least once.

## 5. Risk-scoring explanation

```
Event Risk Score   = min(100, severity * 20 * confidence * recency_weight)
Company Risk Score = average of that company's five highest event scores
                      (all of them, if fewer than five)
```

Recency weight: `1.0` for ≤7 days old, `0.8` for 8–30 days, `0.6` beyond that.
`as_of` (what "now" means for recency) defaults to the actual current time but is a
parameter of `score_events()`, so results are reproducible if you pin it.

Classification: `LOW` <40, `MEDIUM` 40–69.99, `HIGH` 70–100.

Implemented as a genuine Spark job — explicit `StructType` schema (no `inferSchema`), a
`Window` function ranked by score within each company partition to pick the top five,
and a second window to pick each company's top-3 categories by frequency. Nothing here
runs through pandas.

Against the reference numbers in `DATA_DICTIONARY.md` (`received: 150, valid: 132,
rejected: 8, duplicate: 10, companies: 24`), this pipeline produces exactly those five
numbers — see `outputs/sample_output/ingest_summary.json`.

## 6. Deduplication strategy

Per the brief, `event_id` is not a reliable key (3 of 10 duplicates keep the original ID,
7 get a new one, and some records have no ID at all). Instead, two events are treated as
the same underlying incident if they share:

- the same company (matched aggressively — lowercased, legal suffixes like `Ltd.`/`Pvt
  Ltd` stripped, punctuation and whitespace collapsed)
- the same canonical category
- the same normalized description (lowercased, punctuation stripped, whitespace
  collapsed)

Within a duplicate group, the record with the most complete optional fields
(`event_id`, `country`, `source`) is kept; ties keep the first-seen record. This
preserves the more informative copy instead of an arbitrary one.

A related but separate problem: two events can refer to the same company under
different formatting *without* being duplicates (different incidents, e.g. "kestrel
airlines" and "Kestrel Airlines" reporting two unrelated stories). Left alone this
double-counts companies. `canonicalize_company_names()` runs after validation and before
dedup: it groups every event by the same aggressive company key used for dedup matching
and rewrites `company_name` to the most common formatting seen for that company, so
company-level aggregation counts them as one. This is what brought
`companies_processed` from 28 down to the reference's 24.

## 7. Assumptions

- Low-confidence records (0.05–0.24) are kept, not filtered — the scoring formula
  already multiplies by `confidence`, so they sink on their own rather than needing a
  separate threshold.
- `country` and `source` normalization is a best-effort alias table (`USA`/`U.S.A.`/
  `usa` → `United States`, etc.) covering the variants actually observed in the data; it
  is not an exhaustive country-name normalizer.
- Category mapping is an explicit lookup table of every variant seen in the data
  dictionary, not a fuzzy matcher — an unrecognized category is rejected rather than
  guessed at.
- `/ingest` results are cached in memory per process; restarting the API loses the
  cache until `/ingest` is called again (see §8).

## 8. Known limitations

- **In-memory cache, not a database.** Fine for a single-process demo; a restart drops
  all state. There's no persistence layer (SQLite/Postgres) as suggested for Level 2.
- **No auth, no rate limiting.** Every endpoint is open.
- **Single-node Spark (`local[*]`).** Appropriate for 150 records; wouldn't need to
  change for a real cluster except the `.master()` config.
- **Output writes go to timestamped `outputs/run_<timestamp>/` directories rather than
  one fixed path.** This was a deliberate fix, not the original design: Spark's
  `overwrite` mode clears the target directory by deleting its contents first, and the
  sandbox this was built in doesn't allow deleting previously-written files (only
  overwriting them) — repeat `/ingest` calls to a fixed path failed on the second run.
  Writing each run to its own directory sidesteps that and, as a side effect, keeps a
  history of every run instead of clobbering the last one. `outputs/latest_run.json` is
  a small pointer file (overwritten, not recreated) that always names the most recent
  run's directory.
- **Country/category normalization tables are closed lists.** New unseen variants would
  need a code change, not just data.

## 9. What I'd improve with more time

- Persist ingest results (SQLite would be enough) instead of an in-memory cache, and add
  incremental ingestion (only re-score new/changed records).
- Structured JSON logging instead of the current human-readable format, for real log
  aggregation.
- A confidence-weighted "why this score" breakdown per company in `/companies/{name}`
  (which events drove the top-5 average).
- Docker Compose for the API + mock data source together.
- Fuzzy category/country matching (e.g. `rapidfuzz`) instead of closed lookup tables, to
  handle variants not seen in this specific dataset.

## AI tools used

Built with Claude (Anthropic) as a pair-programming collaborator: scaffolding the module
layout, writing the initial implementation of each stage, and iterating based on test
failures (e.g. the CSV writer rejecting the `top_categories` array column, and the
company-count mismatch traced back to needing `canonicalize_company_names`). All output
was reviewed, run against the real data, and checked against the reference summary in
`DATA_DICTIONARY.md` before being treated as done — happy to walk through any part of it
or make a live change on request.
