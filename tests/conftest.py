import os

# Tests run against the bundled events_api.json instead of requiring the
# mock_api.py HTTP server to be running -- keeps `pytest` a one-command,
# no-setup affair. The API-fetch path itself (pagination/retry/timeout) is
# exercised separately and manually against a live `python data/mock_api.py`
# (see README -> Setup).
os.environ.setdefault("SIGNALWATCH_USE_JSON_FALLBACK", "1")

import pytest  # noqa: E402

from app.processing.spark_processing import stop_spark  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _shutdown_spark_after_suite():
    yield
    stop_spark()
