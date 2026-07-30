"""PySpark stage: risk scoring and company aggregation.

Takes the cleaned, deduplicated events and does everything with an explicit
Spark schema -- no pandas, no inferSchema. Results are collected back into
plain Python objects at the end so the rest of the app (FastAPI, tests)
doesn't need a live Spark session lying around between requests.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

# Must be set before SparkSession is created; this sandbox's hostname
# doesn't resolve via DNS, which otherwise makes the JVM gateway fail.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from app.models.schemas import CleanEvent  # noqa: E402
from app.utils.logging_config import get_logger  # noqa: E402

logger = get_logger(__name__)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("company_name", T.StringType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("severity", T.IntegerType(), False),
        T.StructField("confidence", T.DoubleType(), False),
        T.StructField("published_at", T.TimestampType(), False),
        T.StructField("country", T.StringType(), True),
        T.StructField("source", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
    ]
)

TOP_N_EVENTS_PER_COMPANY = 5
TOP_N_CATEGORIES_PER_COMPANY = 3

_spark: Optional[SparkSession] = None


def get_spark() -> SparkSession:
    """Lazily create (or reuse) a local SparkSession."""
    global _spark
    if _spark is None:
        logger.info("Starting local SparkSession")
        _spark = (
            SparkSession.builder.master("local[*]")
            .appName("signalwatch")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


def stop_spark() -> None:
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None


def _events_to_dataframe(spark: SparkSession, events: list[CleanEvent]) -> DataFrame:
    rows = [
        (
            e.event_id,
            e.company_name,
            e.category,
            e.severity,
            float(e.confidence),
            e.published_at.astimezone(timezone.utc).replace(tzinfo=None),
            e.country,
            e.source,
            e.description,
        )
        for e in events
    ]
    return spark.createDataFrame(rows, schema=EVENT_SCHEMA)


def score_events(
    events: list[CleanEvent], as_of: Optional[datetime] = None
) -> tuple[list[dict], list[dict]]:
    """Compute event-level and company-level risk scores with PySpark.

    Returns (event_score_dicts, company_score_dicts), both plain Python
    structures ready to hand to pydantic models / JSON serialization.
    """
    if not events:
        return [], []

    as_of = as_of or datetime.now(timezone.utc)
    as_of_naive = as_of.astimezone(timezone.utc).replace(tzinfo=None)

    spark = get_spark()
    df = _events_to_dataframe(spark, events)

    df = df.withColumn("as_of", F.lit(as_of_naive).cast(T.TimestampType()))
    df = df.withColumn("days_old", F.datediff(F.col("as_of"), F.col("published_at")))
    df = df.withColumn(
        "recency_weight",
        F.when(F.col("days_old") <= 7, F.lit(1.0))
        .when(F.col("days_old") <= 30, F.lit(0.8))
        .otherwise(F.lit(0.6)),
    )
    df = df.withColumn(
        "event_risk_score",
        F.round(
            F.least(
                F.lit(100.0),
                F.col("severity") * F.lit(20.0) * F.col("confidence") * F.col("recency_weight"),
            ),
            2,
        ),
    )

    # --- company aggregation: average of each company's top-5 event scores ---
    rank_window = Window.partitionBy("company_name").orderBy(F.col("event_risk_score").desc())
    ranked = df.withColumn("rank", F.row_number().over(rank_window))
    top_n = ranked.filter(F.col("rank") <= TOP_N_EVENTS_PER_COMPANY)

    company_scores_df = top_n.groupBy("company_name").agg(
        F.round(F.avg("event_risk_score"), 2).alias("risk_score")
    )
    company_scores_df = company_scores_df.withColumn(
        "risk_level",
        F.when(F.col("risk_score") >= 70, F.lit("HIGH"))
        .when(F.col("risk_score") >= 40, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    event_counts_df = df.groupBy("company_name").agg(F.count(F.lit(1)).alias("event_count"))

    # top categories per company by frequency
    cat_counts = df.groupBy("company_name", "category").agg(F.count(F.lit(1)).alias("cat_count"))
    cat_window = Window.partitionBy("company_name").orderBy(F.col("cat_count").desc())
    cat_ranked = cat_counts.withColumn("cat_rank", F.row_number().over(cat_window))
    top_categories_df = (
        cat_ranked.filter(F.col("cat_rank") <= TOP_N_CATEGORIES_PER_COMPANY)
        .groupBy("company_name")
        .agg(F.collect_list("category").alias("top_categories"))
    )

    # most frequent country per company (for display in /companies)
    country_counts = df.filter(F.col("country").isNotNull()).groupBy(
        "company_name", "country"
    ).agg(F.count(F.lit(1)).alias("country_count"))
    country_window = Window.partitionBy("company_name").orderBy(F.col("country_count").desc())
    top_country_df = (
        country_counts.withColumn("country_rank", F.row_number().over(country_window))
        .filter(F.col("country_rank") == 1)
        .select("company_name", F.col("country").alias("country"))
    )

    companies = (
        company_scores_df.join(event_counts_df, "company_name")
        .join(top_categories_df, "company_name")
        .join(top_country_df, "company_name", how="left")
        .orderBy(F.col("risk_score").desc())
    )

    event_rows = [
        row.asDict()
        for row in df.select(
            "event_id",
            "company_name",
            "category",
            "severity",
            "confidence",
            "published_at",
            "country",
            "source",
            "description",
            "days_old",
            "recency_weight",
            "event_risk_score",
        ).collect()
    ]
    company_rows = [row.asDict() for row in companies.collect()]

    logger.info(
        "Spark scoring complete: %d events scored, %d companies aggregated",
        len(event_rows),
        len(company_rows),
    )
    return event_rows, company_rows


def write_outputs(
    event_rows: list[dict], company_rows: list[dict], output_dir: str
) -> None:
    """Persist results as JSON, CSV and Parquet, per the brief's requirement.

    Callers should pass a fresh, run-specific directory (see
    app.services.pipeline, which suffixes this with a UTC timestamp) rather
    than a fixed path that gets reused across runs. Spark's "overwrite" mode
    clears the target directory by deleting its contents first, which fails
    on filesystems/mounts that don't allow unlinking previously-written
    files (this sandbox's outputs mount is one such case) -- writing each
    run to its own directory sidesteps that entirely, and as a side effect
    keeps a history of every ingest run instead of clobbering the last one.
    """
    os.makedirs(output_dir, exist_ok=True)
    spark = get_spark()

    events_df = spark.createDataFrame(event_rows) if event_rows else None
    companies_df = spark.createDataFrame(company_rows) if company_rows else None

    if events_df is not None:
        events_df.coalesce(1).write.mode("overwrite").json(
            os.path.join(output_dir, "events_scored_json")
        )
        events_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            os.path.join(output_dir, "events_scored_csv")
        )
        events_df.coalesce(1).write.mode("overwrite").parquet(
            os.path.join(output_dir, "events_scored_parquet")
        )

    if companies_df is not None:
        companies_df.coalesce(1).write.mode("overwrite").json(
            os.path.join(output_dir, "company_scores_json")
        )
        # CSV has no array type -- flatten top_categories to a delimited
        # string for this format only; JSON/Parquet keep the real array.
        companies_csv_df = companies_df.withColumn(
            "top_categories", F.concat_ws("; ", F.col("top_categories"))
        )
        companies_csv_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            os.path.join(output_dir, "company_scores_csv")
        )
        companies_df.coalesce(1).write.mode("overwrite").parquet(
            os.path.join(output_dir, "company_scores_parquet")
        )

    logger.info("Wrote pipeline outputs (json/csv/parquet) to %s", output_dir)
