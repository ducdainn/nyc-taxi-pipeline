"""
/// filepath: gx/run_checkpoint.py
Great Expectations checkpoint for Bronze layer validation.

Usage:
    python gx/run_checkpoint.py

Output:
    HTML validation report at gx/uncommitted/data_docs/local_site/index.html
"""

from __future__ import annotations

import glob
import os
import sys
import webbrowser
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

import great_expectations as gx

# ── Config ──────────────────────────────────────────────────────────
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GX_DIR = Path(__file__).resolve().parent

# Resolve BRONZE_PATH: prefer env var, fall back to PROJECT_ROOT-relative path.
# .env has container paths (/opt/data/processed) which don't exist on the host,
# so we check existence and fall back to the local dev path.
_env_processed = os.getenv("PROCESSED_DATA_PATH", "")
_local_processed = str(PROJECT_ROOT / "data" / "processed")
PROCESSED_DATA_PATH = (
    _env_processed if _env_processed and Path(_env_processed).exists() else _local_processed
)
BRONZE_PATH = Path(PROCESSED_DATA_PATH) / "bronze" / "yellow_trips"

# All required columns in the Bronze schema
REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "_ingested_at",
    "_source_file",
    # _year and _month are Spark partition columns (encoded in directory names,
    # not stored inside the parquet files), so we skip them here.
]


def load_bronze_data() -> pd.DataFrame:
    """Load Bronze parquet data into pandas DataFrame.

    Spark writes partitions as _year=YYYY/_month=M which pyarrow treats
    as hidden directories. We glob for *.parquet files explicitly.
    """
    if not BRONZE_PATH.exists():
        raise FileNotFoundError(f"Bronze data not found at {BRONZE_PATH}")

    logger.info("Loading Bronze data from {}", BRONZE_PATH)

    parquet_files = sorted(
        glob.glob(str(BRONZE_PATH / "**" / "*.parquet"), recursive=True)
    )
    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found under {BRONZE_PATH}")

    logger.info("Found {} parquet part-files", len(parquet_files))
    df = pd.concat(
        [pd.read_parquet(f, engine="pyarrow") for f in parquet_files],
        ignore_index=True,
    )
    logger.info("Loaded {} rows, {} columns", len(df), len(df.columns))
    return df


def build_expectations(validator) -> None:
    """Define all expectations for the Bronze yellow_trips data."""

    # ── 1. Column existence ─────────────────────────────────────────
    for col in REQUIRED_COLUMNS:
        validator.expect_column_to_exist(col)

    # ── 2. Not-null checks (critical columns) ──────────────────────
    validator.expect_column_values_to_not_be_null(
        "tpep_pickup_datetime", mostly=0.99
    )
    validator.expect_column_values_to_not_be_null("trip_distance", mostly=0.99)
    validator.expect_column_values_to_not_be_null("fare_amount", mostly=0.99)
    validator.expect_column_values_to_not_be_null("total_amount", mostly=0.99)
    validator.expect_column_values_to_not_be_null("PULocationID", mostly=0.99)
    validator.expect_column_values_to_not_be_null("DOLocationID", mostly=0.99)

    # ── 3. Value ranges ─────────────────────────────────────────────
    validator.expect_column_values_to_be_between(
        "fare_amount", min_value=-50, max_value=500, mostly=0.95
    )
    validator.expect_column_values_to_be_between(
        "trip_distance", min_value=0, max_value=100, mostly=0.95
    )
    validator.expect_column_values_to_be_between(
        "total_amount", min_value=-50, max_value=1000, mostly=0.95
    )
    validator.expect_column_values_to_be_between(
        "passenger_count", min_value=0, max_value=9, mostly=0.95
    )

    # ── 4. Accepted value sets ──────────────────────────────────────
    validator.expect_column_values_to_be_in_set(
        "payment_type", [0, 1, 2, 3, 4, 5, 6], mostly=0.99
    )
    validator.expect_column_values_to_be_in_set(
        "RatecodeID", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 99.0], mostly=0.95
    )
    validator.expect_column_values_to_be_in_set(
        "store_and_fwd_flag", ["Y", "N", None], mostly=0.95
    )

    # ── 5. Row count ────────────────────────────────────────────────
    validator.expect_table_row_count_to_be_between(
        min_value=1000, max_value=10_000_000
    )

    # Save the suite (keep all expectations even if some fail during definition)
    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("Expectation suite saved with {} expectations", len(REQUIRED_COLUMNS) + 12)


def run() -> bool:
    """Run the full GX Bronze validation pipeline."""
    # ── Create GX context ───────────────────────────────────────────
    context = gx.get_context(context_root_dir=str(GX_DIR))

    # ── Load data ───────────────────────────────────────────────────
    df = load_bronze_data()

    # ── Setup datasource + asset ────────────────────────────────────
    datasource = context.sources.add_or_update_pandas(name="bronze_pandas")
    try:
        asset = datasource.get_asset("yellow_trips")
    except LookupError:
        asset = datasource.add_dataframe_asset(name="yellow_trips")
    batch_request = asset.build_batch_request(dataframe=df)

    # ── Create expectation suite ────────────────────────────────────
    context.add_or_update_expectation_suite("bronze_yellow_trips")

    # ── Get validator + build expectations ──────────────────────────
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="bronze_yellow_trips",
    )
    build_expectations(validator)

    # ── Validate ────────────────────────────────────────────────────
    logger.info("Running validation...")
    result = validator.validate()

    # ── Build HTML data docs ────────────────────────────────────────
    context.build_data_docs()

    docs_path = GX_DIR / "uncommitted" / "data_docs" / "local_site" / "index.html"
    logger.info("Data docs generated at: {}", docs_path)

    # ── Report results ──────────────────────────────────────────────
    stats = result.statistics
    logger.info(
        "Results: {}/{} passed | {:.1f}% success rate",
        stats["successful_expectations"],
        stats["evaluated_expectations"],
        stats["success_percent"],
    )

    if result.success:
        logger.success(
            "Bronze validation PASSED — all {} expectations met!",
            stats["successful_expectations"],
        )
    else:
        logger.error(
            "Bronze validation FAILED — {} of {} expectations failed",
            stats["unsuccessful_expectations"],
            stats["evaluated_expectations"],
        )
        # Log individual failures
        for r in result.results:
            if not r.success:
                logger.warning(
                    "  FAILED: {} | column={}",
                    r.expectation_config.expectation_type,
                    r.expectation_config.kwargs.get("column", "N/A"),
                )

    return result.success


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)

