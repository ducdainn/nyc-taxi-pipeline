"""
Great Expectations checkpoint runner for Bronze layer validation.

Validates bronze.yellow_trips table in DuckDB.

Usage:
    python gx/run_checkpoint.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
except ImportError:
    logger.error("great_expectations not installed. Run: pip install great-expectations")
    sys.exit(1)

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
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/data/processed/nyc_taxi.duckdb")
MAX_SAMPLE_ROWS = os.getenv("MAX_SAMPLE_ROWS", 10000)

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
    """Load Bronze data from DuckDB with sampling for memory efficiency."""
    
    if not Path(DUCKDB_PATH).exists():
        raise FileNotFoundError(f"DuckDB file not found: {DUCKDB_PATH}")
    
    logger.info("Loading Bronze data from DuckDB: {}", DUCKDB_PATH)
    
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    
    total_rows = conn.execute("SELECT COUNT(*) FROM bronze.yellow_trips").fetchone()[0]
    logger.info("Total rows in bronze.yellow_trips: {:,}", total_rows)
    
    if total_rows > MAX_SAMPLE_ROWS:
        logger.info("Sampling {:,} rows for validation (from {:,} total)", MAX_SAMPLE_ROWS, total_rows)
        df = conn.execute(f"""
            SELECT * FROM bronze.yellow_trips 
            USING SAMPLE {MAX_SAMPLE_ROWS}
        """).fetchdf()
    else:
        df = conn.execute("SELECT * FROM bronze.yellow_trips").fetchdf()
    
    conn.close()
    
    logger.info("Loaded {:,} rows for validation", len(df))
    return df


def build_expectations(validator) -> None:
    """Build expectation suite for Bronze layer."""
    
    required_columns = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
        "PULocationID", "DOLocationID", "payment_type", "fare_amount",
        "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "Airport_fee",
        "_ingested_at", "_source_file", "_year", "_month"
    ]
    
    for col in required_columns:
        validator.expect_column_to_exist(col)
    
    validator.expect_column_values_to_not_be_null("tpep_pickup_datetime")
    validator.expect_column_values_to_not_be_null("tpep_dropoff_datetime")
    validator.expect_column_values_to_not_be_null("trip_distance")
    validator.expect_column_values_to_not_be_null("fare_amount")
    validator.expect_column_values_to_not_be_null("total_amount")
    validator.expect_column_values_to_not_be_null("PULocationID")
    validator.expect_column_values_to_not_be_null("DOLocationID")
    
    validator.expect_column_values_to_be_between(
        "fare_amount", min_value=-500, max_value=500, mostly=0.99
    )
    validator.expect_column_values_to_be_between(
        "trip_distance", min_value=0, max_value=100, mostly=0.99
    )
    validator.expect_column_values_to_be_between(
        "total_amount", min_value=-1000, max_value=1000, mostly=0.99
    )
    
    validator.expect_column_values_to_be_in_set(
        "payment_type", value_set=[0, 1, 2, 3, 4, 5, 6], mostly=0.99
    )
    
    validator.expect_table_row_count_to_be_between(
        min_value=1000, max_value=MAX_SAMPLE_ROWS + 10000
    )


def run_checkpoint() -> bool:
    """Run GX checkpoint and return success status."""
    
    logger.info("Starting Great Expectations validation")
    
    df = load_bronze_data()
    
    context = gx.get_context()
    
    datasource = context.sources.add_or_update_pandas("bronze_datasource")
    data_asset = datasource.add_dataframe_asset(name="yellow_trips")
    
    batch_request = data_asset.build_batch_request(dataframe=df)
    
    expectation_suite_name = "bronze_yellow_trips_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    
    build_expectations(validator)
    
    validator.save_expectation_suite(discard_failed_expectations=False)
    
    checkpoint = context.add_or_update_checkpoint(
        name="bronze_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    )
    
    result = checkpoint.run()
    
    success = result.success
    
    if success:
        logger.success("Great Expectations validation PASSED")
    else:
        logger.error("Great Expectations validation FAILED")
        for validation_result in result.list_validation_results():
            stats = validation_result.statistics
            logger.error(
                "Stats: evaluated={} successful={} unsuccessful={}",
                stats.get("evaluated_expectations", 0),
                stats.get("successful_expectations", 0),
                stats.get("unsuccessful_expectations", 0),
            )
            
            for result_item in validation_result.results:
                if not result_item.success:
                    exp_config = result_item.expectation_config
                    exp_type = exp_config.expectation_type
                    column = exp_config.kwargs.get("column", "Unknown")
                    logger.warning(
                        "FAILED RULE: Type='{}' | Column='{}' | Details={}", 
                        exp_type, column, result_item.result
                    )
    
    return success


def main() -> None:
    try:
        success = run_checkpoint()
        sys.exit(0 if success else 1)
    except FileNotFoundError as exc:
        logger.error("Validation failed: {}", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Validation failed: {}", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
