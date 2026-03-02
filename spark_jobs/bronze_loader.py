"""
/// filepath: spark_jobs/bronze_loader.py
PySpark job: Load raw TLC Parquet → Bronze layer (partitioned Parquet for DuckDB).

Usage (standalone):
    spark-submit spark_jobs/bronze_loader.py --year 2025 --month 1

Usage (from Airflow SparkSubmitOperator):
    application="spark_jobs/bronze_loader.py"
    application_args=["--year", "2025", "--month", "1"]

Output:
    data/processed/bronze/yellow_trips/_year=2025/_month=01/*.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Config ──────────────────────────────────────────────────────────
load_dotenv()

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/opt/data/raw")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "/opt/data/processed")
TLC_TAXI_TYPE = os.getenv("TLC_TAXI_TYPE", "yellow")

# ── Yellow Taxi Schema (TLC official) ───────────────────────────────
# https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),         # Giữ Integer (INT32)
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),     
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),        
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),        
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])


def get_spark_session(app_name: str = "bronze_loader") -> SparkSession:
    """Create or get existing SparkSession."""
    builder = SparkSession.builder.appName(app_name)

    if SPARK_MASTER.startswith("spark://"):
        builder = builder.master(SPARK_MASTER)
    else:
        builder = builder.master("local[*]")

    return builder.getOrCreate()


def build_raw_path(year: int, month: int, taxi_type: str) -> Path:
    """Build path to raw parquet file."""
    return Path(RAW_DATA_PATH) / taxi_type / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def build_bronze_path(taxi_type: str) -> Path:
    """Build path to bronze output directory."""
    return Path(PROCESSED_DATA_PATH) / "bronze" / f"{taxi_type}_trips"


def load_bronze(
    year: int,
    month: int,
    taxi_type: str | None = None,
) -> dict:
    """
    Load raw TLC Parquet into Bronze layer.

    - Enforces schema (casts columns to correct types)
    - Adds metadata columns: _ingested_at, _source_file, _year, _month
    - Writes partitioned Parquet to bronze layer
    - Returns reconciliation stats

    Args:
        year: Data year (e.g., 2025)
        month: Data month (1-12)
        taxi_type: Taxi type (default from env)

    Returns:
        dict with input_count, output_count, partition_path
    """
    taxi_type = taxi_type or TLC_TAXI_TYPE
    raw_path = build_raw_path(year, month, taxi_type)
    bronze_path = build_bronze_path(taxi_type)

    # ── Validate input exists ───────────────────────────────────────
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    logger.info("Starting Bronze load | raw_path={} year={} month={}", raw_path, year, month)

    # ── Create Spark session ────────────────────────────────────────
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Read raw parquet with schema enforcement ────────────────────
    df_raw = spark.read.schema(YELLOW_TAXI_SCHEMA).parquet(str(raw_path))
    input_count = df_raw.count()
    logger.info("Raw file loaded | rows={}", input_count)

    # ── Add metadata columns ────────────────────────────────────────
    ingested_at = datetime.now(timezone.utc).isoformat()
    source_file = raw_path.name

    df_bronze = (
        df_raw
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_year", F.lit(year))
        .withColumn("_month", F.lit(month))
    )

    # ── Write partitioned parquet ───────────────────────────────────
    partition_path = bronze_path / f"_year={year}" / f"_month={month}"
    logger.info("Writing Bronze layer | path={}", bronze_path)

    (
        df_bronze
        .write
        .mode("overwrite")
        .partitionBy("_year", "_month")
        .parquet(str(bronze_path))
    )

    # ── Reconciliation ───────────────────────────────
    df_verify = (
        spark.read.parquet(str(bronze_path))
        .filter(f"_year = {year} AND _month = {month}")
    )
    output_count = df_verify.count()

    # ── Log reconciliation ──────────────────────────────────────────
    if input_count == output_count:
        logger.success(
            "Reconciliation PASSED | input={} output={} diff=0",
            input_count,
            output_count,
        )
    else:
        logger.warning(
            "Reconciliation MISMATCH | input={} output={} diff={}",
            input_count,
            output_count,
            input_count - output_count,
        )

    stats = {
        "input_count": input_count,
        "output_count": output_count,
        "partition_path": str(partition_path),
        "year": year,
        "month": month,
    }

    logger.info("Bronze load complete | stats={}", stats)
    return stats


# ── CLI entrypoint ──────────────────────────────────────────────────
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load raw TLC Parquet into Bronze layer."
    )
    parser.add_argument(
        "--year", type=int, required=True, help="Data year (e.g., 2025)"
    )
    parser.add_argument(
        "--month", type=int, required=True, help="Data month (1-12)"
    )
    parser.add_argument(
        "--taxi-type",
        type=str,
        default=None,
        choices=["yellow", "green", "fhv", "fhvhv"],
        help=f"Taxi type (default: {TLC_TAXI_TYPE} from .env)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if not 1 <= args.month <= 12:
        logger.error("Invalid month: {} — must be 1-12", args.month)
        sys.exit(1)

    if args.year < 2009:
        logger.error("Invalid year: {} — TLC data starts from 2009", args.year)
        sys.exit(1)

    try:
        stats = load_bronze(
            year=args.year,
            month=args.month,
            taxi_type=args.taxi_type,
        )
        logger.info("Final stats: {}", stats)
    except FileNotFoundError as exc:
        logger.error("Bronze load failed | {}", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Bronze load failed | {}", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()


