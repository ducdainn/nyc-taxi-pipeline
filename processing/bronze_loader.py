"""
Bronze loader: Load raw TLC Parquet → DuckDB Bronze schema.

Usage:
    python processing/bronze_loader.py --year 2025 --month 1

Output:
    DuckDB bronze.yellow_trips table with year/month partition columns
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/opt/data/raw")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/data/processed/nyc_taxi.duckdb")
TLC_TAXI_TYPE = os.getenv("TLC_TAXI_TYPE", "yellow")


def build_raw_path(year: int, month: int, taxi_type: str) -> Path:
    return Path(RAW_DATA_PATH) / taxi_type / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def load_bronze(year: int, month: int, taxi_type: str | None = None) -> dict:
    """
    Load raw TLC Parquet into DuckDB Bronze schema using PyArrow.
    
    - Reads raw parquet via pyarrow.parquet.read_table
    - Adds metadata columns: _ingested_at, _source_file, _year, _month
    - Writes to DuckDB bronze.yellow_trips table
    """
    taxi_type = taxi_type or TLC_TAXI_TYPE
    raw_path = build_raw_path(year, month, taxi_type)

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    logger.info("Starting Bronze load | raw_path={} year={} month={}", raw_path, year, month)

    start_time = datetime.now()

    table = pq.read_table(raw_path)
    input_count = table.num_rows
    logger.info("Raw file loaded via PyArrow | rows={:,}", input_count)

    duckdb_dir = Path(DUCKDB_PATH).parent
    duckdb_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DUCKDB_PATH))

    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS bronze.yellow_trips (
            VendorID INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            RatecodeID BIGINT,
            store_and_fwd_flag VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            payment_type BIGINT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            congestion_surcharge DOUBLE,
            Airport_fee DOUBLE,
            _ingested_at TIMESTAMP,
            _source_file VARCHAR,
            _year INTEGER,
            _month INTEGER
        )
    """)

    conn.execute(f"""
        DELETE FROM bronze.yellow_trips 
        WHERE _year = {year} AND _month = {month}
    """)
    logger.info("Cleared existing data for year={} month={}", year, month)

    ingested_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    source_file = raw_path.name

    conn.register('raw_data', table)

    conn.execute(f"""
        INSERT INTO bronze.yellow_trips
        SELECT 
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee,
            '{ingested_at}'::TIMESTAMP as _ingested_at,
            '{source_file}' as _source_file,
            {year} as _year,
            {month} as _month
        FROM raw_data
    """)

    output_count = conn.execute(f"""
        SELECT COUNT(*) FROM bronze.yellow_trips 
        WHERE _year = {year} AND _month = {month}
    """).fetchone()[0]

    conn.close()

    duration = (datetime.now() - start_time).total_seconds()

    if input_count == output_count:
        logger.success(
            "Reconciliation PASSED | input={:,} output={:,} diff=0 duration={:.2f}s",
            input_count, output_count, duration
        )
    else:
        logger.warning(
            "Reconciliation MISMATCH | input={:,} output={:,} diff={:,}",
            input_count, output_count, input_count - output_count
        )

    stats = {
        "input_count": input_count,
        "output_count": output_count,
        "year": year,
        "month": month,
        "duration_seconds": duration,
    }

    logger.info("Bronze load complete | stats={}", stats)
    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load raw TLC Parquet into DuckDB Bronze schema.")
    parser.add_argument("--year", type=int, required=True, help="Data year (e.g., 2025)")
    parser.add_argument("--month", type=int, required=True, help="Data month (1-12)")
    parser.add_argument(
        "--taxi-type", type=str, default=None,
        choices=["yellow", "green", "fhv", "fhvhv"],
        help=f"Taxi type (default: {TLC_TAXI_TYPE})"
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
        stats = load_bronze(year=args.year, month=args.month, taxi_type=args.taxi_type)
        logger.info("Final stats: {}", stats)
    except FileNotFoundError as exc:
        logger.error("Bronze load failed | {}", exc)
        sys.exit(1)
    except Exception as exc:
        logger.exception("Bronze load failed | {}", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()


