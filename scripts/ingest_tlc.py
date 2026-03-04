"""
/// filepath: scripts/ingest_tlc.py
Download NYC TLC trip-data Parquet files from the public HTTP endpoint.

Usage (standalone):
    python scripts/ingest_tlc.py --year 2024 --month 1
    python scripts/ingest_tlc.py --year 2024 --month 1 --taxi-type green

Usage (called from Airflow / another module):
    from scripts.ingest_tlc import download_tlc_file
    path = download_tlc_file(year=2024, month=1)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from loguru import logger

# ── Config ──────────────────────────────────────────────────────────
load_dotenv()

TLC_BASE_URL = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")
TLC_TAXI_TYPE = os.getenv("TLC_TAXI_TYPE", "yellow")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw")

MAX_RETRIES = 3
BACKOFF_BASE = 2          # seconds — exponential: 2, 4, 8 …
CHUNK_SIZE = 8 * 1024     # 8 KB streaming chunks
REQUEST_TIMEOUT = 300     # 5 min per request
MIN_FILE_SIZE = 10_000_000  # 10 MB — valid TLC parquet files are typically 50-100 MB


# ── Core logic ──────────────────────────────────────────────────────
def build_url(year: int, month: int, taxi_type: str) -> str:
    """Build the TLC download URL for a given year/month/taxi-type."""
    return f"{TLC_BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def build_dest(year: int, month: int, taxi_type: str) -> Path:
    """Build the local destination path (creates parent dirs if needed)."""
    dest_dir = Path(RAW_DATA_PATH) / taxi_type
    dest_dir.mkdir(parents=True, exist_ok=True)
    return dest_dir / f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def download_tlc_file(
    year: int,
    month: int,
    taxi_type: str | None = None,
) -> Path:
    """
    Download a single TLC Parquet file.

    - Idempotent: skips if the file already exists and is non-empty.
    - Retries up to MAX_RETRIES with exponential backoff.
    - Validates file size > 0 after download.

    Returns the local Path of the downloaded file.
    Raises RuntimeError on permanent failure.
    """
    taxi_type = taxi_type or TLC_TAXI_TYPE
    url = build_url(year, month, taxi_type)
    dest = build_dest(year, month, taxi_type)

    # ── Idempotency check ───────────────────────────────────────────
    if dest.exists():
        file_size = dest.stat().st_size
        if file_size >= MIN_FILE_SIZE:
            logger.info(
                "File already exists — skipping | file={} size={:.1f} MB",
                dest,
                file_size / 1e6,
            )
            return dest
        else:
            logger.warning(
                "Existing file too small (corrupt?) — re-downloading | file={} size={} bytes",
                dest,
                file_size,
            )
            dest.unlink()

    # ── Download with retry ─────────────────────────────────────────
    logger.info("Starting download | url={}", url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            bytes_written = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
                    bytes_written += len(chunk)

            # ── Post-download validation ────────────────────────────
            if bytes_written == 0:
                raise ValueError("Downloaded file is 0 bytes")

            logger.success(
                "Download complete | file={} size={:.1f} MB attempt={}/{}",
                dest,
                bytes_written / 1e6,
                attempt,
                MAX_RETRIES,
            )
            return dest

        except (requests.RequestException, ValueError) as exc:
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Attempt {}/{} failed — retrying in {}s | error={}",
                attempt,
                MAX_RETRIES,
                wait,
                exc,
            )

            # Clean up partial file
            if dest.exists():
                dest.unlink()

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Failed to download {url} after {MAX_RETRIES} attempts: {exc}"
                ) from exc

            time.sleep(wait)

    # unreachable, but keeps mypy happy
    raise RuntimeError(f"Failed to download {url}")


# ── CLI entrypoint ──────────────────────────────────────────────────
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download NYC TLC trip-data Parquet files."
    )
    parser.add_argument(
        "--year", type=int, required=True, help="Data year (e.g. 2025)"
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
        path = download_tlc_file(
            year=args.year,
            month=args.month,
            taxi_type=args.taxi_type,
        )
        logger.info("Output path: {}", path)
    except RuntimeError as exc:
        logger.error("Ingestion failed | {}", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

