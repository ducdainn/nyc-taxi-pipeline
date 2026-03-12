# 🚕 NYC Taxi Analytics Pipeline

> **Modern ELT Pipeline** | Medallion Architecture | Docker Compose | Airflow + dbt + DuckDB + Metabase

![Pipeline Status](https://img.shields.io/badge/status-production--ready-brightgreen)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Airflow](https://img.shields.io/badge/airflow-2.9.3-red)
![dbt](https://img.shields.io/badge/dbt-1.8.8-orange)
![DuckDB](https://img.shields.io/badge/duckdb-1.1.3-yellow)

---

## 📋 Overview

An end-to-end **Modern ELT pipeline** ingesting NYC TLC Yellow Taxi public data (~3M rows/month), processing through a **Medallion architecture** (Bronze → Silver → Gold), and serving analytics via a **Metabase dashboard**.

All services run locally via Docker Compose, mirroring cloud architecture patterns:
- **Snowflake** → DuckDB (columnar OLAP)
- **S3** → Local volume
- **dbt Cloud** → dbt-core
- **MWAA** → Apache Airflow

### 🎯 Key Features

| Feature | Implementation |
|---------|---------------|
| **Medallion Architecture** | Bronze (raw) → Silver (cleaned) → Gold (analytics-ready) |
| **Incremental Processing** | dbt incremental models on `fact_trips` |
| **Data Quality** | Great Expectations + dbt tests |
| **Orchestration** | Airflow DAG with TaskGroups |
| **Visualization** | Metabase connected directly to DuckDB |
| **Documentation** | dbt docs with lineage graph |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           NYC Taxi Analytics Pipeline                        │
└─────────────────────────────────────────────────────────────────────────────┘

[TLC Website — HTTP Parquet]
         │
         ▼
┌─────────────────────────┐
│  scripts/ingest_tlc.py  │  ← Monthly download, idempotent, retry logic
└─────────────────────────┘
         │ raw .parquet → data/raw/yellow/
         ▼
┌───────────────────────────────┐
│  processing/bronze_loader.py  │  ← PyArrow schema enforcement + metadata
└───────────────────────────────┘
         │ → DuckDB bronze schema
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DuckDB                                          │
│  ┌─────────────┐    ┌─────────────────┐    ┌──────────────────────────────┐ │
│  │   BRONZE    │ →  │     SILVER      │ →  │            GOLD              │ │
│  │             │    │                 │    │                              │ │
│  │ yellow_trips│    │ stg_yellow_trips│    │ fact_trips (INCREMENTAL)     │ │
│  │             │    │ stg_taxi_zones  │    │ dim_zones                    │ │
│  │             │    │                 │    │ dim_datetime                 │ │
│  └─────────────┘    └─────────────────┘    └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
         │                    ▲
         │                    │ dbt build
         ▼                    │
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Great Expectations                                 │
│                    Bronze layer validation checkpoint                        │
└─────────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Metabase                                        │
│                    Direct DuckDB JDBC connection                             │
│              (No Postgres export — DuckDB is single source)                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Apache Airflow                                     │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                    nyc_taxi_pipeline DAG                             │   │
│   │                                                                      │   │
│   │  [ingest_tlc] → [bronze_load] → [gx_validate] → [dbt_transform]     │   │
│   │                                                                      │   │
│   │  Schedule: 0 6 5 * * (5th of each month, 6AM UTC)                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│   PostgreSQL: Airflow metadata ONLY (no business data)                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Cloud Equivalent |
|-------|------|------------------|
| **Extraction** | Python + requests | AWS Lambda / Cloud Run |
| **Processing** | Python + Pandas + PyArrow | AWS Glue (small-scale) |
| **Storage (OLAP)** | DuckDB | Snowflake / BigQuery |
| **Storage (Meta)** | PostgreSQL | RDS (Airflow metadata only) |
| **Transformation** | dbt-duckdb | dbt Cloud |
| **Data Quality** | Great Expectations + dbt tests | Monte Carlo / Soda |
| **Orchestration** | Apache Airflow 2.9 | MWAA / Cloud Composer |
| **Visualization** | Metabase + DuckDB driver | Looker / Superset |
| **Infrastructure** | Docker Compose | Terraform + ECS/GKE |

> **Engineering Note:** PySpark was evaluated and removed. PyArrow/Pandas handles ~3M rows in seconds locally with zero cluster overhead. Spark would be appropriate at 50M+ rows or in a distributed environment.

---

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── dags/
│   └── nyc_taxi_pipeline.py        # Main Airflow DAG
├── scripts/
│   └── ingest_tlc.py               # HTTP download TLC parquet
├── processing/
│   └── bronze_loader.py            # PyArrow: raw parquet → DuckDB Bronze
├── dbt_nyc/
│   ├── models/
│   │   ├── bronze/                 # Source declarations
│   │   ├── silver/                 # stg_yellow_trips, stg_taxi_zones
│   │   └── gold/                   # fact_trips (INCREMENTAL), dim_*
│   ├── seeds/
│   │   └── taxi_zones.csv          # Static zone lookup
│   ├── tests/                      # Custom singular tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── gx/
│   ├── great_expectations.yml
│   ├── run_checkpoint.py           # GX validation entry point
│   └── expectations/
│       └── bronze_yellow_trips.json
├── config/
│   └── init-db.sh                  # PostgreSQL init script
├── docker/
│   ├── airflow.Dockerfile          # Custom Airflow image
│   └── metabase.Dockerfile         # Custom Metabase image
├── docs/
│   └── screenshots/                # Dashboard + lineage screenshots
├── data/                           # git-ignored
│   ├── raw/yellow/                 # yellow_tripdata_YYYY-MM.parquet
│   └── processed/
│       └── nyc_taxi.duckdb         # Single DuckDB file (all schemas)
├── metabase-plugins/               # DuckDB driver for Metabase
├── .env.example
├── .gitignore
├── Makefile
├── requirements.txt
├── docker-compose.yml
├── PROJECT_PLAN.md
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (4GB+ RAM allocated)
- Git
- Make (optional, for convenience commands)

### 1. Clone & Setup

```bash
git clone https://github.com/yourusername/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

# Copy environment template
cp .env.example .env

# Generate security keys
make generate-keys
# Copy the output keys to .env file
```

### 2. Start Services

```bash
# Start all containers
make up

# Or without Make:
docker compose up -d

# Wait for all services to be healthy (~2-3 minutes)
docker compose ps
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Metabase** | http://localhost:3000 | (setup on first visit) |

### 4. Trigger Pipeline

```bash
# Option 1: Via Airflow UI
# Go to http://localhost:8080 → DAGs → nyc_taxi_pipeline → Trigger

# Option 2: Via CLI
docker compose exec airflow-webserver airflow dags trigger nyc_taxi_pipeline
```

### 5. Connect Metabase to DuckDB

1. Open http://localhost:3000
2. Complete initial setup
3. **Admin** → **Databases** → **Add Database**
4. Select **DuckDB**
5. Database file: `/opt/data/processed/nyc_taxi.duckdb`
6. Leave MotherDuck token empty
7. Click **Save**

---

## 📊 Data Model

### Bronze Layer (Raw)
- `bronze.yellow_trips` — Raw TLC data with metadata columns

### Silver Layer (Cleaned)
- `silver.stg_yellow_trips` — Filtered, cast, deduplicated trips
- `silver.stg_taxi_zones` — Zone lookup from seed

### Gold Layer (Analytics)
- `gold.fact_trips` — **INCREMENTAL** fact table with all trip metrics
- `gold.dim_zones` — Zone dimension with borough groupings
- `gold.dim_datetime` — Time dimension with rush hour flags

### Key Metrics Available
- `total_amount` — Total fare including tips and surcharges
- `trip_distance` — Miles traveled
- `trip_duration_minutes` — Duration in minutes
- `fare_per_mile` — Calculated efficiency metric
- `tip_percentage` — Tip as percentage of fare

---

## 🔧 Development Commands

```bash
# Start all services
make up

# Stop all services
make down

# View logs (all services)
make logs

# View logs (specific service)
make logs s=airflow-webserver

# Full reset (destroy volumes, rebuild)
make reset

# Re-initialize Airflow
make init-airflow

# Generate security keys
make generate-keys
```

### Manual Component Runs

```bash
# Run ingestion
docker compose exec airflow-webserver python /opt/airflow/scripts/ingest_tlc.py --year 2025 --month 1

# Run bronze loader
docker compose exec airflow-webserver python /opt/airflow/processing/bronze_loader.py --year 2025 --month 1

# Run dbt
docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt_nyc && dbt build --profiles-dir /opt/airflow/dbt_nyc"

# Run Great Expectations
docker compose exec airflow-webserver python /opt/airflow/gx/run_checkpoint.py

# View dbt docs
docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt_nyc && dbt docs generate && dbt docs serve --port 8081"
```

---

## 📈 Dashboard Charts

The Metabase dashboard includes:

1. **Monthly Revenue Trend** — Line chart showing `SUM(total_amount)` by month
2. **Top 10 Pickup Zones** — Horizontal bar chart by trip count
3. **Hourly Demand Heatmap** — Trip count by hour × day of week

---

## ✅ Data Quality

### Great Expectations (Bronze Layer)
- Column existence validation
- Null checks on critical columns
- Value range validation (fare: 0-500, distance: 0-100)
- Payment type accepted values
- Row count bounds per monthly load

### dbt Tests (Silver/Gold Layers)
- `not_null` — Critical columns
- `unique` — Primary keys
- `accepted_values` — Categorical columns
- `relationships` — Foreign key integrity

---

## 🔄 Pipeline Schedule

| Component | Schedule | Description |
|-----------|----------|-------------|
| **DAG** | `0 6 5 * *` | 5th of each month, 6AM UTC |
| **Retries** | 2 | With 5-minute delay |
| **Catchup** | Enabled | Backfill historical months |

---

## 🐛 Troubleshooting

### Containers not starting
```bash
# Check logs
docker compose logs

# Reset everything
make reset
```

### Airflow DAG not visible
```bash
# Check for import errors
docker compose exec airflow-webserver airflow dags list-import-errors
```

### DuckDB connection issues in Metabase
- Ensure path is `/opt/data/processed/nyc_taxi.duckdb`
- Leave MotherDuck token empty
- Check Metabase logs: `docker compose logs metabase`

### Out of Memory errors
- Increase Docker Desktop memory allocation (4GB+ recommended)
- The pipeline uses sampling for GX validation to prevent OOM

---

## 📚 References

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [dbt Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt DuckDB Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup)
- [Great Expectations](https://docs.greatexpectations.io)
- [Airflow TaskGroups](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups)
- [Metabase DuckDB Driver](https://github.com/MotherDuck-Open-Source/metabase-duckdb-driver)

---

## 📄 License

MIT License — See [LICENSE](LICENSE) for details.

---

<p align="center">
  Built with ❤️ for the Data Engineering community
</p>


