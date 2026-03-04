"""
/// filepath: dags/nyc_taxi_pipeline.py
Airflow DAG: NYC Taxi Analytics Pipeline

Orchestrates the full ELT flow:
  ingest → spark bronze → GX validate → dbt seed → dbt silver → dbt gold → dbt test

Schedule: 5th of each month at 6 AM UTC
  (TLC publishes previous-month data by the 5th)

Jinja context:
  logical_date = start of the data interval
  → logical_date.year / .month gives the month we want to process
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# ── Default args ────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Paths inside the Airflow container ──────────────────────────────
SCRIPTS_DIR = "/opt/airflow/scripts"
SPARK_JOBS_DIR = "/opt/airflow/spark_jobs"
GX_DIR = "/opt/airflow/gx"
DBT_DIR = "/opt/airflow/dbt_nyc"
DBT_PROFILES_DIR = "/opt/airflow/dbt_nyc"

# Override dbt bronze_path for container mount layout
DBT_VARS = '{"bronze_path": "/opt/data/processed/bronze/yellow_trips/**/*.parquet"}'

# ── Jinja templates — resolved at runtime by Airflow ────────────────
# Schedule "0 6 5 * *" runs on 5th of each month to process PREVIOUS month's data
# Use macros.datetime to subtract 1 month from execution_date
# e.g., execution_date = 2025-02-05 → process Jan 2025 data
PREV_MONTH = "{{ execution_date - macros.timedelta(days=execution_date.day) }}"
YEAR_TMPL = "{{ (execution_date - macros.timedelta(days=execution_date.day)).year }}"
MONTH_TMPL = "{{ (execution_date - macros.timedelta(days=execution_date.day)).month }}"

# ── Failure callback ────────────────────────────────────────────────
def _on_failure(context):
    """Log task failure details to Airflow logger."""
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    exec_date = context["logical_date"]
    exception = context.get("exception", "N/A")
    print(
        f"[ALERT] Task FAILED | dag={dag_id} task={task_id} "
        f"date={exec_date} error={exception}"
    )


# ── DAG definition ──────────────────────────────────────────────────
with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC Taxi ELT: ingest → bronze → validate → silver → gold → test",
    schedule="0 6 5 * *",
    start_date=datetime(2025, 2, 5),  # Start from Feb 2025 to process Jan 2025 data
    catchup=True,
    max_active_runs=1,
    tags=["nyc-taxi", "elt", "medallion"],
    doc_md=__doc__,
    on_failure_callback=_on_failure,
) as dag:

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TaskGroup: Ingestion  (download → spark bronze → GX validate)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with TaskGroup(
        "ingestion",
        tooltip="Download raw data → Spark Bronze → GX validation",
    ) as ingestion_group:

        ingest_tlc = BashOperator(
            task_id="ingest_tlc",
            bash_command=(
                f"python {SCRIPTS_DIR}/ingest_tlc.py "
                f"--year {YEAR_TMPL} --month {MONTH_TMPL}"
            ),
            doc="Download TLC parquet file for the given year/month.",
        )

        spark_bronze = BashOperator(
            task_id="spark_bronze",
            bash_command=(
                f"python {SPARK_JOBS_DIR}/bronze_loader.py "
                f"--year {YEAR_TMPL} --month {MONTH_TMPL}"
            ),
            env={
                "SPARK_MASTER": "local[*]",  # local mode inside Airflow worker
            },
            append_env=True,
            doc="Run PySpark Bronze loader: schema enforcement + metadata cols.",
        )

        gx_validate = BashOperator(
            task_id="gx_validate_bronze",
            bash_command=f"python {GX_DIR}/run_checkpoint.py",
            doc="Great Expectations validation on Bronze layer.",
        )

        ingest_tlc >> spark_bronze >> gx_validate

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TaskGroup: Transform  (dbt seed → silver → gold → test)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    with TaskGroup(
        "transform",
        tooltip="dbt seed → Silver → Gold → tests",
    ) as transform_group:

        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=(
                f"cd {DBT_DIR} && dbt seed "
                f"--profiles-dir {DBT_PROFILES_DIR} "
                f"--vars '{DBT_VARS}'"
            ),
            doc="Load CSV seeds (taxi_zones) into DuckDB.",
        )

        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            bash_command=(
                f"cd {DBT_DIR} && dbt run --select silver "
                f"--profiles-dir {DBT_PROFILES_DIR} "
                f"--vars '{DBT_VARS}'"
            ),
            doc="Build Silver layer: stg_yellow_trips, stg_taxi_zones.",
        )

        dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=(
                f"cd {DBT_DIR} && dbt run --select gold "
                f"--profiles-dir {DBT_PROFILES_DIR} "
                f"--vars '{DBT_VARS}'"
            ),
            doc="Build Gold layer: fact_trips (incremental), dim_zones, dim_datetime.",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                f"cd {DBT_DIR} && dbt test "
                f"--profiles-dir {DBT_PROFILES_DIR} "
                f"--vars '{DBT_VARS}'"
            ),
            doc="Run all dbt tests: not_null, unique, accepted_values, relationships.",
        )

        dbt_seed >> dbt_run_silver >> dbt_run_gold >> dbt_test

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Pipeline flow: ingestion → transform
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ingestion_group >> transform_group

