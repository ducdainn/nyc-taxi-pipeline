"""
Airflow DAG: NYC Taxi Analytics Pipeline

Orchestrates the full ELT flow:
  ingest → bronze (PyArrow→DuckDB) → GX validate → dbt (seed→silver→gold→test)

Schedule: 5th of each month at 6 AM UTC
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SCRIPTS_DIR = "/opt/airflow/scripts"
PROCESSING_DIR = "/opt/airflow/processing"
GX_DIR = "/opt/airflow/gx"
DBT_DIR = "/opt/airflow/dbt_nyc"
DBT_PROFILES_DIR = "/opt/airflow/dbt_nyc"

YEAR_TMPL = "{{ (execution_date - macros.timedelta(days=execution_date.day + 365)).year }}"
MONTH_TMPL = "{{ (execution_date - macros.timedelta(days=execution_date.day + 365)).month }}"


def _on_failure(context):
    ti = context["task_instance"]
    print(
        f"[ALERT] Task FAILED | dag={ti.dag_id} task={ti.task_id} "
        f"date={context['logical_date']} error={context.get('exception', 'N/A')}"
    )


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC Taxi ELT: ingest → bronze → validate → silver → gold → test",
    schedule="0 6 5 * *",
    start_date=datetime(2026, 1, 5),
    catchup=True,
    max_active_runs=1,
    tags=["nyc-taxi", "elt", "medallion"],
    doc_md=__doc__,
    on_failure_callback=_on_failure,
) as dag:

    with TaskGroup("ingestion", tooltip="Download → Bronze → GX validation") as ingestion_group:

        ingest_tlc = BashOperator(
            task_id="ingest_tlc",
            bash_command=f"python {SCRIPTS_DIR}/ingest_tlc.py --year {YEAR_TMPL} --month {MONTH_TMPL}",
            doc="Download TLC parquet file.",
        )

        bronze_load = BashOperator(
            task_id="bronze_load",
            bash_command=f"python {PROCESSING_DIR}/bronze_loader.py --year {YEAR_TMPL} --month {MONTH_TMPL}",
            doc="Load raw parquet into DuckDB Bronze schema (PyArrow).",
        )

        gx_validate = BashOperator(
            task_id="gx_validate_bronze",
            bash_command=f"python {GX_DIR}/run_checkpoint.py",
            doc="Great Expectations validation on Bronze layer.",
        )

        ingest_tlc >> bronze_load >> gx_validate

    with TaskGroup("transform", tooltip="dbt clean → seed → Silver → Gold → tests") as transform_group:

        dbt_clean = BashOperator(
            task_id="dbt_clean",
            bash_command=f"cd {DBT_DIR} && dbt clean --profiles-dir {DBT_PROFILES_DIR}",
            doc="Clear dbt cache/target.",
        )

        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR}",
            doc="Load CSV seeds (taxi_zones).",
        )

        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            bash_command=f"cd {DBT_DIR} && dbt run --select silver --profiles-dir {DBT_PROFILES_DIR}",
            doc="Build Silver layer.",
        )

        dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=f"cd {DBT_DIR} && dbt run --select gold --profiles-dir {DBT_PROFILES_DIR}",
            doc="Build Gold layer (incremental).",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
            doc="Run dbt tests.",
        )

        dbt_clean >> dbt_seed >> dbt_run_silver >> dbt_run_gold >> dbt_test

    ingestion_group >> transform_group
