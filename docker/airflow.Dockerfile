FROM apache/airflow:2.9.3

USER airflow

RUN pip install --no-cache-dir \
    dbt-duckdb==1.8.3 \
    great-expectations==0.18.19 \
    pandas==2.2.3 \
    pyarrow==17.0.0 \
    python-dotenv==1.0.1 \
    loguru==0.7.2 \
    requests==2.32.3 \
    duckdb==1.1.3 \
    sqlalchemy==1.4.53 \
    psycopg2-binary==2.9.9




