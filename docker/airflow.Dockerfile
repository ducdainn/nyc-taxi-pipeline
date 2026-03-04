## filepath: docker/airflow.Dockerfile
FROM apache/airflow:2.9.3

USER root

# ── Install OpenJDK 17 (required for PySpark) ─────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jdk-headless \
       procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Set JAVA_HOME ─────────────────────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow



