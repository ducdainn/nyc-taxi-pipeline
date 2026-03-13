## filepath: docker/metabase.Dockerfile
# Use Ubuntu-based image instead of Alpine for better glibc compatibility with DuckDB
FROM eclipse-temurin:11-jre-jammy

# Install required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download Metabase JAR
ARG METABASE_VERSION=v0.49.6
RUN mkdir -p /app /plugins \
    && curl -fSL -o /app/metabase.jar \
       "https://downloads.metabase.com/${METABASE_VERSION}/metabase.jar"

# Download DuckDB driver for Metabase from MotherDuck (official)
# Using version 0.2.10 which is compatible with Metabase 0.49.x
ARG DUCKDB_DRIVER_VERSION=0.2.10
RUN curl -fSL -o /plugins/duckdb.metabase-driver.jar \
    "https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/${DUCKDB_DRIVER_VERSION}/duckdb.metabase-driver.jar" \
    && ls -la /plugins/

WORKDIR /app

EXPOSE 3000

ENV MB_PLUGINS_DIR=/plugins
ENV JAVA_OPTS="-Xmx2g -Djava.awt.headless=true"

CMD ["java", "-jar", "/app/metabase.jar"]
