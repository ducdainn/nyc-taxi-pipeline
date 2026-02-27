#!/bin/bash
## filepath: config/init-db.sh
# Creates the Metabase database and user inside the shared PostgreSQL instance.
# Runs automatically on first postgres container start via /docker-entrypoint-initdb.d/

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $MB_DB_USER WITH PASSWORD '$MB_DB_PASS';
    CREATE DATABASE $MB_DB_DBNAME OWNER $MB_DB_USER;
    GRANT ALL PRIVILEGES ON DATABASE $MB_DB_DBNAME TO $MB_DB_USER;
EOSQL

