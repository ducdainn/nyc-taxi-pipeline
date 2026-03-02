import duckdb

con = duckdb.connect('data/duckdb/nyc_taxi.duckdb')

con.execute("""
    CREATE OR REPLACE VIEW yellow_bronze AS 
    SELECT * FROM read_parquet('data/processed/bronze/yellow_trips/*/*/*.parquet')
""")

print(con.execute("SELECT VendorID, tpep_pickup_datetime, total_amount FROM yellow_bronze LIMIT 5").df())
print(f"Tổng số dòng trong DuckDB: {con.execute('SELECT count(*) FROM yellow_bronze').fetchone()[0]}")