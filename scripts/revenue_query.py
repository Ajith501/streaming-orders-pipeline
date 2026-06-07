import duckdb

duckdb.sql("""
SELECT *
FROM '/Users/ajit/airflow-etl/dags/output/revenue/*.parquet'

LIMIT 100
""").show()