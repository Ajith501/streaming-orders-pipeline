from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergCompaction") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "/opt/airflow/dags/warehouse") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    ) \
    .getOrCreate()

print("Starting compaction job...")

# Orders compaction
spark.sql("""
CALL my_catalog.system.rewrite_data_files(
    table => 'default.orders_iceberg'
)
""")

print("Orders compaction done")

# Payments compaction
spark.sql("""
CALL my_catalog.system.rewrite_data_files(
    table => 'default.payments_iceberg'
)
""")

print("Payments compaction done")

# Revenue compaction (if you created table)
spark.sql("""
CALL my_catalog.system.rewrite_data_files(
    table => 'default.revenue_iceberg'
)
""")

print("Revenue compaction done")