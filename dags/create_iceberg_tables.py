from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergPipeline") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "/opt/airflow/dags/warehouse") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
    .getOrCreate()

# Create namespace FIRST
spark.sql("CREATE NAMESPACE IF NOT EXISTS my_catalog.default")

# Orders table
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.default.orders_iceberg (
    order_id STRING,
    customer_id STRING,
    amount INT,
    status STRING,
    event_time TIMESTAMP
)
USING iceberg
""")

# Payments table
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.default.payments_iceberg (
    payment_id STRING,
    order_id STRING,
    customer_id STRING,
    amount INT,
    method STRING,
    status STRING,
    country STRING,
    event_time TIMESTAMP
)
USING iceberg
""")

# Validate
spark.sql("SHOW TABLES IN my_catalog.default").show()
spark.sql("SELECT COUNT(*) FROM my_catalog.default.orders_iceberg").show()
spark.sql("SELECT COUNT(*) FROM my_catalog.default.payments_iceberg").show()

print("Iceberg tables created successfully")