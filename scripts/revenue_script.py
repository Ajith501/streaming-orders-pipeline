from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder \
    .appName("RevenueBatch") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "/opt/airflow/dags/warehouse") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

print("Spark started")

# Read from Iceberg
orders_df = spark.read.format("iceberg").load("my_catalog.default.orders_iceberg").alias("o")
payments_df = spark.read.format("iceberg").load("my_catalog.default.payments_iceberg").alias("p")

print("Orders count:", orders_df.count())
print("Payments count:", payments_df.count())
orders_df.select("order_id").distinct().show(10)
payments_df.select("order_id").distinct().show(10)

# Join
joined_df = orders_df.join(
    payments_df,
    on="order_id",
    how="inner"
)
joined_df.select("order_id").distinct().show(10)
print("Joined count:", joined_df.count())
# Filter success payments
success_payments = joined_df.filter(col("p.status") == "success")

# Aggregate revenue
revenue = success_payments.agg(
    sum(col("p.amount")).alias("total_revenue")
)

revenue.show()

# Write to Iceberg (not parquet)
revenue.writeTo("my_catalog.default.revenue_iceberg") \
    .createOrReplace()