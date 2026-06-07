from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("OrdersIcebergStream") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "/opt/airflow/dags/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("payment_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("method", StringType()),
    StructField("status", StringType()),
    StructField("country", StringType()),
    StructField("event_time", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "payments") \
    .option("startingOffsets", "earliest") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING) as json_string")

parsed_df = value_df.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")

# convert timestamp
parsed_df = parsed_df.withColumn(
    "event_time",
    col("event_time").cast("timestamp")
)

query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/airflow/dags/output/checkpoints/payments_iceberg") \
    .option("maxOffsetsPerTrigger", 1000) \
    .trigger(once=True) \
    .start("my_catalog.default.payments_iceberg")

query.awaitTermination()