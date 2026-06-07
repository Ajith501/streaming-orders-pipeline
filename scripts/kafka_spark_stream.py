from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaOrdersStream") \
    .getOrCreate()



spark.sparkContext.setLogLevel("ERROR")

# Schema
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("status", StringType()),
    StructField("event_time", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert to string
value_df = df.selectExpr("CAST(value AS STRING) as json_string")

# Parse JSON
parsed_df = value_df.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")

# Fix timestamp (IMPORTANT)
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp("event_time")
)

# Write to parquet (ONLY orders)
query = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/opt/airflow/dags/output/checkpoints/orders") \
    .trigger(once=True) \
    .start("/opt/airflow/dags/output/orders")

query.awaitTermination()