from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *
from pyspark.sql.functions import window, sum

spark = SparkSession.builder \
    .appName("KafkaSparkStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", IntegerType()),
    StructField("status", StringType()),
    StructField("event_time", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING) as json_string")

parsed_df = value_df.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp("event_time")
)
orders_query = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/opt/airflow/dags/output/checkpoints/orders") \
    .trigger(once=True) \
    .start("/opt/airflow/dags/output/orders")
revenue = parsed_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window("event_time", "1 minute")
    ).agg(
        sum("amount").alias("total_revenue")
    )

revenue_query = revenue.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/opt/airflow/dags/output/checkpoints/revenue") \
    .trigger(once=True) \
    .start("/opt/airflow/dags/output/revenue")


orders_query.awaitTermination()
revenue_query.awaitTermination()