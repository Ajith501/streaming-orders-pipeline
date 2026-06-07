
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergPipeline") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "/opt/airflow/dags/warehouse") \
    .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
    .getOrCreate()
spark.sql("SHOW TABLES IN my_catalog.default").show()
spark.sql("SELECT COUNT(*) FROM my_catalog.default.orders_iceberg").show()
spark.sql("SELECT COUNT(*) FROM my_catalog.default.payments_iceberg").show()

print("Iceberg tables created successfully")