from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="kafka_spark_etl",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
) as dag:

    run_orders = BashOperator(
        task_id="run_orders",
        bash_command="""
spark-submit --master local[*] \
--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
/opt/airflow/scripts/orders_stream_iceberg.py
"""
    )
    run_payments = BashOperator(
        task_id="run_payments",
        bash_command="""
spark-submit --master local[*] \
--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
/opt/airflow/scripts/payments_stream_iceberg.py
"""
    )
    run_revenue_script = BashOperator(
        task_id="run_revenue_job",
        bash_command="""
spark-submit --master local[*] \
--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
/opt/airflow/scripts/revenue_script.py
"""
    )

    sleep_task = BashOperator(
    task_id="wait_for_data",
    bash_command="sleep 300"
)
run_compaction = BashOperator(
    task_id="run_compaction",
    bash_command=(
        "spark-submit "
        "--master local[*] "
        "--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 "
        "/opt/airflow/scripts/compaction_job.py"
    )
)
run_orders >> run_payments >> sleep_task  >> run_revenue_script >> run_compaction