from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="kafka_spark_etl",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 "
            "/opt/airflow/dags/scripts/kafka_spark_stream.py"
        ),
    )