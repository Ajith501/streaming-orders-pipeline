FROM apache/airflow:2.8.1

USER root

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-17-jdk curl

# Install Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    | tar -xz -C /opt/

ENV SPARK_HOME=/opt/spark-3.4.1-bin-hadoop3
ENV PATH="${PATH}:${SPARK_HOME}/bin"

USER airflow