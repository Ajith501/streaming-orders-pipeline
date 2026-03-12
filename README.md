# Streaming Orders Pipeline

Real-time streaming ETL pipeline using Kafka, Spark Structured Streaming, and Airflow.

## Architecture

Kafka → Spark Streaming → Bronze → Silver → Gold → DuckDB

## Features

- Kafka event ingestion
- Spark Structured Streaming
- Watermark + window aggregations
- Data quality validation
- Airflow orchestration with micro batch ETL
