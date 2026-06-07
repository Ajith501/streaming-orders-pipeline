# Kafka Spark Iceberg Data Pipeline

## Overview
End-to-end real-time data pipeline using Kafka, Spark Structured Streaming, Apache Iceberg, and Airflow.

## Architecture
Kafka → Spark → Iceberg → Airflow → Revenue Aggregation → Compaction

## Features
- Multi-topic streaming (orders & payments)
- Micro-batch processing using trigger-once
- Iceberg tables for ACID and scalability
- Revenue aggregation pipeline
- Compaction to handle small file problem

## Tech Stack
- Apache Kafka (Redpanda)
- Apache Spark (PySpark)
- Apache Iceberg
- Apache Airflow
- Docker

## Key Learnings
- Handling streaming + batch hybrid pipelines
- Debugging Iceberg metadata issues
- Managing Kafka offsets and checkpoints
- Solving small file problem using compaction