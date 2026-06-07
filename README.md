# 🚀 Kafka Spark Iceberg Data Pipeline

## 📌 Overview

This project implements an end-to-end real-time data pipeline using Apache Kafka, Spark Structured Streaming, Apache Iceberg, and Airflow.

It simulates a production-grade system where order and payment events are ingested, processed, stored in a lakehouse architecture, and used for downstream analytics such as revenue calculation and storage optimization.

---

## 🏗️ System Architecture

```
            +----------------------+
            |   Event Producers    |
            | (Orders & Payments)  |
            +----------+-----------+
                       |
                       v
            +----------------------+
            |      Kafka Topics    |
            |  orders | payments   |
            +----------+-----------+
                       |
                       v
        +----------------------------------+
        |  Spark Structured Streaming      |
        |  (Trigger Once Micro-batching)   |
        +----------+-----------+-----------+
                   |           |
                   v           v
     +----------------+   +----------------+
     | Orders Iceberg |   | Payments Iceberg|
     |     Table      |   |     Table       |
     +--------+-------+   +--------+--------+
              |                    |
              +---------+----------+
                        |
                        v
             +----------------------+
             |  Revenue Batch Job   |
             |  (Join + Aggregate)  |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | Revenue Iceberg Table|
             +----------+-----------+
                        |
                        v
             +----------------------+
             |   Compaction Job     |
             | (rewrite_data_files) |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | Optimized Data Files |
             +----------------------+

        (Orchestrated via Airflow DAG)
```

---

## 🧭 Architecture Explanation

- Kafka ingests real-time order and payment events  
- Spark Structured Streaming processes data in micro-batches  
- Data is stored in Apache Iceberg tables (ACID compliant)  
- A batch job calculates revenue by joining orders and payments  
- Compaction optimizes small files created by streaming  
- Airflow orchestrates the entire pipeline  

---

## ⚙️ Key Features

### 🔹 Real-Time Streaming
- Multi-topic ingestion (orders, payments)
- Fault-tolerant processing with checkpointing
- Micro-batch execution using trigger once

---

### 🔹 Lakehouse Storage (Apache Iceberg)
- ACID-compliant tables
- Snapshot-based versioning
- Scalable data storage

---

### 🔹 Batch + Streaming Hybrid
- Streaming ingestion layer
- Batch analytics layer
- Real-world hybrid architecture

---

### 🔹 Revenue Analytics
- Join orders and payments
- Filter successful transactions
- Aggregate total revenue

---

### 🔹 Data Optimization
- Handles small file problem
- Uses Iceberg compaction (rewrite_data_files)
- Improves query performance

---

### 🔹 Workflow Orchestration (Airflow)
- DAG-based execution
- Task flow:
  Orders → Payments → Revenue → Compaction
- Scheduled pipeline runs

---

## 🛠️ Tech Stack

- Apache Kafka (Redpanda)
- Apache Spark (PySpark)
- Apache Iceberg
- Apache Airflow
- Docker

---

## 📊 Data Flow

1. Producers generate order and payment events  
2. Events are pushed to Kafka topics  
3. Spark reads from Kafka and writes to Iceberg  
4. Revenue job joins Iceberg tables  
5. Compaction optimizes file storage  
6. Airflow orchestrates execution  

---

## 🧠 Key Challenges & Learnings

- Fixed environment mismatch (local vs Docker paths)
- Resolved Iceberg metadata issues
- Handled Kafka offset inconsistencies
- Debugged join misalignment causing null revenue
- Solved small file problem using compaction
- Managed streaming + batch orchestration in Airflow

---

## 🚀 How to Run

1. Start Docker services (Kafka, Airflow, Spark)
2. Run event producers
3. Trigger Airflow DAG
4. Monitor Iceberg tables and revenue output

---

## 📈 Future Improvements

- Add partitioning (event_time)
- Implement data quality checks
- Add monitoring and alerting
- Extend analytics use cases

---

## 🎯 Resume Summary

Built an end-to-end real-time data pipeline using Kafka, Spark Structured Streaming, Apache Iceberg, and Airflow; implemented multi-topic ingestion, revenue aggregation, and compaction to optimize storage and performance.

---

## 💬 Author

This project simulates a real-world data engineering pipeline with streaming ingestion, lakehouse storage, and production-level debugging and optimization.

---

## ⭐ Highlights

- Real-time data processing
- Lakehouse architecture
- Distributed system debugging
- Performance optimization using compaction
- End-to-end pipeline orchestration