# 🕵️‍♂️ Real-Time Fraud Detection Engine

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Confluent Kafka](https://img.shields.io/badge/Confluent_Kafka-000000?style=for-the-badge&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)

>An enterprise-grade, event-driven data pipeline built on Databricks to identify fraudulent transactions in real-time and alert customers instantly.

## 📖 Overview

This project implements a robust streaming architecture to detect malicious financial activity. By combining high-throughput transactional data with structured customer profiles, the system evaluates transactions on the fly. When anomalous behavior is detected, an automated alert is triggered and dispatched directly to the affected customer's email.

## 🏗️ Architecture

The pipeline seamlessly merges streaming events with batch-loaded dimension data:

*   **Streaming Ingestion (Transactions):** Live transaction data is ingested continuously via **Confluent Kafka**.
*   **Dimension Data (Customers):** Customer profile data (including email addresses and account status) is stored in **PostgreSQL**.
*   **Data Integration:** Customer data is synchronized into the Databricks ecosystem as a **Slowly Changing Dimension (SCD) Type using **LakeFlow Connect**.
*   **Processing Engine:** **Databricks Structured Streaming** joins the live Kafka streams with the latest SCD Type 1 customer dimension data to evaluate fraud rules in real-time.
*   **Medallion architecture :** Data is organised using delta tables into three progressive layers—Bronze (raw), Silver (cleaned), and Gold (aggregated)
*   **Alerting Mechanism:** Evaluated suspicious records trigger an automated SMTP/API-based email notification to the customer.

## 🚨 Fraud Detection Typologies
>1. **High usage alert**: Whenever there is a transaction value greater than transaction limit for a customer, a custom email notification will be sent to the customer email address.
>2. **Fraud card alert**: Whenever there is a transaction with a card for a card marked as fraud, a custom email notification will be sent to the customer email address.

## 🛠️ Tech Stack & Features

| Component | Technology / Method | Purpose |
| :--- | :--- | :--- |
| **Stream Broker** | Confluent Kafka | High-throughput live transaction event bus |
| **Relational DB** | PostgreSQL | Source of truth for customer dimension data |
| **Data Integration** | Databricks LakeFlow Connect | Automated ingestion of Postgres data (SCD Type 1) |
| **Compute / ETL** | LakeFlow Declarative Pipeline | Stateful stream processing and join logic |
| **Language** | Python PySpark | ELT pipeline design |

## 🚀 Getting Started

### Prerequisites

*   A Databricks Workspace (Free tier will work).
*   Confluent Cloud account with active Kafka Topics.
*   PostgreSQL Database with a `customers` table.
*   Pyhton and VS Code installed in local system

### Setup Instructions

1.  **Initialize the PostgreSQL:**
    * Cretae a customer table in any PostgresSQL provider
    * Use the `customers_historic.sql` to load a cut-over data
    * Use the `customers_incremental.sql` to load a incremental type data
    *   **MUST DO THIS STEP!!!: Run an update query after loading the cut-over data to update the `email` field (otherwise the ELT pipeline will fail when need to send email)**

2. **Initialize the Kafka Topic:**
    *   Make a Confluent account and create a Kafka topic.
    *   Use the bootstrap server, api key, api secrect and the topic name in the Databricks project.
    *   Stream some data to the Kafka topic (code provided in the Kafka producer scripts).

3.  **Create a secret scope:** append the URL `#secrets/createScope` like -> `https://<account>.cloud.databricks.com/#secrets/createScope`

4. Create a Google Account password "for sending emails"  

5.  **Configure Databricks Secrets:** Use the given notebook `secret_scope_put` to put in the secret scopes

6.  **Initialize LakeFlow Connect:**
    *   Navigate to **Jobs & Pipeline** in your Databricks workspace.
    *   Create a new **Ingestion Pipeline** connection to your PostgreSQL database.
    *   Configure the pipeline to sync the `customers` table with an **SCD Type 1** update strategy (History -> off).

7.  **Run the Pipelines**
    * Run the `FinGuard/fraud_watchlist_file_generator/fraud_watchlist_data_generator` to generate fraud watchlist json files
    * Run the `finguard_customer_load` pipeline along with the Ingestion pipeline in a batch Job, configure it to suit your time
    * Download the folder `kafka_producer` and run the `producer_normal.py` to stream data to Kafka and `producer_fraud_transaction.py` occationally to produce fraud type transaction in the Kafka stream. **(DO NOT RUN THESE FROM DATABRICKS)**
    * Run the main pipeline `finguard_streaming` in Continuous mode to stream, ggregate data and alerts in real time.

## 📫 Automated Alerting

When a transaction violates any of the rules defined above, the system fetches the `email_address` from the joined PostgreSQL dimension table and dispatches an alert.
