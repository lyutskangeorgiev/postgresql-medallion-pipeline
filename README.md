# PostgreSQL Medallion Data Pipeline

This repository contains a complete Data Engineering pipeline implementing the **Medallion Architecture** (Bronze, Silver, Gold) entirely within **PostgreSQL**. 

The project simulates an end-to-end ELT (Extract, Load, Transform) process for an e-commerce platform. It starts with intentionally messy raw data and progressively refines it through data quality checks, standardization, and aggregation to produce business-ready KPIs.

## Architecture Overview

The pipeline is divided into distinct schemas representing the different states of data maturity:

* **Raw / Source (`raw` schema):** Simulates the upstream source systems (CRM and E-commerce DB). It generates "dirty" data containing duplicates, inconsistent formats, and missing values. It also includes scripts to export this data to CSV format to simulate file-based ingestion.
* ** Bronze Layer (`bronze` schema):** The landing zone. Data is ingested exactly as it exists in the source (either via direct SQL inserts or `COPY` from CSVs) and appended with an ingestion timestamp (`bronze_ingest_ts`).
* ** Silver Layer (`silver` schema):** The cleansed and conformed layer. This step features heavy transformations using Common Table Expressions (CTEs), including:
  * **Deduplication:** Keeping only the most recent records based on the ingestion timestamp.
  * **Standardization:** Normalizing phone numbers (RegEx), standardizing multi-format dates, and fixing text casing.
  * **Data Quality & Quarantining:** Instead of dropping bad data, invalid records (e.g., missing customer IDs, negative quantities) are flagged with `is_valid = FALSE` and assigned a `quarantine_reason`.
* ** Gold Layer (`gold` schema):** The business presentation layer. Highly denormalized tables built for BI and analytics. It aggregates lifetime customer metrics, calculates KPIs (completion rate, revenue per day), and ranks customers by total revenue.

## Execution Order

To run the pipeline from start to finish, execute the SQL files in the following exact order. 

*(Note: The pipeline supports dual ingestion patterns, allowing you to simulate both direct SQL database transfers and CSV file loads).*

### 1. Source Data Generation
1. `pipeline/raw/src_customers.sql` - *Creates and populates the raw mock customer data.*
2. `pipeline/raw/src_orders.sql` - *Creates and populates the raw mock order data.*
3. `pipeline/raw/export_to_csv.sql` - *Exports the raw tables to CSV (adjust file paths in the script to match your local machine).*

### 2. Bronze Ingestion
*(You can run both methods to test the different ingestion patterns)*
4. `pipeline/bronze/customers_from_sql.sql`
5. `pipeline/bronze/orders_from_sql.sql`
6. `pipeline/bronze/customers_from_csv.sql`
7. `pipeline/bronze/orders_from_csv.sql`

### 3. Silver Transformation
8. `pipeline/silver/create_customers.sql` - *Initializes the Silver schema and customer table structure.*
9. `pipeline/silver/create_orders.sql` - *Initializes the Silver schema and order table structure.*
10. `pipeline/silver/customers.sql` - *Executes the cleaning, validation, and transformation logic for customers.*
11. `pipeline/silver/orders.sql` - *Executes the cleaning, validation, and transformation logic for orders.*

### 4. Gold Aggregation
12. `pipeline/gold/create_customer_order_summary.sql` - *Initializes the Gold schema and summary table structure.*
13. `pipeline/gold/customer_order_summary.sql` - *Aggregates Silver data into final business metrics and rankings.*

## Prerequisites
* **PostgreSQL** installed and running.
* A SQL client (e.g., pgAdmin, DBeaver, or psql).
* If testing the CSV ingestion, ensure the PostgreSQL server has read/write permissions to the directory specified in the `export_to_csv.sql` and `*_from_csv.sql` files.
