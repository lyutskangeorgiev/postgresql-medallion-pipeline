CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.customers;

CREATE TABLE bronze.customers (
	customer_id TEXT,
	first_name TEXT,
	last_name TEXT,
	email TEXT,
	phone TEXT,
	country TEXT,
	city TEXT,
	signup_date TEXT,
	customer_segment TEXT,
	loyalty_points TEXT,
	bronze_ingest_ts TIMESTAMP DEFAULT NOW()
);

TRUNCATE TABLE bronze.customers;

COPY bronze.customers (
	customer_id,
	first_name,
	last_name,
	email,
	phone,
	country,
	city,
	signup_date,
	customer_segment,
	loyalty_points
)
FROM 'C:/Projects/Medallion_Project/pipeline/raw/src_customers_csv.csv' WITH (FORMAT CSV, HEADER);

