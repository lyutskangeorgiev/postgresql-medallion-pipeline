CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.customers CASCADE;

CREATE TABLE silver.customers (
	customer_id TEXT PRIMARY KEY,
	first_name TEXT,
	last_name TEXT,
	full_name TEXT,
	email TEXT,
	is_valid_email BOOLEAN,
	phone TEXT,
	country_code TEXT,
	city TEXT,
	signup_date DATE,
	customer_segment TEXT,
	loyalty_points INTEGER,
	loyalty_tier TEXT,
	bronze_ingest_ts TIMESTAMP
);