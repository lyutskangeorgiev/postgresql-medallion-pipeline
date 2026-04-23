CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.orders;

CREATE TABLE silver.orders (
	order_id TEXT PRIMARY KEY,
	customer_id TEXT,	
	product_id TEXT,
	order_date DATE,
	quantity INTEGER,
	unit_price NUMERIC,
	order_total NUMERIC,
	order_status TEXT,
	shipping_city TEXT,
	is_valid BOOlEAN,
	quarantine_reason TEXT,
	bronze_ingest_ts TIMESTAMP,

	CONSTRAINT fk_customer_id
		FOREIGN KEY (customer_id) REFERENCES silver.customers(customer_id)
);