CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.orders;

CREATE TABLE bronze.orders (
	order_id TEXT,
	customer_id TEXT,
	product_id TEXT,
	order_date TEXT,
	quantity TEXT,
	unit_price TEXT,
	order_status TEXT,
	shipping_city TEXT,
	bronze_ingest_ts TIMESTAMP DEFAULT NOW()
);

TRUNCATE TABLE bronze.orders;

INSERT INTO bronze.orders (order_id, customer_id, product_id, order_date, quantity, unit_price, order_status, shipping_city)
SELECT order_id, customer_id, product_id, order_date, quantity, unit_price, order_status, shipping_city FROM raw.src_orders