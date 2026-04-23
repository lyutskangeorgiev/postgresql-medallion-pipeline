CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.customer_order_summary;

CREATE TABLE gold.customer_order_summary (
	customer_id TEXT PRIMARY KEY,
	full_name TEXT,
	email TEXT,
	customer_segment TEXT,
	loyalty_tier TEXT,
	loyalty_points INTEGER,
	city TEXT,
	country_code TEXT,
	signup_date DATE,
	customer_age_days INTEGER,
	total_orders BIGINT,
	completed_orders BIGINT,
	cancelled_orders BIGINT,
	shipped_orders BIGINT,
	pending_orders BIGINT,
	total_revenue NUMERIC,
	avg_order_value NUMERIC,
	max_order_value NUMERIC,
	min_order_value NUMERIC,
	first_order_date DATE,
	last_order_date DATE,
	active_months BIGINT,
	completion_rate NUMERIC,
	revenue_per_day NUMERIC,
	revenue_rank INTEGER
);