TRUNCATE TABLE silver.orders;

WITH
-- Step 1: Deduplication:
------------------------------------------------------------------------------------------
orders_rows AS (
	SELECT order_id, customer_id, product_id, order_date, quantity, unit_price, 
	order_status, shipping_city, bronze_ingest_ts,
	ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY bronze_ingest_ts DESC) as rn
	FROM bronze.orders
),

orders_deduplicated AS (
	SELECT order_id, customer_id, product_id, order_date, quantity, unit_price, 
	order_status, shipping_city, bronze_ingest_ts
	FROM orders_rows
	WHERE rn = 1
),

-- Step 2: Date Conversion:
------------------------------------------------------------------------------------------
orders_date_conversed AS (
	SELECT order_id, customer_id, product_id, 
	CASE 
		WHEN order_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND order_date != '9999-99-99'
		THEN CAST(order_date AS DATE)
		ELSE NULL 
	END AS order_date,
	quantity, unit_price, 
	order_status, shipping_city, bronze_ingest_ts
	FROM orders_deduplicated
),

-- Step 3: Numeric Conversion:
------------------------------------------------------------------------------------------
orders_numeric_conversed AS (
	SELECT order_id, customer_id, product_id, order_date, 
	CASE 
		WHEN quantity ~ '^[0-9]+$' THEN CAST (quantity AS INTEGER)
		ELSE NULL 
	END AS quantity,
	CASE 
		WHEN unit_price ~ '^[0-9]+(\.[0-9]+)?$' THEN CAST(unit_price AS NUMERIC)
		ELSE NULL 
	END AS unit_price, 
	order_status, shipping_city, bronze_ingest_ts
	FROM orders_date_conversed
),

-- Step 4: Status Standardisation + Shipping City Whitespace Trimming:
------------------------------------------------------------------------------------------
orders_status_standardized AS (
	SELECT order_id, customer_id, product_id, order_date, quantity, unit_price, 
	UPPER(TRIM(order_status)) AS order_status, TRIM(shipping_city) AS shipping_city, bronze_ingest_ts
	FROM orders_numeric_conversed
),

-- Step 5: Derived Column: order_total:
------------------------------------------------------------------------------------------
orders_with_order_total AS (
	SELECT order_id, customer_id, product_id, order_date, quantity, unit_price,
	CASE 
		WHEN quantity IS NULL OR unit_price IS NULL THEN NULL
		ELSE quantity * unit_price 
	END AS order_total, 
	order_status, shipping_city, bronze_ingest_ts
	FROM orders_status_standardized
),

-- Step 6: Validation Flag:
------------------------------------------------------------------------------------------
orders_with_quarantine_reason AS (
	SELECT order_id, customer_id, product_id, order_date, quantity, unit_price,
	order_total, order_status, shipping_city, bronze_ingest_ts,
	CASE
		WHEN customer_id IS NULL THEN 'Missing Customer'
		WHEN order_date IS NULL THEN 'Missing Order Date'
		WHEN unit_price IS NULL THEN 'Missing Price Per Unit'
		WHEN quantity IS NULL OR quantity < 1 THEN 'Missing/Invalid Quantity'
		ELSE NULL
	END AS quarantine_reason
	FROM orders_with_order_total
),

orders_final AS (
	SELECT order_id, customer_id, product_id, order_date, quantity, unit_price,
	order_total, order_status, shipping_city, bronze_ingest_ts,
	CASE
		WHEN quarantine_reason IS NULL THEN TRUE
		ELSE FALSE
	END AS is_valid,
	quarantine_reason
	FROM orders_with_quarantine_reason
)

-- Load Transformed Data in orders:
------------------------------------------------------------------------------------------
INSERT INTO silver.orders (
	order_id, customer_id, product_id, order_date, quantity, unit_price,
	order_total, order_status, shipping_city, is_valid, quarantine_reason, bronze_ingest_ts
)
SELECT order_id, customer_id, product_id, order_date, quantity, unit_price,
order_total, order_status, shipping_city, is_valid, quarantine_reason, bronze_ingest_ts 
FROM orders_final;