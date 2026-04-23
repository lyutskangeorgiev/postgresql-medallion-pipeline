TRUNCATE TABLE silver.customers CASCADE;

WITH
-- Step 1 Deduplication:
------------------------------------------------------------------------------------------
customers_rows AS (
	SELECT customer_id, first_name, last_name, email, phone, country, city, signup_date, 
	customer_segment, loyalty_points, bronze_ingest_ts,
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY bronze_ingest_ts DESC) AS rn
	FROM bronze.customers
),

customers_deduplicated AS (
	SELECT customer_id, first_name, last_name, email, phone, country, city, signup_date, 
	customer_segment, loyalty_points, bronze_ingest_ts
	FROM customers_rows
	WHERE rn = 1
),
-- Step 2 Name Standardisation:
------------------------------------------------------------------------------------------
customers_name_standardized AS (
	SELECT customer_id, INITCAP(TRIM(first_name)) AS first_name, INITCAP(TRIM(last_name)) AS last_name, 
	email, phone, country, city, signup_date, 
	customer_segment, loyalty_points, bronze_ingest_ts
	FROM customers_deduplicated
),

-- Step 3 Email Validation Flag + Email Lower Cased and Trimmed:
------------------------------------------------------------------------------------------
customers_email_is_valid AS (
	SELECT customer_id, first_name, last_name, LOWER(TRIM(email)) AS email, 
	CASE 
		WHEN email ~ '^.+@.+\..+$' THEN TRUE
		ELSE FALSE
	END AS is_valid_email,
	phone, country, city, signup_date, customer_segment, loyalty_points, bronze_ingest_ts
	FROM customers_name_standardized
),

-- Step 4 Phone Normalisation:
------------------------------------------------------------------------------------------
customers_phone_normalized AS (
	SELECT customer_id, first_name, last_name, email, is_valid_email, regexp_replace (phone, '\D', '', 'g') AS phone, 
	country, city, signup_date, customer_segment, loyalty_points, bronze_ingest_ts
	FROM customers_email_is_valid
),

-- Step 5 Country Code Standardisation + City Trimmed:
------------------------------------------------------------------------------------------
customers_country_code_standardized AS (
	SELECT customer_id, first_name, last_name, email, is_valid_email, phone, 
	CASE
		WHEN UPPER(TRIM(country)) IN ('USA', 'UNITED STATES', 'U.S.', 'US') THEN 'US'
		ELSE country
	END AS country,
	TRIM(city) AS city, signup_date, customer_segment, loyalty_points, bronze_ingest_ts
	FROM customers_phone_normalized
),

-- Step 6 Multi-Format Date Conversion:
------------------------------------------------------------------------------------------
customers_date_conversed AS (
	SELECT customer_id, first_name, last_name, email, is_valid_email, phone, country, city,
	CASE
		--YYYY-MM--DD
		WHEN signup_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN CAST(signup_date AS DATE)
		--DD/MM/YYYY
		WHEN signup_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN TO_DATE(signup_date, 'DD/MM/YYYY')
		--Mon DD YYYY
		WHEN LOWER(signup_date) ~ '^[a-z]{3} [0-9]{2} [0-9]{4}$' THEN TO_DATE(signup_date, 'Mon DD YYYY')
		ELSE NULL
	END AS signup_date,
	customer_segment, loyalty_points, bronze_ingest_ts
	FROM customers_country_code_standardized
),

-- Step 7 NULL Defaults:
------------------------------------------------------------------------------------------
customers_nulls_defaulted AS (
	SELECT customer_id, first_name, last_name, email, is_valid_email, phone, country, city, signup_date,
	CASE
		WHEN customer_segment IS NULL THEN 'UNKNOWN'
		ELSE UPPER(customer_segment)
	END AS customer_segment,
	CASE
		WHEN loyalty_points IS NULL THEN 0
		ELSE CAST(loyalty_points AS INTEGER) 
	END AS loyalty_points,
	bronze_ingest_ts
	FROM customers_date_conversed
),

-- Step 8 Derived Columns:
------------------------------------------------------------------------------------------
customers_derived_columns AS (
	SELECT customer_id, first_name, last_name,
	CONCAT(first_name, ' ', last_name) as full_name, email, is_valid_email, phone, country,
	city, signup_date, customer_segment, loyalty_points,
	CASE
		WHEN loyalty_points >= 1000 THEN 'Gold'
		WHEN loyalty_points < 1000 AND loyalty_points >= 500 THEN 'Silver'
		ELSE 'Bronze'
	END AS loyalty_tier,
	bronze_ingest_ts
	FROM customers_nulls_defaulted
)

-- Load Transformed Data in customers:
------------------------------------------------------------------------------------------
INSERT INTO silver.customers (
	customer_id, first_name, last_name, full_name, email, is_valid_email, phone, 
	country_code, city, signup_date, customer_segment, loyalty_points, loyalty_tier, bronze_ingest_ts
)

SELECT customer_id, first_name, last_name, full_name, email, is_valid_email, phone, country,
	city, signup_date, customer_segment, loyalty_points, loyalty_tier, bronze_ingest_ts
FROM customers_derived_columns