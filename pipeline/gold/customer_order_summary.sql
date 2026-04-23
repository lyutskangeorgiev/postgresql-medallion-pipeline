TRUNCATE TABLE gold.customer_order_summary;

WITH

-- Step 1: Aggregation In orders
------------------------------------------------------------------------------------------
orders_aggregated AS (
	SELECT customer_id, COUNT(order_id) AS total_orders, 
	COUNT(CASE
		WHEN order_status = 'COMPLETED' THEN order_status
		ELSE NULL
	END) AS completed_orders,
	COUNT(CASE
		WHEN order_status = 'CANCELLED' THEN order_status
		ELSE NULL
	END) AS cancelled_orders,
	COUNT(CASE
		WHEN order_status = 'SHIPPED' THEN order_status
		ELSE NULL
	END) AS shipped_orders,
	COUNT(CASE
		WHEN order_status = 'PENDING' THEN order_status
		ELSE NULL
	END) AS pending_orders,	
	ROUND(SUM(CASE
		WHEN is_valid = TRUE THEN order_total
		ELSE 0
	END), 2) AS total_revenue,
	ROUND(AVG(order_total), 2) AS avg_order_value, 
	MAX(order_total) AS max_order_value,
	MIN(order_total) AS min_order_value, 
	MIN(order_date) AS first_order_date, 
	MAX(order_date) AS last_order_date,
	COUNT(DISTINCT(CASE
		WHEN order_id IS NOT NULL THEN DATE_TRUNC('month', order_date)
		ELSE NULL
	END)) AS active_months
	FROM silver.orders
	WHERE is_valid = TRUE
	GROUP BY customer_id
),

-- Step 2 Derived Business Metrics in customers
------------------------------------------------------------------------------------------
customers_business_metrics AS (
	SELECT customer_id, full_name, email, country_code, city, signup_date,
	(CURRENT_DATE - signup_date) AS customer_age_days,
	customer_segment, loyalty_points, loyalty_tier
	FROM silver.customers
),


final_business_metrics AS (
	SELECT cbm.customer_id, oa.total_orders, oa.completed_orders, oa.cancelled_orders, oa.shipped_orders, oa.pending_orders, 
	cbm.full_name, cbm.email, cbm.country_code, cbm.city, cbm.signup_date, cbm.customer_age_days,
	cbm.customer_segment, cbm.loyalty_points, cbm.loyalty_tier,
	CASE
		WHEN oa.total_orders != 0 THEN ROUND((((oa.completed_orders * 1.0) / oa.total_orders) * 100), 1)
		ELSE 0
	END AS completion_rate,
	CASE
		WHEN cbm.customer_age_days != 0 AND oa.total_revenue != 0 THEN ROUND(((oa.total_revenue * 1.0) / cbm.customer_age_days), 4)
		ELSE 0
	END AS revenue_per_day,
	oa.total_revenue, oa.avg_order_value, oa.max_order_value, oa.min_order_value, oa.first_order_date, oa.last_order_date, oa.active_months
	FROM customers_business_metrics cbm
	LEFT JOIN orders_aggregated oa ON oa.customer_id = cbm.customer_id
),

-- Step 3 Ranked By Revenue
------------------------------------------------------------------------------------------
final_with_revenue_ranked AS (
	SELECT customer_id, total_orders, completed_orders, cancelled_orders, shipped_orders, pending_orders, 
	full_name, email, country_code, city, signup_date, customer_age_days,
	customer_segment, loyalty_points, loyalty_tier, completion_rate, revenue_per_day,
	total_revenue, RANK() OVER(ORDER BY COALESCE(total_revenue, 0) DESC) AS revenue_rank,
	avg_order_value, max_order_value, min_order_value, first_order_date, last_order_date, active_months
	FROM final_business_metrics
)

-- Load The Data In The Final Table
------------------------------------------------------------------------------------------
INSERT INTO gold.customer_order_summary (
	customer_id, full_name, email, customer_segment, loyalty_tier, loyalty_points, city,
	country_code, signup_date, customer_age_days, total_orders, completed_orders, cancelled_orders, 
	shipped_orders, pending_orders, total_revenue, avg_order_value, max_order_value, min_order_value, 
	first_order_date, last_order_date, active_months, completion_rate, revenue_per_day, revenue_rank
)

SELECT customer_id, full_name, email, customer_segment, loyalty_tier, loyalty_points, city,
	country_code, signup_date, customer_age_days, total_orders, completed_orders, cancelled_orders, 
	shipped_orders, pending_orders, total_revenue, avg_order_value, max_order_value, min_order_value, 
	first_order_date, last_order_date, active_months, completion_rate, revenue_per_day, revenue_rank
FROM final_with_revenue_ranked;
