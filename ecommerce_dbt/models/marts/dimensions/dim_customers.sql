{{ config(materialized='table') }}


WITH ranked_customers as (
    SELECT
	customer_id,
	customer_name,
	email,
	signup_date,
	country,
	marketing_source,
	ROW_NUMBER() OVER (
	    PARTITION BY customer_id
	    ORDER BY signup_date DESC
	) AS rn
    FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_name,
    email,
    signup_date,
    country,
    marketing_source,
    DATEDIFF(day, signup_date, CURRENT_DATE) AS customer_age_days
FROM ranked_customers WHERE rn = 1
