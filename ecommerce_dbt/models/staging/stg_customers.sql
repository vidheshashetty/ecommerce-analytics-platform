{{ config(materialized='view') }}

SELECT
    customer_id,

    TRIM(name) AS customer_name,

    LOWER(TRIM(email)) AS email,

    CAST(signup_date AS DATE) AS signup_date,

    TRIM(country) AS country,

    LOWER(TRIM(marketing_source)) AS marketing_source

FROM ECOMMERCE_DB.BRONZE.BRONZE_CUSTOMERS

WHERE customer_id IS NOT NULL
