{{ config(materialized='view') }}

SELECT
    product_id,

    TRIM(product_name) AS product_name,

    LOWER(TRIM(category)) AS category,

    CAST(price AS NUMBER(10,2)) AS price

FROM ECOMMERCE_DB.BRONZE.BRONZE_PRODUCTS

WHERE product_id IS NOT NULL
AND price > 0
