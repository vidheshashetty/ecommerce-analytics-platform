{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    category,
    price,

    CASE
        WHEN price < 50 THEN 'low'
        WHEN price BETWEEN 50 AND 200 THEN 'medium'
        ELSE 'high'
    END AS price_segment

FROM {{ ref('stg_products') }}
