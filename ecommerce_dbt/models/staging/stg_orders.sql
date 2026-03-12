{{ config(materialized='view') }}

SELECT
    order_id,

    customer_id,

    product_id,

    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,

    CAST(price AS NUMBER(10,2)) AS price,

    CAST(quantity AS INT) AS quantity,

    price * quantity AS order_total,

    LOWER(TRIM(order_status)) AS order_status

FROM ECOMMERCE_DB.BRONZE.BRONZE_ORDERS

WHERE order_id IS NOT NULL
AND customer_id IS NOT NULL
AND product_id IS NOT NULL
AND price > 0
AND quantity > 0
