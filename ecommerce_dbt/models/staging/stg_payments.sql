{{ config(materialized='view') }}

SELECT
    payment_id,

    order_id,

    LOWER(TRIM(payment_method)) AS payment_method,

    CAST(payment_amount AS NUMBER(10,2)) AS payment_amount,

    LOWER(TRIM(payment_status)) AS payment_status,

    CAST(payment_timestamp AS TIMESTAMP) AS payment_timestamp

FROM ECOMMERCE_DB.BRONZE.BRONZE_PAYMENTS

WHERE payment_id IS NOT NULL
AND order_id IS NOT NULL
AND payment_amount > 0
