{{ config(materialized='view') }}

SELECT

    event_id,

    customer_id,

    product_id,

    LOWER(TRIM(event_type)) AS event_type,

    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,

    LOWER(TRIM(device_type)) AS device_type,

    CAST(session_duration AS INT) AS session_duration

FROM ECOMMERCE_DB.BRONZE.BRONZE_EVENTS

WHERE event_id IS NOT NULL
AND customer_id IS NOT NULL
AND product_id IS NOT NULL
