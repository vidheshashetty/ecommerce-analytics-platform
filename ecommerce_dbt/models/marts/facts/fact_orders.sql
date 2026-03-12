{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_timestamp,
    o.price,
    o.quantity,
    o.order_total,
    o.order_status
FROM {{ ref('stg_orders') }} o

{% if is_incremental() %}

WHERE o.order_id >
      (SELECT MAX(order_id) FROM {{ this }})

{% endif %}
