{{ config(materialized='table') }}

WITH source AS (

    SELECT
        order_id,
        customer_id,
        product_id,
        amount,
        order_date
    FROM {{ source('raw', 'orders_2') }}

),

final AS (

    SELECT
        o.order_id,
        o.order_date,
        o.customer_id,
        c.customer_name,
        o.product_id,
        p.product_name,
        o.amount
    FROM source o
    LEFT JOIN {{ ref('dim_customers') }} c
        ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_products') }} p
        ON o.product_id = p.product_id

)

SELECT * FROM final
