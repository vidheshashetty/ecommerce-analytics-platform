{{ config(materialized='table') }}

WITH orders_daily AS (

SELECT
    DATE(order_timestamp) AS o_date,
    COUNT(order_id) AS total_orders,
    SUM(order_total) AS total_revenue
FROM {{ ref('fact_orders') }}
GROUP BY 1

),

active_users AS (

SELECT
    DATE(event_timestamp) AS a_date,
    COUNT(DISTINCT customer_id) AS daily_active_users
FROM {{ ref('stg_events') }}
GROUP BY 1

),

event_counts AS (

SELECT
    DATE(event_timestamp) AS e_date,
    COUNT(CASE WHEN event_type = 'product_view' THEN 1 END) AS product_views,
    COUNT(CASE WHEN event_type = 'checkout' THEN 1 END) AS checkouts
FROM {{ ref('stg_events') }}
GROUP BY 1

),

user_activity AS (

SELECT
    DATE(event_timestamp) AS activity_date,
    customer_id
FROM {{ ref('stg_events') }}

),

retention AS (

WITH daily_users AS (

SELECT
    DATE(event_timestamp) AS activity_date,
    customer_id
FROM {{ ref('stg_events') }}

),

yesterday_users AS (

SELECT
    activity_date,
    customer_id
FROM daily_users

),

today_users AS (

SELECT
    activity_date,
    customer_id
FROM daily_users

)

SELECT
    y.activity_date + 1 AS r_date,

    COUNT(DISTINCT t.customer_id) AS returning_users,

    COUNT(DISTINCT y.customer_id) AS yesterday_users,

    COUNT(DISTINCT t.customer_id) /
    NULLIF(COUNT(DISTINCT y.customer_id),0) AS retention_rate

FROM yesterday_users y

LEFT JOIN today_users t
    ON y.customer_id = t.customer_id
    AND t.activity_date = DATEADD(day,1,y.activity_date)

GROUP BY 1

)

SELECT
    o.o_date,

    o.total_orders,

    o.total_revenue,

    a.daily_active_users,

    ROUND(o.total_revenue / NULLIF(o.total_orders,0),2) AS avg_order_value,

    ROUND(e.checkouts / NULLIF(e.product_views,0),2) AS conversion_rate,

    ROUND(r.retention_rate * 100,2) AS retention_rate

FROM orders_daily o

LEFT JOIN active_users a
    ON o.o_date = a.a_date

LEFT JOIN event_counts e
    ON o.o_date = e.e_date

LEFT JOIN retention r
    ON o.o_date = r.r_date
