{{ config(materialized='table') }}

WITH order_summary AS (
    SELECT
        order_id,
        COUNT(order_item_id) AS total_items,
        SUM(quantity) AS total_quantity,
        SUM(item_subtotal) AS calculated_item_total
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

orders_base AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.total_amount,
        COALESCE(s.total_items, 0) AS total_items,
        COALESCE(s.total_quantity, 0) AS total_quantity
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN order_summary s ON o.order_id = s.order_id
)

SELECT
    o.order_id,
    c.customer_key,
    o.customer_id,
    o.order_date,
    o.status,
    o.total_amount,
    o.total_items,
    o.total_quantity
FROM orders_base o
LEFT JOIN {{ ref('dim_customers_scd2') }} c
    ON o.customer_id = c.customer_id
    AND o.order_date >= c.valid_from
    AND (o.order_date < c.valid_to OR c.valid_to IS NULL)
