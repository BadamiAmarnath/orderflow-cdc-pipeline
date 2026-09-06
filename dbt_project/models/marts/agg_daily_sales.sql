{{ config(materialized='table') }}

WITH daily_metrics AS (
    SELECT
        CAST(order_date AS DATE) AS sale_date,
        COUNT(order_id) AS total_orders,
        SUM(CASE WHEN status != 'cancelled' THEN total_amount ELSE 0 END) AS gross_revenue,
        SUM(CASE WHEN status = 'delivered' THEN total_amount ELSE 0 END) AS delivered_revenue,
        COUNT(CASE WHEN status = 'delivered' THEN 1 END) AS delivered_orders,
        COUNT(CASE WHEN status = 'shipped' THEN 1 END) AS shipped_orders,
        COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_orders,
        COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled_orders,
        AVG(CASE WHEN status != 'cancelled' THEN total_amount ELSE NULL END) AS average_order_value,
        SUM(total_quantity) AS total_units_sold
    FROM {{ ref('fct_orders') }}
    GROUP BY CAST(order_date AS DATE)
)

SELECT
    sale_date,
    total_orders,
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(delivered_revenue, 2) AS delivered_revenue,
    delivered_orders,
    shipped_orders,
    pending_orders,
    cancelled_orders,
    ROUND(COALESCE(average_order_value, 0.0), 2) AS average_order_value,
    total_units_sold,
    ROUND((CAST(cancelled_orders AS DOUBLE) / NULLIF(total_orders, 0)) * 100.0, 2) AS cancellation_rate_pct
FROM daily_metrics
ORDER BY sale_date DESC
