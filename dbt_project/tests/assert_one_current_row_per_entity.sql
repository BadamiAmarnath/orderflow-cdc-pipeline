-- Test: Exactly one is_current = TRUE row per customer_id
WITH customer_current_counts AS (
    SELECT
        customer_id,
        SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS current_rows
    FROM {{ ref('dim_customers_scd2') }}
    GROUP BY customer_id
    HAVING SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) != 1
),

product_current_counts AS (
    SELECT
        product_id,
        SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS current_rows
    FROM {{ ref('dim_products_scd2') }}
    GROUP BY product_id
    HAVING SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) != 1
)

SELECT 'customers' AS entity_type, customer_id AS entity_id, current_rows FROM customer_current_counts
UNION ALL
SELECT 'products' AS entity_type, product_id AS entity_id, current_rows FROM product_current_counts
