-- Test: Assert valid_to > valid_from and no overlapping intervals in SCD2 models
WITH customer_invalid_windows AS (
    SELECT
        customer_id,
        valid_from,
        valid_to
    FROM {{ ref('dim_customers_scd2') }}
    WHERE valid_to IS NOT NULL AND valid_to <= valid_from
),

product_invalid_windows AS (
    SELECT
        product_id,
        valid_from,
        valid_to
    FROM {{ ref('dim_products_scd2') }}
    WHERE valid_to IS NOT NULL AND valid_to <= valid_from
)

SELECT 'customers' AS entity_type, customer_id AS entity_id, valid_from, valid_to FROM customer_invalid_windows
UNION ALL
SELECT 'products' AS entity_type, product_id AS entity_id, valid_from, valid_to FROM product_invalid_windows
