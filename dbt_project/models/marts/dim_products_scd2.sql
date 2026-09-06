{{ config(materialized='table') }}

WITH raw_changes AS (
    SELECT
        product_id,
        name,
        category,
        price,
        valid_from,
        captured_at,
        updated_at,
        LAG(price) OVER (PARTITION BY product_id ORDER BY valid_from ASC, captured_at ASC) AS prev_price,
        LAG(category) OVER (PARTITION BY product_id ORDER BY valid_from ASC, captured_at ASC) AS prev_category
    FROM {{ ref('stg_products') }}
),

distinct_states AS (
    SELECT
        product_id,
        name,
        category,
        price,
        valid_from,
        captured_at
    FROM raw_changes
    WHERE prev_price IS NULL 
       OR prev_price != price 
       OR prev_category != category
),

scd2_windows AS (
    SELECT
        product_id,
        name,
        category,
        price,
        valid_from,
        LEAD(valid_from) OVER (
            PARTITION BY product_id 
            ORDER BY valid_from ASC, captured_at ASC
        ) AS valid_to
    FROM distinct_states
)

SELECT
    md5(cast(product_id as varchar) || '-' || cast(valid_from as varchar)) AS product_key,
    product_id,
    name,
    category,
    price,
    valid_from,
    valid_to,
    CASE 
        WHEN valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current
FROM scd2_windows
