{{ config(materialized='table') }}

WITH raw_changes AS (
    SELECT
        customer_id,
        name,
        email,
        address,
        valid_from,
        captured_at,
        created_at,
        updated_at,
        LAG(address) OVER (PARTITION BY customer_id ORDER BY valid_from ASC, captured_at ASC) AS prev_address,
        LAG(email) OVER (PARTITION BY customer_id ORDER BY valid_from ASC, captured_at ASC) AS prev_email
    FROM {{ ref('stg_customers') }}
),

-- Filter only meaningful attribute changes (or initial insert)
distinct_states AS (
    SELECT
        customer_id,
        name,
        email,
        address,
        valid_from,
        captured_at
    FROM raw_changes
    WHERE prev_address IS NULL 
       OR prev_address != address 
       OR prev_email != email
),

scd2_windows AS (
    SELECT
        customer_id,
        name,
        email,
        address,
        valid_from,
        LEAD(valid_from) OVER (
            PARTITION BY customer_id 
            ORDER BY valid_from ASC, captured_at ASC
        ) AS valid_to
    FROM distinct_states
)

SELECT
    md5(cast(customer_id as varchar) || '-' || cast(valid_from as varchar)) AS customer_key,
    customer_id,
    name,
    email,
    address,
    valid_from,
    valid_to,
    CASE 
        WHEN valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current
FROM scd2_windows
