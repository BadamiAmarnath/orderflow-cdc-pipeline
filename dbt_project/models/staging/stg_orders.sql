WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        LOWER(TRIM(status)) AS status,
        CAST(total_amount AS DOUBLE) AS total_amount,
        operation,
        captured_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY captured_at DESC
        ) AS row_num
    FROM stg_orders_cdc
)

-- Latest known state of each order
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    captured_at AS last_updated_at
FROM source_data
WHERE row_num = 1
