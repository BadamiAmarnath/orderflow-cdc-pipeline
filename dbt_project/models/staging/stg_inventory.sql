WITH source_data AS (
    SELECT
        product_id,
        warehouse_id,
        CAST(stock_level AS INTEGER) AS stock_level,
        operation,
        captured_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY captured_at DESC
        ) AS row_num
    FROM stg_inventory_cdc
)

SELECT
    product_id,
    warehouse_id,
    stock_level,
    captured_at,
    updated_at
FROM source_data
WHERE row_num = 1
