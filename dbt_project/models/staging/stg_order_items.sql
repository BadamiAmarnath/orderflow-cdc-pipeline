WITH source_data AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS DOUBLE) AS unit_price,
        operation,
        captured_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id 
            ORDER BY captured_at DESC
        ) AS row_num
    FROM stg_order_items_cdc
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    ROUND(quantity * unit_price, 2) AS item_subtotal,
    captured_at
FROM source_data
WHERE row_num = 1
