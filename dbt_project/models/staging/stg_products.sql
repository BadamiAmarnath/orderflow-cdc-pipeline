WITH source_data AS (
    SELECT
        product_id,
        TRIM(name) AS name,
        TRIM(category) AS category,
        CAST(price AS DOUBLE) AS price,
        operation,
        valid_from,
        captured_at,
        updated_at
    FROM stg_products_cdc
)

SELECT * FROM source_data
