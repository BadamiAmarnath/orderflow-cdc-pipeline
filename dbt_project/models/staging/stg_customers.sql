WITH source_data AS (
    SELECT
        customer_id,
        TRIM(name) AS name,
        LOWER(TRIM(email)) AS email,
        TRIM(address) AS address,
        operation,
        valid_from,
        captured_at,
        created_at,
        updated_at
    FROM stg_customers_cdc
)

SELECT * FROM source_data
