{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Clean and standardize order data from bronze
SELECT
    order_id,
    customer_id,
    order_date,
    CAST(total_amount AS NUMERIC(10, 2)) as total_amount,
    TRIM(status) as status
FROM {{ source('bronze', 'bronze_orders') }}
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND total_amount > 0 