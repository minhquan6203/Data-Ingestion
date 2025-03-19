{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- Clean and standardize customer data from bronze
SELECT
    customer_id,
    TRIM(name) as name,
    LOWER(TRIM(email)) as email,
    registration_date,
    TRIM(country) as country
FROM {{ source('bronze', 'bronze_customers') }}
WHERE customer_id IS NOT NULL
  AND email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' 