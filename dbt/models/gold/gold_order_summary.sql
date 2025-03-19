{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- Customer order summary aggregation
SELECT 
    c.customer_id,
    c.name as customer_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    MAX(o.order_date) as last_order_date
FROM {{ ref('silver_orders') }} o
JOIN {{ ref('silver_customers') }} c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name 