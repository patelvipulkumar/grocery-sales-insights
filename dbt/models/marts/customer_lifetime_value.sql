{{ config(
    materialized='table',
    description='Customer lifetime value and purchase metrics'
) }}

SELECT
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT DATE(order_date)) as days_active,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    MAX(order_date) as last_purchase_date,
    MIN(order_date) as first_purchase_date,
    DATE_DIFF(MAX(order_date), MIN(order_date), DAY) as customer_tenure_days,
    ROUND(SUM(amount) / (DATE_DIFF(MAX(order_date), MIN(order_date), DAY) + 1), 2) as daily_avg_spent
FROM {{ ref('st_sales') }}
GROUP BY customer_id
