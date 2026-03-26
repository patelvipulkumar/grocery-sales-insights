{{ config(
    materialized='table',
    description='Customer lifetime value and purchase metrics'
) }}

SELECT
    customer_id,
    COUNT(DISTINCT sales_id) as total_orders,
    COUNT(DISTINCT DATE(sales_date)) as days_active,
    SUM(total_price) as total_spent,
    AVG(total_price) as avg_order_value,
    MAX(sales_date) as last_purchase_date,
    MIN(sales_date) as first_purchase_date,
    DATE_DIFF(DATE(MAX(sales_date)), DATE(MIN(sales_date)), DAY) as customer_tenure_days,
    ROUND(SUM(total_price) / (DATE_DIFF(DATE(MAX(sales_date)), DATE(MIN(sales_date)), DAY) + 1), 2) as daily_avg_spent
FROM {{ ref('st_sales') }}
GROUP BY customer_id
