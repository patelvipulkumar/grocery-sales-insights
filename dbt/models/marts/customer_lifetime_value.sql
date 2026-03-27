-- High-level logic:
-- Computes customer lifetime value signals from order history, including spend,
-- tenure, value tiers, and global spend ranking for identifying top customers.

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
    ROUND(SUM(total_price) / (DATE_DIFF(DATE(MAX(sales_date)), DATE(MIN(sales_date)), DAY) + 1), 2) as daily_avg_spent,
    ROUND((SUM(total_price) / NULLIF(COUNT(DISTINCT sales_id), 0)) * COUNT(DISTINCT sales_id), 2) as estimated_clv,
    NTILE(5) OVER (ORDER BY SUM(total_price) DESC) as value_quintile,
    CASE
        WHEN NTILE(5) OVER (ORDER BY SUM(total_price) DESC) = 1 THEN 'Most Valuable'
        WHEN NTILE(5) OVER (ORDER BY SUM(total_price) DESC) = 2 THEN 'High Value'
        WHEN NTILE(5) OVER (ORDER BY SUM(total_price) DESC) = 3 THEN 'Mid Value'
        WHEN NTILE(5) OVER (ORDER BY SUM(total_price) DESC) = 4 THEN 'Low Value'
        ELSE 'Emerging'
    END as customer_value_tier,
    RANK() OVER (ORDER BY SUM(total_price) DESC) as customer_spend_rank
FROM {{ ref('st_sales') }}
GROUP BY customer_id
