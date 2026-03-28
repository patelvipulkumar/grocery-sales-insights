-- High-level logic:
-- Computes customer lifetime value signals from order history, including spend,
-- tenure, value tiers, and global spend ranking for identifying top customers.

{{ config(
    materialized='table',
    description='Customer lifetime value and purchase metrics'
) }}

with customer_metrics as (
    select
        customer_id,
        count(distinct sales_id) as total_orders,
        count(distinct date(sales_date)) as days_active,
        sum(total_price) as total_spent,
        avg(total_price) as avg_order_value,
        max(sales_date) as last_purchase_date,
        min(sales_date) as first_purchase_date,
        date_diff(date(max(sales_date)), date(min(sales_date)), day) as customer_tenure_days,
        round(sum(total_price) / (date_diff(date(max(sales_date)), date(min(sales_date)), day) + 1), 2) as daily_avg_spent,
        round((sum(total_price) / nullif(count(distinct sales_id), 0)) * count(distinct sales_id), 2) as estimated_clv,
        ntile(5) over (order by sum(total_price) desc) as value_quintile,
        case
            when ntile(5) over (order by sum(total_price) desc) = 1 then 'Most Valuable'
            when ntile(5) over (order by sum(total_price) desc) = 2 then 'High Value'
            when ntile(5) over (order by sum(total_price) desc) = 3 then 'Mid Value'
            when ntile(5) over (order by sum(total_price) desc) = 4 then 'Low Value'
            else 'Emerging'
        end as customer_value_tier,
        rank() over (order by sum(total_price) desc) as customer_spend_rank
    from {{ ref('st_sales') }}
    group by customer_id
)

select
    cm.customer_id,
    c.full_name as customer_name,
    c.first_name,
    c.last_name,
    cm.total_orders,
    cm.days_active,
    cm.total_spent,
    cm.avg_order_value,
    cm.last_purchase_date,
    cm.first_purchase_date,
    cm.customer_tenure_days,
    cm.daily_avg_spent,
    cm.estimated_clv,
    cm.value_quintile,
    cm.customer_value_tier,
    cm.customer_spend_rank
from customer_metrics cm
left join {{ ref('st_customers') }} c
    on cm.customer_id = c.customer_id
