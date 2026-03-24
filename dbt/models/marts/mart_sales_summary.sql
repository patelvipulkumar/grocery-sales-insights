{{ config(
    materialized='table',
    description='Sales summary mart providing aggregated sales metrics by time, geography, and product'
) }}

with sales_summary as (
    select
        year,
        month,
        day,
        day_of_week,
        customer_country,
        customer_country_code,
        salesperson_country,
        category_name,
        class,
        price_category,
        count(distinct sales_id) as transaction_count,
        sum(quantity) as total_units_sold,
        sum(total_price) as total_revenue,
        sum(estimated_profit) as total_profit,
        avg(total_price) as avg_transaction_value,
        min(total_price) as min_transaction_value,
        max(total_price) as max_transaction_value,
        count(distinct customer_id) as unique_customers,
        count(distinct product_id) as unique_products,
        count(distinct salesperson_id) as unique_salespeople,
        sum(discount) as total_discounts_given
    from {{ ref('fct_sales_summary') }}
    group by year, month, day, day_of_week, customer_country, customer_country_code, salesperson_country, category_name, class, price_category
),

enriched_summary as (
    select
        ss.*,
        case
            when day_of_week = 1 then 'Sunday'
            when day_of_week = 2 then 'Monday'
            when day_of_week = 3 then 'Tuesday'
            when day_of_week = 4 then 'Wednesday'
            when day_of_week = 5 then 'Thursday'
            when day_of_week = 6 then 'Friday'
            when day_of_week = 7 then 'Saturday'
        end as day_name,
        case
            when month in (12, 1, 2) then 'Q1'
            when month in (3, 4, 5) then 'Q2'
            when month in (6, 7, 8) then 'Q3'
            else 'Q4'
        end as quarter,
        round(total_profit / nullif(total_revenue, 0) * 100, 2) as profit_margin_percentage,
        round(total_revenue / nullif(unique_customers, 0), 2) as revenue_per_customer,
        round(total_units_sold / nullif(unique_customers, 0), 2) as units_per_customer,
        round(total_discounts_given / nullif(total_revenue, 0) * 100, 2) as discount_percentage,
        current_timestamp() as dbt_loaded_at
    from sales_summary ss
)

select *
from enriched_summary
