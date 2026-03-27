-- High-level logic:
-- Produces business-facing revenue and profit summaries by time, geography,
-- and product/category, including ranking fields for hot sellers and
-- category leaders used in BI dashboards.

{{ config(
    materialized='table',
    description='Sales summary mart providing aggregated sales metrics by time, geography, and product'
) }}

with sales_summary as (
    select
        ss.year,
        ss.month,
        ss.day,
        ss.day_of_week,
        ss.customer_country,
        ss.customer_country_code,
        ss.salesperson_country,
        ss.product_id,
        p.product_name,
        p.category_name,
        p.class,
        p.price_category,
        count(distinct ss.sales_id) as transaction_count,
        sum(ss.quantity) as total_units_sold,
        sum(ss.total_price) as total_revenue,
        sum(ss.estimated_profit) as total_profit,
        avg(ss.total_price) as avg_transaction_value,
        min(ss.total_price) as min_transaction_value,
        max(ss.total_price) as max_transaction_value,
        count(distinct ss.customer_id) as unique_customers,
        count(distinct ss.product_id) as unique_products,
        count(distinct ss.salesperson_id) as unique_salespeople,
        sum(ss.discount) as total_discounts_given
    from {{ ref('fct_sales_summary') }} ss
    left join {{ ref('st_products') }} p
        on ss.product_id = p.product_id
    group by ss.year, ss.month, ss.day, ss.day_of_week, ss.customer_country, ss.customer_country_code, ss.salesperson_country, ss.product_id, p.product_name, p.category_name, p.class, p.price_category
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
        rank() over (
            partition by year, month
            order by total_units_sold desc, total_revenue desc
        ) as monthly_hot_selling_product_rank,
        rank() over (
            partition by year, month, category_name
            order by total_revenue desc
        ) as monthly_category_revenue_rank,
        current_timestamp() as dbt_loaded_at
    from sales_summary ss
)

select *
from enriched_summary
