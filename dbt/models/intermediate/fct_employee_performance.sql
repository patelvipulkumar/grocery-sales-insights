{{ config(
    materialized='table',
    description='Employee performance fact table with sales metrics and team productivity'
) }}

with sales_performance as (
    select
        e.employee_id,
        e.full_name as employee_name,
        e.city_name as employee_city,
        e.country_name as employee_country,
        e.experience_level,
        count(distinct s.sales_id) as total_sales_transactions,
        count(distinct s.customer_id) as unique_customers_served,
        sum(s.quantity) as total_units_sold,
        sum(s.total_price) as total_sales_revenue,
        avg(s.total_price) as avg_transaction_value,
        max(s.sales_date) as last_sale_date,
        sum(s.discount) as total_discounts_given,
        e.years_employed
    from {{ ref('st_employees') }} e
    left join {{ ref('st_sales') }} s on e.employee_id = s.salesperson_id
    group by 1, 2, 3, 4, 5, 13
),

performance_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_performance_id,
        employee_id,
        employee_name,
        employee_city,
        employee_country,
        experience_level,
        total_sales_transactions,
        unique_customers_served,
        total_units_sold,
        total_sales_revenue,
        avg_transaction_value,
        total_discounts_given,
        years_employed,
        round(total_sales_revenue / nullif(total_sales_transactions, 0), 2) as revenue_per_transaction,
        current_timestamp() as dbt_loaded_at
    from sales_performance
)

select *
from performance_metrics
