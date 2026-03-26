{{ config(
    materialized='table',
    description='Employee performance mart with sales metrics, productivity, and tier rankings'
) }}

with employee_performance as (
    select
        ep.employee_performance_id,
        ep.employee_id,
        ep.employee_name,
        ep.employee_city,
        ep.employee_country,
        ep.experience_level,
        ep.total_sales_transactions,
        ep.unique_customers_served,
        ep.total_units_sold,
        ep.total_sales_revenue,
        ep.avg_transaction_value,
        ep.total_discounts_given,
        ep.revenue_per_transaction,
        ep.years_employed,
        case
            when ep.total_sales_revenue >= 100000 then 'Top Performer'
            when ep.total_sales_revenue >= 50000 then 'Strong Performer'
            when ep.total_sales_revenue >= 10000 then 'Average Performer'
            else 'Development Needed'
        end as performance_tier,
        round(
            safe_divide(
                100.0 * ep.total_sales_revenue,
                sum(ep.total_sales_revenue) over (partition by ep.employee_country)
            ),
            2
        ) as pct_country_revenue,
        rank() over (partition by ep.employee_country order by ep.total_sales_revenue desc) as country_rank,
        rank() over (order by ep.total_sales_revenue desc) as company_rank,
        current_timestamp() as dbt_loaded_at
    from {{ ref('fct_employee_performance') }} ep
)

select *
from employee_performance
