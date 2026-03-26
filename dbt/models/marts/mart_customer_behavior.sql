{{ config(
    materialized='table',
    description='Customer behavior mart with engagement, purchase patterns, and segmentation'
) }}

with customer_behavior as (
    select
        cb.customer_behavior_id,
        cb.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.city_name as customer_city,
        c.country_name as customer_country,
        c.country_code as customer_country_code,
        cast(null as date) as date_of_birth,
        cast(null as string) as gender,
        cast(null as date) as registration_date,
        cb.purchase_count,
        cb.distinct_products_purchased,
        cb.total_quantity,
        cb.total_spend,
        cb.avg_transaction_value,
        cb.customer_value_segment,
        cb.engagement_status,
        cb.last_purchase_date,
        cb.first_purchase_date,
        cb.days_between_first_last_purchase,
        cb.total_discount_received,
        cb.avg_discount_per_transaction,
        date_diff(current_date(), date(cb.last_purchase_date), day) as days_since_last_purchase,
        date_diff(current_date(), date(cb.first_purchase_date), day) as customer_tenure_days,
        round(cb.total_spend / nullif(cb.days_between_first_last_purchase, 0), 2) as daily_avg_spend,
        cast(null as int64) as age,
        cast('Unknown' as string) as age_group
    from {{ ref('fct_customer_behavior') }} cb
    left join {{ ref('st_customers') }} c on cb.customer_id = c.customer_id
)

select *
from customer_behavior
