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
        c.date_of_birth,
        c.gender,
        c.registration_date,
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
        date_diff(current_date(), cb.last_purchase_date, day) as days_since_last_purchase,
        date_diff(current_date(), cb.first_purchase_date, day) as customer_tenure_days,
        round(cb.total_spend / nullif(cb.days_between_first_last_purchase, 0), 2) as daily_avg_spend,
        -- Additional demographic insights
        case
            when c.date_of_birth is not null then extract(year from current_date()) - extract(year from c.date_of_birth)
            else null
        end as age,
        case
            when c.date_of_birth is not null then
                case
                    when extract(year from current_date()) - extract(year from c.date_of_birth) < 25 then '18-24'
                    when extract(year from current_date()) - extract(year from c.date_of_birth) < 35 then '25-34'
                    when extract(year from current_date()) - extract(year from c.date_of_birth) < 45 then '35-44'
                    when extract(year from current_date()) - extract(year from c.date_of_birth) < 55 then '45-54'
                    when extract(year from current_date()) - extract(year from c.date_of_birth) < 65 then '55-64'
                    else '65+'
                end
            else 'Unknown'
        end as age_group
    from {{ ref('fct_customer_behavior') }} cb
    left join {{ ref('st_customers') }} c on cb.customer_id = c.customer_id
)

select *
from customer_behavior
