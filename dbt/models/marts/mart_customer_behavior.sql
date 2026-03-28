-- High-level logic:
-- Creates customer engagement and value intelligence by combining behavioral
-- metrics with RFM scoring, segment assignment, and suggested marketing
-- actions for campaign targeting.

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
        cb.data_as_of_date,
        cb.last_purchase_date,
        cb.first_purchase_date,
        cb.days_between_first_last_purchase,
        cb.total_discount_received,
        cb.avg_discount_per_transaction,
        date_diff(cb.data_as_of_date, date(cb.last_purchase_date), day) as days_since_last_purchase,
        date_diff(cb.data_as_of_date, date(cb.first_purchase_date), day) as customer_tenure_days,
        round(cb.total_spend / nullif(cb.days_between_first_last_purchase + 1, 0), 2) as daily_avg_spend,
        cast(null as int64) as age,
        cast('Unknown' as string) as age_group
    from {{ ref('fct_customer_behavior') }} cb
    left join {{ ref('st_customers') }} c on cb.customer_id = c.customer_id
),

rfm_scored as (
    select
        cb.*,
        6 - ntile(5) over (order by cb.days_since_last_purchase asc) as recency_score,
        ntile(5) over (order by cb.purchase_count asc) as frequency_score,
        ntile(5) over (order by cb.total_spend asc) as monetary_score
    from customer_behavior cb
),

segmented as (
    select
        rs.*,
        cast(rs.recency_score as string) || cast(rs.frequency_score as string) || cast(rs.monetary_score as string) as rfm_score,
        case
            when rs.recency_score >= 4 and rs.frequency_score >= 4 and rs.monetary_score >= 4 then 'Champions'
            when rs.recency_score >= 4 and rs.frequency_score >= 3 then 'Loyal Customers'
            when rs.recency_score = 5 and rs.frequency_score <= 2 then 'New Customers'
            when rs.recency_score between 3 and 4 and rs.monetary_score >= 4 then 'Potential Loyalists'
            when rs.recency_score <= 2 and rs.frequency_score >= 4 and rs.monetary_score >= 4 then 'At Risk High Value'
            when rs.recency_score <= 2 and rs.frequency_score <= 2 then 'Hibernating'
            else 'Needs Attention'
        end as rfm_segment,
        case
            when rs.recency_score <= 2 and rs.monetary_score >= 4 then 'Win-back premium offers'
            when rs.recency_score >= 4 and rs.frequency_score >= 4 then 'VIP retention and loyalty rewards'
            when rs.recency_score = 5 and rs.frequency_score <= 2 then 'Welcome and first-repeat incentives'
            when rs.frequency_score >= 4 and rs.monetary_score between 2 and 3 then 'Bundle and cross-sell campaigns'
            else 'General nurture campaigns'
        end as marketing_action
    from rfm_scored rs
)

select *
from segmented
