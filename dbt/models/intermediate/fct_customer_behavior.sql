{{ config(
    materialized='table',
    description='Customer behavior fact table capturing purchase patterns and engagement metrics'
) }}

with customer_purchases as (
    select
        st.customer_id,
        count(distinct st.sales_id) as purchase_count,
        count(distinct st.product_id) as distinct_products_purchased,
        sum(st.quantity) as total_quantity,
        sum(st.total_price) as total_spend,
        avg(st.total_price) as avg_transaction_value,
        max(st.sales_date) as last_purchase_date,
        min(st.sales_date) as first_purchase_date,
        date_diff(date(max(st.sales_date)), date(min(st.sales_date)), day) as days_between_first_last_purchase,
        sum(st.discount) as total_discount_received,
        avg(st.discount) as avg_discount_per_transaction
    from {{ ref('st_sales') }} st
    group by st.customer_id
),

behavior_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_behavior_id,
        customer_id,
        purchase_count,
        distinct_products_purchased,
        total_quantity,
        total_spend,
        avg_transaction_value,
        last_purchase_date,
        first_purchase_date,
        days_between_first_last_purchase,
        total_discount_received,
        avg_discount_per_transaction,
        case
            when purchase_count >= 50 then 'High Value'
            when purchase_count >= 20 then 'Medium Value'
            else 'Low Value'
        end as customer_value_segment,
        case
            when date_diff(current_date(), date(last_purchase_date), day) <= 30 then 'Active'
            when date_diff(current_date(), date(last_purchase_date), day) <= 90 then 'At Risk'
            else 'Dormant'
        end as engagement_status,
        current_timestamp() as dbt_loaded_at
    from customer_purchases
)

select *
from behavior_metrics
