-- High-level logic:
-- Generates customer-level cross-sell recommendations using product affinity
-- from transaction co-purchases, then ranks candidates by blended behavior
-- and affinity signals.

{{ config(
    materialized='table',
    description='Product recommendations mart with cross-sell and upsell opportunities based on customer behavior'
) }}

with customer_products as (
    select
        ss.customer_id,
        ss.product_id,
        p.product_name,
        p.category_name,
        p.class,
        p.price_category,
        count(distinct ss.sales_id) as purchase_frequency,
        sum(ss.quantity) as total_units_bought,
        sum(ss.total_price) as total_spent_on_product,
        max(ss.sales_date) as last_purchase_date,
        row_number() over (
            partition by ss.customer_id
            order by count(distinct ss.sales_id) desc, sum(ss.total_price) desc
        ) as product_rank
    from {{ ref('fct_sales_summary') }} ss
    left join {{ ref('st_products') }} p
        on ss.product_id = p.product_id
    group by ss.customer_id, ss.product_id, p.product_name, p.category_name, p.class, p.price_category
),

transaction_pairs as (
    select
        s1.product_id as anchor_product_id,
        s2.product_id as related_product_id,
        count(distinct s1.transaction_number) as co_purchase_txn_count,
        sum(s2.quantity) as related_units_sold
    from {{ ref('fct_sales_summary') }} s1
    join {{ ref('fct_sales_summary') }} s2
        on s1.transaction_number = s2.transaction_number
        and s1.product_id != s2.product_id
    group by s1.product_id, s2.product_id
),

candidate_recommendations as (
    select
        cp.customer_id,
        cp.product_id as purchased_product,
        cp.product_name as purchased_product_name,
        cp.category_name as purchased_category,
        cp.class as purchased_class,
        cp.price_category as purchased_price_category,
        tp.related_product_id as recommended_product,
        rp.product_name as recommended_product_name,
        rp.category_name as recommended_category,
        rp.class as recommended_class,
        rp.price_category as recommended_price_category,
        tp.co_purchase_txn_count,
        tp.related_units_sold,
        cp.purchase_frequency,
        cp.total_spent_on_product,
        (
            0.45 * cp.purchase_frequency +
            0.35 * tp.co_purchase_txn_count +
            0.20 * log(1 + cp.total_spent_on_product)
        ) as recommendation_score
    from customer_products cp
    join transaction_pairs tp
        on cp.product_id = tp.anchor_product_id
    left join {{ ref('st_products') }} rp
        on tp.related_product_id = rp.product_id
    left join customer_products already_owned
        on cp.customer_id = already_owned.customer_id
        and tp.related_product_id = already_owned.product_id
    where cp.product_rank <= 10
      and already_owned.product_id is null
      and tp.co_purchase_txn_count >= 2
),

ranked_recommendations as (
    select
        cr.*,
        row_number() over (
            partition by cr.customer_id
            order by cr.recommendation_score desc, cr.co_purchase_txn_count desc
        ) as recommendation_rank
    from candidate_recommendations cr
),

recommendations as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'purchased_product', 'recommended_product']) }} as recommendation_id,
        customer_id,
        purchased_product,
        purchased_product_name,
        purchased_category,
        purchased_class,
        purchased_price_category,
        recommended_product,
        recommended_product_name,
        recommended_category,
        recommended_class,
        recommended_price_category,
        co_purchase_txn_count,
        related_units_sold,
        recommendation_score,
        recommendation_rank,
        case
            when recommendation_rank = 1 then 'High Priority'
            when recommendation_rank <= 3 then 'Medium Priority'
            else 'Low Priority'
        end as recommendation_priority,
        case
            when purchased_category = recommended_category then 'Same Category'
            when purchased_class = recommended_class then 'Same Class'
            else 'Cross Category'
        end as recommendation_type,
        case
            when purchased_price_category = recommended_price_category then 'Same Price Tier'
            when purchased_price_category = 'Budget' and recommended_price_category in ('Standard', 'Premium', 'Luxury') then 'Upsell'
            when purchased_price_category = 'Standard' and recommended_price_category in ('Premium', 'Luxury') then 'Upsell'
            when purchased_price_category = 'Premium' and recommended_price_category = 'Luxury' then 'Upsell'
            else 'Downsell'
        end as price_movement,
        current_timestamp() as dbt_loaded_at
    from ranked_recommendations
    where recommendation_rank <= 10
)

select *
from recommendations
