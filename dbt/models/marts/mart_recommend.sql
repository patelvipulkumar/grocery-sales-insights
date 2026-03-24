{{ config(
    materialized='table',
    description='Product recommendations mart with cross-sell and upsell opportunities based on customer behavior'
) }}

with customer_products as (
    select
        ss.customer_id,
        ss.product_id,
        p.category_name,
        p.class,
        p.price_category,
        count(distinct ss.sales_id) as purchase_frequency,
        sum(ss.total_price) as total_spent_on_product,
        max(ss.sales_date) as last_purchase_date,
        row_number() over (partition by ss.customer_id order by count(distinct ss.sales_id) desc) as product_rank
    from {{ ref('fct_sales_summary') }} ss
    left join {{ ref('st_products') }} p on ss.product_id = p.product_id
    group by ss.customer_id, ss.product_id, p.category_name, p.class, p.price_category
),

product_affinities as (
    select
        cp1.customer_id,
        cp1.product_id as purchased_product,
        cp1.category_name as purchased_category,
        cp1.class as purchased_class,
        cp1.price_category as purchased_price_category,
        cp2.product_id as recommended_product,
        cp2.category_name as recommended_category,
        cp2.class as recommended_class,
        cp2.price_category as recommended_price_category,
        count(distinct cp2.purchase_frequency) as co_purchase_count,
        rank() over (partition by cp1.customer_id, cp1.product_id order by count(distinct cp2.purchase_frequency) desc) as recommendation_rank
    from customer_products cp1
    join customer_products cp2
        on cp1.customer_id = cp2.customer_id
        and cp1.product_id != cp2.product_id
        and cp1.product_rank <= 5
        and cp2.product_rank <= 10
    group by cp1.customer_id, cp1.product_id, cp1.category_name, cp1.class, cp1.price_category,
             cp2.product_id, cp2.category_name, cp2.class, cp2.price_category
),

recommendations as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'purchased_product', 'recommended_product']) }} as recommendation_id,
        customer_id,
        purchased_product,
        purchased_category,
        purchased_class,
        purchased_price_category,
        recommended_product,
        recommended_category,
        recommended_class,
        recommended_price_category,
        co_purchase_count,
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
            when cast(replace(recommended_price_category, 'Premium', '3') as string) > cast(replace(purchased_price_category, 'Premium', '3') as string) then 'Upsell'
            else 'Downsell'
        end as price_movement,
        current_timestamp() as dbt_loaded_at
    from product_affinities
    where recommendation_rank <= 5
)

select *
from recommendations
