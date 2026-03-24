{{ config(
    materialized='table',
    description='Product dimension with category and pricing details'
) }}

with products as (
    select
        p.ProductID,
        p.ProductName,
        p.Price,
        p.CategoryID,
        p.Class,
        p.ModifyDate,
        p.Resistant,
        p.IsAllergic,
        p.VitalityDays,
        c.CategoryName
    from {{ source('grocery_raw', 'products') }} p
    left join {{ ref('categories') }} c on p.CategoryID = c.CategoryID
)

select
    {{ dbt_utils.generate_surrogate_key(['ProductID']) }} as product_key,
    ProductID as product_id,
    ProductName as product_name,
    Price as price,
    CategoryID as category_id,
    CategoryName as category_name,
    Class as class,
    ModifyDate as modify_date,
    Resistant as resistant,
    IsAllergic as is_allergic,
    VitalityDays as vitality_days,
    case
        when Price < 5 then 'Budget'
        when Price < 15 then 'Standard'
        when Price < 50 then 'Premium'
        else 'Luxury'
    end as price_category,
    current_timestamp() as dbt_loaded_at
from products
