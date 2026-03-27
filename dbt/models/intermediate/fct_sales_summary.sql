-- High-level logic:
-- Builds a transaction-level analytical fact by enriching sales with product,
-- customer, and employee context; also derives estimated profit for downstream
-- marts and dashboard KPIs.

{{ config(
    materialized='incremental',
    unique_key='sales_summary_id',
    description='Aggregated sales summary fact table with product, customer, and transaction details'
) }}

with sales_data as (
    select
        st.sales_id,
        st.salesperson_id,
        st.customer_id,
        st.product_id,
        st.quantity,
        st.discount,
        st.total_price,
        st.sales_date,
        st.transaction_number,
        st.year,
        st.month,
        st.day,
        st.day_of_week,
        -- Join with products for additional product info
        p.product_name,
        p.category_name,
        p.class,
        p.price_category,
        p.price,
        -- Join with customers for demographic info
        c.city_name as customer_city,
        c.country_name as customer_country,
        c.country_code as customer_country_code,
        -- Join with employees for salesperson info
        e.full_name as salesperson_name,
        e.city_name as salesperson_city,
        e.country_name as salesperson_country,
        -- Calculate profit margin (assuming cost is 70% of price)
        st.total_price - (st.quantity * p.price * 0.7) as estimated_profit
    from {{ ref('st_sales') }} st
    left join {{ ref('st_products') }} p on st.product_id = p.product_id
    left join {{ ref('st_customers') }} c on st.customer_id = c.customer_id
    left join {{ ref('st_employees') }} e on st.salesperson_id = e.employee_id
)

select
    {{ dbt_utils.generate_surrogate_key(['sales_id']) }} as sales_summary_id,
    sales_id,
    salesperson_id,
    salesperson_name,
    salesperson_city,
    salesperson_country,
    customer_id,
    customer_city,
    customer_country,
    customer_country_code,
    product_id,
    product_name,
    category_name,
    class,
    price_category,
    quantity,
    discount,
    total_price,
    estimated_profit,
    sales_date,
    transaction_number,
    year,
    month,
    day,
    day_of_week,
    current_timestamp() as dbt_loaded_at
from sales_data

{% if is_incremental() %}
where sales_date > (
    select coalesce(max(sales_date), timestamp('1900-01-01'))
    from {{ this }}
)
{% endif %}
