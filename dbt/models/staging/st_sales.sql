-- High-level logic:
-- Standardizes raw sales transactions, derives calendar fields, and computes
-- transaction value from product price and discount for downstream facts/marts.

{{ config(materialized='incremental', unique_key='sales_id') }}

with raw as (
    select *
    from {{ source('grocery_raw', 'sales') }}
),

products as (
    select
        ProductID,
        Price
    from {{ source('grocery_raw', 'products') }}
)

select
    cast(r.SalesID as string) as sales_id,
    r.SalesPersonID as salesperson_id,
    r.CustomerID as customer_id,
    r.ProductID as product_id,
    r.Quantity as quantity,
    r.Discount as discount,
    cast(p.Price as numeric) * r.Quantity - cast(r.Discount as numeric) as total_price,
    cast(r.SalesDate as timestamp) as sales_date,
    r.TransactionNumber as transaction_number,
    extract(year from cast(r.SalesDate as timestamp)) as year,
    extract(month from cast(r.SalesDate as timestamp)) as month,
    extract(day from cast(r.SalesDate as timestamp)) as day,
    extract(dayofweek from cast(r.SalesDate as timestamp)) as day_of_week
from raw r
left join products p on r.ProductID = p.ProductID
where r.SalesDate is not null
