{{ config(materialized='incremental', unique_key='SalesID') }}

with raw as (
    select *
    from {{ source('grocery_raw', 'sales') }}
)

select
    cast(SalesID as string) as sales_id,
    SalesPersonID as salesperson_id,
    CustomerID as customer_id,
    ProductID as product_id,
    Quantity as quantity,
    Discount as discount,
    TotalPrice as total_price,
    cast(SalesDate as timestamp) as sales_date,
    TransactionNumber as transaction_number,
    extract(year from cast(SalesDate as timestamp)) as year,
    extract(month from cast(SalesDate as timestamp)) as month,
    extract(day from cast(SalesDate as timestamp)) as day,
    extract(dayofweek from cast(SalesDate as timestamp)) as day_of_week
from raw
where SalesDate is not null
