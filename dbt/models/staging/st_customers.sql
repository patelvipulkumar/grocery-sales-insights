{{ config(
    materialized='table',
    description='Customer dimension with demographic and account details'
) }}

with customers as (
    select
        c.CustomerID,
        c.FirstName,
        c.MiddleInitial,
        c.LastName,
        c.cityID,
        c.Address,
        ci.CityName,
        ci.Zipcode,
        co.CountryName,
        co.CountryCode
    from {{ source('grocery_raw', 'customers') }} c
    left join {{ ref('cities') }} ci on c.cityID = ci.CityID
    left join {{ ref('countries') }} co on ci.CountryID = co.CountryID
)

select
    {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} as customer_key,
    CustomerID as customer_id,
    concat(FirstName, ' ', coalesce(MiddleInitial, ''), ' ', LastName) as full_name,
    FirstName as first_name,
    MiddleInitial as middle_initial,
    LastName as last_name,
    cityID as city_id,
    CityName as city_name,
    Zipcode as zipcode,
    CountryName as country_name,
    CountryCode as country_code,
    Address as address,
    current_timestamp() as dbt_loaded_at
from customers
