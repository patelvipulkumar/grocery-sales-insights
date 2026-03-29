-- High-level logic:
-- Defines the cities lookup dimension from seed data.
-- This model supports geographic analysis and customer/employee location mapping.

{{ config(
    materialized='table',
    description='Cities lookup seed data'
) }}

select
    cast(CityID as int64) as CityID,
    CityName,
    Zipcode,
    cast(CountryID as int64) as CountryID
from {{ ref('cities') }}