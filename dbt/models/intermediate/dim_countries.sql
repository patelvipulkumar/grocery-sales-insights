-- High-level logic:
-- Defines the countries lookup dimension from seed data.
-- This model supports geographic analysis and regional aggregations.

{{ config(
    materialized='table',
    description='Countries lookup seed data'
) }}

select
    cast(CountryID as int64) as CountryID,
    CountryName,
    CountryCode
from {{ ref('countries') }}