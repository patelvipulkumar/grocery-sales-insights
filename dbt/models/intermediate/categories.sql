-- High-level logic:
-- Defines the categories lookup dimension from seed data.
-- This model should be materialized as a table and supports product categorization.

{{ config(
    materialized='table',
    description='Categories lookup seed data'
) }}

select
    cast(CategoryID as int64) as CategoryID,
    CategoryName
from {{ seed('categories') }}
