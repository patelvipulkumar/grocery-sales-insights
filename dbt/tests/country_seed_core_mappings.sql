with expected as (
    select 'United States' as country_name, 'US' as country_code union all
    select 'Canada', 'CA' union all
    select 'Germany', 'DE' union all
    select 'United Kingdom', 'GB' union all
    select 'India', 'IN' union all
    select 'Australia', 'AU'
),
actual as (
    select CountryName as country_name, CountryCode as country_code
    from {{ ref('countries') }}
)
select
    expected.country_name,
    expected.country_code as expected_country_code,
    actual.country_code as actual_country_code
from expected
left join actual using (country_name)
where actual.country_code is distinct from expected.country_code