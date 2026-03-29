with forbidden as (
    select 'Colorado' as city_name union all
    select 'Kansas' union all
    select 'Oklahoma' union all
    select 'Jersey' union all
    select 'Washington'
),
actual as (
    select CityName as city_name
    from {{ ref('cities') }}
)
select actual.city_name
from actual
inner join forbidden using (city_name)