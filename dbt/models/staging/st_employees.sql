{{ config(
    materialized='table',
    description='Employee dimension with department and role information'
) }}

with employees as (
    select
        e.EmployeeID,
        e.FirstName,
        e.MiddleInitial,
        e.LastName,
        e.BirthDate,
        e.Gender,
        e.CityID,
        e.HireDate,
        c.CityName,
        c.Zipcode,
        co.CountryName,
        co.CountryCode
    from {{ source('grocery_raw', 'employees') }} e
    left join {{ ref('cities') }} c on e.CityID = c.CityID
    left join {{ ref('countries') }} co on c.CountryID = co.CountryID
)

select
    {{ dbt_utils.generate_surrogate_key(['EmployeeID']) }} as employee_key,
    EmployeeID as employee_id,
    concat(FirstName, ' ', coalesce(MiddleInitial, ''), ' ', LastName) as full_name,
    FirstName as first_name,
    MiddleInitial as middle_initial,
    LastName as last_name,
    BirthDate as birth_date,
    Gender as gender,
    CityID as city_id,
    CityName as city_name,
    Zipcode as zipcode,
    CountryName as country_name,
    CountryCode as country_code,
    HireDate as hire_date,
    extract(year from current_date()) - extract(year from HireDate) as years_employed,
    case
        when extract(year from current_date()) - extract(year from HireDate) >= 10 then 'Senior'
        when extract(year from current_date()) - extract(year from HireDate) >= 5 then 'Mid-level'
        else 'Junior'
    end as experience_level,
    current_timestamp() as dbt_loaded_at
from employees
