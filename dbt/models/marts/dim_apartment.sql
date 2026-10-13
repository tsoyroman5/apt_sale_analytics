select
    apartment_seq,
    any_value(apartment_name)       as apartment_name,
    any_value(build_year)           as build_year,
    any_value(district_code)        as district_code,
    any_value(neighborhood_code)    as neighborhood_code,
    any_value(neighborhood_name)    as neighborhood_name
from {{ ref('stg_apt_transactions') }}
group by apartment_seq