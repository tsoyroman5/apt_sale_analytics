with bounds as (
    select min(deal_date) as d_min, max(deal_date) as d_max
    from {{ ref('stg_apt_transactions') }}
)

select
    d                                   as date_day,
    extract(year from d)                as year,
    extract(quarter from d)             as quarter,
    extract(month from d)               as month,
    format_date('%Y-%m', d)             as year_month,
    format_date('%A', d)                as weekday_name
from bounds, unnest(generate_date_array(d_min, d_max)) as d