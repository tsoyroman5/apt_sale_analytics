select apartment_seq
from {{ ref('stg_apt_transactions') }}
group by apartment_seq
having count(distinct format('%t', apartment_name))     > 1
    or count(distinct format('%t', build_year))         > 1
    or count(distinct format('%t', district_code))      > 1
    or count(distinct format('%t', neighborhood_code))  > 1
    or count(distinct format('%t', neighborhood_name))  > 1