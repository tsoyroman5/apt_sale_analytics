select 'unparsed_registration_date' as failure, count(*) as n
from {{ source('raw', 'apt_transactions_raw') }}
where nullif(trim(rgstDate), '') is not null
  and safe.parse_date('%y.%m.%d', trim(rgstDate)) is null
having count(*) > 0

union all

select 'unparsed_cancellation_date', count(*)
from {{ source('raw', 'apt_transactions_raw') }}
where nullif(trim(cdealDay), '') is not null
  and safe.parse_date('%y.%m.%d', trim(cdealDay)) is null
having count(*) > 0

union all

select 'district_mismatch', count(*)
from {{ ref('stg_apt_transactions') }}
where district_code != request_district_code
having count(*) > 0