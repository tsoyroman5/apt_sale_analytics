select
    *,
    (not is_cancelled
     and active_deal_group_size >= {{ var('bulk_deal_min_group_size') }})   as is_bulk_deal
from {{ ref('stg_apt_transactions') }}