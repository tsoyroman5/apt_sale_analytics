select
    transaction_id,
    apartment_seq,
    deal_date,
    apartment_dong,
    floor_no,
    exclusive_area_m2,
    deal_amount_10k_krw,
    deal_amount_10k_krw * 10000                                         as deal_amount_krw,
    safe_divide(deal_amount_10k_krw * 10000, exclusive_area_m2)         as price_per_m2_krw,
    deal_type,
    coalesce(buyer_type, 'not_reported')                                as buyer_type,
    coalesce(seller_type, 'not_reported')                               as seller_type,
    is_cancelled,
    cancellation_date,
    registration_date,
    active_deal_group_size,
    is_bulk_deal
from {{ ref('int_apt_sales_classified') }}