with source as (
    select * from {{ source('raw', 'apt_transactions_raw') }}
),

staged as (

    select
        -- request params tagged by the EL script
        lawd_cd                                                         as request_district_code,
        deal_ymd                                                        as request_month,

        -- location
        nullif(trim(sggCd), '')                                         as district_code,
        nullif(trim(umdCd), '')                                         as neighborhood_code,
        nullif(trim(umdNm), '')                                         as neighborhood_name,
        nullif(trim(jibun), '')                                         as lot_number,
        nullif(trim(roadNm), '')                                        as road_name,

        -- apartment
        nullif(trim(aptSeq), '')                                        as apartment_seq,
        nullif(trim(aptNm), '')                                         as apartment_name,
        nullif(trim(aptDong), '')                                       as apartment_dong,
        safe_cast(trim(buildYear) as int64)                             as build_year,

        -- unit
        safe_cast(trim(excluUseAr) as numeric)                          as exclusive_area_m2,
        safe_cast(trim(floor) as int64)                                 as floor_no,

        -- deal
        safe_cast(replace(trim(dealAmount), ',', '') as int64)          as deal_amount_10k_krw,
        safe.date(
            safe_cast(trim(dealYear) as int64),
            safe_cast(trim(dealMonth) as int64),
            safe_cast(trim(dealDay) as int64)
        )                                                               as deal_date,
        nullif(trim(dealingGbn), '')                                    as deal_type,
        nullif(trim(buyerGbn), '')                                      as buyer_type,
        nullif(trim(slerGbn), '')                                       as seller_type,
        nullif(trim(estateAgentSggNm), '')                              as agent_location,
        nullif(trim(landLeaseholdGbn), '')                              as land_leasehold_flag,

        -- cancellation / registration: kept raw until formats are confirmed
        coalesce(trim(cdealType), '') = 'O'                             as is_cancelled,
        safe.parse_date('%y.%m.%d', nullif(trim(cdealDay), ''))         as cancellation_date,
        safe.parse_date('%y.%m.%d', nullif(trim(rgstDate), ''))         as registration_date

    from source

),

keyed as (

    select
        to_hex(md5(concat(
            coalesce(apartment_seq, ''), '|',
            coalesce(apartment_dong, ''), '|',
            coalesce(cast(floor_no as string), ''), '|',
            coalesce(cast(exclusive_area_m2 as string), ''), '|',
            coalesce(cast(deal_date as string), ''), '|',
            coalesce(cast(deal_amount_10k_krw as string), '')
        )))                                                             as deal_group_key,
        *
    from staged

),

numbered as (

    select
        *,
        count(*) over (partition by deal_group_key)                     as deal_group_size,
        row_number() over (
            partition by deal_group_key
            order by registration_date, lot_number, buyer_type, seller_type,
                     agent_location, cancellation_date
        )                                                               as deal_seq_in_group
    from keyed

)

select
    concat(deal_group_key, '-', cast(deal_seq_in_group as string))      as transaction_id,
    *
from numbered
