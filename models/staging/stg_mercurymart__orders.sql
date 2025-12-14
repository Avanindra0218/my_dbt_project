select
    order_id::number as order_id,
    customer_id::number as customer_id,
    order_date::date as order_date,
    upper(status) as order_status,
    coupon_code,
    campaign_id::number as campaign_id
from {{ source('mercurymart_raw', 'RAW_ORDERS') }}
