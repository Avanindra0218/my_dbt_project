select
    p.payment_id::number as payment_id,
    p.order_id::number as order_id,
    p.payment_date::date as payment_date,
    p.payment_method,
    p.amount::number(38,2) as amount,
    upper(p.status) as status
from {{ source('mercurymart_raw', 'RAW_PAYMENTS') }} p
