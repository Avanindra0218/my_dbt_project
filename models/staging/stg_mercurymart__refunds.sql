select
    refund_id::number as refund_id,
    payment_id::number as payment_id,
    order_id::number as order_id,
    refund_date::date as refund_date,
    amount::number(38,2) as refund_amount,
    reason
from {{ source('mercurymart_raw', 'RAW_REFUNDS') }}
