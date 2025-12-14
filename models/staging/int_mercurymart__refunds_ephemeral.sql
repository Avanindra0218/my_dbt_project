{{ config(materialized='ephemeral') }}

select
    order_id::number as order_id,
    sum(amount) as total_refund_amount
from {{ source('mercurymart_raw', 'RAW_REFUNDS') }}
group by order_id
