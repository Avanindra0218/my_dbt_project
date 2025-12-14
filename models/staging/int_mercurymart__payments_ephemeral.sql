{{ config(materialized='ephemeral') }}

select
    order_id::number as order_id,
    sum(
        case
            when upper(status) = 'SUCCESS' then amount
            else 0
        end
    ) as total_payment_amount
from {{ source('mercurymart_raw', 'RAW_PAYMENTS') }}
group by order_id
