{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

with order_gross as (

    select
        order_id,
        cast(sum(gross_amount) as number(38,2)) as gross_amount
    from {{ ref('stg_mercurymart__order_items') }}
    group by order_id

)

select
    o.order_id,
    o.order_date,
    o.customer_id,

    cast(og.gross_amount as number(38,2)) as gross_amount,

    cast(coalesce(p.total_payment_amount, 0) as number(38,2)) as payment_amount,
    cast(coalesce(r.total_refund_amount, 0) as number(38,2)) as refund_amount,

    cast(
        coalesce(p.total_payment_amount, 0)
      - coalesce(r.total_refund_amount, 0)
        as number(38,2)
    ) as net_amount,

    case
        when
            cast(
                coalesce(p.total_payment_amount, 0)
              - coalesce(r.total_refund_amount, 0)
              as number(38,2)
            ) > 0
        then 'PAID'
        else 'UNPAID'
    end as payment_status,

    dayname(o.order_date) as order_day_of_week

from {{ ref('stg_mercurymart__orders') }} o
left join order_gross og
    on o.order_id = og.order_id
left join {{ ref('int_mercurymart__payments_ephemeral') }} p
    on o.order_id = p.order_id
left join {{ ref('int_mercurymart__refunds_ephemeral') }} r
    on o.order_id = r.order_id

{% if is_incremental() %}
where o.order_date > (
    select max(order_date) from {{ this }}
)
{% endif %}

