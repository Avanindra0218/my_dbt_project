{{ config(materialized='table') }}

with orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        count(distinct order_id) as total_orders
    from {{ ref('stg_mercurymart__orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.segment,

    o.first_order_date,
    o.total_orders,

    case
        when o.total_orders >= 20 then 'PLATINUM'
        when o.total_orders >= 10 then 'GOLD'
        when o.total_orders >= 5 then 'SILVER'
        else 'BRONZE'
    end as loyalty_tier

from {{ ref('stg_mercurymart__customers') }} c
left join orders o
    on c.customer_id = o.customer_id
