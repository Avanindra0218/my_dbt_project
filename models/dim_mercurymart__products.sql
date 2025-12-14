{{ config(materialized='table') }}

select
    product_id,
    product_name,
    brand,
    category,
    supplier_id,
    unit_price,

    case
        when unit_price < 50 then 'LOW'
        when unit_price between 50 and 200 then 'MEDIUM'
        else 'HIGH'
    end as price_bucket,

    case
        when unit_price is not null then 'ACTIVE'
        else 'INACTIVE'
    end as product_status

from {{ ref('stg_mercurymart__products') }}
