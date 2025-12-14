select
    order_item_id::number as order_item_id,
    order_id::number as order_id,
    product_id::number as product_id,
    quantity::number as quantity,
    unit_price::number(38,2) as unit_price,
    quantity * unit_price as gross_amount
from {{ source('mercurymart_raw', 'RAW_ORDER_ITEMS') }}

