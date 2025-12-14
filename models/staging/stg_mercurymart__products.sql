select
    product_id::number as product_id,
    product_name,
    category,
    brand,
    unit_price::number(38,2) as unit_price,
    supplier_id::number as supplier_id
from {{ source('mercurymart_raw', 'RAW_PRODUCTS') }}
