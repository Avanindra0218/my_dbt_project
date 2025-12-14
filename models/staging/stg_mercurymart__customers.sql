select
    customer_id::number as customer_id,
    initcap(first_name) as first_name,
    initcap(last_name) as last_name,
    lower(email) as email,
    signup_date::date as signup_date,
    upper(country) as country,
    segment
from {{ source('mercurymart_raw', 'RAW_CUSTOMERS') }}
