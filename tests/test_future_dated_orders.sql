select *
from {{ ref('fct_mercurymart__orders_daily') }}
where order_date > current_date
