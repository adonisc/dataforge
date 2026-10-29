-- Deleted customers must not have new revenue in the current period.
--
-- Old orders are kept (erasure removes identity, not transactions), but a new order for
-- a deleted customer means the erasure didn't propagate or an id was reused.

select
    f.customer_id,
    count(*)            as order_count,
    sum(f.net_amount_usd) as revenue_usd,
    max(f.order_ts)     as most_recent_order

from {{ ref('fct_orders') }} f

inner join {{ ref('dim_customer') }} d
    on  f.customer_key = d.customer_key
    and d.is_deleted

where f.order_ts > d.valid_from
group by 1
having count(*) > 0
