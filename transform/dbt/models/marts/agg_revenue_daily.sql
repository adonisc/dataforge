{{
    config(
        materialized='incremental',
        unique_key=['revenue_date', 'channel', 'currency_code'],
        incremental_strategy='merge',
        tags=['marts', 'aggregate', 'bi']
    )
}}

-- Pre-aggregated because it backs a dashboard queried hundreds of times a day.
-- Only worth doing when queries x rows scanned is large and the grain is stable.

with orders as (

    select * from {{ ref('fct_orders') }}

    {% if is_incremental() %}
    where order_date >= (select max(revenue_date) from {{ this }})
                        - interval '{{ var("late_arrival_window_days") }} days'
    {% endif %}

)

select
    order_date                                        as revenue_date,
    channel,
    currency_code,

    count(*)                                          as order_count,
    count(distinct customer_id)                       as distinct_customers,
    sum(gross_amount)                                 as gross_amount,
    sum(discount_amount)                              as discount_amount,
    sum(net_amount)                                   as net_amount,
    sum(net_amount_usd)                               as net_amount_usd,

    -- Show data quality next to the number, e.g. how much revenue had no FX rate.
    sum(case when has_missing_fx_rate then 1 else 0 end) as rows_missing_fx_rate,

    count(*) filter (where order_status in ('cancelled', 'refunded')) as cancelled_count,

    current_timestamp                                 as _built_at

from orders
group by 1, 2, 3
