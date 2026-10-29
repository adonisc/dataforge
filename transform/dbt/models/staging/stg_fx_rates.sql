{{ config(materialized='view', tags=['staging', 'finance', 'tier-1']) }}

{#
    USD->USD is always 1.0, but the feed may or may not include it. The row is added
    only for dates where it's missing; adding it every time would create duplicates that
    double USD orders in fct_orders. A uniqueness test on the grain guards this.
#}

with source as (

    select * from {{ source('silver', 'fx_rates') }}

),

missing_usd as (

    -- One identity row per rate_date that has no USD rate of its own.
    select
        rate_date,
        'USD'                       as currency_code,
        cast(1.0 as decimal(18, 8)) as rate_to_usd,
        max(retrieved_at)           as retrieved_at
    from source
    where rate_date not in (
        select rate_date from source where currency_code = 'USD'
    )
    group by rate_date

),

combined as (

    select rate_date, currency_code, rate_to_usd, retrieved_at from source
    union all
    select rate_date, currency_code, rate_to_usd, retrieved_at from missing_usd

)

select
    rate_date,
    currency_code,
    cast(rate_to_usd as decimal(18, 8)) as rate_to_usd,
    retrieved_at
from combined
