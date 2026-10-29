{#
    Physical layout is set per adapter.

    * No `partition_by`: Snowflake doesn't use it and DuckDB rejects it. Lake
      partitioning comes from the contract (`spec.partitioning`).
    * `cluster_by` only on Snowflake; DuckDB errors on it.
#}
{%- set is_snowflake = target.type == 'snowflake' -%}
{{
    config(
        materialized='incremental',
        unique_key=['order_key'],
        incremental_strategy='merge',
        cluster_by=(['channel', 'order_status'] if is_snowflake else none),
        on_schema_change='append_new_columns',
        tags=['marts', 'fact', 'tier-1']
    )
}}

-- One row per order.
--
-- * Incremental merge, since old orders rarely change and full refreshes get expensive.
-- * The customer key is looked up as of the order time, not today.
-- * Currency is converted once here, so every dashboard reports the same revenue.

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
    where order_date >= (select max(order_date) from {{ this }})
                        - interval '{{ var("late_arrival_window_days") }} days'
    {% endif %}

),

fx as (

    select * from {{ ref('stg_fx_rates') }}

),

customer as (

    select customer_key, customer_id, valid_from, valid_to
    from {{ ref('dim_customer') }}

),

joined as (

    select
        orders.*,

        -- Point-in-time dimension resolution.
        customer.customer_key,

        -- Left join: a missing FX rate gets flagged, not turned into NULL revenue.
        fx.rate_to_usd

    from orders

    left join customer
        on  orders.customer_id = customer.customer_id
        and orders.order_ts   >= customer.valid_from
        and orders.order_ts    < customer.valid_to

    left join fx
        on  orders.currency_code = fx.currency_code
        and orders.order_date    = fx.rate_date

)

select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
    order_id,
    coalesce(customer_key, '-1')                         as customer_key,
    customer_id,
    customer_email_token,

    order_status,
    channel,
    currency_code,
    promo_code,

    gross_amount,
    discount_amount,
    net_amount,
    cast(net_amount * rate_to_usd as decimal(18, 2))     as net_amount_usd,
    rate_to_usd,

    order_ts,
    order_date,
    updated_at,

    -- Quality flags on the fact, so consumers can filter to trusted rows.
    (rate_to_usd  is null) as has_missing_fx_rate,
    (customer_key is null) as has_unmatched_customer,

    _run_id,
    _contract_version,
    current_timestamp      as _built_at

from joined
