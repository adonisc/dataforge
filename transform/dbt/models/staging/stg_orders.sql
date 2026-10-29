{{
    config(
        materialized='view',
        tags=['staging', 'commerce', 'tier-1']
    )
}}

-- Staging only renames, casts and derives simple columns. No joins or business logic.

with source as (

    select * from {{ source('silver', 'orders') }}

),

renamed as (

    select
        -- identifiers ---------------------------------------------------------------
        order_id,
        customer_id,
        customer_email_token,

        -- descriptors ---------------------------------------------------------------
        lower(order_status)                                   as order_status,
        lower(channel)                                        as channel,
        upper(currency_code)                                  as currency_code,
        promo_code,

        -- measures ------------------------------------------------------------------
        -- Cast explicitly so upstream precision changes can't slip through.
        cast(gross_amount    as decimal(18, 2))               as gross_amount,
        cast(coalesce(discount_amount, 0) as decimal(18, 2))  as discount_amount,
        cast(gross_amount - coalesce(discount_amount, 0)
             as decimal(18, 2))                               as net_amount,

        -- timestamps ------------------------------------------------------------------
        order_ts,
        updated_at,
        order_date,

        -- platform metadata -------------------------------------------------------------
        _ingested_at,
        _run_id,
        _contract_version,
        _record_hash

    from source

)

select * from renamed
