{#
    dbt requires on_schema_change for incremental models with a contract.
    append_new_columns adds new attributes without a full rebuild.
#}
{{
    config(
        materialized='incremental',
        unique_key=['customer_key'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        tags=['marts', 'dimension', 'pii']
    )
}}

-- Type 2 dimension, so reports like "revenue by segment last quarter" use the segment
-- at the time, not today's.
--
-- The surrogate key hashes (natural key, valid_from), so a rebuild produces the same keys.

with source as (

    select * from {{ ref('stg_customers') }}

    {% if is_incremental() %}
    -- Re-read a window so late arrivals in older partitions are picked up.
    where source_ts >= (
        select coalesce(max(valid_from), '1900-01-01')
        from {{ this }}
    ) - interval '{{ var("late_arrival_window_days") }} days'
    {% endif %}

),

versioned as (

    select
        customer_id,
        email_token,
        email_domain,
        full_name,
        phone_last4,
        country_code,
        customer_segment,
        signup_ts,
        is_deleted,
        _record_hash                     as attribute_hash,
        source_ts                        as valid_from,
        lead(source_ts) over (
            partition by customer_id
            order by source_lsn
        )                                as next_change_ts
    from source

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from']) }} as customer_key,
    customer_id,
    email_token,
    email_domain,
    full_name,
    phone_last4,
    country_code,
    customer_segment,
    signup_ts,
    is_deleted,
    attribute_hash,
    valid_from,
    -- A far-future date instead of NULL keeps `between` queries simple.
    coalesce(next_change_ts, cast('9999-12-31' as timestamp)) as valid_to,
    (next_change_ts is null and not is_deleted)               as is_current

from versioned
