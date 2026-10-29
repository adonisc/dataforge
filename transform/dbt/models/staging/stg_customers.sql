{{ config(materialized='view', tags=['staging', 'commerce', 'pii']) }}

-- Keep deleted customers: their orders still need something to join to.

with source as (

    select * from {{ source('silver', 'customers') }}

)

select
    customer_id,
    email_token,
    email_for_segmentation_domain                as email_domain,
    full_name_masked                             as full_name,
    phone_masked                                 as phone_last4,
    upper(country_code)                          as country_code,
    lower(coalesce(customer_segment, 'unknown')) as customer_segment,
    signup_ts,
    source_ts,
    source_lsn,
    _is_deleted                                  as is_deleted,
    _ingested_at,
    _record_hash

from source
