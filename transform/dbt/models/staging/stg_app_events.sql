{{ config(materialized='view', tags=['staging', 'product', 'tier-2']) }}

select
    event_id,
    session_id,
    customer_id,
    lower(event_name)                     as event_name,
    coalesce(lower(platform), 'unknown')  as platform,
    event_ts,
    event_date,
    -- Keep the JSON as a string so producer changes don't break this model.
    properties                            as properties_json,
    _ingested_at

from {{ source('silver', 'app_events') }}
