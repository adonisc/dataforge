{#
    Generic test: an SCD2 chain must have no overlapping validity intervals.

    No two versions of a key may be valid at the same time; otherwise point-in-time
    joins fan out and double count. Reusable on any type 2 dimension.
#}
{% test scd2_no_overlapping_versions(model, key_column, from_column='valid_from', to_column='valid_to') %}

with versions as (

    select
        {{ key_column }}  as natural_key,
        {{ from_column }} as valid_from,
        {{ to_column }}   as valid_to,
        lead({{ from_column }}) over (
            partition by {{ key_column }}
            order by {{ from_column }}
        ) as next_valid_from
    from {{ model }}

)

select
    natural_key,
    valid_from,
    valid_to,
    next_valid_from
from versions
where next_valid_from is not null
  and valid_to > next_valid_from

{% endtest %}
