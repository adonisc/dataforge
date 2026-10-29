{#
    Generic test: an aggregate must still sum to its source fact, within tolerance.

    Catches an aggregate drifting from its fact (e.g. after a late backfill), which no
    other test would notice. Tolerance is a fraction, since rounding isn't exact everywhere.
#}
{% test aggregate_reconciles_to_fact(
        model, fact_model, fact_measure, agg_measure,
        grain_column, agg_grain_column, tolerance=0.001
) %}

with fact_totals as (

    select
        {{ grain_column }}      as grain,
        sum({{ fact_measure }}) as fact_total
    from {{ ref(fact_model) }}
    group by 1

),

agg_totals as (

    select
        {{ agg_grain_column }} as grain,
        sum({{ agg_measure }}) as agg_total
    from {{ model }}
    group by 1

),

compared as (

    select
        coalesce(f.grain, a.grain)          as grain,
        coalesce(f.fact_total, 0)           as fact_total,
        coalesce(a.agg_total, 0)            as agg_total,
        abs(coalesce(f.fact_total, 0) - coalesce(a.agg_total, 0)) as absolute_difference
    from fact_totals f
    full outer join agg_totals a on f.grain = a.grain

)

select *
from compared
where absolute_difference > abs(fact_total) * {{ tolerance }}
  and absolute_difference > 0.01

{% endtest %}
