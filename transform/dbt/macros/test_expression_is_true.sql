{#
    Model-level assertion over an aggregate expression.

    Good for threshold checks like "fewer than 1% of rows have no FX rate", where the
    condition is allowed but should stay rare.
#}
{% test expression_is_true(model, expression) %}

select 1 as failure
from {{ model }}
having not ({{ expression }})

{% endtest %}
