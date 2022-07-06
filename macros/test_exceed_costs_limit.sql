{% test exceed_costs_limit(model, column_name) %}

with validation as (
    select
        {{ column_name }} as limit_field
    from {{ model }}
),

validation_errors as (
    select
        limit_field
    from validation
    where limit_field > 5000
)

select *
from validation_errors

{% endtest %}