{% macro expression_is_true(model, expression) %}
select *
from {{ model }}
where not ({{ expression }})
{% endmacro %}

