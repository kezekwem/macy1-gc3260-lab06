{% macro safe_timestamp(column) %}
    {% if target.type in ['postgres', 'postgresql'] %}
        NULLIF(BTRIM({{ column }}::text), '')::timestamp
    {% elif target.type == 'sqlite' %}
        CASE WHEN trim({{ column }}) = '' THEN NULL ELSE datetime({{ column }}) END
    {% else %}
        NULLIF({{ column }}, '')
    {% endif %}
{% endmacro %}


{% macro safe_numeric(column, precision=None, scale=None) %}
    {% if target.type in ['postgres', 'postgresql'] %}
        NULLIF(BTRIM({{ column }}::text), '')::numeric{% if precision and scale %}({{ precision }}, {{ scale }}){% endif %}
    {% elif target.type == 'sqlite' %}
        CAST(NULLIF(trim({{ column }}), '') AS REAL)
    {% else %}
        CAST(NULLIF({{ column }}, '') AS NUMERIC)
    {% endif %}
{% endmacro %}


{% macro delivery_minutes_expr(dropoff_col, pickup_col) %}
    {% if target.type in ['postgres', 'postgresql'] %}
        EXTRACT(epoch FROM ({{ dropoff_col }} - {{ pickup_col }})) / 60.0
    {% elif target.type == 'sqlite' %}
        (strftime('%s', {{ dropoff_col }}) - strftime('%s', {{ pickup_col }})) / 60.0
    {% else %}
        NULL
    {% endif %}
{% endmacro %}


{% macro bool_true() %}
    {% if target.type == 'sqlite' %}1{% else %}TRUE{% endif %}
{% endmacro %}


{% macro bool_false() %}
    {% if target.type == 'sqlite' %}0{% else %}FALSE{% endif %}
{% endmacro %}


{% macro cancel_return_rate_expr(status_column='status') %}
    {% if target.type in ['postgres', 'postgresql'] %}
        (COUNT(*) FILTER (WHERE {{ status_column }} IN ('canceled', 'returned'))::numeric / NULLIF(COUNT(*), 0))
    {% elif target.type == 'sqlite' %}
        (CAST(SUM(CASE WHEN {{ status_column }} IN ('canceled', 'returned') THEN 1 ELSE 0 END) AS REAL) / NULLIF(COUNT(*), 0))
    {% else %}
        NULL
    {% endif %}
{% endmacro %}


{% macro avg_on_time_expr(flag_column='on_time_flag') %}
    {% if target.type in ['postgres', 'postgresql'] %}
        AVG(CASE WHEN {{ flag_column }} THEN 1 ELSE 0 END)::numeric(5, 4)
    {% elif target.type == 'sqlite' %}
        AVG(CASE WHEN {{ flag_column }} = 1 THEN 1.0 ELSE 0.0 END)
    {% else %}
        AVG(CASE WHEN {{ flag_column }} THEN 1 ELSE 0 END)
    {% endif %}
{% endmacro %}


{% macro avg_delivery_minutes_expr(column='delivery_minutes') %}
    {% if target.type in ['postgres', 'postgresql'] %}
        AVG({{ column }})::numeric(6, 2)
    {% else %}
        AVG({{ column }})
    {% endif %}
{% endmacro %}
