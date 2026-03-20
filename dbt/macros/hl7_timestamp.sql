{% macro hl7_timestamp(column) %}
    to_timestamp({{ column }}, 'YYYYMMDDHH24MISS')
{% endmacro %}
