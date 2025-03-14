{% test column_value_length(model, column_name, length) %}
    select * from {{ model }} where 1 = 1 and len({{ column_name }}) != {{ length }}
{% endtest %}
