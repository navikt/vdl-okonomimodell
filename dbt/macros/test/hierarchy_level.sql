{% test hierarchy_level_length(model, column_name, hierarchy_code, length) %}
    select *
    from {{ model }}
    where 1 = 1 and har_hierarki = true and len({{ column_name }}) != {{ length }}
{% endtest %}

{% test hierarchy_level_not_null(model, column_name, hierarchy_code, length, segment) %}
    select *
    from {{ model }}
    where
        1 = 1
        and har_hierarki = true
        and len({{ segment }}_segment_kode) = {{ length }}
        and {{ column_name }} is null
{% endtest %}
