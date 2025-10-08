{% test segment_length(
    model,
    column_name,
    length,
    segment_type,
    code_column="kode",
    has_hierarchy_column="har_hierarki",
    segment_type_column="segment_type"
) %}
    select *
    from {{ model }}
    where
        1 = 1
        and len({{ column_name }}) != {{ length }}
        and {{ segment_type_column }} = '{{ segment_type }}'
        or (
            len({{ code_column }}) > {{ length }}
            and {{ column_name }} is null
            and {{ has_hierarchy_column }} = true
            and {{ segment_type_column }} = '{{ segment_type }}'
        )
{% endtest %}
