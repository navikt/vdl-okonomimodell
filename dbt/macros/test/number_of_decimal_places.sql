{% test number_of_decimal_places(model, column_name, scale=2) %}

    {{ config(severity="warn") }}

    select *
    from {{ model }}
    where length(split(parse_json(raw):{{ column_name }}, '.')[1]) > 2

{% endtest %}
