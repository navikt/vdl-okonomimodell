{% test have_hierarchy(model, segment_type) %}
    select *
    from {{ model }}
    where
        1 = 1
        and er_aktiv
        and har_hierarki = false
        and posterbar_fra_dato <= current_date
        and coalesce(posterbar_til_dato, '9999-01-01')
        >= '{{ var("forste_regnskapsdato") }}'
        and segment_type = '{{ segment_type }}'
{% endtest %}
