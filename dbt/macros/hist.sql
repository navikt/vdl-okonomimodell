{% macro hist(args) %}
    with
        src as (select '2024-01-01'::timestamp as t, 1 as id, 'foo' as val),
        -- Kanskje flyttes til oppbygging av hist-tabell?
        valid_to_from as (
            select
                *,
                t as _valid_from,
                lead(t) over (partition by id order by t) as _valid_to
            from changed_records
        ),

        invalidate_deletes as (
            select
                * exclude(_valid_to),
                case when _is_deleted then _deleted_at else _valid_to end as _valid_to
            from valid_to_from
        ),
        final as (select 'todo' as todo)
    select *
    from final
{% endmacro %}
