{% macro hist_deletes(from, entity_key, loaded_at) %}
    {{
        config(
            materialized="table",
            unique_key="_hist_record_hash",
            on_schema_change="fail",
        )
    }}

    {% if is_incremental() %}
    -- TODO: Handle incremental load
    {% else %} {{ _hist__full_refresh_d(from, entity_key, loaded_at) }}
    {% endif %}
{% endmacro %}

{% macro _hist__full_refresh_d(from, entity_key, loaded_at) %}

    with
        src as (select * from {{ from }}),

        metadata as (
            select
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _hist_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(entity_key + [loaded_at]) }}
                as _hist_record_hash,
                {{ loaded_at }} as _hist_loaded_at,
                lead(_hist_loaded_at, 1, null) over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) as _hist_next_loaded_at,
            from src
        ),

        unique_loaded_at as (select distinct _hist_loaded_at from metadata),

        load_times as (
            select
                _hist_loaded_at,
                lead(_hist_loaded_at, 1, null) over (
                    order by _hist_loaded_at
                ) as _hist_next_loaded_at,
            from unique_loaded_at
        ),

        deletes as (
            select
                metadata.*,
                case
                    when
                        coalesce(metadata._hist_next_loaded_at, '1'::timestamp)
                        != coalesce(load_times._hist_next_loaded_at, '1'::timestamp)
                    then true
                    else false
                end as _hist_is_deleted
            from metadata
            inner join
                load_times on metadata._hist_loaded_at = load_times._hist_loaded_at
            where _hist_is_deleted
        ),

        final as (select * from deletes)

    select *
    from final

{% endmacro %}
