{% macro scd2(
    relation,
    unique_key="_hist_record_hash",
    entity_key="_hist_entity_key_hash",
    created_at="_hist_record_created_at",
    loaded_at="_hist_loaded_at"
) %}
    {{
        config(
            materialized="incremental",
            unique_key=unique_key,
            on_schema_change="fail",
        )
    }}
    with
        _scd2_cte as (
            {% if is_incremental() %}
                {{
                    _scd2__incremental(
                        relation, unique_key, entity_key, created_at, loaded_at
                    )
                }}
            {% else %}
                {{
                    _scd2__full_refresh(
                        relation, unique_key, entity_key, created_at, loaded_at
                    )
                }}
            {% endif %}
        ),

        _final as (select *, from _scd2_cte)
    select *
    from _final

{% endmacro %}

{% macro _scd2__incremental(relation, unique_key, entity_key, created_at, loaded_at) %}
    with
        _src as (
            select
                *,
                current_timestamp as _scd2_record_updated_at,
                _scd2_record_updated_at as _scd2_record_created_at
            from {{ relation }}
            where {{ created_at }} > (select max({{ created_at }}) from {{ this }})
        ),

        _last_valid_records as (
            select
                this.* exclude(
                    _scd2_valid_from, _scd2_valid_to, _scd2_record_updated_at
                ),
                current_timestamp as _scd2_record_updated_at
            from {{ this }} as this
            inner join _src on this.{{ entity_key }} = _src.{{ entity_key }}
            where this._scd2_valid_to is null
        ),

        _union_records as (
            select *
            from _src
            union all
            select *
            from _last_valid_records
        ),

        _valid_to_from as (
            select
                *,
                {{ loaded_at }} as _scd2_valid_from,
                lead(_scd2_valid_from) over (
                    partition by {{ entity_key }} order by _scd2_valid_from
                ) as _scd2_valid_to
            from _union_records
        ),

        _macro_final as (select * from _valid_to_from)
    select *
    from _macro_final

{% endmacro %}

{% macro _scd2__full_refresh(relation, unique_key, entity_key, created_at, loaded_at) %}
    with
        _src as (select * from {{ relation }}),

        _valid_to_from as (
            select
                *,
                {{ loaded_at }} as _scd2_valid_from,
                lead(_scd2_valid_from) over (
                    partition by {{ entity_key }} order by _scd2_valid_from
                ) as _scd2_valid_to
            from _src
        ),

        _meta_data as (
            select
                *,
                current_timestamp as _scd2_record_updated_at,
                _scd2_record_updated_at as _scd2_record_created_at
            from _valid_to_from
        ),

        _macro_final as (select * from _meta_data)
    select *
    from _macro_final
{% endmacro %}
