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

{% endmacro %}

{% macro _scd2__incremental(relation, unique_key, entity_key, created_at, loaded_at) %}
    with
        src as (
            select
                *,
                current_timestamp as _scd2_record_updated_at,
                _scd2_record_updated_at as _scd2_record_created_at
            from {{ relation }}
            where {{ created_at }} > (select max({{ created_at }}) from {{ this }})
        ),

        last_valid_records as (
            select
                this.* exclude(_scd2_valid_from, _scd2_valid_to, _scd2_record_updated_at),
                current_timestamp as _scd2_record_updated_at
            from {{ this }} as this
            inner join src on this.{{ entity_key }} = src.{{ entity_key }}
            where this._scd2_valid_to is null
        ),

        union_records as (
            select *
            from src
            union all
            select *
            from last_valid_records
        ),

        valid_to_from as (
            select
                *,
                {{ loaded_at }} as _scd2_valid_from,
                lead(_scd2_valid_from) over (
                    partition by {{ entity_key }} order by _scd2_valid_from
                ) as _scd2_valid_to
            from union_records
        ),

        final as (select * from valid_to_from)
    select *
    from final

{% endmacro %}

{% macro _scd2__full_refresh(relation, unique_key, entity_key, created_at, loaded_at) %}
    with
        src as (select * from {{ relation }}),

        valid_to_from as (
            select
                *,
                {{ loaded_at }} as _scd2_valid_from,
                lead(_scd2_valid_from) over (
                    partition by {{ entity_key }} order by _scd2_valid_from
                ) as _scd2_valid_to
            from src
        ),

        meta_data as (
            select
                *,
                current_timestamp as _scd2_record_updated_at,
                _scd2_record_updated_at as _scd2_record_created_at
            from valid_to_from
        ),

        final as (select * from meta_data)
    select *
    from final
{% endmacro %}
