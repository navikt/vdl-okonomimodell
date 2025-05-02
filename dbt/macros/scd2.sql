{% macro scd2(relation, entity_key, check_cols, loaded_at) %}
    {{ log("scd2 macro called", info=True) }}

    {{
        config(
            materialized="incremental",
            entity_key="_scd2_record_hash",
            on_schema_change="fail",
        )
    }}

    {% if is_incremental() %}
        {{ _scd2__incremental(relation, entity_key, check_cols, loaded_at) }}
    {% else %} {{ _scd2__full_refresh(relation, entity_key, check_cols, loaded_at) }}
    {% endif %}
{% endmacro %}

{% macro _scd2__incremental(relation, entity_key, check_cols, loaded_at) %}
    with
        src as (
            select *
            from {{ relation }}
            where {{ loaded_at }} > (select max(_scd2_loaded_at) from {{ this }})
        ),

        union_last_records as (
            select *
            from src
            union all
            select {{ dbt_utils.star(from=relation, quote_identifiers=false) }}
            from {{ this }} as this
            where
                _scd2_loaded_at = (
                    select max(_scd2_loaded_at)
                    from {{ this }} as old
                    where old._scd2_entity_key_hash = this._scd2_entity_key_hash
                )
        ),

        meta_hashes as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _scd2_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _scd2_check_cols_hash,
                {{ loaded_at }} as _scd2_loaded_at,
            from union_last_records
        ),

        -- Alternative som finner siste rad per unique key
        src_hash as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _scd2_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _scd2_check_cols_hash,
                {{ loaded_at }} as _scd2_loaded_at,
            from src
        ),

        last_records as (
            select
                {{ dbt_utils.star(from=relation, quote_identifiers=false) }},
                _scd2_entity_key_hash,
                _scd2_check_cols_hash,
                _scd2_loaded_at
            from {{ this }} as this
            qualify
                max(_scd2_loaded_at) over (partition by _scd2_entity_key_hash)
                = this._scd2_loaded_at
        ),

        alternate_union_last_records as (
            select *, true as _scd2_is_new_record
            from src_hash
            union all
            select *, false as _scd2_is_new_record
            from last_records
        ),

        --
        last_values as (
            select
                *,
                lag(_scd2_check_cols_hash, 1, '1') over (
                    partition by _scd2_entity_key_hash order by _scd2_loaded_at
                ) _scd2_last_check_cols_hash,
            from alternate_union_last_records
        ),

        changed_records as (
            select
                *,
                _scd2_check_cols_hash
                != _scd2_last_check_cols_hash as _scd2_record_has_change
            from last_values
            where _scd2_record_has_change
        ),

        filter_out_existing_records as (
            select * from changed_records where _scd2_is_new_record
        ),

        meta_columns as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(entity_key + [loaded_at]) }}
                as _scd2_record_hash,
                '{{ relation }}' as _scd2_input__relation,
                {{ entity_key }} as _scd2_input__entity_key,
                {{ check_cols }} as _scd2_input__check_cols,
                '{{ loaded_at }}' as _scd2_input__loaded_at,
                current_timestamp as _scd2_record_created_at,
            from filter_out_existing_records
        ),

        final as (select * from meta_columns)

    select *
    from final
{% endmacro %}

{% macro _scd2__full_refresh(relation, entity_key, check_cols, loaded_at) %}
    with
        src as (
            select
                relation.*,
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _scd2_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _scd2_check_cols_hash,
                {{ loaded_at }} as _scd2_loaded_at,
            from {{ relation }} as relation
        ),

        last_values as (
            select
                *,
                lag(_scd2_check_cols_hash, 1, '1') over (
                    partition by _scd2_entity_key_hash order by _scd2_loaded_at
                ) _scd2_last_check_cols_hash,
            from src
        ),

        changed_records as (
            select
                *,
                case
                    when _scd2_check_cols_hash != _scd2_last_check_cols_hash then true
                end as _scd2_record_has_change
            from last_values
            where _scd2_record_has_change
        ),

        meta_columns as (
            select
                changed_records.*,
                {{ dbt_utils.generate_surrogate_key(entity_key + [loaded_at]) }}
                as _scd2_record_hash,
                '{{ relation }}' as _scd2_input__relation,
                {{ entity_key }} as _scd2_input__entity_key,
                {{ check_cols }} as _scd2_input__check_cols,
                '{{ loaded_at }}' as _scd2_input__loaded_at,
                true as _scd2_is_new_record,
            from changed_records
        ),

        record_timestamps as (
            select meta_columns.*, current_timestamp as _scd2_record_created_at,
            from meta_columns
        ),

        final as (select * from record_timestamps)

    select *
    from final
    order by _scd2_loaded_at, _scd2_entity_key_hash
{% endmacro %}
