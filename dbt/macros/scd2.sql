{% macro scd2(relation, unique_key, check_cols, loaded_at) %}
    {{ log("scd2 macro called", info=True) }}

    {{
        config(
            materialized="incremental",
            unique_key="_scd2_record_hash",
            on_schema_change="fail",
        )
    }}

    {% if is_incremental() %}
        {{ _scd2__incremental(relation, unique_key, check_cols, loaded_at) }}
    {% else %} {{ _scd2__full_refresh(relation, unique_key, check_cols, loaded_at) }}
    {% endif %}
{% endmacro %}

{% macro _scd2__incremental(relation, unique_key, check_cols, loaded_at) %}
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
                    where old._scd2_unique_key_hash = this._scd2_unique_key_hash
                )
        ),

        meta_hashes as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(unique_key) }}
                as _scd2_unique_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _scd2_check_cols_hash,
                {{ loaded_at }} as _scd2_loaded_at,
            from union_last_records
        ),

        last_values as (
            select
                *,
                lag(_scd2_check_cols_hash, 1, '1') over (
                    partition by _scd2_unique_key_hash order by _scd2_loaded_at
                ) _scd2_last_check_cols_hash,
            from meta_hashes
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
                {{ dbt_utils.generate_surrogate_key(unique_key + [loaded_at]) }}
                as _scd2_record_hash,
                '{{ relation }}' as _scd2_input__relation,
                {{ unique_key }} as _scd2_input__unique_key,
                {{ check_cols }} as _scd2_input__check_cols,
                '{{ loaded_at }}' as _scd2_input__loaded_at,
            from changed_records
        ),

        record_timestamps as (
            select
                meta_columns.*,
                current_timestamp as _scd2_record_loaded_at,
                current_timestamp as _scd2_updated_at,
            from meta_columns
        ),

        final as (select * from record_timestamps)

    select *
    from union_last_records
{% endmacro %}

{% macro _scd2__full_refresh(relation, unique_key, check_cols, loaded_at) %}
    with
        src as (
            select
                relation.*,
                {{ dbt_utils.generate_surrogate_key(unique_key) }}
                as _scd2_unique_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _scd2_check_cols_hash,
                {{ loaded_at }} as _scd2_loaded_at,
            from {{ relation }} as relation
        ),

        last_values as (
            select
                *,
                lag(_scd2_check_cols_hash, 1, '1') over (
                    partition by _scd2_unique_key_hash order by _scd2_loaded_at
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
                {{ dbt_utils.generate_surrogate_key(unique_key + [loaded_at]) }}
                as _scd2_record_hash,
                '{{ relation }}' as _scd2_input__relation,
                {{ unique_key }} as _scd2_input__unique_key,
                {{ check_cols }} as _scd2_input__check_cols,
                '{{ loaded_at }}' as _scd2_input__loaded_at,
            from changed_records
        ),

        record_timestamps as (
            select
                meta_columns.*,
                current_timestamp as _scd2_record_loaded_at,
                current_timestamp as _scd2_updated_at,
            from meta_columns
        ),

        final as (select * from record_timestamps)

    select *
    from final
    order by _scd2_loaded_at, _scd2_unique_key_hash
{% endmacro %}
