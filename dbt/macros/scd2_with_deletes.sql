{% macro scd2_d(relation, unique_key, check_cols, loaded_at) %}
    {{ log("scd2 macro called", info=True) }}

    {{
        config(
            materialized="table",
            unique_key="_scd2_record_hash",
            on_schema_change="fail",
        )
    }}

    {% if is_incremental() %}
    -- TODO: Handle incremental load
    {% else %} {{ _scd2__full_refresh_d(relation, unique_key, check_cols, loaded_at) }}
    {% endif %}
{% endmacro %}

{% macro _scd2__incremental_d(relation, unique_key, check_cols, loaded_at) %}
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
            where _scd2_loaded_at > (select max({{ loaded_at }}) from {{ this }})
        ),

        new_unique_load_times as (select distinct _scd2_loaded_at from src),

        unique_load_times as (
            select distinct _scd2_loaded_at
            from src
            union all
            select max(_scd2_loaded_at) as _scd2_loaded_at
            from {{ this }}
        ),

        load_times as (
            select
                _scd2_loaded_at,
                lead(_scd2_loaded_at) over (
                    order by _scd2_loaded_at
                ) as _scd2_next_loaded_at
            from unique_load_times
        ),

        update_next_load_times as (
            select
                this.* exclude(_scd2_unique_key_next_load_time, _scd2_next_loaded_at),
                lead(this._scd2_loaded_at) over (
                    partition by this._scd2_unique_key_hash
                    order by this._scd2_loaded_at
                ) as _scd2_unique_key_next_load_time
                load_times._scd2_next_loaded_at,
            from {{ this }} as this
            inner join load_times on this._scd2_loaded_at = load_times._scd2_loaded_at
        )

    select *
    from {{ this }}
    where 1 = 2

{% endmacro %}

{% macro _scd2__full_refresh_d(relation, unique_key, check_cols, loaded_at) %}
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

        unique_key_load_times as (
            select
                *,
                lead(_scd2_loaded_at) over (
                    partition by _scd2_unique_key_hash order by _scd2_loaded_at
                ) as _scd2_unique_key_next_load_time,
            from src
        ),

        unique_load_times as (
            select distinct _scd2_loaded_at, from src order by _scd2_loaded_at
        ),

        load_times as (
            select
                _scd2_loaded_at,
                lead(_scd2_loaded_at) over (
                    order by _scd2_loaded_at
                ) as _scd2_next_loaded_at
            from unique_load_times
        ),

        deletes as (
            select
                unique_key_load_times.*,
                load_times._scd2_next_loaded_at,
                case
                    when
                        unique_key_load_times._scd2_unique_key_next_load_time
                        != load_times._scd2_next_loaded_at
                    then true
                    when
                        unique_key_load_times._scd2_unique_key_next_load_time is null
                        and load_times._scd2_next_loaded_at is not null
                    then true
                    else false
                end as _scd2_unique_key_is_deleted,
                case
                    when _scd2_unique_key_is_deleted
                    then load_times._scd2_next_loaded_at
                    else null
                end as _scd2_unique_key_deleted_at
            from unique_key_load_times
            left join
                load_times
                on unique_key_load_times._scd2_loaded_at = load_times._scd2_loaded_at
        ),

        last_values as (
            select
                *,
                lag(_scd2_check_cols_hash, 1, '1') over (
                    partition by _scd2_unique_key_hash order by _scd2_loaded_at
                ) _scd2_last_check_cols_hash,
                lag(_scd2_unique_key_is_deleted, 1, false) over (
                    partition by _scd2_unique_key_hash order by _scd2_loaded_at
                ) _scd2_last_unique_key_is_deleted,
            from deletes
        ),

        changed_records as (
            select
                *,
                case
                    when _scd2_last_unique_key_is_deleted
                    then true
                    when _scd2_unique_key_is_deleted
                    then true
                    when _scd2_check_cols_hash != _scd2_last_check_cols_hash
                    then true
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
