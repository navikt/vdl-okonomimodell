{% macro hist_d(relation, unique_key, check_cols, loaded_at) %}
    {{ log("hist macro called", info=True) }}

    {{
        config(
            materialized="table",
            unique_key="_hist_record_hash",
            on_schema_change="fail",
        )
    }}

    {% if is_incremental() %}
    -- TODO: Handle incremental load
    {% else %} {{ _hist__full_refresh_d(relation, unique_key, check_cols, loaded_at) }}
    {% endif %}
{% endmacro %}

{% macro _hist__incremental_d(relation, unique_key, check_cols, loaded_at) %}
    with
        src as (
            select
                relation.*,
                {{ dbt_utils.generate_surrogate_key(unique_key) }}
                as _hist_unique_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _hist_check_cols_hash,
                {{ loaded_at }} as _hist_loaded_at,
            from {{ relation }} as relation
            where _hist_loaded_at > (select max({{ loaded_at }}) from {{ this }})
        ),

        new_unique_load_times as (select distinct _hist_loaded_at from src),

        unique_load_times as (
            select distinct _hist_loaded_at
            from src
            union all
            select max(_hist_loaded_at) as _hist_loaded_at
            from {{ this }}
        ),

        load_times as (
            select
                _hist_loaded_at,
                lead(_hist_loaded_at) over (
                    order by _hist_loaded_at
                ) as _hist_next_loaded_at
            from unique_load_times
        ),

        update_next_load_times as (
            select
                this.* exclude(_hist_unique_key_next_load_time, _hist_next_loaded_at),
                lead(this._hist_loaded_at) over (
                    partition by this._hist_unique_key_hash
                    order by this._hist_loaded_at
                ) as _hist_unique_key_next_load_time
                load_times._hist_next_loaded_at,
            from {{ this }} as this
            inner join load_times on this._hist_loaded_at = load_times._hist_loaded_at
        )

    select *
    from {{ this }}
    where 1 = 2

{% endmacro %}

{% macro _hist__full_refresh_d(relation, unique_key, check_cols, loaded_at) %}
    with
        src as (
            select
                relation.*,
                {{ dbt_utils.generate_surrogate_key(unique_key) }}
                as _hist_unique_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _hist_check_cols_hash,
                {{ loaded_at }} as _hist_loaded_at,
            from {{ relation }} as relation
        ),

        unique_key_load_times as (
            select
                *,
                lead(_hist_loaded_at) over (
                    partition by _hist_unique_key_hash order by _hist_loaded_at
                ) as _hist_unique_key_next_load_time,
            from src
        ),

        unique_load_times as (
            select distinct _hist_loaded_at, from src order by _hist_loaded_at
        ),

        load_times as (
            select
                _hist_loaded_at,
                lead(_hist_loaded_at) over (
                    order by _hist_loaded_at
                ) as _hist_next_loaded_at
            from unique_load_times
        ),

        deletes as (
            select
                unique_key_load_times.*,
                load_times._hist_next_loaded_at,
                case
                    when
                        unique_key_load_times._hist_unique_key_next_load_time
                        != load_times._hist_next_loaded_at
                    then true
                    when
                        unique_key_load_times._hist_unique_key_next_load_time is null
                        and load_times._hist_next_loaded_at is not null
                    then true
                    else false
                end as _hist_unique_key_is_deleted,
                case
                    when _hist_unique_key_is_deleted
                    then load_times._hist_next_loaded_at
                    else null
                end as _hist_unique_key_deleted_at
            from unique_key_load_times
            left join
                load_times
                on unique_key_load_times._hist_loaded_at = load_times._hist_loaded_at
        ),

        last_values as (
            select
                *,
                lag(_hist_check_cols_hash, 1, '1') over (
                    partition by _hist_unique_key_hash order by _hist_loaded_at
                ) _hist_last_check_cols_hash,
                lag(_hist_unique_key_is_deleted, 1, false) over (
                    partition by _hist_unique_key_hash order by _hist_loaded_at
                ) _hist_last_unique_key_is_deleted,
            from deletes
        ),

        changed_records as (
            select
                *,
                case
                    when _hist_last_unique_key_is_deleted
                    then true
                    when _hist_unique_key_is_deleted
                    then true
                    when _hist_check_cols_hash != _hist_last_check_cols_hash
                    then true
                end as _hist_record_has_change
            from last_values
            where _hist_record_has_change
        ),

        meta_columns as (
            select
                changed_records.*,
                {{ dbt_utils.generate_surrogate_key(unique_key + [loaded_at]) }}
                as _hist_record_hash,
                '{{ relation }}' as _hist_input__relation,
                {{ unique_key }} as _hist_input__unique_key,
                {{ check_cols }} as _hist_input__check_cols,
                '{{ loaded_at }}' as _hist_input__loaded_at,
            from changed_records
        ),

        record_timestamps as (
            select
                meta_columns.*,
                current_timestamp as _hist_record_loaded_at,
                current_timestamp as _hist_updated_at,
            from meta_columns
        ),

        final as (select * from record_timestamps)

    select *
    from final
    order by _hist_loaded_at, _hist_unique_key_hash
{% endmacro %}
