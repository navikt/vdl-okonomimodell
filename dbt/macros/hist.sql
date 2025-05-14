{% macro hist(from, entity_key, check_cols, loaded_at) %}
    {{
        config(
            materialized="incremental",
            entity_key="_hist_record_hash",
            on_schema_change="fail",
        )
    }}

    {% if is_incremental() %}
        {{ _hist__incremental(from, entity_key, check_cols, loaded_at) }}
    {% else %} {{ _hist__full_refresh(from, entity_key, check_cols, loaded_at) }}
    {% endif %}
{% endmacro %}

{% macro _hist__incremental(from, entity_key, check_cols, loaded_at) %}
    with
        src as (
            select *
            from {{ from }}
            where {{ loaded_at }} > (select max(_hist_loaded_at) from {{ this }})
        ),

        src_hash as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _hist_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _hist_check_cols_hash,
                {{ loaded_at }} as _hist_loaded_at,
            from src
        ),

        last_records as (
            select
                {{ dbt_utils.star(from=from, quote_identifiers=false) }},
                _hist_entity_key_hash,
                _hist_check_cols_hash,
                _hist_loaded_at
            from {{ this }} as this
            qualify
                max(_hist_loaded_at) over (partition by _hist_entity_key_hash)
                = this._hist_loaded_at
        ),

        union_records as (
            select *, true as _hist_is_new_record
            from src_hash
            union all
            select *, false as _hist_is_new_record
            from last_records
        ),

        entity_key_load_times as (
            select
                *,
                lead(_hist_loaded_at) over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) as _hist_entity_key_next_load_time,
            from union_records
        ),

        unique_load_times as (
            select distinct _hist_loaded_at,
            from src_hash
            union all
            select max(_hist_loaded_at) as _hist_loaded_at,
            from {{ this }}
            order by 1
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
                entity_key_load_times.*,
                load_times._hist_next_loaded_at,
                case
                    when
                        entity_key_load_times._hist_entity_key_next_load_time
                        != load_times._hist_next_loaded_at
                    then true
                    when
                        entity_key_load_times._hist_entity_key_next_load_time is null
                        and load_times._hist_next_loaded_at is not null
                    then true
                    else false
                end as _hist_entity_key_is_deleted,
                case
                    when _hist_entity_key_is_deleted
                    then load_times._hist_next_loaded_at
                    else null
                end as _hist_entity_key_deleted_at
            from entity_key_load_times
            left join
                load_times
                on entity_key_load_times._hist_loaded_at = load_times._hist_loaded_at
        ),

        --
        last_values as (
            select
                *,
                lag(_hist_check_cols_hash, 1, '1') over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) _hist_last_check_cols_hash,
                lag(_hist_entity_key_is_deleted, 1, false) over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) _hist_last_entity_key_is_deleted,
            from deletes
        ),

        changed_records as (
            select
                *,
                case
                    when _hist_last_entity_key_is_deleted
                    then true
                    when _hist_entity_key_is_deleted
                    then true
                    when _hist_check_cols_hash != _hist_last_check_cols_hash
                    then true
                end as _hist_record_has_change
            from last_values
            where _hist_record_has_change
        ),

        filter_out_existing_records as (
            -- TODO: Må kanskje ta høyde for at en gammel record kan oppdatere seg
            select * from changed_records where _hist_is_new_record
        ),

        meta_columns as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(entity_key + [loaded_at]) }}
                as _hist_record_hash,
                '{{ from }}' as _hist_input__from,
                {{ entity_key }} as _hist_input__entity_key,
                {{ check_cols }} as _hist_input__check_cols,
                '{{ loaded_at }}' as _hist_input__loaded_at,
                current_timestamp as _hist_record_created_at,
            from filter_out_existing_records
        ),

        final as (select * from meta_columns)

    select *
    from final
{% endmacro %}

{% macro _hist__full_refresh(from, entity_key, check_cols, loaded_at) %}
    with
        src as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _hist_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _hist_check_cols_hash,
                {{ loaded_at }} as _hist_loaded_at,
            from {{ from }}
        ),

        entity_key_load_times as (
            select
                *,
                lead(_hist_loaded_at) over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) as _hist_entity_key_next_load_time,
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
                entity_key_load_times.*,
                load_times._hist_next_loaded_at,
                case
                    when
                        entity_key_load_times._hist_entity_key_next_load_time
                        != load_times._hist_next_loaded_at
                    then true
                    when
                        entity_key_load_times._hist_entity_key_next_load_time is null
                        and load_times._hist_next_loaded_at is not null
                    then true
                    else false
                end as _hist_entity_key_is_deleted,
                case
                    when _hist_entity_key_is_deleted
                    then load_times._hist_next_loaded_at
                    else null
                end as _hist_entity_key_deleted_at
            from entity_key_load_times
            left join
                load_times
                on entity_key_load_times._hist_loaded_at = load_times._hist_loaded_at
        ),

        last_values as (
            select
                *,
                lag(_hist_check_cols_hash, 1, '1') over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) _hist_last_check_cols_hash,
                lag(_hist_entity_key_is_deleted, 1, false) over (
                    partition by _hist_entity_key_hash order by _hist_loaded_at
                ) _hist_last_entity_key_is_deleted,
            from deletes
        ),

        changed_records as (
            select
                *,
                case
                    when _hist_last_entity_key_is_deleted
                    then true
                    when _hist_entity_key_is_deleted
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
                {{ dbt_utils.generate_surrogate_key(entity_key + [loaded_at]) }}
                as _hist_record_hash,
                '{{ from }}' as _hist_input__from,
                {{ entity_key }} as _hist_input__entity_key,
                {{ check_cols }} as _hist_input__check_cols,
                '{{ loaded_at }}' as _hist_input__loaded_at,
                true as _hist_is_new_record,
            from changed_records
        ),

        record_timestamps as (
            select meta_columns.*, current_timestamp as _hist_record_created_at,
            from meta_columns
        ),

        final as (select * from record_timestamps)

    select *
    from final
    order by _hist_loaded_at, _hist_entity_key_hash
{% endmacro %}
