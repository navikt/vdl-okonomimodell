{% macro convert_dbt_snapshot(from, entity_key, check_cols) %}
    with

        src as (select * from {{ from }}),

        converted as (
            select
                *,
                dbt_valid_from::timestamp as _hist_loaded_at,
                {{ dbt_utils.generate_surrogate_key(entity_key) }}
                as _hist_entity_key_hash,
                {{ dbt_utils.generate_surrogate_key(check_cols) }}
                as _hist_check_cols_hash,
                {{ dbt_utils.generate_surrogate_key(entity_key + ["_hist_loaded_at"]) }}
                as _hist_record_hash,
                null as _hist_entity_key_next_load_time,
                null as _hist_next_loaded_at,
                null as _hist_entity_key_is_deleted,
                null as _hist_entity_key_deleted_at,
                null as _hist_last_check_cols_hash,
                null as _hist_last_entity_key_is_deleted,
                null as _hist_record_has_change,
                null as _hist_input__from,
                null as _hist_input__entity_key,
                null as _hist_input__check_cols,
                null as _hist_input__loaded_at,
                null as _hist_record_created_at
            from src
        ),

        final as (
            select
                * exclude(
                    dbt_scd_id,
                    dbt_updated_at,
                    dbt_valid_from,
                    dbt_valid_to,
                    dbt_is_deleted
                )
            from converted
        )

    select *
    from final

{% endmacro %}
