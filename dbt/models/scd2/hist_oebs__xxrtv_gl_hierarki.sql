{{
    config(
        materialized="incremental",
        unique_key="_uid",
    )
}}

with
    source as (
        select
            flex_value_set_id,
            hierarchy_code,
            flex_value_id,
            flex_value,
            description,
            flex_value_id_parent,
            flex_value_parent,
            description_parent,
            flex_value_set_name,
            -- inbound-metadata
            _oppdatert_tidspunkt as _inbound__load_time  -- inbound_loadtime
        from {{ source("oebs", "segmet_hierarki__test") }}
    ),

    source_data as (
        select
            flex_value_set_id,
            hierarchy_code,
            flex_value_id,
            flex_value,
            description,
            flex_value_id_parent,
            flex_value_parent,
            description_parent,
            flex_value_set_name,
            -- inbound-metadata
            _inbound__load_time,

        from source
    ),

    hash_key as (
        select
            *,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "flex_value_set_id",
                        "flex_value_id",
                        "description",
                        "flex_value_id_parent",
                        "flex_value_parent",
                        "description_parent",
                        "flex_value_set_name",
                    ]
                )
            }} as hash_key,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "flex_value",
                        "hierarchy_code",
                    ]
                )
            }} as id,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "flex_value_set_id",
                        "hierarchy_code",
                        "_inbound__load_time",
                    ]
                )
            }} as _uid,
        from source_data
    ),

    equal_hash_keys as (
        select
            *,
            row_number() over (
                partition by id, hash_key order by _inbound__load_time
            ) as n
        from hash_key
    ),

    filter_equal_hash_keys as (select * from equal_hash_keys where n = 1),

    hist_columns_names as (
        select
            *,
            hash_key as _hist_check_cols_hash,
            id as _hist_key_hash,
            cast(false as boolean) as _hist_is_deleted,
            cast(null as timestamp) as _hist_last_appered_at,
            _inbound__load_time as _hist_updated_at,
            _hist_updated_at as _hist_valid_from,
            lead(_hist_updated_at) over (
                partition by id order by _hist_updated_at
            ) as _hist_valid_to,
        from filter_equal_hash_keys
    ),

    final as (
        select *,
        -- flex_value_set_id,
        -- hierarchy_code,
        -- flex_value_id,
        -- flex_value,
        -- description,
        -- flex_value_id_parent,
        -- flex_value_parent,
        -- description_parent,
        -- flex_value_set_name,
        -- -- inbound-metadata
        -- _inbound__load_time,
        -- hash_key,
        -- --
        from hist_columns_names
    )

select *
from final
