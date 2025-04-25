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
            cast(false as boolean) as er_slettet,
            cast(null as timestamp) as slettet_dato
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

    final as (
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
            hash_key,
            --
            _inbound__load_time as gyldig_fra_tidspunkt,
            lead(_inbound__load_time) over (
                partition by id order by _inbound__load_time
            ) as gyldig_til_tidspunkt,
        from filter_equal_hash_keys
    )

select *
from final
