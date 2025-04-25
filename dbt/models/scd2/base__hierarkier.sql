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
            _oppdatert_tidspunkt  -- inbound_loadtime
        from {{ source("oebs", "segmet_hierarki__test") }}
        {% if is_incremental() %}
            where
                _oppdatert_tidspunkt
                > (select max(_oppdatert_tidspunkt) from {{ this }})
        {% endif %}
    ),
    {% if is_incremental() %}
        previous as (select * from {{ this }} where valid_to_ts is null),
        deleted_table as (
            select
                *,
                source.flex_value_id is null er_slettet,
                case when source.flex_value_id then valid_from_ts as slettet_dato -- kanskje max i source
            from previous
            left join
                source
                on previous.flex_value_id = source.flex_value_id
                and previous.hierarchy_code = source.hierarchy_code
        )
    {% endif %}

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
            _oppdatert_tidspunkt,
            cast(false as boolean) as er_slettet,
            cast(null as timestamp) as slettet_dato
        from source
        {% if is_incremental() %}
            union all
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
                _oppdatert_tidspunkt,
                er_slettet,
                slettet_dato
            from deleted_table
        {% endif %}
    ),
    hash_key as (
        select
            *,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "flex_value_set_id",
                        "hierarchy_code",
                        "flex_value_id",
                        "flex_value",
                        "description",
                        "flex_value_id_parent",
                        "flex_value_parent",
                        "description_parent",
                        "flex_value_set_name",
                        "_oppdatert_tidspunkt",
                    ]
                )
            }} as hash_key,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "flex_value_set_id",
                        "hierarchy_code",
                    ]
                )
            }} as id,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "flex_value_set_id",
                        "hierarchy_code",
                        "_oppdatert_tidspunkt",
                    ]
                )
            }} as _uid,
        from source_data
    ),
    lag_hash_key as (
        select
            *,
            lag(hash_key, 1) over (
                partition by id order by _oppdatert_tidspunkt
            ) as lag_hash_key
        from hash_key
    ),
    filter_hash_key as (
        select *, row_number() over (partition by id order by _oppdatert_tidspunkt)
        from lag_hash_key
        where hash_key != lag_hash_key
    ),

    final as (
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
        _oppdatert_tidspunkt,
        hash_key,
        --
        _oppdatert_tidspunkt as valid_from_ts,
        case when 
            er_slettet then 
                slettet_dato 
            else 
                lead(_oppdatert_tidspunkt, 1) over (
                    partition by id order by _oppdatert_tidspunkt
                ) 
        end as valid_to_ts
    )
