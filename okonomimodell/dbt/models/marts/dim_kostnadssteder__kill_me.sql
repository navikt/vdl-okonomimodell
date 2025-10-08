{{
    config(
        materialized="table",
    )
}}

with
    source as (
        select *
        from {{ ref("dim_kostnadssteder_per_ar_tertial") }}
        where  -- er_siste_gyldige
            ar_tertial = 202501
    ),

    column_selection as (
        select
            segment_id as pk_dim_kostnadssteder,
            {{
                dbt_utils.star(
                    from=ref("dim_kostnadssteder_per_ar"),
                    quote_identifiers=false,
                    except=[
                        "pk_dim_kostnadssteder_per_ar",
                        "segment_id",
                        "ar",
                    ],
                )
            }},
            2025 as ar
        from source
    ),
    final as (select * from column_selection)
select *
from final
