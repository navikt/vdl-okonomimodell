{{
    config(
        materialized="table",
    )
}}

with 
    source as ( 
        select * 
        from {{ ref("dim_fullmakter_per_ar") }}
        where er_siste_gyldige
    ),

    column_selection as (
        select  
            segment_id as pk_dim_fullmakter,
            {{
                dbt_utils.star(
                    from=ref("dim_fullmakter_per_ar"),
                    quote_identifiers=false,
                    except=[
                        "pk_dim_fullmakter_per_ar",
                        "pk_dim_fullmakter",
                        "segment_id",
                        ]
                )
            }}  
        from source
    ),
    final as (
        select * 
        from column_selection
    )
select * from final