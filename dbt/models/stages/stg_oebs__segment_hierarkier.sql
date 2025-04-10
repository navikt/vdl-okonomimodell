with
    source as (
        select 
            {{
                dbt_utils.star(
                    from=ref("snapshot__xxrtv_gl_hierarki_v__v3"),
                    quote_identifiers=false,
                    prefix="raw__",
                )
            }}
        from {{ ref("snapshot__xxrtv_gl_hierarki_v__v3") }}
    ),

    ar as (
        select * from {{ ref("stg__ar") }}
    ),

    per_year as (
        select * 
        from source 
        join ar on raw__dbt_valid_from <= dateadd(day,-1,to_date(cast(ar.ar+1 as varchar),'yyyy'))
        and dateadd(day,-1,to_date(cast(ar.ar+1 as varchar),'yyyy')) < coalesce(raw__dbt_valid_to,to_date('9999','yyyy'))
    ),


    derived_columnns as (
        select
            ar,
            cast(raw__flex_value as varchar(200)) as kode,
            cast(raw__flex_value_set_name as varchar(200)) as segment_type,
            cast(raw__flex_value_id as int) as id,
            cast(raw__description as varchar(200)) as beskrivelse,
            cast(raw__flex_value_parent as varchar(200)) as forelder,
            cast(raw__description_parent as varchar(200)) as forelder_beskrivelse,
            cast(raw__flex_value_id_parent as int) as forelder_id,
            cast(raw__hierarchy_code as varchar(200)) as hierarki,

            * exclude ar
        from per_year
    ),
    keyed as (
        select 
            {{
                    dbt_utils.generate_surrogate_key(
                        ["kode"]
                    )
            }} as segment_id,
            {{
                    dbt_utils.generate_surrogate_key(
                        ["kode","ar"]
                    )
            }} as segment_id_per_ar,
            *,
            ar = extract(year from current_date) as er_siste_gyldige
        from derived_columnns
    ),

    final as (select * from keyed)

select *
from final
