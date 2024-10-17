with
    source as (
        select 
            {{
                dbt_utils.star(
                    from=ref("snapshot__xxrtv_gl_hierarki_v"),
                    quote_identifiers=false,
                    prefix="raw__",
                )
            }}
        from {{ ref("snapshot__xxrtv_gl_hierarki_v") }}
    ),

    valid as (
        select * 
        from source 
        where raw__dbt_valid_to is null
    ),


    derived_columnns as (
        select
            
            cast(raw__flex_value as varchar(200)) as kode,
            cast(raw__flex_value_set_name as varchar(200)) as segment_type,
            cast(raw__flex_value_id as int) as id,
            cast(raw__description as varchar(200)) as beskrivelse,
            cast(raw__flex_value_parent as varchar(200)) as forelder,
            cast(raw__description_parent as varchar(200)) as forelder_beskrivelse,
            cast(raw__flex_value_id_parent as int) as forelder_id,
            cast(raw__hierarchy_code as varchar(200)) as hierarki,
            *
        from valid
    ),

    final as (select * from derived_columnns)

select *
from final
