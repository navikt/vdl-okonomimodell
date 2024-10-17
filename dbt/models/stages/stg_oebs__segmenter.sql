with

    source as (
        select 
            {{
                dbt_utils.star(
                    from=ref("snapshot__xxrtv_fist_gl_segment_v"),
                    quote_identifiers=false,
                    prefix="raw__",
                )
            }} 
        from {{ ref("snapshot__xxrtv_fist_gl_segment_v") }}
    ),

    valid as (
        select * 
        from source 
        where raw__dbt_valid_to is null
    ),

    derived_columnns as (
        select
            cast(raw__flex_value_id as int) as id,
            cast(raw__flex_value_set_name as varchar(200)) as segment_type,
            cast(raw__flex_value as varchar(200)) as kode,
            cast(raw__description as varchar(200)) as beskrivelse,
            cast(raw__segment_num as varchar(200)) as konto_gruppe_kode,
            cast(raw__creation_date as date) as opprettet_i_kilde,
            cast(raw__last_update_date as date) as oppdatert_i_kilde,
            cast(raw__start_date_active as date) as posterbar_fra_dato,
            cast(raw__end_date_active as date) as posterbar_til_dato,
            raw__summary_flag = 'Y' as er_summeringsniva,
            raw__posterbar = 'Y' as er_posterbar,
            raw__attribute19 = 'Y' as er_budsjetterbar,
            raw__enabled_flag = 'Y' as er_aktiv,
            cast(raw__attribute10 as varchar(200)) as attribute10,
            cast(raw__attribute11 as varchar(200)) as attribute11,
            cast(raw__attribute12 as varchar(200)) as attribute12,
            cast(raw__attribute13 as varchar(200)) as attribute13,
            cast(raw__attribute14 as varchar(200)) as attribute14,
            cast(raw__attribute15 as varchar(200)) as attribute15,
            *  
        from valid
    ),
    
    keyed as (
        select 
            {{
                    dbt_utils.generate_surrogate_key(
                        ["kode"]
                    )
            }} as segment_id,
            * 
        from derived_columnns
    ),

    final as (select * from keyed)

select *
from final
order by segment_type