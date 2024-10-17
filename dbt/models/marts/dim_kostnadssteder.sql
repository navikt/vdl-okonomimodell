{{
    config(
        materialized="table",
    )
}}

with 
    source as ( 
        select * 
        from {{ ref("int_segmenter") }}
    ),

    column_selection as (
        select 
            segment_id as pk_dim_kostnadssteder,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            rapporteringsniva_5,
            rapporteringsniva_5_beskrivelse,
            rapporteringsniva_4,
            rapporteringsniva_4_beskrivelse,
            rapporteringsniva_3,
            rapporteringsniva_3_beskrivelse,
            rapporteringsniva_2,
            rapporteringsniva_2_beskrivelse,
            rapporteringsniva_1,
            rapporteringsniva_1_beskrivelse
        from source
        where segment_type = 'OR_KSTED'
    ),

    final as (
        select * 
        from column_selection
    )
select * from final