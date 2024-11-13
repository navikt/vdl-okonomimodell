{{
    config(
        materialized="table",
    )
}}

with 
    source as ( 
        select * 
        from {{ ref("int_segmenter") }}
        where er_siste_gyldige
    ),

    column_selection as (
        select 
            segment_id as pk_dim_kostnadssteder,
            kode, 
            beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv, 
            har_hierarki,
            kostnadsstedsniva_5,
            kostnadsstedsniva_5_beskrivelse,
            kostnadsstedsniva_4,
            kostnadsstedsniva_4_beskrivelse,
            kostnadsstedsniva_3,
            kostnadsstedsniva_3_beskrivelse,
            kostnadsstedsniva_2,
            kostnadsstedsniva_2_beskrivelse,
            kostnadsstedsniva_1,
            kostnadsstedsniva_1_beskrivelse,
            kostnadsstedstotal_niva,
            kostnadsstedstotal_niva_beskrivelse
        from source
        where segment_type = 'OR_KSTED'
    ),

    depricated as (
        select *,
            kode as kostnadssteder_segment_kode,
            beskrivelse as kostnadssteder_segment_beskrivelse,
            kostnadsstedstotal_niva as kostnadssteder_segment_kode_niva_0,
            kostnadsstedstotal_niva_beskrivelse as kostnadssteder_segment_beskrivelse_niva_0,
            kostnadsstedsniva_1 as kostnadssteder_segment_kode_niva_1,
            kostnadsstedsniva_1_beskrivelse as kostnadssteder_segment_beskrivelse_niva_1,
            kostnadsstedsniva_2 as kostnadssteder_segment_kode_niva_2,
            kostnadsstedsniva_2_beskrivelse as kostnadssteder_segment_beskrivelse_niva_2,
            kostnadsstedsniva_3 as kostnadssteder_segment_kode_niva_3,
            kostnadsstedsniva_3_beskrivelse as kostnadssteder_segment_beskrivelse_niva_3,
            kostnadsstedsniva_4 as kostnadssteder_segment_kode_niva_4,
            kostnadsstedsniva_4_beskrivelse as kostnadssteder_segment_beskrivelse_niva_4,
            kostnadsstedsniva_5 as kostnadssteder_segment_kode_niva_5,
            kostnadsstedsniva_5_beskrivelse as kostnadssteder_segment_beskrivelse_niva_5,
            har_hierarki as _har_hierarki
        from column_selection 
    ),
    
    final as (
        select * 
        from depricated
    )
select * from final