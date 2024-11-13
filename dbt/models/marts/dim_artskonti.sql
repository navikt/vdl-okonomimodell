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
            segment_id as pk_dim_artskonti,
            kode,
            beskrivelse,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv,
            har_hierarki,
            artskonto,
            artskonto_beskrivelse,
            konto_tre_siffer,
            konto_tre_siffer_beskrivelse,
            budsjett_niva,
            budsjett_niva_beskrivelse,
            kontogruppe,
            kontogruppe_beskrivelse,
            kontoklasse,
            kontoklasse_beskrivelse,
            artskonto_total_niva,
            artskonto_total_niva_beskrivelse
        from source
        where segment_type = 'OR_ART'
    ),
    depricated as (
        select *,
            kode as artskonti_segment_kode,
            artskonto as artskonti_segment_kode_niva_4,
            konto_tre_siffer as artskonti_segment_kode_niva_3,
            budsjett_niva as artskonti_segment_kode_niva_2_5,
            kontogruppe as artskonti_segment_kode_niva_2,
            kontoklasse as artskonti_segment_kode_niva_1,
            artskonto_total_niva as artskonti_segment_kode_niva_0,
            beskrivelse as artskonti_segment_beskrivelse,
            artskonto_beskrivelse as artskonti_segment_beskrivelse_niva_4,
            konto_tre_siffer_beskrivelse as artskonti_segment_beskrivelse_niva_3,
            budsjett_niva_beskrivelse as artskonti_segment_beskrivelse_niva_2_5,
            kontogruppe_beskrivelse as artskonti_segment_beskrivelse_niva_2,
            kontoklasse_beskrivelse as artskonti_segment_beskrivelse_niva_1,
            artskonto_total_niva_beskrivelse as artskonti_segment_beskrivelse_niva_0,
            0 as er_ytelse_konto,
            har_hierarki as _har_hierarki
        from column_selection 
    ),
    final as (
        select * 
        from depricated
    )
select * from final