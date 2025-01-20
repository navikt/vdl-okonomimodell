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

    anskaffelseskategori as ( 
        select * 
        from {{ ref("stg_csv__artskonto_kategori") }}
    ),

    column_selection as (
        select 
            segment_id_per_ar as pk_dim_artskonti_per_ar,
            segment_id,
            kode,
            beskrivelse,
            ar,
            posterbar_fra_dato,
            posterbar_til_dato,
            er_summeringsniva,
            er_posterbar,
            er_budsjetterbar,
            er_aktiv,
            er_siste_gyldige,
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
            artskonto_totalniva,
            artskonto_totalniva_beskrivelse
        from source
        where segment_type = 'OR_ART'
    ),

    include_anskaffelseskategori as (
        select * from column_selection 
        left join anskaffelseskategori
        on column_selection.kode = anskaffelseskategori.artskonto
    ),
    
    final as (
        select * 
        from include_anskaffelseskategori
    )
select * from final