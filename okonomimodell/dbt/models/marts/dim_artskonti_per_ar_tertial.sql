{{
    config(
        materialized="table",
    )
}}

with
    source as (select * from {{ ref("int_segmenter__tertial") }}),

    column_selection as (
        select
            segment_id_per_ar_tertial as pk_dim_artskonti_per_ar_tertial,
            segment_id,
            kode,
            beskrivelse,
            ar_tertial,
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

    final as (
        select * from column_selection
    )
select *
from final
