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
            segment_id_per_ar as pk_dim_produkter_per_ar,
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
            produkt,
            produkt_beskrivelse,
            produktgruppe,
            produktgruppe_beskrivelse,
            produktkategori,
            produktkategori_beskrivelse,
            produkttype,
            produkttype_beskrivelse,
            produkttotal_niva,
            produkttotal_niva_beskrivelse
        from source
        where segment_type = 'OR_FORMAL'
    ),
    depricated as (
        select *,
            beskrivelse as produkter_segment_beskrivelse,
            produkt_beskrivelse as produkter_segment_beskrivelse_niva_4,
            produktgruppe_beskrivelse as produkter_segment_beskrivelse_niva_3,
            produktkategori_beskrivelse as produkter_segment_beskrivelse_niva_2,
            produkttype_beskrivelse as produkter_segment_beskrivelse_niva_1,
            produkttotal_niva_beskrivelse as produkter_segment_beskrivelse_niva_0,
            kode as produkter_segment_kode,
            produkt as produkter_segment_kode_niva_4,
            produktgruppe as produkter_segment_kode_niva_3,
            produktkategori as produkter_segment_kode_niva_2,
            produkttype as produkter_segment_kode_niva_1,
            produkttotal_niva as produkter_segment_kode_niva_0,
            har_hierarki as _har_hierarki
        from column_selection
    ), 

    final as (
        select * 
        from depricated
    )
select * from final