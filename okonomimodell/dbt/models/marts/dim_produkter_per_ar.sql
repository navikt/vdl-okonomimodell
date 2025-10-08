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
            produkt_totalniva,
            produkt_totalniva_beskrivelse
        from source
        where segment_type = 'OR_FORMAL'
    ),

    final as (
        select * 
        from column_selection
    )
select * from final